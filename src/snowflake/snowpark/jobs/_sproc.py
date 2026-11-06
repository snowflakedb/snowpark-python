#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Implementation of the ``StoredProcedure`` target."""

import contextlib
import hashlib
import math
import os
import shutil
import sys
import tempfile
import threading
from typing import Any, Dict, List, Optional, Sequence

import cloudpickle

from snowflake.snowpark.jobs import _runner
from snowflake.snowpark.jobs._environment import Environment
from snowflake.snowpark.jobs._handles import QueryJob
from snowflake.snowpark.jobs._program import Program
from snowflake.snowpark.jobs._utils import (
    JOB_MARKER,
    STORED_PROCEDURE_KIND,
    qualify,
    require_warehouse,
    run_sql_nowait,
    send_telemetry,
    sql_str,
)
from snowflake.snowpark.types import ArrayType, VariantType

SPROC_ENV_FIELDS = [
    "packages",
    "requirements",
    "imports",
    "secrets",
    "external_access_integrations",
    "artifact_repositories",
]


def render_args(args: Optional[Sequence[Any]]) -> str:
    """Renders ``args`` as an ``ARRAY_CONSTRUCT(...)`` expression."""
    items = []
    for a in args or []:
        if a is None:
            items.append("NULL")
        elif isinstance(a, bool):
            items.append("TRUE" if a else "FALSE")
        elif isinstance(a, float):
            if not math.isfinite(a):
                raise ValueError(f"Stored procedure args cannot be {a!r}")
            items.append(repr(a))
        elif isinstance(a, int):
            items.append(repr(a))
        elif isinstance(a, str):
            items.append(sql_str(a))
        else:
            raise ValueError(
                "Stored procedure args must be str, int, float, bool or None, "
                f"got {type(a).__name__}"
            )
    return f"ARRAY_CONSTRUCT({', '.join(items)})"


def call_sql(fqn: str, args: Optional[Sequence[Any]]) -> str:
    return f"CALL {fqn}({render_args(args)})"


# cloudpickle's by-value registry is process-wide: register, pickle and unregister must
# not interleave between threads.
_PICKLE_LOCK = threading.Lock()


@contextlib.contextmanager
def _pickle_by_value(fn):
    """Makes cloudpickle ship the function's module by value if it is a local module."""
    module = sys.modules.get(getattr(fn, "__module__", None) or "")
    registered = False
    if (
        module is not None
        and module.__name__ != "__main__"
        and not module.__name__.startswith("snowflake.")
        and "site-packages" not in (getattr(module, "__file__", "") or "site-packages")
    ):
        try:
            cloudpickle.register_pickle_by_value(module)
            registered = True
        except ValueError:  # already registered
            pass
    try:
        yield
    finally:
        if registered:
            cloudpickle.unregister_pickle_by_value(module)


class PreparedProgram:
    """The runner function and imports of a program, ready for ``sproc.register``."""

    def __init__(self, runner, imports: List[Any], fn: Optional[Any], tmpdir) -> None:
        self.runner = runner
        self.imports = imports
        self.fn = fn
        self._tmpdir = tmpdir

    def cleanup(self) -> None:
        if self._tmpdir:
            shutil.rmtree(self._tmpdir, ignore_errors=True)
            self._tmpdir = None


def prepare_program(
    program: Program, entrypoint: Optional[str], env: Environment
) -> PreparedProgram:
    kind = program.kind
    extra_imports: List[Any] = list(env.imports or [])
    if kind == "function":
        if entrypoint:
            raise ValueError("entrypoint is not supported for function programs")
        spec = {"kind": "function", "fn": program.fn}
        return PreparedProgram(
            _runner.make_runner(spec), extra_imports, program.fn, None
        )
    if kind == "workspace":
        raise ValueError(
            "Program.workspace() is only supported by the CodeBundle target"
        )
    if kind == "ref":
        raise ValueError("Program.ref() cannot be prepared for registration")

    file, func = program.split_entrypoint(entrypoint)
    if kind == "stage":
        if not file:
            raise ValueError("Program.stage() requires an entrypoint")
        if not program.path.lower().endswith((".zip", ".py")):
            raise ValueError(
                "The StoredProcedure target imports staged files, not stage "
                f"directories: stage a .zip or .py file instead of {program.path!r}"
            )
        archive = program.path.rsplit("/", 1)[-1]
        if archive.lower().endswith(".py") and file != archive:
            raise ValueError(
                f"The entrypoint {file!r} does not match the staged file {archive!r}: "
                "a staged .py file is its own entrypoint"
            )
        spec = {"kind": "files", "archive": archive, "root": "", "file": file}
        if func:
            spec["func"] = func
        return PreparedProgram(
            _runner.make_runner(spec), [program.path] + extra_imports, None, None
        )

    # local file or directory: ship a filtered copy as one directory import
    if kind == "file":
        if program.path.lower().endswith(".ipynb"):
            raise ValueError(
                "Notebooks are not supported by the StoredProcedure target; "
                "use target=CodeBundle(compute_pool=...)"
            )
        file = file or os.path.basename(program.path)
        if file != os.path.basename(program.path):
            raise ValueError(
                f"entrypoint {file!r} does not match the file {program.path!r}"
            )
    elif not file:
        raise ValueError("Program.directory() requires an entrypoint")
    files = program.iter_files()
    rels = {rel for _, rel in files}
    if file not in rels:
        raise ValueError(
            f"Entrypoint {file!r} is not part of the program files ({sorted(rels)[:10]})"
        )
    digest = hashlib.sha256()
    for full, rel in sorted(files, key=lambda x: x[1]):
        digest.update(rel.encode() + b"\0")
        with open(full, "rb") as f:
            digest.update(f.read())
    dirname = f"snowpark_job_{digest.hexdigest()[:16]}"
    tmp = tempfile.mkdtemp(prefix="snowpark_jobs_")
    root = os.path.join(tmp, dirname)
    for full, rel in files:
        dest = os.path.join(root, *rel.split("/"))
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        shutil.copyfile(full, dest)
    spec = {"kind": "files", "archive": f"{dirname}.zip", "root": dirname, "file": file}
    if func:
        spec["func"] = func
    return PreparedProgram(_runner.make_runner(spec), [root] + extra_imports, None, tmp)


def _register_kwargs(env: Environment) -> Dict[str, Any]:
    env.check_supported("StoredProcedure", SPROC_ENV_FIELDS)
    kwargs: Dict[str, Any] = {}
    packages = env.resolved_packages()
    if packages:
        kwargs["packages"] = packages
    if env.secrets:
        kwargs["secrets"] = dict(env.secrets)
    if env.external_access_integrations:
        kwargs["external_access_integrations"] = list(env.external_access_integrations)
    if env.artifact_repositories:
        if len(env.artifact_repositories) > 1:
            raise ValueError(
                "The StoredProcedure target supports only one artifact repository"
            )
        kwargs["artifact_repository"] = env.artifact_repositories[0]
    return kwargs


def _register(session, prepared: PreparedProgram, env: Environment, **extra):
    kwargs = _register_kwargs(env)
    lock = _PICKLE_LOCK if prepared.fn else contextlib.nullcontext()
    with lock:
        ctx = _pickle_by_value(prepared.fn) if prepared.fn else contextlib.nullcontext()
        with ctx:
            return session.sproc.register(
                prepared.runner,
                return_type=VariantType(),
                input_types=[ArrayType()],
                imports=prepared.imports or None,
                _emit_ast=False,
                **kwargs,
                **extra,
            )


def submit(
    session,
    target,
    program: Program,
    *,
    args,
    entrypoint,
    execution_name,
    env: Environment,
    stage,
) -> QueryJob:
    if execution_name:
        raise ValueError("execution_name is only supported by the CodeBundle target")
    if stage:
        raise ValueError(
            "stage is only used by deploy() with the StoredProcedure target: a one-off "
            "run is registered as an anonymous procedure"
        )
    if target.execute_as not in (None, "caller"):
        raise ValueError(
            "One-off runs (submit) of a stored procedure always use caller's rights; "
            f"execute_as={target.execute_as!r} is only supported by deploy()."
        )
    require_warehouse(session)
    rendered = render_args(args)
    send_telemetry(session, "submit", STORED_PROCEDURE_KIND)
    prepared = prepare_program(program, entrypoint, env)
    try:
        sp = _register(session, prepared, env, anonymous=True)
    finally:
        prepared.cleanup()
    query = f"{sp._anonymous_sp_sql}CALL {sp.name}({rendered}) {JOB_MARKER}"
    async_job = session._execute_sproc_internal(
        query=query,
        is_return_table=False,
        block=False,
    )
    return QueryJob(session, async_job, STORED_PROCEDURE_KIND)


def deploy(
    session, target, program: Program, *, name, env: Environment, stage, replace
):
    from snowflake.snowpark.jobs._deployments import ProcedureDeployment

    if not stage:
        raise ValueError(
            "deploy() with the StoredProcedure target requires stage=<internal stage> "
            "for the procedure's code"
        )
    require_warehouse(session)
    fqn = qualify(session, name)
    send_telemetry(session, "deploy", STORED_PROCEDURE_KIND)
    prepared = prepare_program(program, None, env)
    try:
        _register(
            session,
            prepared,
            env,
            name=fqn,
            is_permanent=True,
            stage_location=stage,
            replace=replace,
            execute_as=target.execute_as or "owner",
        )
    finally:
        prepared.cleanup()
    return ProcedureDeployment(session, fqn, target)


def run_procedure(session, fqn: str, args) -> QueryJob:
    require_warehouse(session)
    send_telemetry(session, "run", STORED_PROCEDURE_KIND)
    async_job = run_sql_nowait(session, f"{call_sql(fqn, args)} {JOB_MARKER}")
    return QueryJob(session, async_job, STORED_PROCEDURE_KIND)
