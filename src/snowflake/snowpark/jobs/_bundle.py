#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Implementation of the ``CodeBundle`` target."""

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from snowflake.snowpark.jobs._bundle_spec import (
    CODE_BUNDLE_ENV_FIELDS,
    MANIFEST_FILE,
    build_spec,
    dump_spec,
    metadata_comment,
    requirements_text,
)
from snowflake.snowpark.jobs._environment import Environment
from snowflake.snowpark.jobs._handles import BundleRun
from snowflake.snowpark.jobs._packaging import LAUNCHER_FILE, package_and_upload
from snowflake.snowpark.jobs._program import Program
from snowflake.snowpark.jobs._utils import (
    BUNDLE_EXECUTION_PREFIX,
    CODE_BUNDLE_KIND,
    TEMP_BUNDLE_PREFIX,
    identifier_safe,
    qualify,
    random_suffix,
    require_warehouse,
    run_sql,
    run_sql_nowait,
    send_telemetry,
    sql_str,
    using_warehouse,
)


@dataclass
class PreparedBundle:
    """A code bundle program that is available at ``location`` (a stage path)."""

    location: str
    entrypoint: Optional[str]
    # for staged programs: the manifest in the staged code, or None if it cannot be read
    spec: Optional[Dict[str, Any]]
    # the manifest already lives at ``location``; otherwise it must be sent as a
    # WITH SPECIFICATION clause
    uploaded: bool
    compute_type: str
    # whether the launcher for function entry points is part of the bundle
    has_launcher: bool = False


def is_function_entry(entrypoint: Optional[str]) -> bool:
    return bool(entrypoint) and ":" in entrypoint


def launcher_call(entrypoint: str, args: List[str]):
    """The ``(ENTRYPOINT, ARGUMENTS)`` that run a ``file.py:func`` entry point."""
    if is_function_entry(entrypoint):
        return LAUNCHER_FILE, [entrypoint, *args]
    return entrypoint, args


def check_args(args: Optional[Sequence[Any]]) -> List[str]:
    out = list(args or [])
    for a in out:
        if not isinstance(a, str):
            raise ValueError(
                f"Code bundle arguments must be strings, got {type(a).__name__}: {a!r}"
            )
    return out


def prepare(
    session,
    target,
    program: Program,
    *,
    entrypoint: Optional[str],
    env: Environment,
    stage: Optional[str],
    require_entry: bool = True,
    inherited_config: bool = False,
) -> PreparedBundle:
    """Makes ``program`` available as a stage path.

    ``inherited_config`` is set when the target and environment come from an existing
    deployment rather than from the caller: staged programs then skip the check that
    rejects settings (their own ``code_bundle.yml`` wins anyway).
    """
    env.check_supported("CodeBundle", CODE_BUNDLE_ENV_FIELDS)
    if program.kind == "function":
        raise ValueError(
            "Callables and Program.function() are not supported by the CodeBundle "
            "target: use Program.file(path, entrypoint_func=...) or "
            "Program.directory(path, 'file.py:func') with a function entry point, or "
            "the StoredProcedure target."
        )
    if program.kind == "ref":
        raise ValueError("Program.ref() cannot be packaged")
    if program.kind in ("stage", "workspace"):
        return _prepare_staged(
            session, target, program, entrypoint, env, inherited_config
        )

    on_pool = target.compute_pool is not None
    if env.secrets and not on_pool:
        raise ValueError(
            "Environment.secrets is not supported by the CodeBundle target on a "
            "warehouse; use CodeBundle(compute_pool=...)"
        )
    if not on_pool:
        require_warehouse(session, target.warehouse)
    query_warehouse = target.warehouse
    if on_pool and not query_warehouse:
        query_warehouse = session.get_current_warehouse()

    packages = env.resolved_packages()
    requirements = requirements_text(packages)
    spec = build_spec(
        compute_pool=target.compute_pool,
        query_warehouse=query_warehouse,
        runtime_version=target.runtime_version,
        env=env,
        has_requirements=bool(requirements),
    )
    compute_type = "compute_pool" if on_pool else "warehouse"

    file, func = program.split_entrypoint(entrypoint)
    if func and not file.lower().endswith(".py"):
        raise ValueError(
            f"Function entry points are only supported for .py files, got {file!r}"
        )
    if program.kind == "file" and file is None:
        file = os.path.basename(program.path)
    if not file and require_entry:
        raise ValueError("An entrypoint is required for Program.directory()")
    _check_entry(file, on_pool)
    packaged = package_and_upload(
        session, program, dump_spec(spec), requirements, stage, file
    )
    entry = f"{file}:{func}" if func else file
    return PreparedBundle(packaged.stage_path, entry, spec, True, compute_type, True)


def read_staged_manifest(session, location: str) -> Optional[Dict[str, Any]]:
    """The ``code_bundle.yml`` of staged code, or ``None`` if it cannot be read."""
    import yaml

    try:
        stream = session.file.get_stream(location.rstrip("/") + "/" + MANIFEST_FILE)
        spec = yaml.safe_load(stream.read().decode())
    except Exception:
        return None
    return spec if isinstance(spec, dict) else None


def _prepare_staged(
    session, target, program: Program, entrypoint, env, inherited_config: bool
) -> PreparedBundle:
    """Staged and workspace programs bring their own ``code_bundle.yml``, which defines
    the compute and the environment: the target and environment settings are rejected
    instead of being silently ignored."""
    file, func = program.split_entrypoint(entrypoint)
    if func:
        raise ValueError(
            "Function entry points (file.py:func) need the launcher that "
            "session.jobs adds to the code it uploads, so they are not supported "
            f"for Program.{program.kind}(); use a script entrypoint or a local "
            "Program.file()/Program.directory()."
        )
    if not inherited_config:
        settings = [
            name
            for name, value in (
                ("compute_pool", target.compute_pool),
                ("warehouse", target.warehouse),
                ("runtime_version", target.runtime_version),
            )
            if value
        ] + [f"Environment.{name}" for name in env.set_fields()]
        if settings:
            raise ValueError(
                f"Program.{program.kind}() runs the code_bundle.yml in the staged code, "
                "which defines the compute and the environment, so do not set "
                f"{', '.join(settings)}: edit that file instead. (To pick the "
                "warehouse of a warehouse bundle, use session.use_warehouse().)"
            )
    location = program.path.rstrip("/")
    spec = read_staged_manifest(session, location)
    compute_type = ((spec or {}).get("bundle") or {}).get("compute_type") or "warehouse"
    # notebooks need a compute pool; if the manifest cannot be read, the server decides
    _check_entry(file, compute_type == "compute_pool" or spec is None)
    return PreparedBundle(location, file, spec, False, compute_type)


def _check_entry(file: Optional[str], on_pool: bool) -> None:
    if file and file.lower().endswith(".ipynb") and not on_pool:
        raise ValueError(
            "Notebook entrypoints are only supported on a compute pool: "
            "use CodeBundle(compute_pool=...)"
        )


def _entry_clauses(
    entrypoint: Optional[str],
    args: Optional[Sequence[Any]],
    execution_name: Optional[str],
) -> str:
    if not entrypoint:
        raise ValueError("An entrypoint is required")
    # a function entry point runs through the launcher, which gets it as first argument
    entrypoint, checked = launcher_call(entrypoint, check_args(args))
    sql = f" ENTRYPOINT = {sql_str(entrypoint)}"
    if checked:
        sql += f" ARGUMENTS = ({', '.join(sql_str(a) for a in checked)})"
    if execution_name:
        sql += f" EXECUTION_NAME = {sql_str(execution_name)}"
    return sql


def execute_sql(
    ref: str,
    entrypoint: Optional[str],
    args: Optional[Sequence[Any]] = None,
    execution_name: Optional[str] = None,
    spec: Optional[Dict[str, Any]] = None,
) -> str:
    """``EXECUTE CODE BUNDLE`` for a bundle ``ref`` (``<fqn>`` or ``FROM '<stage path>'``)."""
    sql = f"EXECUTE CODE BUNDLE {ref}" + _entry_clauses(
        entrypoint, args, execution_name
    )
    if spec is not None:
        dumped = dump_spec(spec)
        if "$$" in dumped:
            raise ValueError(
                "The code bundle specification contains '$$' (for example in an "
                "environment variable value), which cannot be sent in a "
                "WITH SPECIFICATION clause"
            )
        sql += f" WITH SPECIFICATION $$\n{dumped}$$"
    return sql


def default_execution_name(entrypoint: Optional[str]) -> str:
    entry, _, func = (entrypoint or "run").partition(":")
    short = os.path.splitext(os.path.basename(entry))[0]
    if func:
        short = f"{short}_{func}"
    return f"{BUNDLE_EXECUTION_PREFIX}{identifier_safe(short, 30)}_{random_suffix()}"


def run_async(session, statement: str, warehouse: Optional[str] = None) -> str:
    """Starts ``statement`` without waiting; ``warehouse`` is switched to while submitting."""
    with using_warehouse(session, warehouse):
        return run_sql_nowait(session, statement).query_id


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
) -> BundleRun:
    check_args(args)
    prepared = prepare(
        session, target, program, entrypoint=entrypoint, env=env, stage=stage
    )
    send_telemetry(session, "submit", CODE_BUNDLE_KIND)
    name = execution_name or default_execution_name(prepared.entrypoint)
    # the manifest is always the one in the uploaded or staged code: nothing is sent
    # with WITH SPECIFICATION
    if prepared.compute_type != "compute_pool":
        require_warehouse(session, target.warehouse)
        sql = execute_sql(
            f"FROM {sql_str(prepared.location)}", prepared.entrypoint, args, name
        )
        qid = run_async(session, sql, target.warehouse)
        return BundleRun(session, qid, prepared.compute_type, None, name)

    # anonymous execution is not available on a compute pool: use a temporary bundle
    db = session.get_current_database()
    schema = session.get_current_schema()
    if not db or not schema:
        raise ValueError("A current database and schema are required for compute pools")
    temp = f"{db}.{schema}.{TEMP_BUNDLE_PREFIX}{random_suffix(12)}"
    run_sql(session, f"CREATE CODE BUNDLE {temp} FROM {sql_str(prepared.location)}")
    try:
        qid = run_async(session, execute_sql(temp, prepared.entrypoint, args, name))
    except Exception:
        run_sql(session, f"DROP CODE BUNDLE IF EXISTS {temp}")
        raise
    manager = getattr(session, "_jobs", None)
    if manager is not None:
        manager._temp_bundles.add(temp)  # dropped at session close, if idle by then
    return BundleRun(session, qid, prepared.compute_type, temp, name)


def bundle_comment(
    prepared: PreparedBundle, env: Environment, warehouse: Optional[str] = None
) -> Optional[str]:
    """The metadata comment of a bundle whose manifest session.jobs generated. A staged
    program brings its own manifest, which is then the only source."""
    if not prepared.uploaded:
        return None
    return metadata_comment(
        prepared.spec, prepared.has_launcher, env.resolved_packages(), warehouse
    )


def deploy(
    session, target, program: Program, *, name, env: Environment, stage, replace
):
    from snowflake.snowpark.jobs._deployments import CodeBundleDeployment
    from snowflake.snowpark.jobs._manager import JobManager

    prepared = prepare(
        session,
        target,
        program,
        entrypoint=None,
        env=env,
        stage=stage,
        require_entry=False,
    )
    fqn = qualify(session, name)
    send_telemetry(session, "deploy", CODE_BUNDLE_KIND)
    create = "CREATE OR REPLACE" if replace else "CREATE"
    comment = bundle_comment(prepared, env, target.warehouse)
    run_sql(
        session,
        f"{create} CODE BUNDLE {fqn} FROM {sql_str(prepared.location)}"
        + (f" COMMENT = {sql_str(comment)}" if comment else ""),
    )
    if not prepared.uploaded:
        # a staged program defines the bundle: describe what was created, exactly as
        # get_deployment() would
        manager = getattr(session, "_jobs", None) or JobManager(session)
        deployment = manager._code_bundle_deployment(fqn)
        deployment._entrypoint = prepared.entrypoint
        deployment._stage = stage
        return deployment
    return CodeBundleDeployment(
        session,
        fqn,
        target,
        spec=prepared.spec,
        entrypoint=prepared.entrypoint,
        compute_type=prepared.compute_type,
        env=env,
        stage=stage,
        has_launcher=prepared.has_launcher,
    )
