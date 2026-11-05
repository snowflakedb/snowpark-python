#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Implementation of the ``CodeBundle`` target."""

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from snowflake.snowpark.jobs._bundle_spec import (
    CODE_BUNDLE_ENV_FIELDS,
    build_spec,
    dump_spec,
    requirements_text,
)
from snowflake.snowpark.jobs._environment import Environment
from snowflake.snowpark.jobs._handles import BundleRun
from snowflake.snowpark.jobs._packaging import package_and_upload
from snowflake.snowpark.jobs._program import Program
from snowflake.snowpark.jobs._utils import (
    BUNDLE_EXECUTION_PREFIX,
    CODE_BUNDLE_KIND,
    identifier_safe,
    qualify,
    random_suffix,
    require_warehouse,
    send_telemetry,
    sql_str,
)


@dataclass
class PreparedBundle:
    """A code bundle program that is available at ``location`` (a stage path)."""

    location: str
    entrypoint: Optional[str]
    spec: Dict[str, Any]
    # the manifest already lives at ``location``; otherwise it must be sent as a
    # WITH SPECIFICATION clause
    uploaded: bool
    compute_type: str


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
) -> PreparedBundle:
    env.check_supported("CodeBundle", CODE_BUNDLE_ENV_FIELDS)
    if program.kind == "function":
        raise ValueError(
            "Function programs are not supported by the CodeBundle target in v1; "
            "use Program.file() or Program.directory() with a script entrypoint, "
            "or the StoredProcedure target."
        )
    if program.kind == "ref":
        raise ValueError("Program.ref() cannot be packaged")

    on_pool = target.compute_pool is not None
    if env.secrets and not on_pool:
        raise ValueError(
            "Environment.secrets is not supported by the CodeBundle target on a "
            "warehouse; use CodeBundle(compute_pool=...)"
        )
    if not on_pool:
        require_warehouse(session)
    query_warehouse = target.query_warehouse
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
    if func:
        raise ValueError(
            "Function entrypoints (file.py:func) are not supported by the CodeBundle "
            "target; use a script entrypoint."
        )
    if program.kind in ("stage", "workspace"):
        if requirements:
            raise ValueError(
                "Environment.packages/requirements cannot be added to a pre-staged "
                "program; put a requirements file into the staged bundle instead."
            )
        location = program.path.rstrip("/")
        _check_entry(file, on_pool)
        return PreparedBundle(location, file, spec, False, compute_type)

    if program.kind == "file" and file is None:
        file = os.path.basename(program.path)
    if not file and require_entry:
        raise ValueError("An entrypoint is required for Program.directory()")
    _check_entry(file, on_pool)
    packaged = package_and_upload(
        session, program, dump_spec(spec), requirements, stage, file
    )
    return PreparedBundle(packaged.stage_path, file, spec, True, compute_type)


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
    sql = f" ENTRYPOINT = {sql_str(entrypoint)}"
    checked = check_args(args)
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
    short = os.path.splitext(os.path.basename(entrypoint or "run"))[0]
    return f"{BUNDLE_EXECUTION_PREFIX}{identifier_safe(short, 30)}_{random_suffix()}"


def run_async(session, statement: str) -> str:
    return session.sql(statement).collect_nowait().query_id


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
    spec = None if prepared.uploaded else prepared.spec
    if target.compute_pool is None:
        sql = execute_sql(
            f"FROM {sql_str(prepared.location)}", prepared.entrypoint, args, name, spec
        )
        qid = run_async(session, sql)
        return BundleRun(session, qid, prepared.compute_type, None, name)

    # anonymous execution is not available on a compute pool: use a temporary bundle
    db = session.get_current_database()
    schema = session.get_current_schema()
    if not db or not schema:
        raise ValueError("A current database and schema are required for compute pools")
    temp = f"{db}.{schema}.{BUNDLE_EXECUTION_PREFIX}{random_suffix(12)}"
    session.sql(
        f"CREATE CODE BUNDLE {temp} FROM {sql_str(prepared.location)}"
    ).collect()
    try:
        qid = run_async(
            session, execute_sql(temp, prepared.entrypoint, args, name, spec)
        )
    except Exception:
        session.sql(f"DROP CODE BUNDLE IF EXISTS {temp}").collect()
        raise
    return BundleRun(session, qid, prepared.compute_type, temp, name)


def deploy(
    session, target, program: Program, *, name, env: Environment, stage, replace
):
    from snowflake.snowpark.jobs._deployments import CodeBundleDeployment

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
    session.sql(
        f"{create} CODE BUNDLE {fqn} FROM {sql_str(prepared.location)}"
    ).collect()
    return CodeBundleDeployment(
        session,
        fqn,
        target,
        spec=prepared.spec,
        entrypoint=prepared.entrypoint,
        compute_type=prepared.compute_type,
        env=env,
        stage=stage,
    )
