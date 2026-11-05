#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Any, Dict, List, Optional, Sequence, Union

from snowflake.snowpark.jobs._deployments import (
    CodeBundleDeployment,
    Deployment,
    ProcedureDeployment,
)
from snowflake.snowpark.jobs._environment import Environment
from snowflake.snowpark.jobs._handles import (
    _BUNDLE_STATUS,
    BundleRun,
    Job,
    JobStatus,
    QueryJob,
    TaskRun,
    _map_query_status,
)
from snowflake.snowpark.jobs._program import Program, as_program
from snowflake.snowpark.jobs._targets import StoredProcedure, Target, check_target
from snowflake.snowpark.jobs._utils import (
    BUNDLE_EXECUTION_PREFIX,
    CODE_BUNDLE_KIND,
    STORED_PROCEDURE_KIND,
    qualify,
    split_fqn,
    sql_str,
    unquote_ident,
)


class JobManager:
    """Submit, deploy and schedule Snowpark Python programs. Access it as
    ``session.jobs``.

    Example::

        >>> def count_rows(session, table):
        ...     return session.table(table).count()
        >>> job = session.jobs.submit(count_rows, args=["my_table"])  # doctest: +SKIP
        >>> job.wait()  # doctest: +SKIP
        <JobStatus.SUCCEEDED: 'SUCCEEDED'>
        >>> job.result()  # doctest: +SKIP
        42
    """

    def __init__(self, session) -> None:
        self._session = session

    # ---- submit / deploy ----

    def submit(
        self,
        program: Union[Program, Any],
        target: Optional[Target] = None,
        *,
        args: Optional[Sequence[Any]] = None,
        entrypoint: Optional[str] = None,
        execution_name: Optional[str] = None,
        env: Optional[Environment] = None,
        stage: Optional[str] = None,
    ) -> Job:
        """Runs ``program`` once, asynchronously, and returns a handle to the run.

        Args:
            program: A :class:`Program` or a callable ``fn(session, *args)``.
            target: :class:`StoredProcedure` (the default) or :class:`CodeBundle`.
            args: Arguments passed to the program. Code bundles accept strings only.
            entrypoint: Overrides the program's entrypoint.
            execution_name: Name of the code bundle run.
            env: The :class:`Environment` of the run.
            stage: Stage for the uploaded code; code bundles default to the session
                stage.
        """
        program = as_program(program)
        if program.kind == "ref":
            self._check_ref_args(program, target, env, stage)
            deployment = self.get_deployment(program.path)
            self._check_ref_target(deployment, target)
            return deployment.run(
                args=args, entrypoint=entrypoint, execution_name=execution_name
            )
        target = StoredProcedure() if target is None else check_target(target)
        return target._submit(
            self._session,
            program,
            args=args,
            entrypoint=entrypoint,
            execution_name=execution_name,
            env=env or Environment(),
            stage=stage,
        )

    def deploy(
        self,
        program: Union[Program, Any],
        target: Optional[Target] = None,
        *,
        name: str,
        env: Optional[Environment] = None,
        stage: Optional[str] = None,
        replace: bool = False,
    ) -> Deployment:
        """Creates a permanent stored procedure or code bundle for ``program``."""
        program = as_program(program)
        if program.kind == "ref":
            raise ValueError("Program.ref() is already deployed; use get_deployment()")
        target = StoredProcedure() if target is None else check_target(target)
        return target._deploy(
            self._session,
            program,
            name=name,
            env=env or Environment(),
            stage=stage,
            replace=replace,
        )

    # ---- lookup ----

    def get_deployment(self, name: str) -> Deployment:
        """Returns the deployment named ``name`` (a code bundle or a stored procedure)."""
        fqn = qualify(self._session, name)
        db, schema, short = split_fqn(fqn)
        wanted = unquote_ident(short)
        scope = f"IN SCHEMA {db}.{schema}"
        pattern = sql_str(wanted)
        for row in self._session.sql(
            f"SHOW CODE BUNDLES LIKE {pattern} {scope}"
        ).collect():
            if row["name"] == wanted:
                return CodeBundleDeployment(
                    self._session, fqn, spec=self._load_spec(fqn)
                )
        for row in self._session.sql(
            f"SHOW PROCEDURES LIKE {pattern} {scope}"
        ).collect():
            if row["name"] == wanted and row["is_builtin"] == "N":
                args = row["arguments"].split(" RETURN ")[0]
                if not args.upper().endswith("(ARRAY)"):
                    raise ValueError(
                        f"Procedure {fqn} has signature {args}; only procedures with "
                        "the (ARRAY) signature created by session.jobs are supported"
                    )
                return ProcedureDeployment(self._session, fqn)
        raise ValueError(f"No code bundle or stored procedure named {fqn} was found")

    def _load_spec(self, fqn: str) -> Optional[Dict[str, Any]]:
        """Best effort: reads the manifest from the stage the latest version came from."""
        import yaml

        try:
            row = self._session.sql(f"DESCRIBE CODE BUNDLE {fqn}").collect()[0]
            source = row["last_version_source_location_uri"]
            if not source:
                return None
            stream = self._session.file.get_stream(
                source.rstrip("/") + "/code_bundle.yml"
            )
            return yaml.safe_load(stream.read().decode())
        except Exception:
            return None

    def get(self, job_id: str) -> Job:
        """Returns the job with the given id (``query:<id>``, ``bundle:<id>`` or
        ``task:<task>/<id>``)."""
        kind, sep, rest = job_id.partition(":")
        if not sep or not rest:
            raise ValueError(f"Invalid job id: {job_id!r}")
        if kind == "query":
            return QueryJob(self._session, self._session.create_async_job(rest))
        if kind == "bundle":
            return BundleRun(self._session, rest)
        if kind == "task":
            task, slash, qid = rest.rpartition("/")
            if not slash or not task or not qid:
                raise ValueError(f"Invalid task job id: {job_id!r}")
            return TaskRun(self._session, task, qid)
        raise ValueError(f"Unknown job id kind {kind!r} in {job_id!r}")

    def list(
        self,
        *,
        target: Optional[str] = None,
        status: Optional[Union[str, JobStatus]] = None,
        limit: int = 100,
    ) -> List[Job]:
        """Lists recent jobs submitted through this API, newest first.

        Only the most recent 1000 rows of the code bundle history and of the query
        history are searched, so older jobs may not be listed.

        Args:
            target: ``"stored_procedure"`` or ``"code_bundle"``.
            status: Keep only jobs with this status.
            limit: Maximum number of jobs to return.
        """
        if target is not None and not isinstance(target, str):
            target = getattr(target, "kind", target)
        if target not in (None, STORED_PROCEDURE_KIND, CODE_BUNDLE_KIND):
            raise ValueError(
                f"target must be {STORED_PROCEDURE_KIND!r} or {CODE_BUNDLE_KIND!r}"
            )
        wanted = JobStatus(status) if status is not None else None
        found = []  # (start time, job)
        if target in (None, CODE_BUNDLE_KIND):
            found += self._list_bundle_runs(wanted)
        if target in (None, STORED_PROCEDURE_KIND):
            found += self._list_procedure_jobs(wanted)
        found.sort(key=lambda x: str(x[0] or ""), reverse=True)
        return [job for _, job in found[:limit]]

    def _list_bundle_runs(self, wanted: Optional[JobStatus]):
        rows = self._session.sql(
            "SELECT * FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.CODE_BUNDLE_HISTORY("
            "RESULT_LIMIT => 1000)) "
            f"WHERE STARTSWITH(EXECUTION_NAME, {sql_str(BUNDLE_EXECUTION_PREFIX)})"
        ).collect()
        out = []
        for r in rows:
            d = r.as_dict()
            if wanted is not None and _BUNDLE_STATUS.get(d["STATUS"]) != wanted:
                continue
            compute = (d.get("COMPUTE_TYPE") or "").lower() or None
            out.append(
                (
                    d.get("START_TIME"),
                    BundleRun(
                        self._session,
                        d["QUERY_ID"],
                        compute,
                        execution_name=d.get("EXECUTION_NAME"),
                        row=d,
                    ),
                )
            )
        return out

    def _list_procedure_jobs(self, wanted: Optional[JobStatus]):
        # the marker is split so that this statement does not match itself
        rows = self._session.sql(
            "SELECT QUERY_ID, START_TIME, EXECUTION_STATUS FROM TABLE("
            "INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER(RESULT_LIMIT => 1000)) "
            "WHERE CONTAINS(QUERY_TEXT, '/* snowpark' || '.jobs */')"
        ).collect()
        out = []
        for r in rows:
            if wanted is not None and _map_query_status(str(r[2]).upper()) != wanted:
                continue
            job = QueryJob(self._session, self._session.create_async_job(r[0]))
            out.append((r[1], job))
        return out

    # ---- ref helpers ----

    @staticmethod
    def _check_ref_args(program: Program, target, env, stage) -> None:
        if env is not None or stage is not None:
            raise ValueError("env and stage cannot be combined with Program.ref()")

    @staticmethod
    def _check_ref_target(deployment: Deployment, target: Optional[Target]) -> None:
        if target is not None and check_target(target).kind != deployment.target:
            raise ValueError(
                f"{deployment.name} is a {deployment.target}, which conflicts with the "
                f"given target {target!r}"
            )
