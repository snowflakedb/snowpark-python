#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
from typing import Any, Dict, List, Optional, Sequence, Union

from snowflake.snowpark.jobs._bundle_spec import (
    MANIFEST_FILE,
    config_from_spec,
    parse_metadata_comment,
)
from snowflake.snowpark.jobs._deployments import (
    CodeBundleDeployment,
    Deployment,
    ProcedureDeployment,
)
from snowflake.snowpark.jobs._environment import Environment, parse_requirements_text
from snowflake.snowpark.jobs._handles import (
    _BUNDLE_STATUS,
    BundleRun,
    Job,
    JobStatus,
    QueryJob,
    TaskRun,
    _map_query_status,
)
from snowflake.snowpark.jobs._packaging import LAUNCHER_FILE
from snowflake.snowpark.jobs._program import Program, as_program
from snowflake.snowpark.jobs._targets import StoredProcedure, Target, check_target
from snowflake.snowpark.jobs._utils import (
    BUNDLE_EXECUTION_PREFIX,
    CODE_BUNDLE_KIND,
    STORED_PROCEDURE_KIND,
    TEMP_BUNDLE_RE,
    qualify,
    run_sql,
    split_fqn,
    split_name,
    sql_str,
    to_epoch,
    unquote_ident,
    validate_fqn,
    validate_name,
    validate_query_id,
)

_TERMINAL_BUNDLE_STATUSES = ("DONE", "FAILED", "CANCELLED", "DELETED")
# a temporary bundle that no run ever used is only dropped once it is this old
_UNUSED_TEMP_BUNDLE_MIN_AGE = datetime.timedelta(minutes=10)


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
        # temporary bundles of pool runs created through this manager
        self._temp_bundles = set()

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
                stage. Not allowed with the ``StoredProcedure`` target (only ``deploy()``
                uses a stage there).

        A submitted run lasts only as long as the session that submitted it: closing
        that session (``session.close()``) cancels the runs that are still going. To keep
        work going after the process ends, wait for the run before closing, or use
        :meth:`deploy` with ``schedule()``, because scheduled tasks run on their own.

        A one-off run on a compute pool creates a temporary code bundle
        ``SNOWPARK_JOB_TMP_<id>``. It is dropped when the returned handle sees the run
        finish; if the process ends first, :meth:`cleanup` removes it later.
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
        """Returns the deployment named ``name`` (a code bundle or a stored procedure).

        For a code bundle, the target and environment (``compute_pool``, the query
        warehouse, ``runtime_version``, environment variables, secrets, integrations,
        artifact repositories, stage mounts and packages) are rebuilt from the bundle's
        manifest, so that :meth:`~snowflake.snowpark.jobs.CodeBundleDeployment.add_version`
        keeps them. The default entrypoint is not recorded in a bundle: pass
        ``entrypoint=`` when running it. If the manifest cannot be read, ``run()`` works
        but ``add_version()`` and ``run(overrides=...)`` raise ``ValueError``.
        """
        fqn = qualify(self._session, name)
        db, schema, short = split_fqn(fqn)
        wanted = unquote_ident(short)
        scope = f"IN SCHEMA {db}.{schema}"
        pattern = sql_str(wanted)
        for row in run_sql(self._session, f"SHOW CODE BUNDLES LIKE {pattern} {scope}"):
            if row["name"] == wanted:
                return self._code_bundle_deployment(fqn)
        seen = []
        for row in run_sql(self._session, f"SHOW PROCEDURES LIKE {pattern} {scope}"):
            if row["name"] == wanted and row["is_builtin"] == "N":
                args = row["arguments"].split(" RETURN ")[0]
                if args.upper().endswith("(ARRAY)"):
                    return ProcedureDeployment(self._session, fqn)
                seen.append(args)
        if seen:
            raise ValueError(
                f"Procedure {fqn} has no (ARRAY) overload (found: {', '.join(seen)}); "
                "only procedures with the (ARRAY) signature created by session.jobs "
                "are supported"
            )
        raise ValueError(f"No code bundle or stored procedure named {fqn} was found")

    def _code_bundle_deployment(self, fqn: str) -> CodeBundleDeployment:
        """Rebuilds a deployment from what the bundle itself records.

        Sources, in order: the stored version of the bundle (``snow://code
        bundle/...``, which survives the removal of the stage it was created from), the
        metadata comment that ``deploy()`` and ``add_version()`` write, and, for bundles
        created before those existed, the stage the latest version came from.
        """
        describe = self._describe(fqn)
        version_uri = describe.get("default_version_location_uri") or describe.get(
            "last_version_location_uri"
        )
        sources = []
        if version_uri:
            sources.append(("stored version", lambda: self._read_location(version_uri)))
        parsed = parse_metadata_comment(describe.get("comment"))
        if parsed is not None:
            sources.append(("bundle comment", lambda: self._read_comment(parsed)))
        source = describe.get("last_version_source_location_uri")
        if source:
            sources.append(("source stage", lambda: self._read_location(source)))
        meta, problems = None, []
        for label, read in sources:
            try:
                meta = read()
                break
            except Exception as ex:
                problems.append(f"{label}: {ex}")
        if meta is None:
            # the launcher can still be there when the manifest cannot be read
            launcher = any(
                self._has_launcher(base.rstrip("/") + "/")
                for base in (version_uri, source)
                if base
            )
            return CodeBundleDeployment(
                self._session,
                fqn,
                has_launcher=launcher,
                spec_problem="the bundle's manifest could not be read ("
                + ("; ".join(problems) or "DESCRIBE CODE BUNDLE gave no location")
                + ")",
            )
        spec, has_launcher = meta["spec"], meta["has_launcher"]
        try:
            target, env, compute_type, requirements_file = config_from_spec(spec)
        except Exception as ex:
            return CodeBundleDeployment(
                self._session,
                fqn,
                spec=spec,
                has_launcher=has_launcher,
                spec_problem=f"the manifest could not be interpreted: {ex}",
            )
        env_problem = None
        if meta.get("packages") is not None:
            env.packages = list(meta["packages"]) or None
        elif requirements_file:
            try:
                stream = self._session.file.get_stream(meta["base"] + requirements_file)
                env.packages = parse_requirements_text(stream.read().decode()) or None
            except Exception as ex:
                env_problem = (
                    f"the packages in {requirements_file} could not be read: {ex}"
                )
        return CodeBundleDeployment(
            self._session,
            fqn,
            target,
            spec=spec,
            compute_type=compute_type,
            env=env,
            has_launcher=has_launcher,
            env_problem=env_problem,
        )

    def _describe(self, fqn: str) -> Dict[str, Any]:
        try:
            return run_sql(self._session, f"DESCRIBE CODE BUNDLE {fqn}")[0].as_dict()
        except Exception:
            return {}

    def _read_location(self, base: str) -> Dict[str, Any]:
        """Reads the manifest and the launcher from a stage path or a bundle version."""
        import yaml

        base = base.rstrip("/") + "/"
        stream = self._session.file.get_stream(base + MANIFEST_FILE)
        spec = yaml.safe_load(stream.read().decode())
        if not isinstance(spec, dict):
            raise ValueError(f"{MANIFEST_FILE} in {base} is not a mapping")
        return {
            "spec": spec,
            "has_launcher": self._has_launcher(base),
            "base": base,
            "packages": None,
        }

    @staticmethod
    def _read_comment(data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "spec": data["spec"],
            "has_launcher": bool(data.get("has_launcher")),
            "base": None,
            "packages": data.get("packages") or [],
        }

    def _has_launcher(self, base: str) -> bool:
        """Whether the bundle has the launcher that function entry points need."""
        try:
            return bool(run_sql(self._session, f"LIST {sql_str(base + LAUNCHER_FILE)}"))
        except Exception:
            return False

    def get(self, job_id: str) -> Job:
        """Returns the job with the given id (``query:<id>``, ``bundle:<id>`` or
        ``task:<db.schema.task>/<id>``). The ids are the ``id`` of the job handles.

        This can re-attach to a run from another process while the session that
        submitted it is still open; closing that session cancels runs that are still
        going (see :meth:`submit`).

        Raises:
            ValueError: If the id is malformed, before anything is sent to Snowflake.
        """
        if not isinstance(job_id, str):
            raise ValueError(f"Invalid job id: {job_id!r}")
        kind, sep, rest = job_id.partition(":")
        if not sep or not rest:
            raise ValueError(f"Invalid job id: {job_id!r}")
        if kind == "query":
            qid = validate_query_id(rest)
            return QueryJob(self._session, self._session.create_async_job(qid))
        if kind == "bundle":
            return BundleRun(self._session, validate_query_id(rest))
        if kind == "task":
            task, slash, qid = rest.rpartition("/")
            if not slash or not task or not qid:
                raise ValueError(f"Invalid task job id: {job_id!r}")
            return TaskRun(
                self._session, validate_fqn(task, "task name"), validate_query_id(qid)
            )
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
        history are searched, so older jobs may not be listed. Code bundle runs with a
        custom ``execution_name`` and runs started by a schedule are not listed; get
        those with :meth:`get` or ``Schedule.runs()``. A current database is required.

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
        if target in (None, STORED_PROCEDURE_KIND) and not (
            self._session.get_current_database()
        ):
            raise ValueError(
                "list() needs a current database to read the query history: call "
                "session.use_database(...) first"
            )
        found = []  # (start time, job)
        if target in (None, CODE_BUNDLE_KIND):
            found += self._list_bundle_runs(wanted)
        if target in (None, STORED_PROCEDURE_KIND):
            found += self._list_procedure_jobs(wanted)
        found.sort(key=lambda x: to_epoch(x[0]), reverse=True)
        return [job for _, job in found[:limit]]

    def _list_bundle_runs(self, wanted: Optional[JobStatus]):
        rows = run_sql(
            self._session,
            "SELECT * FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.CODE_BUNDLE_HISTORY("
            "RESULT_LIMIT => 1000)) "
            f"WHERE STARTSWITH(EXECUTION_NAME, {sql_str(BUNDLE_EXECUTION_PREFIX)})",
        )
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
        rows = run_sql(
            self._session,
            "SELECT QUERY_ID, START_TIME, EXECUTION_STATUS FROM TABLE("
            "INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER(RESULT_LIMIT => 1000)) "
            "WHERE CONTAINS(QUERY_TEXT, '/* snowpark' || '.jobs */')",
        )
        out = []
        for r in rows:
            if wanted is not None and _map_query_status(str(r[2]).upper()) != wanted:
                continue
            job = QueryJob(
                self._session,
                self._session.create_async_job(r[0]),
                started_at=r[1],
            )
            out.append((r[1], job))
        return out

    # ---- temporary bundles ----

    def cleanup(self, schema: Optional[str] = None) -> List[str]:
        """Drops leftover temporary code bundles of finished compute pool runs.

        Runs on a compute pool create a bundle named ``SNOWPARK_JOB_TMP_<id>`` that is
        dropped when the run's handle sees it finish. If the process ends first (or
        the handle is never polled), the bundle stays; this removes such bundles from
        ``schema`` (``"schema"`` or ``"database.schema"``; default: the current schema).
        A bundle with a pending or running run is never dropped, because dropping it
        while a pool run is starting breaks the run. Bundles that no run ever used are
        dropped once they are 10 minutes old. The prefix is reserved for these bundles.

        Returns:
            The names of the dropped bundles.
        """
        if schema is None:
            db, sch = (
                self._session.get_current_database(),
                self._session.get_current_schema(),
            )
            if not db or not sch:
                raise ValueError("cleanup() needs a current database and schema")
        else:
            parts = split_name(validate_name(schema, "schema"))
            if len(parts) == 2:
                db, sch = parts
            elif len(parts) == 1 and self._session.get_current_database():
                db, sch = self._session.get_current_database(), parts[0]
            else:
                raise ValueError(f"Invalid schema: {schema!r}")
        dropped = []
        # '_' also matches itself; the exact shape is checked below
        pattern = sql_str("SNOWPARK_JOB_TMP_%")
        for row in run_sql(
            self._session, f"SHOW CODE BUNDLES LIKE {pattern} IN SCHEMA {db}.{sch}"
        ):
            name = row["name"]
            if not TEMP_BUNDLE_RE.match(name):
                continue
            fqn = ".".join(
                '"' + str(part).replace('"', '""') + '"'
                for part in (unquote_ident(db), unquote_ident(sch), name)
            )
            if self._drop_if_idle(fqn, name, row["created_on"]):
                dropped.append(fqn)
        return dropped

    def _drop_if_idle(self, fqn: str, name: str, created_on: Any = None) -> bool:
        statuses = [
            str(r[0]).upper()
            for r in run_sql(
                self._session,
                "SELECT STATUS FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA."
                f"CODE_BUNDLE_HISTORY(BUNDLE_NAME => {sql_str(name)}))",
            )
        ]
        if any(st not in _TERMINAL_BUNDLE_STATUSES for st in statuses):
            return False
        if not statuses:
            # no run recorded yet: maybe a submit is just starting its run
            now = datetime.datetime.now(datetime.timezone.utc).timestamp()
            if created_on is None or now - to_epoch(created_on) < (
                _UNUSED_TEMP_BUNDLE_MIN_AGE.total_seconds()
            ):
                return False
        run_sql(self._session, f"DROP CODE BUNDLE IF EXISTS {fqn}")
        self._temp_bundles.discard(fqn)
        return True

    def _cleanup_on_close(self) -> None:
        """Best effort, called when the session closes: never raises."""
        for fqn in list(self._temp_bundles):
            try:
                name = unquote_ident(split_name(fqn)[-1])
                self._drop_if_idle(fqn, name)
            except Exception:
                pass

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
