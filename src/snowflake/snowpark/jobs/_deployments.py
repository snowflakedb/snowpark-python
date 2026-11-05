#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import re
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Sequence

from snowflake.snowpark.jobs import _bundle, _sproc
from snowflake.snowpark.jobs._bundle_spec import merge_overrides
from snowflake.snowpark.jobs._environment import Environment
from snowflake.snowpark.jobs._handles import (
    BundleRun,
    Job,
    QueryJob,
    TaskRun,
    make_task_runs,
    task_history_sql,
)
from snowflake.snowpark.jobs._program import as_program
from snowflake.snowpark.jobs._targets import CodeBundle, StoredProcedure
from snowflake.snowpark.jobs._utils import (
    CODE_BUNDLE_KIND,
    STORED_PROCEDURE_KIND,
    qualify,
    require_warehouse,
    run_sql,
    send_telemetry,
    split_fqn,
    split_name,
    sql_str,
    unquote_ident,
    validate_fqn,
    validate_name,
    validate_warehouse,
)

_logger = logging.getLogger(__name__)


class Schedule:
    """A scheduled task that runs a deployment."""

    def __init__(
        self,
        session,
        task_fqn: str,
        target: Optional[str] = None,
        created_on: Optional[Any] = None,
    ) -> None:
        self._session = session
        self.name = validate_fqn(task_fqn, "task name")
        self._target = target
        self._created_on = created_on

    def _task_created_on(self) -> Optional[Any]:
        if self._created_on is None:
            db, schema, short = split_fqn(self.name)
            rows = run_sql(
                self._session,
                f"SHOW TASKS LIKE {sql_str(unquote_ident(short))} IN SCHEMA {db}.{schema}",
            )
            for r in rows:
                if r["name"] == unquote_ident(short):
                    self._created_on = r["created_on"]
        return self._created_on

    def suspend(self) -> None:
        run_sql(self._session, f"ALTER TASK {self.name} SUSPEND")

    def resume(self) -> None:
        run_sql(self._session, f"ALTER TASK {self.name} RESUME")

    def run_now(self) -> None:
        """Triggers a run now (``EXECUTE TASK``)."""
        run_sql(self._session, f"EXECUTE TASK {self.name}")

    def runs(self, limit: int = 10) -> List[TaskRun]:
        """The most recent runs of the task, newest first."""
        # the history is keyed by name: skip runs of an earlier task with the same name
        created_on = self._task_created_on()
        rows = run_sql(self._session, task_history_sql(self.name, 1000))
        if created_on is not None:
            rows = [
                r
                for r in rows
                if r["SCHEDULED_TIME"] is None or r["SCHEDULED_TIME"] >= created_on
            ]
        return make_task_runs(self._session, self.name, rows, self._target)[:limit]

    def drop(self) -> None:
        run_sql(self._session, f"DROP TASK IF EXISTS {self.name}")

    def __repr__(self) -> str:
        return f"Schedule(name={self.name!r})"


class Deployment(ABC):
    """A deployed program: a stored procedure or a code bundle."""

    target: str

    def __init__(self, session, name: str) -> None:
        self._session = session
        self.name = name

    @abstractmethod
    def run(
        self,
        *,
        args=None,
        entrypoint: Optional[str] = None,
        execution_name: Optional[str] = None,
        overrides: Optional[Dict[str, Any]] = None,
    ) -> Job:
        """Runs the deployment once, asynchronously.

        The run lasts only as long as the session that started it: closing that session
        (``session.close()``) cancels runs that are still going. To keep work going
        after the process ends, wait for the run before closing, or use ``schedule()``,
        because scheduled tasks run on their own.

        For code bundles, ``entrypoint`` is a script (``"jobs/daily.py"``) or a function
        (``"jobs/daily.py:run"``, called as ``run(session, *args)``; this needs a bundle
        deployed by ``session.jobs``). ``args`` are strings and there is no return value.
        """

    @abstractmethod
    def to_sql(
        self, *, args: Optional[Sequence[Any]] = None, entrypoint: Optional[str] = None
    ) -> str:
        """The SQL statement that runs this deployment."""

    @abstractmethod
    def drop(self) -> None:
        """Drops the deployed object."""

    def schedule(
        self,
        cron: str,
        *,
        name: Optional[str] = None,
        warehouse: Optional[str] = None,
        args: Optional[Sequence[Any]] = None,
        entrypoint: Optional[str] = None,
        replace: bool = False,
    ) -> Schedule:
        """Creates and resumes a task that runs this deployment.

        Args:
            cron: A cron expression with a time zone, e.g. ``"0 * * * * UTC"``.
            entrypoint: Code bundles only: a script or a ``"file.py:func"`` function
                entry point, see :meth:`run`.
            name: Task name; defaults to ``<deployment name>_SCHEDULE`` in the
                deployment's schema.
            warehouse: Warehouse of the task. Defaults to the target's ``warehouse``;
                if neither is set, a serverless task is created.
            replace: Replace an existing task of that name (``CREATE OR REPLACE TASK``).
                By default, an existing task is an error.

        Raises:
            ValueError: If the task already exists and ``replace`` is false.
            RuntimeError: If the task was created but could not be resumed. It then
                exists and stays suspended.
        """
        body = self.to_sql(args=args, entrypoint=entrypoint)
        warehouse = validate_warehouse(warehouse)
        if warehouse is None:
            warehouse = validate_warehouse(
                getattr(self._target_config, "warehouse", None)
            )
        if name is not None:
            name = validate_name(name, "schedule name")
        db, schema, short = split_fqn(self.name)
        if name is None:
            candidate = unquote_ident(short) + "_SCHEDULE"
            task = (
                candidate
                if re.fullmatch(r"[A-Z_][A-Z0-9_$]*", candidate)
                else '"' + candidate.replace('"', '""') + '"'
            )
            task_fqn = f"{db}.{schema}.{task}"
        elif len(split_name(name)) == 1:
            task_fqn = f"{db}.{schema}.{name}"
        else:
            task_fqn = qualify(self._session, name)
        wh = f" WAREHOUSE = {warehouse}" if warehouse else ""
        create = "CREATE OR REPLACE TASK" if replace else "CREATE TASK"
        try:
            run_sql(
                self._session,
                f"{create} {task_fqn}{wh} "
                f"SCHEDULE = {sql_str('USING CRON ' + cron)} AS {body}",
            )
        except Exception as ex:
            if not replace and "already exists" in str(ex).lower():
                raise ValueError(
                    f"The task {task_fqn} already exists: pass replace=True to replace "
                    f"it, or choose another name. Server message: {ex}"
                ) from ex
            raise
        try:
            run_sql(self._session, f"ALTER TASK {task_fqn} RESUME")
        except Exception as ex:
            raise RuntimeError(
                f"The task {task_fqn} was created but is suspended, because "
                f"ALTER TASK ... RESUME failed: {ex}"
            ) from ex
        send_telemetry(self._session, "schedule", self.target)
        schedule = Schedule(self._session, task_fqn, self.target)
        schedule._task_created_on()
        return schedule

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={self.name!r})"


class ProcedureDeployment(Deployment):
    """A deployed stored procedure with the ``(ARRAY) RETURNS VARIANT`` signature."""

    target = STORED_PROCEDURE_KIND

    def __init__(
        self, session, name: str, target_config: Optional[StoredProcedure] = None
    ) -> None:
        super().__init__(session, name)
        self._target_config = target_config or StoredProcedure()

    def run(
        self,
        *,
        args=None,
        entrypoint: Optional[str] = None,
        execution_name: Optional[str] = None,
        overrides: Optional[Dict[str, Any]] = None,
    ) -> QueryJob:
        if execution_name:
            raise ValueError(
                "execution_name is only supported by the CodeBundle target"
            )
        if overrides:
            raise ValueError("overrides are only supported for code bundle deployments")
        if entrypoint:
            raise ValueError(
                "entrypoint is not supported for stored procedure deployments"
            )
        return _sproc.run_procedure(self._session, self.name, args)

    def to_sql(
        self, *, args: Optional[Sequence[Any]] = None, entrypoint: Optional[str] = None
    ) -> str:
        if entrypoint:
            raise ValueError(
                "entrypoint is not supported for stored procedure deployments"
            )
        return _sproc.call_sql(self.name, args)

    def drop(self) -> None:
        run_sql(self._session, f"DROP PROCEDURE IF EXISTS {self.name}(ARRAY)")


class CodeBundleDeployment(Deployment):
    """A deployed code bundle. Runs always use the latest version."""

    target = CODE_BUNDLE_KIND

    def __init__(
        self,
        session,
        name: str,
        target_config: Optional[CodeBundle] = None,
        *,
        spec: Optional[Dict[str, Any]] = None,
        entrypoint: Optional[str] = None,
        compute_type: Optional[str] = None,
        env: Optional[Environment] = None,
        stage: Optional[str] = None,
        has_launcher: bool = False,
        spec_problem: Optional[str] = None,
        env_problem: Optional[str] = None,
    ) -> None:
        super().__init__(session, name)
        self._target_config = target_config or CodeBundle()
        self._spec = spec
        self._entrypoint = entrypoint
        self._compute_type = compute_type
        self._env = env or Environment()
        self._stage = stage
        # whether function entry points (file.py:func) can run in this bundle
        self._has_launcher = has_launcher
        # for deployments found with get_deployment(): why the configuration is not known
        self._spec_problem = spec_problem
        self._env_problem = env_problem

    def _entry(self, entrypoint: Optional[str]) -> str:
        entry = entrypoint or self._entrypoint
        if not entry:
            raise ValueError(
                "This deployment has no default entrypoint (it is not recorded in the "
                "bundle): pass entrypoint=..."
            )
        if _bundle.is_function_entry(entry) and not self._has_launcher:
            raise ValueError(
                f"{self.name} does not contain the session.jobs launcher, so the "
                f"function entry point {entry!r} is not available: use a script "
                "entrypoint, or deploy the code again with session.jobs.deploy()."
            )
        return entry

    def run(
        self,
        *,
        args=None,
        entrypoint: Optional[str] = None,
        execution_name: Optional[str] = None,
        overrides: Optional[Dict[str, Any]] = None,
    ) -> BundleRun:
        _bundle.check_args(args)
        entry = self._entry(entrypoint)
        run_wh = None
        if self._compute_type != "compute_pool":
            run_wh = self._target_config.warehouse
            require_warehouse(self._session, run_wh)
        spec = None
        if overrides:
            if self._spec is None:
                raise ValueError(
                    "overrides need the deployment's specification, which could not be "
                    f"loaded: {self._spec_problem or 'unknown reason'}"
                )
            spec = merge_overrides(self._spec, overrides)
        name = execution_name or _bundle.default_execution_name(entry)
        send_telemetry(self._session, "run", CODE_BUNDLE_KIND)
        qid = _bundle.run_async(
            self._session,
            _bundle.execute_sql(self.name, entry, args, name, spec),
            run_wh,
        )
        return BundleRun(self._session, qid, self._compute_type, None, name)

    def to_sql(
        self, *, args: Optional[Sequence[Any]] = None, entrypoint: Optional[str] = None
    ) -> str:
        return _bundle.execute_sql(self.name, self._entry(entrypoint), args)

    def add_version(self, program) -> None:
        """Uploads ``program`` and adds it as a new version. Later runs use it.

        The new version gets the configuration (target, environment) of this deployment.
        For a deployment found with :meth:`JobManager.get_deployment
        <snowflake.snowpark.jobs.JobManager.get_deployment>` that configuration is rebuilt
        from the bundle's manifest; if that is not possible, this raises ``ValueError``
        instead of using defaults.
        """
        if self._spec_problem or self._env_problem:
            raise ValueError(
                f"Cannot add a version to {self.name}: its configuration is not known "
                f"({self._spec_problem or self._env_problem}). Deploy the program again "
                "with session.jobs.deploy(..., replace=True) and the target and "
                "environment you want."
            )
        program = as_program(program)
        prepared = _bundle.prepare(
            self._session,
            self._target_config,
            program,
            entrypoint=None,
            env=self._env,
            stage=self._stage,
            require_entry=False,
        )
        run_sql(
            self._session,
            f"ALTER CODE BUNDLE {self.name} ADD VERSION FROM {sql_str(prepared.location)}",
        )
        comment = _bundle.bundle_comment(prepared, self._env)
        try:
            run_sql(
                self._session,
                f"ALTER CODE BUNDLE {self.name} SET COMMENT = {sql_str(comment or '')}",
            )
        except Exception as ex:  # the version itself is in place
            _logger.warning(
                "Could not update the metadata comment of %s: %s", self.name, ex
            )
        self._spec = prepared.spec
        self._has_launcher = prepared.has_launcher
        if prepared.entrypoint:
            self._entrypoint = prepared.entrypoint

    def drop(self) -> None:
        run_sql(self._session, f"DROP CODE BUNDLE IF EXISTS {self.name}")
