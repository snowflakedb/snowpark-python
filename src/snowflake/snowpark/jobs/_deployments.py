#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

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
    send_telemetry,
    split_fqn,
    split_name,
    sql_str,
    unquote_ident,
    validate_name,
)


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
        self.name = task_fqn
        self._target = target
        self._created_on = created_on

    def _task_created_on(self) -> Optional[Any]:
        if self._created_on is None:
            db, schema, short = split_fqn(self.name)
            rows = self._session.sql(
                f"SHOW TASKS LIKE {sql_str(unquote_ident(short))} IN SCHEMA {db}.{schema}"
            ).collect()
            for r in rows:
                if r["name"] == unquote_ident(short):
                    self._created_on = r["created_on"]
        return self._created_on

    def suspend(self) -> None:
        self._session.sql(f"ALTER TASK {self.name} SUSPEND").collect()

    def resume(self) -> None:
        self._session.sql(f"ALTER TASK {self.name} RESUME").collect()

    def run_now(self) -> None:
        """Triggers a run now (``EXECUTE TASK``)."""
        self._session.sql(f"EXECUTE TASK {self.name}").collect()

    def runs(self, limit: int = 10) -> List[TaskRun]:
        """The most recent runs of the task, newest first."""
        # the history is keyed by name: skip runs of an earlier task with the same name
        created_on = self._task_created_on()
        rows = self._session.sql(task_history_sql(self.name, 1000)).collect()
        if created_on is not None:
            rows = [
                r
                for r in rows
                if r["SCHEDULED_TIME"] is None or r["SCHEDULED_TIME"] >= created_on
            ]
        return make_task_runs(self._session, self.name, rows, self._target)[:limit]

    def drop(self) -> None:
        self._session.sql(f"DROP TASK IF EXISTS {self.name}").collect()

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
        """Runs the deployment once, asynchronously."""

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
    ) -> Schedule:
        """Creates and resumes a task that runs this deployment.

        Args:
            cron: A cron expression with a time zone, e.g. ``"0 * * * * UTC"``.
            name: Task name; defaults to ``<deployment name>_SCHEDULE`` in the
                deployment's schema.
            warehouse: Warehouse of the task. ``None`` creates a serverless task.
        """
        body = self.to_sql(args=args, entrypoint=entrypoint)
        if warehouse is not None:
            warehouse = validate_name(warehouse, "warehouse")
            if len(split_name(warehouse)) != 1:
                raise ValueError(f"Invalid warehouse: {warehouse!r}")
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
        self._session.sql(
            f"CREATE OR REPLACE TASK {task_fqn}{wh} "
            f"SCHEDULE = {sql_str('USING CRON ' + cron)} AS {body}"
        ).collect()
        self._session.sql(f"ALTER TASK {task_fqn} RESUME").collect()
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
        self._session.sql(f"DROP PROCEDURE IF EXISTS {self.name}(ARRAY)").collect()


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
    ) -> None:
        super().__init__(session, name)
        self._target_config = target_config or CodeBundle()
        self._spec = spec
        self._entrypoint = entrypoint
        self._compute_type = compute_type
        self._env = env or Environment()
        self._stage = stage

    def _entry(self, entrypoint: Optional[str]) -> str:
        entry = entrypoint or self._entrypoint
        if not entry:
            raise ValueError(
                "This deployment has no default entrypoint: pass entrypoint=..."
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
        if self._compute_type != "compute_pool":
            require_warehouse(self._session)
        spec = None
        if overrides:
            if self._spec is None:
                raise ValueError(
                    "overrides need the deployment's specification, which is not "
                    "available for deployments loaded with get_deployment()"
                )
            spec = merge_overrides(self._spec, overrides)
        name = execution_name or _bundle.default_execution_name(entry)
        send_telemetry(self._session, "run", CODE_BUNDLE_KIND)
        qid = _bundle.run_async(
            self._session, _bundle.execute_sql(self.name, entry, args, name, spec)
        )
        return BundleRun(self._session, qid, self._compute_type, None, name)

    def to_sql(
        self, *, args: Optional[Sequence[Any]] = None, entrypoint: Optional[str] = None
    ) -> str:
        return _bundle.execute_sql(self.name, self._entry(entrypoint), args)

    def add_version(self, program) -> None:
        """Uploads ``program`` and adds it as a new version. Later runs use it."""
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
        self._session.sql(
            f"ALTER CODE BUNDLE {self.name} ADD VERSION FROM {sql_str(prepared.location)}"
        ).collect()
        self._spec = prepared.spec
        if prepared.entrypoint:
            self._entrypoint = prepared.entrypoint

    def drop(self) -> None:
        self._session.sql(f"DROP CODE BUNDLE IF EXISTS {self.name}").collect()
