#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

from snowflake.snowpark.jobs._utils import (
    CODE_BUNDLE_KIND,
    STORED_PROCEDURE_KIND,
    validate_warehouse,
)

_EXECUTE_AS = ("caller", "owner", "restricted caller")


class Target(ABC):
    """Where a program runs. Use :class:`StoredProcedure` or :class:`CodeBundle`."""

    kind: str

    @abstractmethod
    def _submit(self, session, program, **kwargs):
        ...

    @abstractmethod
    def _deploy(self, session, program, **kwargs):
        ...


@dataclass
class StoredProcedure(Target):
    """Run the program as a Snowflake Python stored procedure (the default target).

    Every procedure created by this target has the signature
    ``(ARGS ARRAY) RETURNS VARIANT``. Runs use the session's current warehouse.

    Args:
        execute_as: ``"caller"``, ``"owner"`` or ``"restricted caller"``. One-off runs
            (``submit``) always use caller's rights; the default for ``deploy`` is
            ``"owner"``.

    There is no ``warehouse`` option: the procedure always runs on the session's current
    warehouse. The queries of a caller's-rights or anonymous procedure follow the
    session's current warehouse while it runs, so switching the warehouse only at
    submit time would move the CALL but not the program's own queries. Use
    ``session.use_warehouse(...)`` for interactive runs and ``schedule(warehouse=...)``
    for scheduled ones.
    """

    execute_as: Optional[str] = None
    kind = STORED_PROCEDURE_KIND

    def __post_init__(self) -> None:
        if self.execute_as is not None and self.execute_as not in _EXECUTE_AS:
            raise ValueError(
                f"execute_as must be one of {_EXECUTE_AS}, got {self.execute_as!r}"
            )

    def _submit(self, session, program, **kwargs):
        from snowflake.snowpark.jobs import _sproc

        return _sproc.submit(session, self, program, **kwargs)

    def _deploy(self, session, program, **kwargs):
        from snowflake.snowpark.jobs import _sproc

        return _sproc.deploy(session, self, program, **kwargs)


@dataclass
class CodeBundle(Target):
    """Run the program as a code bundle (Preview): custom Python on a warehouse or a
    compute pool.

    Args:
        compute_pool: Run on this compute pool. By default the program runs on the
            session's current warehouse.
        warehouse: The execution warehouse when running on a warehouse, or the query
            warehouse (for SQL issued by the program) when running on a compute pool.
            Defaults to the session's current warehouse. On a warehouse, the session
            switches to it while the run is submitted and then switches back; the
            switch is visible to other threads using the same session. If the session
            had no current warehouse, ``warehouse`` stays set on the session afterwards.
        runtime_version: Python version (``"3.12"``) on a warehouse, or runtime name
            (``"V2.9-CPU-PY3.12"``) on a compute pool.

    A code bundle runs scripts. Besides a script entry point (``"jobs/daily.py"``),
    local file and directory programs support function entry points
    (``"jobs/daily.py:run"``): a generated launcher calls ``run(session, *args)`` with
    the session from ``get_active_session()``. Arguments are strings and the return
    value is ignored, because code bundles cannot return values. Callables,
    ``Program.stage()`` and ``Program.workspace()`` programs do not support function
    entry points.

    On a compute pool, each one-off run creates a temporary code bundle named
    ``SNOWPARK_JOB_TMP_<id>``, which the run's handle drops once it sees the run finish.
    If the process ends first, the bundle stays until
    :meth:`JobManager.cleanup <snowflake.snowpark.jobs.JobManager.cleanup>` removes it
    (``Session.close`` also removes the ones that have finished by then; runs it cancels
    are usually still shutting down at that point, so their bundles are left for
    ``cleanup()``).

    ``Program.stage()`` and ``Program.workspace()`` programs bring their own
    ``code_bundle.yml``, which defines the compute and the environment: for them,
    ``compute_pool``, ``warehouse`` and ``runtime_version`` (and any ``Environment``)
    must not be set. Use ``session.use_warehouse()`` to choose the warehouse of a
    warehouse bundle.
    """

    compute_pool: Optional[str] = None
    warehouse: Optional[str] = None
    runtime_version: Optional[str] = None
    kind = CODE_BUNDLE_KIND

    def __post_init__(self) -> None:
        validate_warehouse(self.warehouse)

    def _submit(self, session, program, **kwargs):
        from snowflake.snowpark.jobs import _bundle

        return _bundle.submit(session, self, program, **kwargs)

    def _deploy(self, session, program, **kwargs):
        from snowflake.snowpark.jobs import _bundle

        return _bundle.deploy(session, self, program, **kwargs)


def check_target(target: Any) -> Target:
    if not isinstance(target, Target):
        raise TypeError(
            f"target must be StoredProcedure() or CodeBundle(), got {target!r}"
        )
    return target
