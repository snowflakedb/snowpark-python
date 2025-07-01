#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import logging
import snowflake.snowpark


logger = logging.getLogger(__name__)


class DataframeProfiler:
    """
    Set up profiler to track query history for dataframe operations. To get profiles, call
    get_execution_profile() on a dataframe.
    """

    def __init__(
        self,
        session: "snowflake.snowpark.Session",
    ) -> None:
        self._session = session
        self._query_history = None

    def enable(self) -> None:
        """
        Enables dataframe profiling.
        """
        if self._query_history is None:
            logger.info("Enabling dataframe profiling")
            self._query_history = self._session.query_history(
                include_thread_id=True,
                include_error=True,
                include_dataframe_profiling=True,
            )

    def disable(self) -> None:
        """
        Disable profiler.
        """
        if self._query_history is not None:
            self._session._conn.remove_query_listener(self._query_history)  # type: ignore
            self._query_history = None
