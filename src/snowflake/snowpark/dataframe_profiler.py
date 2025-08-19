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
        self._enabled = False
        self._query_history = None

    def enable(self) -> None:
        """
        Enables dataframe profiling.
        """
        self._enabled = True
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
        self._enabled = False
        if self._query_history is not None:
            self._session._conn.remove_query_listener(self._query_history)  # type: ignore
            self._query_history = None

    def add_describe_query_time(
        self, dataframe_uuid: str, query: str, time: float
    ) -> None:
        """
        Add the time taken to describe a dataframe to query history.
        """
        if self._enabled:
            if dataframe_uuid not in self._query_history._describe_queries:
                self._query_history._describe_queries[dataframe_uuid] = []
            self._query_history._describe_queries[dataframe_uuid].append((query, time))
