#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import os
from abc import abstractmethod
from logging import getLogger
from typing import Dict, List, NamedTuple, Tuple

import snowflake.snowpark

_logger = getLogger(__name__)


class QueryRecord(NamedTuple):
    """Contains the query information returned from the Snowflake database after the query is run."""

    query_id: str
    sql_text: str
    is_describe: bool = None
    thread_id: int = None

    def __repr__(self) -> str:
        if self.is_describe is None and self.thread_id is None:
            return f"QueryRecord(query_id={self.query_id}, sql_text={self.sql_text})"
        elif self.is_describe is not None and self.thread_id is None:
            return f"QueryRecord(query_id={self.query_id}, sql_text={self.sql_text}, is_describe={self.is_describe})"
        elif self.is_describe is None and self.thread_id is not None:
            return f"QueryRecord(query_id={self.query_id}, sql_text={self.sql_text}, thread_id={self.thread_id})"
        else:
            return f"QueryRecord(query_id={self.query_id}, sql_text={self.sql_text}, is_describe={self.is_describe}, thread_id={self.thread_id})"


class QueryListener:
    @abstractmethod
    def _notify(self, query_record: QueryRecord, **kwargs) -> None:
        """
        notify query listener of a query event
        Args:
            query_record: record of the query to notify the listener of
            **kwargs: optional keyword arguments
        Returns:
            None
        """
        pass  # pragma: no cover


class QueryHistory(QueryListener):
    """A context manager that listens to and records SQL queries that are pushed down to the Snowflake database.

    See also:
        :meth:`snowflake.snowpark.Session.query_history`.
    """

    def __init__(
        self,
        session: "snowflake.snowpark.session.Session",
        include_describe: bool = False,
        include_thread_id: bool = False,
        include_error: bool = False,
        include_dataframe_profiling: bool = False,
    ) -> None:
        self.session = session
        self._queries: List[QueryRecord] = []
        self._include_describe = include_describe
        self._include_thread_id = include_thread_id
        self._include_error = include_error
        self._include_dataframe_profiling = include_dataframe_profiling
        # if dataframe profiling is enabled, we will map dataframe plan uuids to query ids
        self._dataframe_queries: Dict[int, List[str]] = {}
        # if dataframe profiling is enabled, we will map dataframe plan uuids to the time taken to describe the dataframe
        self._describe_queries: Dict[str, List[Tuple[str, float]]] = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session._conn.remove_query_listener(self)

    def _notify(self, query_record: QueryRecord, **kwargs) -> None:
        if self._include_dataframe_profiling:
            if "dataframe_uuid" in kwargs:
                df_uuid = kwargs["dataframe_uuid"]
                if df_uuid not in self._dataframe_queries:
                    self._dataframe_queries[df_uuid] = []
                self._dataframe_queries[df_uuid].append(query_record.query_id)
        self._queries.append(query_record)

    @property
    def queries(self) -> List[QueryRecord]:
        return self._queries

    @property
    def include_describe(self) -> bool:
        """When True, QueryRecords for describe queries are recorded by this listener."""
        return self._include_describe

    @property
    def include_thread_id(self) -> bool:
        """When True, thread id of the query are recorded by this listener."""
        return self._include_thread_id

    @property
    def include_error(self) -> bool:
        """When True, queries that have error during execution are recorded by this listener."""
        return self._include_error

    @property
    def dataframe_queries(self) -> Dict[int, List[str]]:
        """Returns a map of dataframe plan uuid to query ids."""
        return self._dataframe_queries


class AstListener(QueryListener):
    def __init__(
        self,
        session: "snowflake.snowpark.session.Session",
        include_failures: bool = False,
    ) -> None:
        """
        Initializes the AstListener.

        Args:
            session: The session to listen to.
            include_failures: When True, the listener will include failed queries in the history. This can be useful
                 for debugging and testing.
        """
        self.session = session
        self._ast_batches: List[str] = []
        self._include_failures = include_failures

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session._conn.remove_query_listener(self)

    def _notify(self, query_record: QueryRecord, **kwargs) -> None:
        if "dataframeAst" in kwargs:
            self._ast_batches.append(kwargs["dataframeAst"])

    @property
    def include_failures(self) -> bool:
        return self._include_failures  # pragma: no cover

    @property
    def base64_batches(self) -> List[str]:
        return self._ast_batches


class _VscHistoryExporter(QueryListener):
    """Exports executed query IDs to a directory watched by the Snowflake VS Code extension.

    For each query, an empty file named after the query ID is created in the
    target directory.

    Note that this exporter is active only when the
    ``SNOWFLAKE_SNOWPARK_VSC_QUERY_HISTORY_DIR`` environment variable is set.
    """

    def __init__(self, query_history_dir: str) -> None:
        """
        Initializes the _VscHistoryExporter.

        Args:
            query_history_dir: Directory into which an empty file named after each
                executed query ID is written. Created if it does not already exist.
        """
        self._query_history_dir = query_history_dir
        try:
            os.makedirs(query_history_dir, exist_ok=True)
        except OSError:
            # Exporting query history is a best-effort feature for the VS Code
            # extension; a failure here must not break the user's session.
            _logger.debug(
                "Failed to create query history directory %s",
                query_history_dir,
                exc_info=True,
            )

    def _notify(self, query_record: QueryRecord, **kwargs) -> None:
        if query_record.is_describe or not query_record.query_id:
            return
        try:
            open(
                os.path.join(self._query_history_dir, query_record.query_id), "w"
            ).close()
        except OSError:
            # Exporting query history is a best-effort feature for the VS Code
            # extension; a failure here must not break the user's session.
            _logger.debug(
                "Failed to write query history file for query id %s in %s",
                query_record.query_id,
                self._query_history_dir,
                exc_info=True,
            )
