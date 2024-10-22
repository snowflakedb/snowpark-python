#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from abc import abstractmethod
from typing import List, NamedTuple

import snowflake.snowpark


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
    def _notify(self, query_record: QueryRecord, *args, **kwargs) -> None:
        """
        notify query listener of a query event
        Args:
            query_record: record of the query to notify the listener of
            *args: optional arguments
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
    ) -> None:
        self.session = session
        self._queries: List[QueryRecord] = []
        self._include_describe = include_describe
        self._include_thread_id = include_thread_id

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session._conn.remove_query_listener(self)

    def _notify(self, query_record: QueryRecord, *args, **kwargs) -> None:
        self._queries.append(query_record)

    @property
    def queries(self) -> List[QueryRecord]:
        return self._queries

    @property
    def include_describe(self) -> bool:
        return self._include_describe

    @property
    def include_thread_id(self) -> bool:
        return self._include_thread_id


class AstListener(QueryListener):
    def __init__(self, session: "snowflake.snowpark.session.Session") -> None:
        self.session = session
        self._ast_batches: List[bytes] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session._conn.remove_query_listener(self)

    def _notify(self, query_record: QueryRecord, *args, **kwargs) -> None:
        if "dataframeAst" in kwargs:
            self._ast_batches.append(kwargs["dataframeAst"])

    @property
    def base64_batches(self) -> List[QueryRecord]:
        return self._ast_batches
