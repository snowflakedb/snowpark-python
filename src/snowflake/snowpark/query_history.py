#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from typing import List, NamedTuple

import snowflake.snowpark


class QueryRecord(NamedTuple):
    """Contains the query information returned from the Snowflake database after the query is run."""

    query_id: str
    sql_text: str


class QueryHistory:
    """A context manager that listens to and records SQL queries that are pushed down to the Snowflake database."""

    def __init__(self, session: "snowflake.snowpark.session.Session") -> None:
        self.session = session
        self._queries: List[QueryRecord] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session._conn.remove_query_listener(self)

    def _add_query(self, query_record: QueryRecord):
        self._queries.append(query_record)

    @property
    def queries(self) -> List[QueryRecord]:
        return self._queries
