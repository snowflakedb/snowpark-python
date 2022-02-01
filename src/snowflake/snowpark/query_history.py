#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from typing import List, NamedTuple

import snowflake.snowpark


class QueryRecord(NamedTuple):
    """ """

    query_id: str
    sql_text: str


class QueryHistoryListener:
    """A context manager that listens to and record sqls pushed down to Snowflake DB."""

    def __init__(self, session: "snowflake.snowpark.Session"):
        self.session = session
        self._queries = []  # type: List[QueryRecord]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session._conn.remove_query_listener(self)

    def _add_query(self, query_record):
        self._queries.append(QueryRecord(*query_record))

    @property
    def queries(self) -> List[QueryRecord]:
        return self._queries
