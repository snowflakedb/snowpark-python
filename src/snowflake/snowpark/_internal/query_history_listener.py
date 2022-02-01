#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import snowflake.snowpark


class QueryHistoryListener:
    def __init__(self, session: "snowflake.snowpark.Session"):
        self.session = session
        self._queries = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session._conn.remove_query_listener(self)

    def _add_query(self, query_record):
        self._queries.append(query_record)

    @property
    def queries(self):
        return self._queries
