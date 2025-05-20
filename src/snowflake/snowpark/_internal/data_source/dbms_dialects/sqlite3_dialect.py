#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark._internal.data_source.dbms_dialects import BaseDialect


class Sqlite3Dialect(BaseDialect):
    def __init__(self, is_query: bool) -> None:
        super().__init__(is_query)
