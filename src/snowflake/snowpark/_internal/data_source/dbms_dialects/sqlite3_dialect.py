#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark._internal.data_source.dbms_dialects.base_dialect import (
    BaseDialect,
)
from snowflake.snowpark.types import StructType


class Sqlite3Dialect(BaseDialect):
    def generate_select_query(self, table_or_query: str, schema: StructType) -> str:
        return f"SELECT * FROM {table_or_query}"
