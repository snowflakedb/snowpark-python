#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark.types import StructType


class BaseDialect:
    @staticmethod
    def generate_select_query(table_or_query: str, schema: StructType) -> str:
        return f"SELECT * FROM {table_or_query}"
