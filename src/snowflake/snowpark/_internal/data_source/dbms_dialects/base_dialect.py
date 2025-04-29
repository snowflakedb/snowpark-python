#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from typing import List, Optional

from snowflake.snowpark.types import StructType


class BaseDialect:
    @staticmethod
    def generate_select_query(
        table_or_query: str, schema: StructType, *, raw_schema: Optional[List[tuple]]
    ) -> str:
        return f"SELECT * FROM {table_or_query}"
