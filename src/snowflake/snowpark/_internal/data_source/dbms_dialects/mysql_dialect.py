#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from typing import List

from snowflake.snowpark._internal.data_source.dbms_dialects import BaseDialect
from snowflake.snowpark.types import StructType


class MysqlDialect(BaseDialect):
    def generate_select_query(
        self,
        table_or_query: str,
        schema: StructType,
        raw_schema: List[tuple],
    ) -> str:
        if table_or_query.lower().startswith("select"):
            return f"""select A.* from ({table_or_query}) A"""
        else:
            return f"""select * from {table_or_query}"""
