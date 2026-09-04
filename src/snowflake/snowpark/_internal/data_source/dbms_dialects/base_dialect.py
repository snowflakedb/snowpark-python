#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from typing import List

from snowflake.snowpark.types import StructType


QUERY_TEMPLATE = "SELECT {cols} FROM {table_or_query} {query_input_alias}"


class BaseDialect:
    @staticmethod
    def _quote_backtick(name: str) -> str:
        """Escape a backtick-quoted identifier: double any embedded backticks."""
        return "`" + name.replace("`", "``") + "`"

    @staticmethod
    def generate_select_query(
        table_or_query: str,
        schema: StructType,
        raw_schema: List[tuple],
        is_query: bool,
        query_input_alias: str,
    ) -> str:
        cols = []
        for raw_field in raw_schema:
            quoted_name = BaseDialect._quote_backtick(raw_field[0])
            if is_query:
                cols.append(f"""{query_input_alias}.{quoted_name} AS {quoted_name}""")
            else:
                cols.append(quoted_name)

        return QUERY_TEMPLATE.format(
            cols=", ".join(cols),
            table_or_query=f"({table_or_query})" if is_query else table_or_query,
            query_input_alias=query_input_alias if is_query else "",
        ).strip()
