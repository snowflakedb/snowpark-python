#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from typing import List

from snowflake.snowpark._internal.data_source.dbms_dialects import BaseDialect
from snowflake.snowpark._internal.data_source.dbms_dialects.base_dialect import (
    QUERY_TEMPLATE,
)
from snowflake.snowpark.types import StructType, MapType, BinaryType


class DatabricksDialect(BaseDialect):
    def generate_select_query(
        self,
        table_or_query: str,
        schema: StructType,
        raw_schema: List[tuple],
        is_query: bool,
        query_input_alias: str,
    ) -> str:
        cols = []
        for field, raw_field in zip(schema.fields, raw_schema):
            quoted_name = self._quote_backtick(raw_field[0])
            field_name = (
                f"{query_input_alias}.{quoted_name}" if is_query else quoted_name
            )
            alias = quoted_name
            # databricks-sql-connector returns list of tuples for MapType
            # here we push down to-dict conversion to Databricks
            if isinstance(field.datatype, MapType):
                cols.append(f"""TO_JSON({field_name}) AS {alias}""")
            elif isinstance(field.datatype, BinaryType):
                cols.append(f"""HEX({field_name}) AS {alias}""")
            else:
                cols.append(f"{field_name} AS {alias}") if is_query else cols.append(
                    field_name
                )
        return QUERY_TEMPLATE.format(
            cols=", ".join(cols),
            table_or_query=f"({table_or_query})" if is_query else table_or_query,
            query_input_alias=query_input_alias if is_query else "",
        ).strip()
