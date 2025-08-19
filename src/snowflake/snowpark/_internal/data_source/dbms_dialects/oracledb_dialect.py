#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from typing import List

from snowflake.snowpark._internal.data_source.dbms_dialects import BaseDialect
from snowflake.snowpark._internal.data_source.dbms_dialects.base_dialect import (
    QUERY_TEMPLATE,
)
from snowflake.snowpark._internal.utils import quote_name
from snowflake.snowpark.types import StructType, TimestampType, TimestampTimeZone


class OracledbDialect(BaseDialect):
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
            field_name = (
                f"{query_input_alias}.{quote_name(raw_field[0], keep_case=True)}"
                if is_query
                else f"{quote_name(raw_field[0], keep_case=True)}"
            )
            if (
                isinstance(field.datatype, TimestampType)
                and field.datatype.tz == TimestampTimeZone.TZ
            ):
                cols.append(
                    f"""TO_CHAR({field_name}, 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM') AS {raw_field[0]}"""
                )
            elif (
                isinstance(field.datatype, TimestampType)
                and field.datatype.tz == TimestampTimeZone.LTZ
            ):
                cols.append(
                    f"""TO_CHAR({field_name} AT TIME ZONE SESSIONTIMEZONE, 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM')  AS {raw_field[0]}"""
                )
            else:
                cols.append(
                    f"{field_name} AS {raw_field[0]}"
                ) if is_query else cols.append(field_name)
        return QUERY_TEMPLATE.format(
            cols=", ".join(cols),
            table_or_query=f"({table_or_query})" if is_query else table_or_query,
            query_input_alias=query_input_alias if is_query else "",
        )
