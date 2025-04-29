#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from typing import List

from snowflake.snowpark._internal.data_source.dbms_dialects import BaseDialect
from snowflake.snowpark._internal.utils import quote_name
from snowflake.snowpark.types import StructType, TimestampType, TimestampTimeZone


class OracledbDialect(BaseDialect):
    def generate_select_query(
        self,
        table_or_query: str,
        schema: StructType,
        raw_schema: List[tuple],
    ) -> str:
        cols = []
        for field, raw_field in zip(schema.fields, raw_schema):
            if (
                isinstance(field.datatype, TimestampType)
                and field.datatype.tz == TimestampTimeZone.TZ
            ):
                cols.append(
                    f"""TO_CHAR({quote_name(raw_field[0], keep_case=True)}, 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM')"""
                )
            elif (
                isinstance(field.datatype, TimestampType)
                and field.datatype.tz == TimestampTimeZone.LTZ
            ):
                cols.append(
                    f"""TO_CHAR({quote_name(raw_field[0], keep_case=True)} AT TIME ZONE SESSIONTIMEZONE, 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM')"""
                )
            else:
                cols.append(quote_name(raw_field[0], keep_case=True))
        return f"""SELECT {" , ".join(cols)} FROM {table_or_query}"""
