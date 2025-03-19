#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark._internal.data_source.dbms_dialects import BaseDialect
from snowflake.snowpark.types import StructType, TimestampType, TimestampTimeZone


class OracledbDialect(BaseDialect):
    def generate_select_query(self, table_or_query: str, schema: StructType) -> str:
        cols = []
        for field in schema.fields:
            if (
                isinstance(field.datatype, TimestampType)
                and field.datatype.tz == TimestampTimeZone.TZ
            ):
                cols.append(
                    f"""TO_CHAR({field.name}, 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM')"""
                )
            elif (
                isinstance(field.datatype, TimestampType)
                and field.datatype.tz == TimestampTimeZone.LTZ
            ):
                cols.append(
                    f"""TO_CHAR({field.name} AT TIME ZONE SESSIONTIMEZONE, 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM')"""
                )
            else:
                cols.append(field.name)
        return f"""SELECT {" , ".join(cols)} FROM {table_or_query}"""
