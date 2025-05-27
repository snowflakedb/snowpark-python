#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from typing import List

from snowflake.snowpark._internal.data_source.dbms_dialects import BaseDialect
from snowflake.snowpark._internal.data_source.drivers.psycopg2_driver import (
    Psycopg2TypeCode,
)
from snowflake.snowpark.types import StructType


class PostgresDialect(BaseDialect):
    @staticmethod
    def generate_select_query(
        table_or_query: str, schema: StructType, raw_schema: List[tuple], is_query: bool
    ) -> str:
        cols = []
        for _field, raw_field in zip(schema.fields, raw_schema):
            # databricks-sql-connector returns list of tuples for MapType
            # here we push down to-dict conversion to Databricks
            type_code = raw_field[1]
            if type_code in (
                Psycopg2TypeCode.JSONB.value,
                Psycopg2TypeCode.JSON.value,
            ):
                cols.append(f"""TO_JSON("{raw_field[0]}")::TEXT AS {raw_field[0]}""")
            elif type_code == Psycopg2TypeCode.CASHOID.value:
                cols.append(
                    f"""CASE WHEN "{raw_field[0]}" IS NULL THEN NULL ELSE FORMAT('"%s"', "{raw_field[0]}"::TEXT) END AS {raw_field[0]}"""
                )
            elif type_code == Psycopg2TypeCode.BYTEAOID.value:
                cols.append(f"""ENCODE("{raw_field[0]}", 'HEX') AS {raw_field[0]}""")
            elif type_code == Psycopg2TypeCode.TIMETZOID.value:
                cols.append(f""""{raw_field[0]}"::TIME AS {raw_field[0]}""")
            elif type_code == Psycopg2TypeCode.INTERVALOID.value:
                cols.append(f""""{raw_field[0]}"::TEXT AS {raw_field[0]}""")
            else:
                cols.append(f'"{raw_field[0]}"')
        return f"""SELECT {", ".join(cols)} FROM {table_or_query}"""
