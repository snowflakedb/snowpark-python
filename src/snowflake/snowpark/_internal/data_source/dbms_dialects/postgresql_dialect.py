#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from typing import List

from snowflake.snowpark._internal.data_source.dbms_dialects import BaseDialect
from snowflake.snowpark._internal.data_source.dbms_dialects.base_dialect import (
    QUERY_TEMPLATE,
)
from snowflake.snowpark._internal.data_source.drivers.psycopg2_driver import (
    Psycopg2TypeCode,
)
from snowflake.snowpark.types import StructType


class PostgresDialect(BaseDialect):
    @staticmethod
    def generate_select_query(
        table_or_query: str,
        schema: StructType,
        raw_schema: List[tuple],
        is_query: bool,
        query_input_alias: str,
    ) -> str:
        cols = []
        for _field, raw_field in zip(schema.fields, raw_schema):
            # databricks-sql-connector returns list of tuples for MapType
            # here we push down to-dict conversion to Databricks
            type_code = raw_field[1]
            field_name = (
                f'{query_input_alias}."{raw_field[0]}"'
                if is_query
                else f'"{raw_field[0]}"'
            )
            if type_code in (
                Psycopg2TypeCode.JSONB.value,
                Psycopg2TypeCode.JSON.value,
            ):
                cols.append(f"""TO_JSON({field_name})::TEXT AS {raw_field[0]}""")
            elif type_code == Psycopg2TypeCode.CASHOID.value:
                cols.append(
                    f"""CASE WHEN {field_name} IS NULL THEN NULL ELSE FORMAT('"%s"', "{raw_field[0]}"::TEXT) END AS {raw_field[0]}"""
                )
            elif type_code == Psycopg2TypeCode.BYTEAOID.value:
                cols.append(f"""ENCODE({field_name}, 'HEX') AS {raw_field[0]}""")
            elif type_code == Psycopg2TypeCode.TIMETZOID.value:
                cols.append(f"""{field_name}::TIME AS {raw_field[0]}""")
            elif type_code == Psycopg2TypeCode.INTERVALOID.value:
                cols.append(f"""{field_name}::TEXT AS {raw_field[0]}""")
            else:
                cols.append(
                    f"{field_name} AS {raw_field[0]}"
                ) if is_query else cols.append(field_name)

        return QUERY_TEMPLATE.format(
            cols=", ".join(cols),
            table_or_query=f"({table_or_query})" if is_query else table_or_query,
            query_input_alias=query_input_alias if is_query else "",
        ).strip()
