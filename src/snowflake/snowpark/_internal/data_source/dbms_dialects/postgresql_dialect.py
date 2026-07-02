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
    def _quote_ident(name: str) -> str:
        """Escape a PostgreSQL identifier: double any embedded double-quotes."""
        return '"' + name.replace('"', '""') + '"'

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
            quoted_name = PostgresDialect._quote_ident(raw_field[0])
            field_name = (
                f"{query_input_alias}.{quoted_name}" if is_query else quoted_name
            )
            alias = quoted_name
            if type_code in (
                Psycopg2TypeCode.JSONB.value,
                Psycopg2TypeCode.JSON.value,
            ):
                cols.append(f"""TO_JSON({field_name})::TEXT AS {alias}""")
            elif type_code == Psycopg2TypeCode.CASHOID.value:
                cols.append(
                    f"""CASE WHEN {field_name} IS NULL THEN NULL ELSE FORMAT('"%s"', {quoted_name}::TEXT) END AS {alias}"""
                )
            elif type_code == Psycopg2TypeCode.BYTEAOID.value:
                cols.append(f"""ENCODE({field_name}, 'HEX') AS {alias}""")
            elif type_code == Psycopg2TypeCode.TIMETZOID.value:
                cols.append(f"""{field_name}::TIME AS {alias}""")
            elif type_code == Psycopg2TypeCode.INTERVALOID.value:
                cols.append(f"""{field_name}::TEXT AS {alias}""")
            else:
                cols.append(f"{field_name} AS {alias}") if is_query else cols.append(
                    field_name
                )

        return QUERY_TEMPLATE.format(
            cols=", ".join(cols),
            table_or_query=f"({table_or_query})" if is_query else table_or_query,
            query_input_alias=query_input_alias if is_query else "",
        ).strip()
