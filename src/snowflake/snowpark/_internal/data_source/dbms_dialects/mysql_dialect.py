#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from typing import List

from snowflake.snowpark._internal.data_source.dbms_dialects import BaseDialect
from snowflake.snowpark._internal.data_source.drivers.pymsql_driver import (
    PymysqlTypeCode,
)
from snowflake.snowpark.types import StructType, TimeType, BinaryType


class MysqlDialect(BaseDialect):
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
                f"{query_input_alias}.`{raw_field[0]}`"
                if is_query
                else f"`{raw_field[0]}`"
            )
            if isinstance(field.datatype, TimeType):
                cols.append(f"""CAST({field_name} AS CHAR) AS {raw_field[0]}""")
            elif (
                isinstance(field.datatype, BinaryType)
                or raw_field[1] == PymysqlTypeCode.BIT
            ):
                cols.append(f"""HEX({field_name}) AS {raw_field[0]}""")
            else:
                cols.append(f"""{field_name} AS {raw_field[0]}""")

        if is_query:
            return f"""SELECT {" , ".join(cols)} FROM ({table_or_query}) {query_input_alias}"""
        else:
            return f"""SELECT {" , ".join(cols)} FROM `{table_or_query}`"""
