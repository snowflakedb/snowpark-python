#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from typing import List

from snowflake.snowpark._internal.data_source.dbms_dialects import BaseDialect
from snowflake.snowpark.types import StructType, MapType, BinaryType


class DatabricksDialect(BaseDialect):
    def generate_select_query(
        self,
        table_or_query: str,
        schema: StructType,
        raw_schema: List[tuple],
        is_query: bool,
    ) -> str:
        cols = []
        for field, raw_field in zip(schema.fields, raw_schema):
            # databricks-sql-connector returns list of tuples for MapType
            # here we push down to-dict conversion to Databricks
            if isinstance(field.datatype, MapType):
                cols.append(f"""TO_JSON(`{raw_field[0]}`) AS {raw_field[0]}""")
            elif isinstance(field.datatype, BinaryType):
                cols.append(f"""HEX(`{raw_field[0]}`) AS {raw_field[0]}""")
            else:
                cols.append(f"`{raw_field[0]}`")
        return f"""SELECT {" , ".join(cols)} FROM {table_or_query}"""
