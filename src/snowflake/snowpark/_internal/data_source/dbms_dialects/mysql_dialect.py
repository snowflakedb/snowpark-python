#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from typing import List

from snowflake.snowpark._internal.data_source.dbms_dialects import BaseDialect
from snowflake.snowpark.types import StructType, TimeType


class MysqlDialect(BaseDialect):
    def generate_select_query(
        self,
        table_or_query: str,
        schema: StructType,
        raw_schema: List[tuple],
    ) -> str:
        is_query = table_or_query.lower().startswith("select")
        cols = []
        for field, raw_field in zip(schema.fields, raw_schema):
            if isinstance(field.datatype, TimeType):
                if is_query:
                    cols.append(f"""CAST(A.{raw_field[0]} AS CHAR) AS {raw_field[0]}""")
                else:
                    cols.append(f"""CAST({raw_field[0]} AS CHAR) AS {raw_field[0]}""")
            else:
                if is_query:
                    cols.append(f"""A.{raw_field[0]} AS {raw_field[0]}""")
                else:
                    cols.append(raw_field[0])

        if is_query:
            return f"""SELECT {" , ".join(cols)} FROM ({table_or_query}) A"""
        else:
            return f"""SELECT {" , ".join(cols)} FROM {table_or_query}"""
