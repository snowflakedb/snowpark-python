#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from typing import List

from snowflake.snowpark.types import StructType


class BaseDialect:
    @staticmethod
    def generate_select_query(
        table_or_query: str,
        schema: StructType,
        raw_schema: List[tuple],
        is_query: bool,
        query_input_alias: str,
    ) -> str:
        cols = []
        for raw_field in raw_schema:
            if is_query:
                cols.append(
                    f"""{query_input_alias}.`{raw_field[0]}` AS {raw_field[0]}"""
                )
            else:
                cols.append(raw_field[0])

        if is_query:
            return f"""SELECT {" , ".join(cols)} FROM ({table_or_query}) {query_input_alias}"""
        else:
            return f"""SELECT {" , ".join(cols)} FROM {table_or_query}"""
