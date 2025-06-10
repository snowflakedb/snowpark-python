#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from typing import List

from snowflake.snowpark._internal.data_source.dbms_dialects import BaseDialect
from snowflake.snowpark._internal.data_source.drivers.pymsql_driver import (
    PymysqlTypeCode,
)
from snowflake.snowpark._internal.utils import (
    random_name_for_temp_object,
    TempObjectType,
)
from snowflake.snowpark.types import StructType, TimeType, BinaryType


class MysqlDialect(BaseDialect):
    def generate_select_query(
        self,
        table_or_query: str,
        schema: StructType,
        raw_schema: List[tuple],
        is_query: bool,
    ) -> str:
        cols = []
        random_table_alias = random_name_for_temp_object(TempObjectType.TABLE)
        for field, raw_field in zip(schema.fields, raw_schema):
            if isinstance(field.datatype, TimeType):
                if is_query:
                    cols.append(
                        f"""CAST({random_table_alias}.`{raw_field[0]}` AS CHAR) AS {raw_field[0]}"""
                    )
                else:
                    cols.append(f"""CAST(`{raw_field[0]}` AS CHAR) AS {raw_field[0]}""")
            elif (
                isinstance(field.datatype, BinaryType)
                or raw_field[1] == PymysqlTypeCode.BIT
            ):
                if is_query:
                    cols.append(
                        f"""HEX({random_table_alias}.`{raw_field[0]}`) AS {raw_field[0]}"""
                    )
                else:
                    cols.append(f"""HEX(`{raw_field[0]}`) AS {raw_field[0]}""")
            else:
                if is_query:
                    cols.append(
                        f"""{random_table_alias}.`{raw_field[0]}` AS {raw_field[0]}"""
                    )
                else:
                    cols.append(f"`{raw_field[0]}`")

        if is_query:
            return f"""SELECT {" , ".join(cols)} FROM ({table_or_query}) {random_table_alias}"""
        else:
            return f"""SELECT {" , ".join(cols)} FROM `{table_or_query}`"""
