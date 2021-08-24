#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from enum import Enum
from typing import Dict, Iterable, Union

from snowflake.snowpark.internal.analyzer.snowflake_plan import SnowflakeCreateTable
from snowflake.snowpark.internal.utils import Utils, _SaveMode


class DataFrameWriter:
    """ """

    def __init__(self, data_frame: "DataFrame"):
        self.__data_frame = data_frame

    def saveAsTable(
        self, table_name: Union[str, Iterable[str]], save_mode: str = "append"
    ):
        # TODO: Should we validate this in the client or just allow the server side to throw the error?
        Utils.validate_object_name(table_name)
        mode = Utils.str_to_enum(save_mode, _SaveMode, "`save_mode`")
        full_table_name = (
            ".".join(table_name) if isinstance(table_name, Iterable) else table_name
        )
        create_table_logic_plan = SnowflakeCreateTable(
            full_table_name, mode, self.__data_frame._DataFrame__plan
        )
        session = self.__data_frame.session
        snowflake_plan = session.analyzer.resolve(create_table_logic_plan)
        session.conn.execute(snowflake_plan)
