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
        self.__save_mode = _SaveMode.APPEND  # TODO: spark default value is errorifexists. Scala snowpark is append.

    def mode(self, save_mode: str) -> "DataFrameWriter":
        self.__save_mode = Utils.str_to_enum(save_mode.lower(), _SaveMode, "`save_mode`")
        return self

    def saveAsTable(self, table_name: Union[str, Iterable[str]]):
        full_table_name = (
            table_name if isinstance(table_name, str) else ".".join(table_name)
        )
        # TODO: Should we validate this in the client or allow the server to throw the error?
        Utils.validate_object_name(full_table_name)
        create_table_logic_plan = SnowflakeCreateTable(
            full_table_name, self.__save_mode, self.__data_frame._DataFrame__plan
        )
        session = self.__data_frame.session
        snowflake_plan = session.analyzer.resolve(create_table_logic_plan)
        session.conn.execute(snowflake_plan)
