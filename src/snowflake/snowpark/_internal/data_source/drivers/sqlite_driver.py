#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from enum import Enum
from typing import Callable, List, Any

from snowflake.snowpark._internal.data_source.drivers import BaseDriver
from snowflake.snowpark.types import StructType
from snowflake.snowpark._internal.data_source.datasource_typing import Connection


class SqliteDriver(BaseDriver):
    def __init__(
        self, create_connection: Callable[[], "Connection"], dbms_type: Enum
    ) -> None:
        super().__init__(create_connection, dbms_type)

    def to_snow_type(self, schema: List[Any]) -> StructType:
        raise NotImplementedError(
            "SQLite is not supported yet. To avoid auto inference, you can manually "
            "specify the Snowpark DataFrame schema using 'custom_schema' in DataFrameReader.dbapi."
        )
