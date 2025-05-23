#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from typing import List, Any

from snowflake.snowpark._internal.data_source.drivers import BaseDriver
from snowflake.snowpark.types import StructType


class SqliteDriver(BaseDriver):
    def to_snow_type(self, schema: List[Any]) -> StructType:
        raise NotImplementedError(
            "SQLite is not supported yet. To avoid auto inference, you can manually "
            "specify the Snowpark DataFrame schema using 'custom_schema' in DataFrameReader.dbapi."
        )
