#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

__all__ = [
    "DataSourceReader",
    "DataSourcePartitioner",
]

from snowflake.snowpark._internal.data_source.datasource_reader import DataSourceReader
from snowflake.snowpark._internal.data_source.datasource_partitioner import (
    DataSourcePartitioner,
)
