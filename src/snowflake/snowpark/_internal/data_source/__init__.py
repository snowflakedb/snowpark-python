#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

__all__ = [
    "DbapiDataSourceReader",
    "DbapiDataSource",
]

from snowflake.snowpark._internal.data_source.datasource_reader import (
    DbapiDataSourceReader,
)
from snowflake.snowpark._internal.data_source.datasource_partitioner import (
    DbapiDataSource,
)
