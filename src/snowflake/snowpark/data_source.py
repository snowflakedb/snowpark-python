#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import List, Iterator, Any, Union

from snowflake.snowpark.types import StructType


class InputPartition:
    def __init__(self, value: Any) -> None:
        self.value = value


class DataSourceReader:
    def __init__(self, schema: StructType) -> None:
        self.schema = schema

    def read(self, partition: InputPartition) -> Iterator[List[Any]]:
        pass


class DataSource:
    def __init__(self) -> None:
        pass

    def reader(self, schema: Union[StructType, str]) -> DataSourceReader:
        pass

    @classmethod
    def name(cls) -> str:
        pass

    def schema(self) -> Union[StructType, str]:
        pass

    def partitions(self) -> List[InputPartition]:
        pass
