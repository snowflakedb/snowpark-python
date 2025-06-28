#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Any, Tuple, Iterator, Union, List

from snowflake.snowpark.types import StructType


class InputPartition:
    def __init__(self, value: Any) -> None:
        self.value = value


class DataSourceReader:
    def __init__(self, schema: StructType) -> None:
        self.schema = schema

    def partitions(self) -> List[InputPartition]:
        pass

    def read(
        self, partition: InputPartition
    ) -> Union[Iterator[Tuple], Iterator[List[Any]]]:
        pass


class DataSource:
    def __init__(self) -> None:
        self._internal_partitions = None

    def reader(self, schema: Union[StructType, str]) -> DataSourceReader:
        pass

    @classmethod
    def name(cls) -> str:
        pass

    def schema(self) -> Union[StructType, str]:
        pass

    def _partitions(self) -> List[InputPartition]:
        if self._internal_partitions is None:
            self._internal_partitions = self.reader(self.schema()).partitions()
        return self._internal_partitions
