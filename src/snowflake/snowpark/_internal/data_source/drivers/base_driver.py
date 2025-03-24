#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from abc import ABC, abstractmethod
from typing import List, Callable, Any, Optional
from snowflake.snowpark._internal.data_source.datasource_typing import (
    Connection,
)
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.types import StructType


class BaseDriver(ABC):
    def __init__(self, create_connection: Callable[[], "Connection"]) -> None:
        self.create_connection = create_connection

    @abstractmethod
    def to_snow_type(self, schema: List[Any]) -> StructType:
        raise NotImplementedError(
            f"{self.__class__.__name__} has not implemented to_snow_type function"
        )

    def prepare_connection(
        self,
        conn: "Connection",
        query_timeout: int = 0,
    ) -> "Connection":
        return conn

    def infer_schema_from_description(self, table_or_query: str) -> StructType:
        conn = self.create_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT * FROM {table_or_query} WHERE 1 = 0")
            raw_schema = cursor.description
            return self.to_snow_type(raw_schema)

        except Exception as exc:
            raise SnowparkDataframeReaderException(
                f"Failed to infer Snowpark DataFrame schema from '{table_or_query}' due to {exc!r}."
                f" To avoid auto inference, you can manually specify the Snowpark DataFrame schema using 'custom_schema' in DataFrameReader.dbapi."
                f" Please check the stack trace for more details."
            ) from exc
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def validate_numeric_precision_scale(
        precision: Optional[int], scale: Optional[int]
    ) -> bool:
        if precision is not None:
            if not (0 <= precision <= 38):
                return False
            if scale is not None and not (0 <= scale <= precision):
                return False
        elif scale is not None:
            return False
        return True
