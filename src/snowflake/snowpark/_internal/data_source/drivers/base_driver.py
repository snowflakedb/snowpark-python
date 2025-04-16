#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from abc import ABC, abstractmethod
import datetime
from typing import List, Callable, Any, Optional, TYPE_CHECKING
from snowflake.snowpark._internal.data_source.datasource_typing import (
    Connection,
)
from snowflake.snowpark._internal.utils import get_sorted_key_for_version
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.types import StructType
from snowflake.connector.options import pandas as pd

if TYPE_CHECKING:
    from snowflake.snowpark.session import Session
    from snowflake.snowpark.dataframe import DataFrame


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

    # convert timestamp and date to string to work around SNOW-1911989
    # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.map.html
    # 'map' is introduced in pandas 2.1.0, before that it is 'applymap'
    @staticmethod
    def df_map_method(pandas_df):
        return (
            pandas_df.applymap
            if get_sorted_key_for_version(str(pd.__version__)) < (2, 1, 0)
            else pandas_df.map
        )

    @staticmethod
    def data_source_data_to_pandas_df(
        data: List[Any], schema: StructType
    ) -> "pd.DataFrame":
        columns = [col.name for col in schema.fields]
        # this way handles both list of object and list of tuples and avoid implicit pandas type conversion
        df = pd.DataFrame([list(row) for row in data], columns=columns, dtype=object)

        df = BaseDriver.df_map_method(df)(
            lambda x: x.isoformat()
            if isinstance(x, (datetime.datetime, datetime.date))
            else x
        )
        # convert binary type to object type to work around SNOW-1912094
        df = BaseDriver.df_map_method(df)(
            lambda x: x.hex() if isinstance(x, (bytearray, bytes)) else x
        )
        return df

    @staticmethod
    def to_result_snowpark_df(
        session: "Session", table_name, schema, _emit_ast: bool = True
    ) -> "DataFrame":
        return session.table(table_name, _emit_ast=_emit_ast)
