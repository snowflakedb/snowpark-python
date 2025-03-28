#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime

from typing import List, Any, Iterator, Type, Callable, Optional

from snowflake.snowpark._internal.data_source.datasource_typing import Connection
from snowflake.snowpark._internal.data_source.drivers.base_driver import BaseDriver
from snowflake.snowpark._internal.utils import (
    get_sorted_key_for_version,
)
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.types import StructType
from snowflake.connector.options import pandas as pd
import logging

logger = logging.getLogger(__name__)


class DataSourceReader:
    def __init__(
        self,
        driver_class: Type[BaseDriver],
        create_connection: Callable[[], "Connection"],
        schema: StructType,
        fetch_size: Optional[int] = 0,
        query_timeout: Optional[int] = 0,
        session_init_statement: Optional[List[str]] = None,
    ) -> None:
        self.driver = driver_class(create_connection)
        self.schema = schema
        self.fetch_size = fetch_size
        self.query_timeout = query_timeout
        self.session_init_statement = session_init_statement

    def read(self, partition: str) -> Iterator[List[Any]]:
        conn = self.driver.prepare_connection(
            self.driver.create_connection(), self.query_timeout
        )
        cursor = conn.cursor()
        try:
            if self.session_init_statement:
                for statement in self.session_init_statement:
                    try:
                        cursor.execute(statement)
                    except BaseException as exc:
                        raise SnowparkDataframeReaderException(
                            f"Failed to execute session init statement: '{statement}' due to exception '{exc!r}'"
                        )
            if self.fetch_size == 0:
                cursor.execute(partition)
                result = cursor.fetchall()
                yield result
            elif self.fetch_size > 0:
                cursor = cursor.execute(partition)
                while True:
                    rows = cursor.fetchmany(self.fetch_size)
                    if not rows:
                        break
                    yield rows
            else:
                raise ValueError("fetch size cannot be smaller than 0")
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def data_source_data_to_pandas_df(
        data: List[Any], schema: StructType
    ) -> "pd.DataFrame":
        columns = [col.name for col in schema.fields]
        # this way handles both list of object and list of tuples and avoid implicit pandas type conversion
        df = pd.DataFrame([list(row) for row in data], columns=columns, dtype=object)

        # convert timestamp and date to string to work around SNOW-1911989
        # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.map.html
        # 'map' is introduced in pandas 2.1.0, before that it is 'applymap'
        def df_map_method(pandas_df):
            return (
                pandas_df.applymap
                if get_sorted_key_for_version(str(pd.__version__)) < (2, 1, 0)
                else pandas_df.map
            )

        df = df_map_method(df)(
            lambda x: x.isoformat()
            if isinstance(x, (datetime.datetime, datetime.date))
            else x
        )
        # convert binary type to object type to work around SNOW-1912094
        df = df_map_method(df)(
            lambda x: x.hex() if isinstance(x, (bytearray, bytes)) else x
        )
        return df
