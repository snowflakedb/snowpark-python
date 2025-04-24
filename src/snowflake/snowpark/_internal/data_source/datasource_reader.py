#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from enum import Enum

from typing import List, Any, Iterator, Type, Callable, Optional

from snowflake.snowpark._internal.data_source.datasource_typing import Connection
from snowflake.snowpark._internal.data_source.drivers.base_driver import BaseDriver
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
        dbms_type: Enum,
        fetch_size: Optional[int] = 0,
        query_timeout: Optional[int] = 0,
        session_init_statement: Optional[List[str]] = None,
        fetch_merge_count: Optional[int] = 1,
    ) -> None:
        self.driver = driver_class(create_connection, dbms_type)
        self.schema = schema
        self.fetch_size = fetch_size
        self.query_timeout = query_timeout
        self.session_init_statement = session_init_statement
        self.fetch_merge_count = fetch_merge_count

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
                cap_size = self.fetch_merge_count * self.fetch_size
                cursor = cursor.execute(partition)
                batch = []
                while True:
                    rows = cursor.fetchmany(self.fetch_size)
                    if not rows:
                        if batch:
                            yield batch
                        break
                    batch.extend(rows)
                    if len(batch) >= cap_size:
                        yield batch
                        batch = []
            else:
                raise ValueError("fetch size cannot be smaller than 0")
        finally:
            cursor.close()
            conn.close()

    def data_source_data_to_pandas_df(self, data: List[Any]) -> "pd.DataFrame":
        return self.driver.data_source_data_to_pandas_df(data, self.schema)
