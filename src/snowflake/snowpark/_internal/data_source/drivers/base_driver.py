#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import time
from abc import ABC, abstractmethod
from typing import List, Callable, Any, Optional
from snowflake.snowpark._internal.data_source.datasource_typing import (
    Connection,
)
from snowflake.snowpark._internal.utils import generate_random_alphanumeric
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.types import StructType, StructField, StringType
import snowflake.snowpark
import logging

PARTITION_TABLE_COLUMN_NAME = "partition"

logger = logging.getLogger(__name__)


class BaseDriver(ABC):
    def __init__(
        self, create_connection: Callable[[], "Connection"], dbms_type: str
    ) -> None:
        self.create_connection = create_connection
        self.dbms_type = dbms_type

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

    def udtf_ingestion(
        self,
        create_connection: Callable[[], "Connection"],
        session: "snowflake.snowpark.Session",
        schema: StructType,
        partition_table: str,
        external_access_integrations: str,
        fetch_size: int = 1000,
    ) -> "snowflake.snowpark.DataFrame":
        from snowflake.snowpark._internal.data_source.utils import UDTF_PACKAGE_MAP

        udtf_name = f"data_source_udtf_{generate_random_alphanumeric(5)}"
        session.udtf.register(
            self.udtf_class_builder(
                create_connection=create_connection, fetch_size=fetch_size
            ),
            name=udtf_name,
            output_schema=StructType(
                [
                    StructField(field.name, StringType(), field.nullable)
                    for field in schema.fields
                ]
            ),
            external_access_integrations=[external_access_integrations],
            packages=UDTF_PACKAGE_MAP.get(self.dbms_type),
        )
        call_udtf_sql = f"""
            select * from {partition_table}, table({udtf_name}({PARTITION_TABLE_COLUMN_NAME}))
            """
        start = time.time()
        res = session.sql(call_udtf_sql)
        logger.info(
            f"ingest data to snowflake udtf takes: {time.time() - start} seconds"
        )
        cols = [res[field.name].cast(field.datatype) for field in schema.fields]
        return res.select(cols)

    def udtf_class_builder(
        self, create_connection: Callable[[], "Connection"], fetch_size: int = 1000
    ) -> type:
        class UDTFIngestion:
            def process(self, query: str):
                conn = create_connection()
                cursor = conn.cursor()
                cursor.execute(query)
                while True:
                    rows = cursor.fetchmany(fetch_size)
                    if not rows:
                        break
                    yield from rows

        return UDTFIngestion

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
