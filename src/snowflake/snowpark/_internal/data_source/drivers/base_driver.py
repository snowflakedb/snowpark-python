#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from abc import ABC, abstractmethod
from typing import List, Callable, Any, Optional
from snowflake.snowpark._internal.data_source.datasource_typing import (
    Connection,
)
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.types import StructType, StructField, StringType
import snowflake.snowpark


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

    def udtf_ingestion(
        self,
        session: "snowflake.snowpark.Session",
        schema: StructType,
        partition_table: str,
        external_access_integrations: str,
        fetch_size: int = 1000,
    ) -> "snowflake.snowpark.DataFrame":
        create_connection = self.create_connection
        driver_package = type(create_connection()).__module__

        class MyUDTFWithOptionalArgs:
            def process(self, query: str):
                conn = create_connection()
                cursor = conn.cursor()
                cursor.execute(query)
                while True:
                    rows = cursor.fetchmany(fetch_size)
                    if not rows:
                        break
                    yield from rows

        session.udtf.register(
            MyUDTFWithOptionalArgs,
            name="dbapi",
            output_schema=StructType(
                [
                    StructField(field.name, StringType(), field.nullable)
                    for field in schema.fields
                ]
            ),
            external_access_integrations=[external_access_integrations],
            packages=[driver_package],
        )
        call_udtf_sql = f"""
            select * from {partition_table}, table(dbapi(partition))
            """
        res = session.sql(call_udtf_sql)
        cols = [res[field.name].cast(field.datatype) for field in schema.fields]
        return res.select(cols)

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
