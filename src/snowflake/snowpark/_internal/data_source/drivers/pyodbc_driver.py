#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import decimal
from enum import Enum
from typing import List, Callable, Any
import logging
from snowflake.snowpark._internal.data_source.drivers import BaseDriver
from snowflake.snowpark._internal.data_source.datasource_typing import Connection
from snowflake.snowpark.types import (
    StructType,
    StringType,
    DecimalType,
    BooleanType,
    DateType,
    TimestampType,
    FloatType,
    BinaryType,
    StructField,
    TimeType,
)


logger = logging.getLogger(__name__)

BASE_PYODBC_TYPE_TO_SNOW_TYPE = {
    int: DecimalType,
    float: FloatType,
    decimal.Decimal: DecimalType,
    datetime.datetime: TimestampType,
    bool: BooleanType,
    str: StringType,
    bytes: BinaryType,
    datetime.date: DateType,
    datetime.time: TimeType,
    bytearray: BinaryType,
}


class PyodbcDriver(BaseDriver):
    def __init__(
        self, create_connection: Callable[[], "Connection"], dbms_type: Enum
    ) -> None:
        super().__init__(create_connection, dbms_type)

    def to_snow_type(self, schema: List[Any]) -> StructType:
        """
        SQLServer to Python datatype mapping
        https://peps.python.org/pep-0249/#description returns the following spec
        name, type_code, display_size, internal_size, precision, scale, null_ok

        SQLServer supported types in Python (outdated):
        https://learn.microsoft.com/en-us/sql/machine-learning/python/python-libraries-and-data-types?view=sql-server-ver16
        """
        fields = []
        for column in schema:
            (
                name,
                type_code,
                display_size,
                internal_size,
                precision,
                scale,
                null_ok,
            ) = column
            snow_type = BASE_PYODBC_TYPE_TO_SNOW_TYPE.get(type_code, None)
            if snow_type is None:
                raise NotImplementedError(f"sql server type not supported: {type_code}")
            if type_code in (int, decimal.Decimal):
                if not self.validate_numeric_precision_scale(precision, scale):
                    logger.debug(
                        f"Snowpark does not support column"
                        f" {name} of type {type_code} with precision {precision} and scale {scale}. "
                        "The default Numeric precision and scale will be used."
                    )
                    precision, scale = None, None
                data_type = snow_type(
                    precision if precision is not None else 38,
                    scale if scale is not None else 0,
                )
            else:
                data_type = snow_type()
            fields.append(StructField(name, data_type, null_ok))
        return StructType(fields)

    def udtf_class_builder(self, fetch_size: int = 1000) -> type:
        create_connection = self.create_connection

        def binary_converter(value):
            return value.hex() if value is not None else None

        class UDTFIngestion:
            def process(self, query: str):
                import pyodbc

                conn = create_connection()
                if (
                    conn.get_output_converter(pyodbc.SQL_BINARY) is None
                    and conn.get_output_converter(pyodbc.SQL_VARBINARY) is None
                    and conn.get_output_converter(pyodbc.SQL_LONGVARBINARY) is None
                ):
                    conn.add_output_converter(pyodbc.SQL_BINARY, binary_converter)
                    conn.add_output_converter(pyodbc.SQL_VARBINARY, binary_converter)
                    conn.add_output_converter(
                        pyodbc.SQL_LONGVARBINARY, binary_converter
                    )
                cursor = conn.cursor()
                cursor.execute(query)
                while True:
                    rows = cursor.fetchmany(fetch_size)
                    if not rows:
                        break
                    yield from rows

        return UDTFIngestion

    def prepare_connection(
        self,
        conn: "Connection",
        query_timeout: int = 0,
    ) -> "Connection":
        conn.timeout = query_timeout
        return conn
