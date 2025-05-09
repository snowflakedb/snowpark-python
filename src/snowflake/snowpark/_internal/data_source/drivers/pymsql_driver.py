#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from enum import Enum
from decimal import Decimal
from datetime import date, datetime, timedelta
from typing import List, Callable, Any, Type
import logging
from snowflake.snowpark._internal.data_source.drivers import BaseDriver
from snowflake.snowpark._internal.data_source.datasource_typing import (
    Connection,
    Cursor,
)
from snowflake.snowpark.types import (
    StructType,
    StringType,
    DecimalType,
    DateType,
    TimestampType,
    FloatType,
    BinaryType,
    StructField,
    TimeType,
    IntegerType,
    TimestampTimeZone,
)


logger = logging.getLogger(__name__)

BASE_PYMYSQL_TYPE_TO_SNOW_TYPE = {
    (0, Decimal): DecimalType,
    (246, Decimal): DecimalType,
    (1, int): IntegerType,
    (2, int): IntegerType,
    (3, int): IntegerType,
    (4, float): FloatType,
    (5, float): FloatType,
    (8, int): IntegerType,
    (9, int): IntegerType,
    (13, int): IntegerType,
    (254, str): StringType,
    (253, str): StringType,
    (252, str): StringType,
    (16, bytes): StringType,
    (254, bytes): BinaryType,
    (253, bytes): BinaryType,
    (252, bytes): BinaryType,
    (10, date): DateType,
    (12, datetime): TimestampType,
    (7, datetime): TimestampType,
    (11, timedelta): TimeType,
    (245, str): StringType,
}


class PymysqlDriver(BaseDriver):
    def __init__(
        self, create_connection: Callable[[], "Connection"], dbms_type: Enum
    ) -> None:
        super().__init__(create_connection, dbms_type)

    def infer_schema_from_description(
        self, table_or_query: str, cursor: "Cursor"
    ) -> StructType:
        if table_or_query.lower().startswith("select"):
            cursor.execute(f"select A.* from ({table_or_query}) A limit 1")
        else:
            cursor.execute(f"select * from `{table_or_query}` limit 1")
        data = cursor.fetchall()
        raw_schema = cursor.description
        raw_types = self.infer_type_from_data(data, len(raw_schema))

        processed_raw_schema = []
        for type, col in zip(raw_types, raw_schema):
            new_col = list(col)
            new_col[1] = (new_col[1], type)
            processed_raw_schema.append(new_col)

        self.raw_schema = processed_raw_schema
        return self.to_snow_type(processed_raw_schema)

    def to_snow_type(self, schema: List[Any]) -> StructType:
        """
        pymysql mysql type to type code mapping:
        https://github.com/PyMySQL/PyMySQL/blob/main/pymysql/constants/FIELD_TYPE.py

        mysql type to snowflake type mapping:
        https://other-docs.snowflake.com/en/connectors/mysql6/view-data#mysql-to-snowflake-data-type-mapping
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
            snow_type = BASE_PYMYSQL_TYPE_TO_SNOW_TYPE.get(type_code, None)
            if snow_type is None:
                raise NotImplementedError(f"mysql type not supported: {type_code}")
            if type_code in ((0, Decimal), (246, Decimal)):
                precision -= 2
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
            elif type_code == (7, datetime):
                data_type = snow_type(TimestampTimeZone.TZ)
            elif type_code == (12, datetime):
                data_type = snow_type(TimestampTimeZone.NTZ)
            else:
                data_type = snow_type()
            fields.append(StructField(name, data_type, null_ok))
        return StructType(fields)

    def udtf_class_builder(self, fetch_size: int = 1000) -> type:
        # create_connection = self.create_connection

        class UDTFIngestion:
            def process(self, query: str):
                pass

        return UDTFIngestion

    def prepare_connection(
        self,
        conn: "Connection",
        query_timeout: int = 0,
    ) -> "Connection":
        conn.read_timeout = query_timeout if query_timeout != 0 else None
        return conn

    def infer_type_from_data(
        self, data: List[tuple], number_of_columns: int
    ) -> List[Type]:

        raw_data_types_set = [set() for _ in range(number_of_columns)]
        for row in data:
            for i, col in enumerate(row):
                raw_data_types_set[i].add(type(col))
        types = [
            type_set.pop()
            if len(type_set) == 1 and not isinstance(next(iter(type_set)), type(None))
            else str
            for type_set in raw_data_types_set
        ]
        return types
