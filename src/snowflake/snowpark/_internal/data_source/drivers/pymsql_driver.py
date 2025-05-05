#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from enum import Enum
from typing import List, Callable, Any
import logging
from snowflake.snowpark._internal.data_source.drivers import BaseDriver
from snowflake.snowpark._internal.data_source.datasource_typing import Connection
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
    VariantType,
    TimestampTimeZone,
)


logger = logging.getLogger(__name__)

BASE_PYMYSQL_TYPE_TO_SNOW_TYPE = {
    0: DecimalType,
    1: IntegerType,
    2: IntegerType,
    3: IntegerType,
    4: FloatType,
    5: FloatType,
    8: IntegerType,
    9: IntegerType,
    13: IntegerType,
    246: DecimalType,
    253: StringType,
    254: StringType,
    252: BinaryType,
    16: StringType,
    10: DateType,
    11: TimeType,
    12: TimestampType,
    7: TimestampType,
    245: VariantType,
}


class PymysqlDriver(BaseDriver):
    def __init__(
        self, create_connection: Callable[[], "Connection"], dbms_type: Enum
    ) -> None:
        super().__init__(create_connection, dbms_type)

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
            if type_code in (0, 246):
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
            elif type_code == 7:
                data_type = snow_type(TimestampTimeZone.TZ)
            elif type_code == 12:
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
