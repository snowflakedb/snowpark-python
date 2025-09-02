#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from typing import List, Any
import logging
from snowflake.snowpark._internal.data_source.drivers import BaseDriver
from snowflake.snowpark._internal.data_source.datasource_typing import Connection
from snowflake.snowpark.types import (
    StructType,
    StringType,
    DecimalType,
    BooleanType,
    DateType,
    DoubleType,
    TimestampType,
    VariantType,
    FloatType,
    BinaryType,
    VectorType,
    TimestampTimeZone,
    StructField,
)

logger = logging.getLogger(__name__)


class OracledbDriver(BaseDriver):
    def to_snow_type(self, schema: List[Any]) -> StructType:
        """
        This is used to convert oracledb raw schema to snowpark structtype.
        Each tuple in the list represent a column and values are as follows:
        column name: str
        data type: str
        precision: int
        scale: int
        nullable: str
        """
        import oracledb

        convert_map_to_use = {
            oracledb.DB_TYPE_VARCHAR: StringType,
            oracledb.DB_TYPE_NVARCHAR: StringType,
            oracledb.DB_TYPE_NUMBER: DecimalType,
            oracledb.DB_TYPE_DATE: DateType,
            oracledb.DB_TYPE_BOOLEAN: BooleanType,
            oracledb.DB_TYPE_BINARY_DOUBLE: DoubleType,
            oracledb.DB_TYPE_BINARY_FLOAT: FloatType,
            oracledb.DB_TYPE_TIMESTAMP: TimestampType,
            oracledb.DB_TYPE_TIMESTAMP_TZ: TimestampType,
            oracledb.DB_TYPE_TIMESTAMP_LTZ: TimestampType,
            oracledb.DB_TYPE_INTERVAL_YM: VariantType,
            oracledb.DB_TYPE_INTERVAL_DS: VariantType,
            oracledb.DB_TYPE_RAW: BinaryType,
            oracledb.DB_TYPE_LONG: StringType,
            oracledb.DB_TYPE_LONG_RAW: BinaryType,
            oracledb.DB_TYPE_ROWID: StringType,
            oracledb.DB_TYPE_UROWID: StringType,
            oracledb.DB_TYPE_CHAR: StringType,
            oracledb.DB_TYPE_BLOB: BinaryType,
            oracledb.DB_TYPE_CLOB: StringType,
            oracledb.DB_TYPE_NCHAR: StringType,
            oracledb.DB_TYPE_NCLOB: StringType,
            oracledb.DB_TYPE_LONG_NVARCHAR: StringType,
            oracledb.DB_TYPE_BFILE: BinaryType,
            oracledb.DB_TYPE_JSON: VariantType,
            oracledb.DB_TYPE_BINARY_INTEGER: DecimalType,
            oracledb.DB_TYPE_XMLTYPE: StringType,
            oracledb.DB_TYPE_OBJECT: VariantType,
            oracledb.DB_TYPE_VECTOR: VectorType,
            oracledb.DB_TYPE_CURSOR: None,  # NOT SUPPORTED
        }

        fields = []
        for column in schema:
            name = column.name
            type_code = column.type_code
            precision = column.precision
            scale = column.scale
            null_ok = column.null_ok
            snow_type = convert_map_to_use.get(type_code, None)
            if snow_type is None:
                # TODO: SNOW-1912068 support types that we don't have now
                raise NotImplementedError(f"oracledb type not supported: {type_code}")
            if type_code == oracledb.DB_TYPE_TIMESTAMP_TZ:
                data_type = snow_type(TimestampTimeZone.TZ)
            elif type_code == oracledb.DB_TYPE_TIMESTAMP_LTZ:
                data_type = snow_type(TimestampTimeZone.LTZ)
            elif snow_type == DecimalType:
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

    @staticmethod
    def prepare_connection(
        conn: "Connection",
        query_timeout: int = 0,
    ) -> "Connection":
        conn.call_timeout = query_timeout * 1000
        if conn.outputtypehandler is None:
            conn.outputtypehandler = output_type_handler
        return conn

    def udtf_class_builder(
        self, fetch_size: int = 1000, schema: StructType = None
    ) -> type:
        create_connection = self.create_connection

        def oracledb_output_type_handler(cursor, metadata):
            from oracledb import (
                DB_TYPE_CLOB,
                DB_TYPE_NCLOB,
                DB_TYPE_LONG,
                DB_TYPE_BLOB,
                DB_TYPE_RAW,
                DB_TYPE_LONG_RAW,
            )

            def convert_to_hex(value):
                return value.hex() if value is not None else None

            if metadata.type_code in (DB_TYPE_CLOB, DB_TYPE_NCLOB):
                return cursor.var(DB_TYPE_LONG, arraysize=cursor.arraysize)
            elif metadata.type_code in (DB_TYPE_BLOB, DB_TYPE_RAW, DB_TYPE_LONG_RAW):
                return cursor.var(
                    DB_TYPE_RAW, arraysize=cursor.arraysize, outconverter=convert_to_hex
                )

        class UDTFIngestion:
            def process(self, query: str):
                conn = create_connection()
                if conn.outputtypehandler is None:
                    conn.outputtypehandler = oracledb_output_type_handler
                cursor = conn.cursor()
                cursor.execute(query)
                while True:
                    rows = cursor.fetchmany(fetch_size)
                    if not rows:
                        break
                    yield from rows

        return UDTFIngestion


def output_type_handler(cursor, metadata):
    import oracledb

    if metadata.type_code in (oracledb.DB_TYPE_CLOB, oracledb.DB_TYPE_NCLOB):
        return cursor.var(oracledb.DB_TYPE_LONG, arraysize=cursor.arraysize)
    elif metadata.type_code == oracledb.DB_TYPE_BLOB:
        return cursor.var(oracledb.DB_TYPE_RAW, arraysize=cursor.arraysize)
