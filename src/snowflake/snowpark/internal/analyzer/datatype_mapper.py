#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import binascii
import math
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from snowflake.snowpark.types.sf_types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    MapType,
    NumericType,
    StringType,
    TimestampType,
    TimeType,
    VariantType,
)
from snowflake.snowpark.types.sp_data_types import (
    ArrayType as SPArrayType,
    BinaryType as SPBinaryType,
    BooleanType as SPBooleanType,
    ByteType as SPByteType,
    DataType as SPDataType,
    DateType as SPDateType,
    DecimalType as SPDecimalType,
    DoubleType as SPDoubleType,
    FloatType as SPFloatType,
    GeographyType as SPGeographyType,
    IntegerType as SPIntegerType,
    LongType as SPLongType,
    MapType as SPMapType,
    NullType as SPNullType,
    ShortType as SPShortType,
    StringType as SPStringType,
    StructType as SPStructType,
    TimestampType as SPTimestampType,
)
from snowflake.snowpark.types.types_package import convert_to_sf_type


class DataTypeMapper:
    MILLIS_PER_DAY = 24 * 3600 * 1000
    MICROS_PER_MILLIS = 1000

    @staticmethod
    # TODO
    def to_sql(value, spark_data_type: SPDataType):
        """Convert a value with SparkSQL DataType to a snowflake compatible sql"""

        # Handle null values
        if type(spark_data_type) in [
            SPNullType,
            SPArrayType,
            SPMapType,
            SPStructType,
            SPGeographyType,
        ]:
            if value is None:
                return "NULL"
        if type(spark_data_type) is SPIntegerType:
            if value is None:
                return "NULL :: int"
        if type(spark_data_type) is SPShortType:
            if value is None:
                return "NULL :: smallint"
        if type(spark_data_type) is SPByteType:
            if value is None:
                return "NULL :: tinyint"
        if type(spark_data_type) is SPLongType:
            if value is None:
                return "NULL :: bigint"
        if type(spark_data_type) is SPFloatType:
            if value is None:
                return "NULL :: float"
        if type(spark_data_type) is SPStringType:
            if value is None:
                return "NULL :: string"
        if type(spark_data_type) is SPDoubleType:
            if value is None:
                return "NULL :: double"
        if type(spark_data_type) is SPBooleanType:
            if value is None:
                return "NULL :: boolean"
        if value is None:
            return "NULL"

        # Not nulls
        if type(spark_data_type) is SPStringType:
            # TODO revisit, original check: if UTF8string or String
            if type(value) is str:
                return (
                    "'"
                    + str(value)
                    .replace("\\", "\\\\")
                    .replace("'", "''")
                    .replace("\n", "\\n")
                    + "'"
                )
        if type(spark_data_type) is SPByteType:
            return str(value) + f":: tinyint"
        if type(spark_data_type) is SPShortType:
            return str(value) + f":: smallint"
        if type(spark_data_type) is SPIntegerType:
            return str(value) + f":: int"
        if type(spark_data_type) is SPLongType:
            return str(value) + f":: bigint"
        if type(spark_data_type) is SPBooleanType:
            return str(value) + f":: boolean"

        # TODO revisit after SNOW-165195 : Add support for all valid SparkSQL and SnowflakeSQL types
        if type(value) is float and type(spark_data_type) is SPFloatType:
            if math.isnan(float(value)):
                cast_value = "'Nan'"
            elif math.isinf(value) and value > 0:
                cast_value = "'Infinity'"
            elif math.isinf(value) and value < 0:
                cast_value = "'-Infinity'"
            else:
                cast_value = f"'{value}'"
            return f"{cast_value} :: {spark_data_type.sql}"

        if type(spark_data_type) is SPDoubleType:
            if math.isnan(float(value)):
                return "'Nan' :: DOUBLE"
            elif math.isinf(value) and value > 0:
                return "'Infinity' :: DOUBLE"
            elif math.isinf(value) and value < 0:
                return "'-Infinity' :: DOUBLE"
            return f"'{value}' :: DOUBLE"

        if type(value) is Decimal and type(spark_data_type) is SPDecimalType:
            # TODO fix circular dependency
            from .analyzer_package import AnalyzerPackage

            package = AnalyzerPackage()
            return f"{value} :: {package.number(spark_data_type.precision, spark_data_type.scale)}"

        if type(spark_data_type) is SPDateType and type(value) is int:
            # add value as number of days to 1970-01-01
            target_date = date(1970, 1, 1) + timedelta(days=value)
            return f"DATE '{target_date.isoformat()}'"

        if type(spark_data_type) is SPTimestampType and type(value) is int:
            # add value as microseconds to 1970-01-01 00:00:00.00.
            target_time = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
                microseconds=value
            )
            trimmed_ms = target_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            return f"TIMESTAMP '{trimmed_ms}'"

        if type(value) in [list, bytearray] and type(spark_data_type) is SPBinaryType:
            return "'{}' :: binary".format(binascii.hexlify(value).decode())

        raise Exception(
            "Unsupported datatype {}, value {} by to_sql()".format(
                spark_data_type, value
            )
        )

    @staticmethod
    def schema_expression(data_type, is_nullable):
        if is_nullable:
            # if isinstance(datatype) == GeographyType:
            #     return "TRY_TO_GEOGRAPHY(NULL)"
            return "NULL :: " + convert_to_sf_type(data_type)

        if isinstance(data_type, NumericType):
            return "0 :: " + convert_to_sf_type(data_type)
        if isinstance(data_type, StringType):
            return "'a' :: STRING"
        if isinstance(data_type, BinaryType):
            return "to_binary(hex_encode(1))"
        if isinstance(data_type, DateType):
            return "date('2020-9-16')"
        if isinstance(data_type, BooleanType):
            return "true"
        if isinstance(data_type, TimeType):
            return "to_time('04:15:29.999')"
        if isinstance(data_type, TimestampType):
            return "to_timestamp_ntz('2020-09-16 06:30:00')"
        if isinstance(data_type, ArrayType):
            return "to_array(0)"
        if isinstance(data_type, MapType):
            return "to_object(parse_json('0'))"
        if isinstance(data_type, VariantType):
            return "to_variant(0)"
        # if isinstance(datatype, GeographyType):
        #    return "true"
        raise Exception(f"Unsupported data type: {data_type.type_name}")

    @staticmethod
    def to_sql_without_cast(value, data_type):
        if value is None:
            return "NULL"
        if isinstance(data_type, SPStringType):
            return f"""{value}"""
        return str(value)
