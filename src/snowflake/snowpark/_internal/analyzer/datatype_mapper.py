#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import binascii
import math
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any

import snowflake.snowpark._internal.analyzer.analyzer_package as analyzer_package
from snowflake.snowpark._internal.sp_types.types_package import convert_to_sf_type
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructType,
    TimestampType,
    TimeType,
    VariantType,
    _GeographyType,
    _NumericType,
)


class DataTypeMapper:
    MILLIS_PER_DAY = 24 * 3600 * 1000
    MICROS_PER_MILLIS = 1000

    @staticmethod
    def to_sql(value: Any, datatype: DataType) -> str:
        """Convert a value with SparkSQL DataType to a snowflake compatible sql"""

        # Handle null values
        if isinstance(
            datatype,
            (NullType, ArrayType, MapType, StructType, _GeographyType),
        ):
            if value is None:
                return "NULL"
        if isinstance(datatype, BinaryType):
            if value is None:
                return "NULL :: binary"
        if isinstance(datatype, IntegerType):
            if value is None:
                return "NULL :: int"
        if isinstance(datatype, ShortType):
            if value is None:
                return "NULL :: smallint"
        if isinstance(datatype, ByteType):
            if value is None:
                return "NULL :: tinyint"
        if isinstance(datatype, LongType):
            if value is None:
                return "NULL :: bigint"
        if isinstance(datatype, FloatType):
            if value is None:
                return "NULL :: float"
        if isinstance(datatype, StringType):
            if value is None:
                return "NULL :: string"
        if isinstance(datatype, DoubleType):
            if value is None:
                return "NULL :: double"
        if isinstance(datatype, BooleanType):
            if value is None:
                return "NULL :: boolean"
        if value is None:
            return "NULL"

        # Not nulls
        if isinstance(datatype, StringType):
            if isinstance(value, str):
                return (
                    "'"
                    + str(value)
                    .replace("\\", "\\\\")
                    .replace("'", "''")
                    .replace("\n", "\\n")
                    + "'"
                )
        if isinstance(datatype, ByteType):
            return str(value) + f" :: tinyint"
        if isinstance(datatype, ShortType):
            return str(value) + f" :: smallint"
        if isinstance(datatype, IntegerType):
            return str(value) + f" :: int"
        if isinstance(datatype, LongType):
            return str(value) + f" :: bigint"
        if isinstance(datatype, BooleanType):
            return str(value) + f" :: boolean"

        if isinstance(value, float) and isinstance(datatype, FloatType):
            if math.isnan(value):
                cast_value = "'Nan'"
            elif math.isinf(value) and value > 0:
                cast_value = "'Infinity'"
            elif math.isinf(value) and value < 0:
                cast_value = "'-Infinity'"
            else:
                cast_value = f"'{value}'"
            return f"{cast_value} :: FLOAT"

        if isinstance(datatype, DoubleType):
            if math.isnan(float(value)):
                return "'Nan' :: DOUBLE"
            elif math.isinf(value) and value > 0:
                return "'Infinity' :: DOUBLE"
            elif math.isinf(value) and value < 0:
                return "'-Infinity' :: DOUBLE"
            return f"'{value}' :: DOUBLE"

        if isinstance(value, Decimal) and isinstance(datatype, DecimalType):
            return f"{value} :: {analyzer_package.AnalyzerPackage.number(datatype.precision, datatype.scale)}"

        if isinstance(datatype, DateType):
            if isinstance(value, int):
                # add value as number of days to 1970-01-01
                target_date = date(1970, 1, 1) + timedelta(days=value)
                return f"DATE '{target_date.isoformat()}'"
            elif isinstance(value, date):
                return f"DATE '{value.isoformat()}'"

        if isinstance(datatype, TimestampType):
            if isinstance(value, int):
                # add value as microseconds to 1970-01-01 00:00:00.00.
                target_time = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
                    microseconds=value
                )
                trimmed_ms = target_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                return f"TIMESTAMP '{trimmed_ms}'"
            elif isinstance(value, datetime):
                trimmed_ms = value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                return f"TIMESTAMP '{trimmed_ms}'"

        if isinstance(datatype, TimeType):
            if isinstance(value, time):
                trimmed_ms = value.strftime("%H:%M:%S.%f")[:-3]
                return f"TIME('{trimmed_ms}')"

        if isinstance(value, (list, bytes, bytearray)) and isinstance(
            datatype, BinaryType
        ):
            return "'{}' :: binary".format(binascii.hexlify(value).decode())

        raise TypeError(
            "Unsupported datatype {}, value {} by to_sql()".format(datatype, value)
        )

    @staticmethod
    def schema_expression(data_type: DataType, is_nullable: bool) -> str:
        if is_nullable:
            # Put _GeographyType here to avoid forgetting it in the future when we support Geography.
            if isinstance(data_type, _GeographyType):
                return "TRY_TO_GEOGRAPHY(NULL)"
            if isinstance(data_type, ArrayType):
                return "PARSE_JSON('NULL') :: ARRAY"
            if isinstance(data_type, MapType):
                return "PARSE_JSON('NULL') :: OBJECT"
            if isinstance(data_type, VariantType):
                return "PARSE_JSON('NULL') :: VARIANT"
            return "NULL :: " + convert_to_sf_type(data_type)

        if isinstance(data_type, _NumericType):
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
        if isinstance(data_type, _GeographyType):
            return "to_geography('POINT(-122.35 37.55)')"
        raise Exception(f"Unsupported data type: {data_type.type_name}")

    @staticmethod
    def to_sql_without_cast(value: Any, data_type: DataType) -> str:
        if value is None:
            return "NULL"
        if isinstance(data_type, StringType):
            return f"""{value}"""
        return str(value)
