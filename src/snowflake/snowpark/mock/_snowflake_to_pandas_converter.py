#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

"""
The converter module is used to convert string into data in pandas dataframe complying with snowflake spec.
for example, when we call pandas.read_csv, we use the converter functions to validate, convert the data into python
objects according to snowflake datatype following the spec. Otherwise, pandas.read_csv takes data as raw string in
most cases.

For full data type spec, please refer to https://docs.snowflake.com/en/sql-reference/data-types.
"""

import datetime
from decimal import Decimal
from typing import Optional, Union

from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.types import (
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampType,
    TimeType,
)

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT = "%H:%M:%S"


def _integer_converter(
    value: str, datatype: DataType, field_optionally_enclosed_by: str = None
) -> Optional[int]:
    if value is None or value == "":
        return None
    if (
        field_optionally_enclosed_by
        and len(value) >= 2
        and value[0] == field_optionally_enclosed_by
        and value[-1] == field_optionally_enclosed_by
    ):
        value = value[1:-1]
    try:
        return int(value)
    except ValueError:
        raise SnowparkSQLException(
            f"[Local Testing] Numeric value '{value}' is not recognized."
        )


def _fraction_converter(
    value: str, datatype: DataType, field_optionally_enclosed_by: str = None
) -> Optional[float]:
    if value is None or value == "":
        return None
    if (
        field_optionally_enclosed_by
        and len(field_optionally_enclosed_by) >= 2
        and value[0] == field_optionally_enclosed_by
        and value[-1] == field_optionally_enclosed_by
    ):
        value = value[1:-1]
    try:
        return float(value)
    except ValueError:
        raise SnowparkSQLException(
            f"[Local Testing] Numeric value '{value}' is not recognized."
        )


def _decimal_converter(
    value: str, datatype: DecimalType, field_optionally_enclosed_by: str = None
) -> Optional[Union[int, Decimal]]:
    if value is None or value == "":
        return None
    if (
        field_optionally_enclosed_by
        and len(field_optionally_enclosed_by) >= 2
        and value[0] == field_optionally_enclosed_by
        and value[-1] == field_optionally_enclosed_by
    ):
        value = value[1:-1]
    try:
        precision = datatype.precision
        scale = datatype.scale
        integer_part = round(float(value))
        integer_part_str = str(integer_part)
        len_integer_part = (
            len(integer_part_str) - 1
            if integer_part_str[0] == "-"
            else len(integer_part_str)
        )
        if len_integer_part > precision:
            raise SnowparkSQLException(f"Numeric value '{value}' is out of range")
        if scale == 0:
            return integer_part
        remaining_decimal_len = min(precision - len(str(integer_part)), scale)
        return Decimal(str(round(float(value), remaining_decimal_len)))
    except ValueError:
        raise SnowparkSQLException(
            f"[Local Testing] Numeric value '{value}' is not recognized."
        )


def _bool_converter(
    value: str, datatype: DataType, field_optionally_enclosed_by: str = None
) -> Optional[bool]:
    if value is None or value == "":
        return None
    if (
        field_optionally_enclosed_by
        and len(field_optionally_enclosed_by) >= 2
        and value[0] == field_optionally_enclosed_by
        and value[-1] == field_optionally_enclosed_by
    ):
        value = value[1:-1]
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    try:
        float_value = float(value)
        return bool(float_value != 0)
    except TypeError:
        raise SnowparkSQLException(
            f"[Local Testing] Boolean value '{value}' is not recognized."
        )


def _string_converter(
    value: str, datatype: DataType, field_optionally_enclosed_by: str = None
) -> Optional[str]:
    if value is None or value == "":
        return value
    if (
        field_optionally_enclosed_by
        and len(field_optionally_enclosed_by) >= 2
        and value[0] == field_optionally_enclosed_by
        and value[-1] == field_optionally_enclosed_by
    ):
        value = value[1:-1]
    return value


def _date_converter(
    value: str, datatype: DataType, field_optionally_enclosed_by: str = None
) -> Optional[datetime.date]:
    if value is None or value == "":
        return None
    if (
        field_optionally_enclosed_by
        and len(field_optionally_enclosed_by) >= 2
        and value[0] == field_optionally_enclosed_by
        and value[-1] == field_optionally_enclosed_by
    ):
        value = value[1:-1]
    try:
        return datetime.datetime.strptime(value, DATE_FORMAT).date()
    except Exception as e:
        raise SnowparkSQLException(
            f"[Local Testing] DATE value '{value}' is not recognized due to error {e!r}."
        )


def _timestamp_converter(
    value: str, datatype: DataType, field_optionally_enclosed_by: str = None
) -> Optional[datetime.datetime]:
    if value is None or value == "":
        return None
    if (
        field_optionally_enclosed_by
        and len(field_optionally_enclosed_by) >= 2
        and value[0] == field_optionally_enclosed_by
        and value[-1] == field_optionally_enclosed_by
    ):
        value = value[1:-1]
    try:
        return datetime.datetime.strptime(value, TIMESTAMP_FORMAT)
    except Exception as e:
        raise SnowparkSQLException(
            f"[Local Testing] TIMESTAMP value '{value}' is not recognized due to error {e!r}."
        )


def _time_converter(
    value: str, datatype: DataType, field_optionally_enclosed_by: str = None
) -> Optional[datetime.time]:
    if value is None or value == "":
        return None
    if (
        field_optionally_enclosed_by
        and len(field_optionally_enclosed_by) >= 2
        and value[0] == field_optionally_enclosed_by
        and value[-1] == field_optionally_enclosed_by
    ):
        value = value[1:-1]
    try:
        return datetime.datetime.strptime(value, TIME_FORMAT).time()
    except Exception as e:
        raise SnowparkSQLException(
            f"[Local Testing] TIMESTAMP value '{value}' is not recognized due to error {e!r}."
        )


CONVERT_MAP = {
    IntegerType: _integer_converter,
    LongType: _integer_converter,
    ByteType: _integer_converter,
    ShortType: _integer_converter,
    DoubleType: _fraction_converter,
    FloatType: _fraction_converter,
    DecimalType: _decimal_converter,
    BooleanType: _bool_converter,
    DateType: _date_converter,
    TimeType: _time_converter,
    TimestampType: _timestamp_converter,
    StringType: _string_converter,
}
