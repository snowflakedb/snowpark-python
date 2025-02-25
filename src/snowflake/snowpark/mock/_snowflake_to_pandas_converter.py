#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
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
from typing import List, Optional, Union

from snowflake.snowpark.mock.exceptions import SnowparkLocalTestingException
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


def _integer_converter(
    value: str,
    datatype: DataType,
    field_optionally_enclosed_by: str = None,
    null_if: Optional[List[str]] = None,
) -> Optional[int]:
    if value is None or value == "" or null_if is not None and value in null_if:
        return None
    try:
        return int(value)
    except Exception as exc:
        SnowparkLocalTestingException.raise_from_error(
            exc, error_message=f"Numeric value '{value}' is not recognized."
        )


def _fraction_converter(
    value: str,
    datatype: DataType,
    field_optionally_enclosed_by: str = None,
    null_if: Optional[List[str]] = None,
) -> Optional[float]:
    if value is None or value == "" or null_if is not None and value in null_if:
        return None
    try:
        return float(value)
    except Exception as exc:
        SnowparkLocalTestingException.raise_from_error(
            exc, error_message=f"Numeric value '{value}' is not recognized."
        )


def _decimal_converter(
    value: str,
    datatype: DecimalType,
    field_optionally_enclosed_by: str = None,
    null_if: Optional[List[str]] = None,
) -> Optional[Union[int, Decimal]]:
    if value is None or value == "" or null_if is not None and value in null_if:
        return None
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
            raise SnowparkLocalTestingException(
                f"Numeric value '{value}' is out of range"
            )
        if scale == 0:
            return integer_part
        remaining_decimal_len = min(precision - len(str(integer_part)), scale)
        return Decimal(str(round(float(value), remaining_decimal_len)))
    except Exception as exc:
        SnowparkLocalTestingException.raise_from_error(
            exc, error_message=f"Numeric value '{value}' is not recognized."
        )


def _bool_converter(
    value: str,
    datatype: DataType,
    field_optionally_enclosed_by: str = None,
    null_if: Optional[List[str]] = None,
) -> Optional[bool]:
    if value is None or value == "" or null_if is not None and value in null_if:
        return None
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    try:
        float_value = float(value)
        return bool(float_value != 0)
    except Exception as exc:
        SnowparkLocalTestingException.raise_from_error(
            exc, error_message=f"Boolean value '{value}' is not recognized."
        )


def _string_converter(
    value: str,
    datatype: DataType,
    field_optionally_enclosed_by: str = None,
    null_if: Optional[List[str]] = None,
) -> Optional[str]:
    if null_if is not None and value in null_if:
        return None
    if value is None or value == "":
        return value
    return value


def _date_converter(
    value: str,
    datatype: DataType,
    format: str,
    field_optionally_enclosed_by: str = None,
    null_if: Optional[List[str]] = None,
) -> Optional[datetime.date]:
    if value is None or value == "" or null_if is not None and value in null_if:
        return None
    try:
        return datetime.datetime.strptime(value, format).date()
    except Exception as exc:
        SnowparkLocalTestingException.raise_from_error(
            exc, error_message=f"DATE value '{value}' is not recognized."
        )


def _timestamp_converter(
    value: str,
    datatype: DataType,
    format: str,
    field_optionally_enclosed_by: str = None,
    null_if: Optional[List[str]] = None,
) -> Optional[datetime.datetime]:
    if value is None or value == "" or null_if is not None and value in null_if:
        return None
    try:
        return datetime.datetime.strptime(value, format)
    except Exception as exc:
        SnowparkLocalTestingException.raise_from_error(
            exc, error_message=f"TIMESTAMP value '{value}' is not recognized."
        )


def _time_converter(
    value: str,
    datatype: DataType,
    format: str,
    field_optionally_enclosed_by: str = None,
    null_if: Optional[List[str]] = None,
) -> Optional[datetime.time]:
    if value is None or value == "" or null_if is not None and value in null_if:
        return None
    try:
        return datetime.datetime.strptime(value, format).time()
    except Exception as exc:
        SnowparkLocalTestingException.raise_from_error(
            exc, error_message=f"TIMESTAMP value '{value}' is not recognized."
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
