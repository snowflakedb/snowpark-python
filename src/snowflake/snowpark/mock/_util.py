#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import math
from functools import cmp_to_key, partial
from typing import Any, Tuple

from snowflake.connector.options import pandas as pd
from snowflake.snowpark.mock._snowflake_data_type import ColumnEmulator
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
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
    TimestampType,
    TimeType,
    VariantType,
    _NumericType,
)

# placeholder map helps convert wildcard to reg. In practice, we convert wildcard to a middle string first,
# and then convert middle string to regex. See the following example:
#   wildcard = "_." -> middle: "_<snowflake-regex-placeholder-for-dot>" -> regex = ".\."
# placeholder string should not contain any special characters used in regex or wildcard
regex_special_characters_map = {
    ".": "<snowflake-regex-placeholder-for-dot>",
    "\\": "<snowflake-regex-placeholder-for-backslash>",
    "^": "<snowflake-regex-placeholder-for-caret>",
    "?": "<snowflake-regex-placeholder-for-question>",
    "+": "<snowflake-regex-placeholder-for-add>",
    "|": "<snowflake-regex-placeholder-for-pipe>",
    "$": "<snowflake-regex-placeholder-for-dollar>",
    "*": "<snowflake-regex-placeholder-for-asterisk>",
    "{": "<snowflake-regex-placeholder-for-left-curly-bracket>",
    "}": "<snowflake-regex-placeholder-for-right-curly-bracket>",
    "[": "<snowflake-regex-placeholder-for-left-square-bracket>",
    "]": "<snowflake-regex-placeholder-for-right-square-bracket>",
    "(": "<snowflake-regex-placeholder-for-left-parenthesis>",
    ")": "<snowflake-regex-placeholder-for-right-parenthesis>",
}

escape_regex_special_characters_map = {
    regex_special_characters_map["."]: "\\.",
    regex_special_characters_map["\\"]: "\\\\",
    regex_special_characters_map["^"]: "\\^",
    regex_special_characters_map["?"]: "\\?",
    regex_special_characters_map["+"]: "\\+",
    regex_special_characters_map["|"]: "\\|",
    regex_special_characters_map["$"]: "\\$",
    regex_special_characters_map["*"]: "\\*",
    regex_special_characters_map["{"]: "\\{",
    regex_special_characters_map["}"]: "\\}",
    regex_special_characters_map["["]: "\\[",
    regex_special_characters_map["]"]: "\\]",
    regex_special_characters_map["("]: "\\(",
    regex_special_characters_map[")"]: "\\)",
}


def convert_wildcard_to_regex(wildcard: str):
    # convert regex in wildcard
    for k, v in regex_special_characters_map.items():
        wildcard = wildcard.replace(k, v)

    # replace wildcard special character with regex
    wildcard = wildcard.replace("_", ".")
    wildcard = wildcard.replace("%", ".*")

    # escape regx in wildcard
    for k, v in escape_regex_special_characters_map.items():
        wildcard = wildcard.replace(k, v)

    wildcard = f"^{wildcard}$"
    return wildcard


def custom_comparator(ascend: bool, null_first: bool, pandas_series: "pd.Series"):
    origin_array = pandas_series.values.tolist()
    array_with_pos = list(zip([i for i in range(len(pandas_series))], origin_array))
    comparator = partial(array_custom_comparator, ascend, null_first)
    array_with_pos.sort(key=cmp_to_key(comparator))
    new_pos = [0] * len(array_with_pos)
    for i in range(len(array_with_pos)):
        new_pos[array_with_pos[i][0]] = i
    return new_pos


def array_custom_comparator(ascend: bool, null_first: bool, a: Any, b: Any):
    value_a, value_b = a[1], b[1]
    if value_a == value_b:
        return 0
    if value_a is None:
        return -1 if null_first else 1
    elif value_b is None:
        return 1 if null_first else -1
    try:
        if math.isnan(value_a) and math.isnan(value_b):
            return 0
        elif math.isnan(value_a):
            ret = 1
        elif math.isnan(value_b):
            ret = -1
        else:
            ret = -1 if value_a < value_b else 1
    except TypeError:
        ret = -1 if value_a < value_b else 1
    return ret if ascend else -1 * ret


def convert_snowflake_datetime_format(format, default_format) -> Tuple[str, int, int]:
    """
    unified processing of the time format
    converting snowflake date/time/timestamp format into python datetime format
    """

    # if this is a PM time in 12-hour format, +12 hour
    hour_delta = 12 if format is not None and "HH12" in format and "PM" in format else 0
    time_fmt = format.upper() if format else default_format
    time_fmt = time_fmt.replace("YYYY", "%Y")
    time_fmt = time_fmt.replace("MM", "%m")
    time_fmt = time_fmt.replace("MON", "%b")
    time_fmt = time_fmt.replace("DD", "%d")
    time_fmt = time_fmt.replace("HH24", "%H")
    time_fmt = time_fmt.replace("HH12", "%H")
    time_fmt = time_fmt.replace("MI", "%M")
    time_fmt = time_fmt.replace("SS", "%S")
    time_fmt = time_fmt.replace("SS", "%S")
    fractional_seconds = 9
    if format is not None and "FF" in format:
        try:
            ff_index = str(format).index("FF")
            # handle precision string 'FF[0-9]' which could be like FF0, FF1, ..., FF9
            if str(format[ff_index + 2 : ff_index + 3]).isdigit():
                fractional_seconds = int(format[ff_index + 2 : ff_index + 3])
                # replace FF[0-9] with %f
                time_fmt = time_fmt[:ff_index] + "%f" + time_fmt[ff_index + 3 :]
            else:
                time_fmt = time_fmt[:ff_index] + "%f" + time_fmt[ff_index + 2 :]
        except ValueError:
            # 'FF' is not in the fmt
            pass

    return time_fmt, hour_delta, fractional_seconds


def process_numeric_time(time: str) -> int:
    """
    deal with time of numeric values, convert the time into value that Python datetime accepts
    spec here: https://docs.snowflake.com/en/sql-reference/functions/to_time#usage-notes

    """
    timestamp_values = int(time)
    if 31536000000000 <= timestamp_values < 31536000000000:  # milliseconds
        timestamp_values = timestamp_values / 1000
    elif timestamp_values >= 31536000000000:
        # nanoseconds
        timestamp_values = timestamp_values / 1000000
    # timestamp_values <  31536000000 are treated as seconds
    return int(timestamp_values)


def process_string_time_with_fractional_seconds(time: str, fractional_seconds) -> str:
    # deal with the fractional seconds part of the input time str, apply precision and reconstruct the time string
    ret = time
    time_parts = ret.split(".")
    if len(time_parts) == 2:
        # there is a part of seconds
        seconds_part = time_parts[1]
        # find the idx that the seconds part ends
        idx = 0
        while idx < len(seconds_part) and seconds_part[idx].isdigit():
            idx += 1
        # truncate to precision
        seconds_part = seconds_part[: min(idx, fractional_seconds)] + seconds_part[idx:]
        ret = f"{time_parts[0]}.{seconds_part}"
    return ret


def fix_drift_between_column_sf_type_and_dtype(col: ColumnEmulator):
    import numpy

    if (
        isinstance(col.sf_type.datatype, _NumericType)
        and col.apply(lambda x: x is None).any()
    ):  # non-object dtype converts None to NaN for numeric columns
        return col
    sf_type_to_dtype = {
        ArrayType: object,
        BinaryType: object,
        BooleanType: bool,
        ByteType: numpy.int8 if not col.sf_type.nullable else "Int8",
        DateType: object,
        DecimalType: numpy.int64 if not col.sf_type.nullable else "Int64",
        DoubleType: numpy.float64,
        FloatType: numpy.float64,
        IntegerType: numpy.int64 if not col.sf_type.nullable else "Int64",
        LongType: numpy.int64 if not col.sf_type.nullable else "Int64",
        NullType: object,
        ShortType: numpy.int8 if not col.sf_type.nullable else "Int8",
        StringType: object,
        TimestampType: "datetime64[ns]",
        TimeType: object,
        VariantType: object,
        MapType: object,
    }
    fixed_type = sf_type_to_dtype.get(type(col.sf_type.datatype), object)
    col = col.astype(fixed_type)
    return col
