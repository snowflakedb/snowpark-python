#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# See https://strftime.org/ and
# https://docs.snowflake.com/en/sql-reference/functions-conversion#date-and-time-formats-in-conversion-functions
import datetime as dt
import re
from typing import Literal, Optional, Union

import numpy as np
import pandas as native_pd
from pandas._libs import lib
from pandas._typing import DateTimeErrorChoices
from pandas.api.types import is_datetime64_any_dtype, is_float_dtype, is_integer_dtype

from snowflake.snowpark import Column
from snowflake.snowpark._internal.analyzer.expression import Interval
from snowflake.snowpark.functions import (
    builtin,
    cast,
    convert_timezone,
    date_part,
    iff,
    to_decimal,
)
from snowflake.snowpark.modin.plugin._internal.utils import pandas_lit
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.types import (
    BooleanType,
    DataType,
    DateType,
    LongType,
    StringType,
    TimestampTimeZone,
    TimestampType,
    VariantType,
    _FractionalType,
)

VALID_TO_DATETIME_DF_KEYS = {
    "year": "year",
    "years": "year",
    "month": "month",
    "months": "month",
    "day": "day",
    "days": "day",
    "h": "hour",
    "hour": "hour",
    "hours": "hour",
    "m": "minute",
    "minute": "minute",
    "minutes": "minute",
    "s": "second",
    "second": "second",
    "seconds": "second",
    "ms": "ms",
    "millisecond": "ms",
    "milliseconds": "ms",
    "us": "us",
    "microsecond": "us",
    "microseconds": "us",
    "ns": "ns",
    "nanosecond": "ns",
    "nanoseconds": "ns",
}
"""
Map of valid column names of a dataframe passed to `to_datetime` to a normalized version that
we can check against in code. Valid column names include plural and abbreviated versions of
the specified time units.
"""

# TODO: SNOW-1127160: support other units
VALID_TO_DATETIME_UNIT = ["D", "s", "ms", "us", "ns"]


def origin_to_ns(
    origin: Union[float, int], unit: Literal["D", "s", "ms", "us", "ns"]
) -> float:
    """
    Converts ``origin`` (given in the specified ``units``) to nanoseconds.
    """
    if unit == "D":
        return origin * 24 * 3600 * (10**9)
    elif unit == "s":
        return origin * (10**9)
    elif unit == "ms":
        return origin * (10**6)
    elif unit == "us":
        return origin * (10**3)
    else:
        assert unit == "ns"
        return origin


def col_to_s(col: Column, unit: Literal["D", "s", "ms", "us", "ns"]) -> Column:
    """
    Converts ``col`` (stored in the specified units) to seconds.
    """
    if unit == "D":
        return col * 24 * 3600
    elif unit == "s":
        return col
    elif unit == "ms":
        return col / 10**3
    elif unit == "us":
        return col / 10**6
    else:
        assert unit == "ns"
        return col / 10**9


PANDAS_DATETIME_FORMAT_TO_SNOWFLAKE_MAPPING = {
    "%Y": "YYYY",
    "%y": "YY",
    "%m": "MM",
    "%-m": "MM",
    "%b": "MON",
    "%B": "MMMM",
    "%d": "DD",
    "%-d": "DD",
    "%a": "DY",
    "%H": "HH24",
    "%I": "HH12",
    "%M": "MI",
    "%S": "SS",
    "%f": "FF",
    "%p": "PM",
    "%z": "TZHTZM",
}

DateTimeOrigin = Optional[
    Union[str, int, float, dt.datetime, native_pd.Timestamp, np.datetime64]
]


def to_snowflake_timestamp_format(datetime_format: str) -> str:
    """
    Convert strftime format to Snowflake format, e.g., from "%d/%m/%Y" to "DD/MM/YYYY"
    Args:
        datetime_format: in strftime format

    Returns:
        Snowflake format
    """
    for k, v in PANDAS_DATETIME_FORMAT_TO_SNOWFLAKE_MAPPING.items():
        datetime_format = datetime_format.replace(k, v)
    return datetime_format


def is_snowflake_timestamp_format_valid(sf_format: str) -> bool:
    """
    Check if a timestamp format valid. It will be invalid if it still contain "%.", i.e., strftime format
    Args:
        sf_format:

    Returns:
        True if it is valid
    """
    return not re.search("%.", sf_format)


def generate_timestamp_col(
    col: Column,
    datatype: DataType,
    *,
    sf_format: Optional[str] = None,
    errors: Literal["raise", "coerce"] = "raise",
    target_tz: Optional[str] = None,
    unit: Literal["D", "s", "ms", "us", "ns"],
    origin: DateTimeOrigin = "unix",
) -> Column:
    """
    Use Snowflake timestamp functions to convert column to timestamp in snowflake

    Args:
        col: the Snowpark column
        datatype: data type of the column
        has_tz: whether timezone is preserved
        sf_format: format specified to parse string to timestamp. If format is given, we deliver the format to to_timestamp
                function
        errors: if 'raise', then invalid parsing will raise an exception, i.e., use to_timestamp* function; if 'coerce',
                then invalid parsing will be set as NaT, i.e., use try_to_timestamp function; note this method cannot be
                used for error = 'ignore'
        target_tz: if not None, convert the value into TIMESTAMP_TZ with the target timezone; otherwise, convert to
                    TIMESTAMP_NTZ
        unit: the unit of values in the integer column (D,s,ms,us,ns)
        origin: "unix", "julian", or timestamp-like representing reference date
    Returns:
        The column under to_timestamp_* function
    """
    assert errors in ["raise", "coerce"], f"errors={errors} cannot be handled here"
    to_timestamp_func_name = "to_timestamp_ntz"
    if errors == "coerce":
        to_timestamp_func_name = "try_" + to_timestamp_func_name
    new_col = col

    # compute the ns offset of the provided origin from the unix epoch
    origin_type = type(origin)
    origin_ns: Union[int, float]
    if origin == "unix":
        origin_ns = 0
    elif is_integer_dtype(origin_type) or is_float_dtype(origin_type):
        # if origin is float or integer: treat as offset from 1970-01-01
        origin_ns = origin_to_ns(origin, unit)  # type: ignore[arg-type]
    elif isinstance(origin, native_pd.Timestamp):
        origin_ns = origin.value
    elif isinstance(origin, str) or is_datetime64_any_dtype(origin_type):
        origin_ns = native_pd.to_datetime(origin).value
    else:
        raise TypeError(
            f"Cannot convert input [{origin}] of type {origin_type} to Timestamp"
        )

    if sf_format:
        if isinstance(datatype, _FractionalType):
            # make sure always cast fractionalType to decimal with scale = 0 so the number can be converted by
            # to_timestamp
            new_col = to_decimal(new_col, precision=38, scale=0)

        # always cast to string because 1) format requires string input; 2) to handle special cases needs string type
        new_col = cast(new_col, StringType())
        # handle a string which have string values like "nan", "nat". We follow pandas semantics to convert them to NaT
        # (or Null in SQL). Note that Snowpark pandas treats any "nan" and "nat" (case insensitive) to NULL for simplicity
        # and consistency; while pandas is mixed with both case sensitive and insensitive behaviors, e.g., "nAn" is
        # invalid when call to_datetime without format but valid when call to_datetime with format.
        new_col = iff(
            builtin("ilike")(new_col, pandas_lit("nan"))
            | builtin("ilike")(new_col, pandas_lit("nat")),
            pandas_lit(None),
            new_col,
        )
        has_tz = "TZHTZM" in sf_format if sf_format is not None else False
        if has_tz:
            to_timestamp_func_name = to_timestamp_func_name.replace("_ntz", "_tz")
        # always cast to string because to_timestamp method with format requires string input
        new_col = cast(new_col, StringType())
        new_col = builtin(to_timestamp_func_name)(new_col, sf_format)
    else:
        if isinstance(datatype, (StringType, VariantType)):
            WarningMessage.mismatch_with_pandas(
                "to_datetime",
                "Snowpark pandas to_datetime uses Snowflake's automatic format "
                "detection to convert string to datetime when a format is not provided. "
                "In this case Snowflake's auto format may yield different result values compared to pandas.",
            )

        from snowflake.snowpark.modin.plugin._internal.type_utils import (
            NUMERIC_SNOWFLAKE_TYPES,
        )

        if isinstance(datatype, tuple(NUMERIC_SNOWFLAKE_TYPES)):
            if isinstance(datatype, BooleanType):
                # otherwise, need to explicitly cast to integer before casting to timestamp, since cast directly from
                # boolean to Timestamp is invalid in Snowflake because boolean is not treated as numeric type in
                # Snowflake
                new_col = cast(new_col, LongType())
            # pandas convert numeric value ns. Scale=9 is used to store nanoseconds
            new_col = col_to_s(to_decimal(new_col, precision=38, scale=9), unit)
        elif (
            target_tz
            and isinstance(datatype, TimestampType)
            and datatype.tz != TimestampTimeZone.TZ
        ):
            # directly call convert_timezone won't work in this case, so we extract the epoch nanoseconds out and
            # convert it to timestamp_tz and then call convert_timezone so that the timezone will be correct
            new_col = (
                to_decimal(date_part("epoch_nanosecond", new_col), 38, 9) / 10**9
            )
        elif (
            not target_tz
            and isinstance(datatype, TimestampType)
            and datatype.tz != TimestampTimeZone.NTZ
        ):
            # when converting from datetime64 from tz aware to tz naive, pandas just extract the datetime and skip the
            # timezone. For example, datetime 1970-01-01 00:00:00+09:00 with type "datetime64[ns, Asia/Tokyo]"
            # (tz-aware) will be converted to 1970-01-01 00:00:00 with type "datetime64[ns]" (tz-naive). In Snowflake,
            # we can use convert_timezone with target_timezone = "UTC" to achieve the same behavior.
            new_col = convert_timezone(
                target_timezone=pandas_lit("UTC"), source_time=new_col
            )
        elif target_tz and isinstance(datatype, DateType):
            # directly call convert_timezone won't work in this case, so we extract the epoch seconds out and
            # convert it to timestamp_tz and then call convert_timezone so that the timezone will be correct
            new_col = date_part("epoch_second", new_col)
        if target_tz:
            to_timestamp_func_name = to_timestamp_func_name.replace("_ntz", "_tz")
        new_col = builtin(to_timestamp_func_name)(new_col)
    new_col = builtin("dateadd")("ns", origin_ns, new_col)
    if target_tz:
        new_col = convert_timezone(
            target_timezone=pandas_lit(target_tz), source_time=new_col
        )
    if errors == "coerce":
        # pandas return NaT when the timestamp is out of bound
        new_col = iff(
            new_col.between(
                pandas_lit(str(native_pd.Timestamp.min)),
                pandas_lit(str(native_pd.Timestamp.max)),
            ),
            new_col,
            None,
        )
    return new_col


def raise_if_to_datetime_not_supported(
    format: str,
    exact: Union[bool, lib.NoDefault] = lib.no_default,
    infer_datetime_format: Union[lib.NoDefault, bool] = lib.no_default,
    origin: DateTimeOrigin = "unix",
    errors: DateTimeErrorChoices = "raise",
) -> None:
    """
    Raise not implemented error to_datetime API has any unsupported parameter or
    parameter value
    Args:
        format: the format argument for to_datetime
        exact: the exact argument for to_datetime
        infer_datetime_format: the infer_datetime_format argument for to_datetime
        origin: the origin argument for to_datetime
        errors: the errors argument for to_datetime
    """
    error_message = None
    if format is not None and not is_snowflake_timestamp_format_valid(
        to_snowflake_timestamp_format(format)
    ):
        # if format is not given, Snowflake's auto format detection may be different from pandas behavior
        error_message = (
            f"Snowpark pandas to_datetime API doesn't yet support given format {format}"
        )
    elif not exact:
        # Snowflake does not allow the format to match anywhere in the target string when exact is False
        error_message = "Snowpark pandas to_datetime API doesn't yet support non exact format matching"
    elif infer_datetime_format != lib.no_default:
        # infer_datetime_format is deprecated since version 2.0.0
        error_message = "Snowpark pandas to_datetime API doesn't support 'infer_datetime_format' parameter"
    elif origin == "julian":
        # default for julian calendar support
        error_message = (
            "Snowpark pandas to_datetime API doesn't yet support julian calendar"
        )
    elif errors == "ignore":
        # ignore requires return the whole original input which is not applicable in Snowfalke
        error_message = "Snowpark pandas to_datetime API doesn't yet support 'ignore' value for errors parameter"

    if error_message:
        ErrorMessage.not_implemented(error_message)


def convert_dateoffset_to_interval(
    value: native_pd.DateOffset,
) -> Interval:
    """
    Converts a pandas DateOffset where value is treated as a timedelta to a Snowpark
    Interval keyword. DateOffset with parameters that replace the offset value is not
    yet supported, so a NotImplemented error is raised.
    """
    # Call DateOffset.kwds to parse the DateOffset into a dictionary of params
    # If doff = pd.DateOffset(years=2, day=1), then doff.kwds returns {'years': 2, 'day': 1}
    dateoffset_dict = value.kwds
    # Handle case where the DateOffset has no argument or an integer argument
    # Ex. pd.DateOffset() -> Timedelta 1 Day, pd.DateOffset(5) -> Timedelta 5 Days
    if not dateoffset_dict:
        return Interval(day=value.n)
    # Handle case where DateOffset offset value is treated as a timedelta
    param_mapping = {
        "years": "year",
        "months": "month",
        "weeks": "week",
        "days": "day",
        "hours": "hour",
        "minutes": "minute",
        "seconds": "second",
        "milliseconds": "millisecond",
        "microseconds": "microsecond",
        "nanoseconds": "nanosecond",
    }
    interval_kwargs = {}
    for interval, offset in dateoffset_dict.items():
        new_param = param_mapping.get(interval)
        if new_param is None:
            # TODO SNOW-1007629: Support DateOffset with replacement offset values
            raise NotImplementedError(
                "DateOffset with parameters that replace the offset value are not yet supported."
            )
        interval_kwargs[new_param] = offset
    return Interval(**interval_kwargs)
