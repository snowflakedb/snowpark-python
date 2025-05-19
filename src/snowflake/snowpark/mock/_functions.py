#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import base64
import binascii
import datetime
import decimal
import json
import logging
import math
import numbers
import operator
import re
import string
import threading
from decimal import Decimal
from functools import partial, reduce
from numbers import Real
from random import randint
from typing import Any, Callable, Optional, Tuple, TypeVar, Union

import pytz

import snowflake.snowpark
from snowflake.snowpark._internal.analyzer.expression import FunctionExpression
from snowflake.snowpark._internal.utils import unalias_datetime_part
from snowflake.snowpark.mock._options import numpy, pandas
from snowflake.snowpark.mock._snowflake_data_type import (
    _TIMESTAMP_TYPE_MAPPING,
    _TIMESTAMP_TYPE_TIMEZONE_MAPPING,
    ColumnEmulator,
    ColumnType,
    TableEmulator,
    get_coerce_result_type,
)
from snowflake.snowpark.mock.exceptions import SnowparkLocalTestingException
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    LongType,
    MapType,
    NullType,
    StringType,
    TimestampTimeZone,
    TimestampType,
    TimeType,
    VariantType,
    _FractionalType,
    _IntegralType,
    _NumericType,
)

from ._telemetry import LocalTestOOBTelemetryService
from ._util import (
    convert_numeric_string_value_to_float_seconds,
    convert_snowflake_datetime_format,
    process_string_time_with_fractional_seconds,
)

RETURN_TYPE = Union[ColumnEmulator, TableEmulator]


_DEFAULT_OUTPUT_FORMAT = {
    DateType: "YYYY-MM-DD",
    TimeType: "HH24:MI:SS",
    TimestampType: "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM",
}

_logger = logging.getLogger(__name__)


class MockedFunction:
    def __init__(
        self,
        name: str,
        func_implementation: Callable,
        distinct: Optional["MockedFunction"] = None,
        pass_column_index: Optional[bool] = None,
        pass_row_index: Optional[bool] = None,
        pass_input_data: Optional[bool] = None,
    ) -> None:
        self.name = name
        self.impl = func_implementation
        self.distinct = distinct or self
        self._pass_row_index = pass_row_index
        self._pass_column_index = pass_column_index
        self._pass_input_data = pass_input_data

    def _check_constant_result(self, input_data, args, result):
        # This function helps automaticallly fill a column with a constant value in certain
        # circumstances. Ideally a mocked function would enable pass_index and generate it's own
        # column filled with constant values, but this works as well as a fallback.

        # If none of the args are column emulators and the function result only has one item
        # assume that the single value should be repeated instead of Null filled. This allows
        # constant expressions like current_date or current_database to fill a column instead
        # of just the first row.
        if (
            not any(isinstance(arg, (ColumnEmulator, TableEmulator)) for arg in args)
            and len(result) == 1
        ):
            resized = result.repeat(len(input_data)).reset_index(drop=True)
            resized.sf_type = result.sf_type
            return resized

        return result

    def __call__(self, *args, input_data=None, row_number=None, **kwargs):

        if self._pass_input_data:
            kwargs["raw_input"] = input_data
        if self._pass_row_index:
            kwargs["row_index"] = list(input_data.index).index(row_number)
        if self._pass_column_index:
            kwargs["column_index"] = input_data.index

        result = self.impl(*args, **kwargs)

        if (
            input_data is not None
            and not self._pass_column_index
            and not self._pass_row_index
        ):
            return self._check_constant_result(
                input_data, args + tuple(kwargs.values()), result
            )

        return result


class MockedFunctionRegistry:
    _instance = None
    _lock_init = threading.Lock()

    def __init__(self) -> None:
        self._registry = dict()
        self._lock = threading.RLock()

    @classmethod
    def get_or_create(cls) -> "MockedFunctionRegistry":
        with cls._lock_init:
            if cls._instance is None:
                cls._instance = MockedFunctionRegistry()
        return cls._instance

    def get_function(
        self, func: Union[FunctionExpression, str]
    ) -> Optional[MockedFunction]:
        if isinstance(func, str):
            func_name = func
            distinct = False
        else:
            func_name = func.name
            distinct = func.is_distinct
        func_name = func_name.lower()

        with self._lock:
            if func_name not in self._registry:
                return None

            function = self._registry[func_name]

        return function.distinct if distinct else function

    def register(
        self,
        snowpark_func: Union[str, Callable],
        func_implementation: Callable,
        *args,
        **kwargs,
    ) -> MockedFunction:
        name = (
            snowpark_func if isinstance(snowpark_func, str) else snowpark_func.__name__
        )
        mocked_function = MockedFunction(name, func_implementation, *args, **kwargs)
        with self._lock:
            self._registry[name] = mocked_function
        return mocked_function

    def unregister(
        self,
        snowpark_func: Union[str, Callable],
    ):
        name = (
            snowpark_func if isinstance(snowpark_func, str) else snowpark_func.__name__
        )

        with self._lock:
            if name in self._registry:
                del self._registry[name]


class LocalTimezone:
    """
    A singleton class that encapsulates conversion to the local timezone.
    This class allows tests to override the local timezone in order to be consistent in different regions.
    """

    LOCAL_TZ: Optional[datetime.timezone] = None

    @classmethod
    def set_local_timezone(cls, tz: Optional[datetime.timezone] = None) -> None:
        """Overrides the local timezone with the given value. When the local timezone is None the system timezone is used."""
        cls.LOCAL_TZ = tz

    @classmethod
    def to_local_timezone(
        cls, d: Optional[datetime.datetime]
    ) -> Optional[datetime.datetime]:
        """Converts an input datetime to the local timezone."""
        return d.astimezone(tz=cls.LOCAL_TZ) if d is not None else d

    @classmethod
    def replace_tz(cls, d: datetime.datetime) -> datetime.datetime:
        """Replaces any existing tz info with the local tz info without adjusting the time."""
        return d.replace(tzinfo=cls.LOCAL_TZ)


def patch(function, *args, **kwargs):
    def decorator(mocking_function):
        mocked_function = MockedFunctionRegistry.get_or_create().register(
            function, mocking_function, *args, **kwargs
        )
        return mocked_function

    return decorator


@patch("min")
def mock_min(column: ColumnEmulator) -> ColumnEmulator:
    if isinstance(
        column.sf_type.datatype, _NumericType
    ):  # TODO: figure out where 5 is coming from
        res = ColumnEmulator(data=round(column.min(), 5), sf_type=column.sf_type)
    else:
        res = ColumnEmulator(data=column.dropna().min(), sf_type=column.sf_type)
    try:
        if math.isnan(res[0]):
            # If original column had na values then na is an expected output
            column_has_na = (
                column[column.apply(lambda x: x is not None)].isna().values.any()
            )
            if not column_has_na:
                return ColumnEmulator(data=[None], sf_type=column.sf_type)
        return ColumnEmulator(data=res, sf_type=column.sf_type)
    except TypeError:  # math.isnan throws TypeError if res[0] is not a number
        return ColumnEmulator(data=res, sf_type=column.sf_type)


@patch("max")
def mock_max(column: ColumnEmulator) -> ColumnEmulator:
    if isinstance(column.sf_type.datatype, _NumericType):
        res = ColumnEmulator(data=round(column.max(), 5), sf_type=column.sf_type)
    else:
        res = ColumnEmulator(data=column.dropna().max(), sf_type=column.sf_type)
    try:
        if math.isnan(res[0]):
            # If original column had na values then na is an expected output
            column_has_na = (
                column[column.apply(lambda x: x is not None)].isna().values.any()
            )
            if not column_has_na:
                return ColumnEmulator(data=[None], sf_type=column.sf_type)
        return ColumnEmulator(data=res, sf_type=column.sf_type)
    except TypeError:
        return ColumnEmulator(data=res, sf_type=column.sf_type)


def _sum(column: ColumnEmulator) -> ColumnEmulator:
    all_item_is_none = True
    res = 0
    for data in column:
        if data is not None:
            try:
                if math.isnan(data):
                    res = math.nan
                    all_item_is_none = False
                    break
            except TypeError:
                pass
            all_item_is_none = False
            try:
                res += float(data)
            except ValueError as exc:
                SnowparkLocalTestingException.raise_from_error(
                    exc, error_message=f"Numeric value '{data}' is not recognized."
                )
    if isinstance(column.sf_type.datatype, DecimalType):
        p, s = column.sf_type.datatype.precision, column.sf_type.datatype.scale
        new_type = DecimalType(min(38, p + 12), s)
    else:
        new_type = column.sf_type.datatype
    return (
        ColumnEmulator(
            data=[res], sf_type=ColumnType(new_type, column.sf_type.nullable)
        )
        if not all_item_is_none
        else ColumnEmulator(
            data=[None], sf_type=ColumnType(new_type, column.sf_type.nullable)
        )
    )


@patch("sum_distinct")
def mock_sum_distinct(column: ColumnEmulator) -> ColumnEmulator:
    column = ColumnEmulator(data=column.unique(), sf_type=column.sf_type)
    return _sum(column)


@patch("sum", distinct=mock_sum_distinct)
def mock_sum(column: ColumnEmulator) -> ColumnEmulator:
    return _sum(column)


@patch("avg")
def mock_avg(column: ColumnEmulator) -> ColumnEmulator:
    if not isinstance(column.sf_type.datatype, (_NumericType, NullType)):
        raise SnowparkLocalTestingException(
            f"Cannot compute avg on a column of type {column.sf_type.datatype}"
        )

    if isinstance(column.sf_type.datatype, NullType) or column.isna().all():
        return ColumnEmulator(data=[None], sf_type=ColumnType(NullType(), True))
    elif isinstance(column.sf_type.datatype, _IntegralType):
        res_type = DecimalType(38, 6)
    elif isinstance(column.sf_type.datatype, DecimalType):
        precision, scale = (
            column.sf_type.datatype.precision,
            column.sf_type.datatype.scale,
        )
        precision = max(38, column.sf_type.datatype.precision + 12)
        if scale <= 6:
            scale = scale + 6
        elif scale < 12:
            scale = 12
        res_type = DecimalType(precision, scale)
    else:
        assert isinstance(column.sf_type.datatype, _FractionalType)
        res_type = FloatType()

    notna = column[~column.isna()]
    res = notna.mean()
    if isinstance(res_type, DecimalType):
        fmt_string = f"{{:.{res_type.scale}f}}"
        res_formatted = fmt_string.format(res)
        res = decimal.Decimal(res_formatted)
    return ColumnEmulator(data=[res], sf_type=ColumnType(res_type, False))


@patch("stddev")
def mock_stddev(column: ColumnEmulator) -> ColumnEmulator:
    if not isinstance(column.sf_type.datatype, (_NumericType, NullType)):
        raise SnowparkLocalTestingException(
            f"Cannot compute stddev on a column of type {column.sf_type.datatype}"
        )

    if isinstance(column.sf_type.datatype, NullType) or column.isna().all():
        return ColumnEmulator(data=[None], sf_type=ColumnType(NullType(), True))
    elif isinstance(column.sf_type.datatype, _IntegralType):
        res_type = DecimalType(38, 6)
    elif isinstance(column.sf_type.datatype, DecimalType):
        precision, scale = (
            column.sf_type.datatype.precision,
            column.sf_type.datatype.scale,
        )
        precision = max(38, column.sf_type.datatype.precision + 12)
        if scale <= 6:
            scale = scale + 6
        elif scale < 12:
            scale = 12
        res_type = DecimalType(precision, scale)
    else:
        assert isinstance(column.sf_type.datatype, _FractionalType)
        res_type = FloatType()

    notna = column[~column.isna()]
    res = notna.std()
    if isinstance(res_type, Decimal):
        res = round(res, scale)
    return ColumnEmulator(data=[res], sf_type=ColumnType(res_type, False))


@patch("approx_percentile_accumulate")
def mock_approx_percentile_accumulate(
    column: Union[TableEmulator, ColumnEmulator]
) -> ColumnEmulator:
    # TODO SNOW-1800512: Fix, returns dummy of 42 for now.
    _logger.warning("TODO SNOW-1800512: Returns dummy value of 42 now, need to fix.")
    return ColumnEmulator(data=42, sf_type=ColumnType(FloatType(), False))


@patch("approx_percentile_estimate")
def mock_approx_percentile_estimate(
    column1: Union[TableEmulator, ColumnEmulator],
    column2: Union[TableEmulator, ColumnEmulator],
) -> ColumnEmulator:
    # TODO SNOW-1800512: Fix, returns dummy of 42 for now.
    _logger.warning("TODO SNOW-1800512: Returns dummy value of 42 now, need to fix.")
    return ColumnEmulator(data=42, sf_type=ColumnType(FloatType(), False))


@patch("covar_samp")
def mock_covar_samp(
    column1: Union[TableEmulator, ColumnEmulator],
    column2: Union[TableEmulator, ColumnEmulator],
) -> ColumnEmulator:
    # TODO SNOW-1800512: Fix, returns dummy of 42 for now.
    _logger.warning("TODO SNOW-1800512: Returns dummy value of 42 now, need to fix.")
    return ColumnEmulator(data=42, sf_type=ColumnType(FloatType(), False))


@patch("corr")
def mock_corr_samp(
    column1: Union[TableEmulator, ColumnEmulator],
    column2: Union[TableEmulator, ColumnEmulator],
) -> ColumnEmulator:
    # TODO SNOW-1800512: Fix, returns dummy of 42 for now.
    _logger.warning("TODO SNOW-1800512: Returns dummy value of 42 now, need to fix.")
    return ColumnEmulator(data=42, sf_type=ColumnType(FloatType(), False))


@patch("count_distinct")
def mock_count_distinct(*cols: ColumnEmulator) -> ColumnEmulator:
    """
    Snowflake does not count rows that contain NULL values, in the mocking implementation
    we iterate over each row and then each col to check if there exists NULL value, if the col is NULL,
    we do not count that row.
    """
    df = TableEmulator()
    for i in range(len(cols)):
        df[cols[i].name] = cols[i]
    df = df.dropna()
    combined = df[df.columns].apply(lambda row: tuple(row), axis=1).dropna()
    res = combined.nunique()
    return ColumnEmulator(data=res, sf_type=ColumnType(LongType(), False))


@patch("count", distinct=mock_count_distinct)
def mock_count(column: Union[TableEmulator, ColumnEmulator]) -> ColumnEmulator:
    if isinstance(column, ColumnEmulator):
        count_column = column.count()
        return ColumnEmulator(data=count_column, sf_type=ColumnType(LongType(), False))
    else:  # TableEmulator # TODO would this branch actually ever happen?
        return ColumnEmulator(data=len(column), sf_type=ColumnType(LongType(), False))


@patch("median")
def mock_median(column: ColumnEmulator) -> ColumnEmulator:
    if isinstance(column.sf_type.datatype, DecimalType):
        return_type = DecimalType(
            column.sf_type.datatype.precision + 3, column.sf_type.datatype.scale + 3
        )
    else:
        return_type = column.sf_type.datatype
    return ColumnEmulator(
        data=round(column.median(), 5) if column.size else [None],
        sf_type=ColumnType(return_type, column.sf_type.nullable),
    )


@patch("covar_pop")
def mock_covar_pop(column1: ColumnEmulator, column2: ColumnEmulator) -> ColumnEmulator:
    non_nan_cnt = 0
    x_sum, y_sum, x_times_y_sum = 0, 0, 0
    for x, y in zip(column1, column2):
        if (x is not None and math.isnan(x)) or (y is not None and math.isnan(y)):
            return ColumnEmulator(
                data=math.nan,
                sf_type=ColumnType(
                    DoubleType(), column1.sf_type.nullable or column2.sf_type.nullable
                ),
            )
        if x is not None and y is not None and not math.isnan(x) and not math.isnan(y):
            non_nan_cnt += 1
            x_times_y_sum += x * y
            x_sum += x
            y_sum += y
    data = (x_times_y_sum - x_sum * y_sum / non_nan_cnt) / non_nan_cnt
    return ColumnEmulator(
        data=data,
        sf_type=ColumnType(
            DoubleType(), column1.sf_type.nullable or column2.sf_type.nullable
        ),
    )


@patch("array_agg")
def mock_array_agg(column: ColumnEmulator, is_distinct: bool) -> ColumnEmulator:
    columns_data = ColumnEmulator(column.unique()) if is_distinct else column
    return ColumnEmulator(
        data=[list(columns_data.dropna())],
        sf_type=ColumnType(ArrayType(), False),
    )


@patch("array_construct")
def mock_array_construct(*columns):
    if len(columns) == 0:
        data = [[]]
    else:
        data = pandas.concat(columns, axis=1).apply(lambda x: list(x), axis=1)
    return ColumnEmulator(data, sf_type=ColumnType(ArrayType(), False))


@patch("listagg")
def mock_listagg(column: ColumnEmulator, delimiter: str, is_distinct: bool):
    columns_data = ColumnEmulator(column.unique()) if is_distinct else column
    # nit todo: returns a string that includes all the non-NULL input values, separated by the delimiter.
    return ColumnEmulator(
        data=delimiter.join([str(v) for v in columns_data.dropna()]),
        sf_type=ColumnType(StringType(16777216), column.sf_type.nullable),
    )


@patch("sqrt")
def mock_sqrt(column: ColumnEmulator):
    result = column.apply(math.sqrt)
    result.sf_type = ColumnType(FloatType(), column.sf_type.nullable)
    return result


@patch("ln")
def mock_ln(column: ColumnEmulator):
    result = column.apply(math.log)
    result.sf_type = ColumnType(FloatType(), column.sf_type.nullable)
    return result


@patch("pow")
def mock_pow(left: ColumnEmulator, right: ColumnEmulator):
    result = left.combine(right, lambda l, r: l**r)
    result.sf_type = ColumnType(FloatType(), left.sf_type.nullable)
    return result


@patch("to_date")
def mock_to_date(
    column: ColumnEmulator,
    fmt: str = None,
    try_cast: bool = False,
):
    """
    https://docs.snowflake.com/en/sql-reference/functions/to_date

    Converts an input expression to a date:

    [x] For a string expression, the result of converting the string to a date.

    [x] For a timestamp expression, the date from the timestamp.

    For a variant expression:

        [x] If the variant contains a string, a string conversion is performed.

        [x] If the variant contains a date, the date value is preserved as is.

        [x] If the variant contains a JSON null value, the output is NULL.

        [x] For NULL input, the output is NULL.

        [x] For all other values, a conversion error is generated.
    """

    if isinstance(column.sf_type.datatype, DateType):
        return column.copy()

    import dateutil.parser

    if not isinstance(fmt, ColumnEmulator):
        fmt = ColumnEmulator([fmt] * len(column), index=column.index)

    def convert_date(data, _fmt):
        try:
            auto_detect = _fmt is None or _fmt.lower() == "auto"
            date_format, _ = convert_snowflake_datetime_format(
                _fmt, default_format="%Y-%m-%d"
            )

            if data is None:
                return None

            if isinstance(column.sf_type.datatype, TimestampType):
                return data.date()
            elif isinstance(column.sf_type.datatype, StringType):
                if data.isdigit():
                    return datetime.datetime.utcfromtimestamp(
                        convert_numeric_string_value_to_float_seconds(data)
                    ).date()
                else:
                    if auto_detect:
                        return dateutil.parser.parse(data).date()
                    else:
                        return datetime.datetime.strptime(data, date_format).date()
            elif isinstance(column.sf_type.datatype, VariantType):
                if not (_fmt is None or (_fmt and str(_fmt).lower() != "auto")):
                    SnowparkLocalTestingException.raise_from_error(
                        TypeError(
                            "[Local Testing] to_date function does not allow format parameter for data of VariantType"
                        )
                    )
                if isinstance(data, str):
                    if data.isdigit():
                        return datetime.datetime.utcfromtimestamp(
                            convert_numeric_string_value_to_float_seconds(data)
                        ).date()
                    else:
                        # for variant type with string value, snowflake auto-detects the format
                        return dateutil.parser.parse(data).date()
                elif isinstance(data, datetime.date):
                    return data
                else:
                    SnowparkLocalTestingException.raise_from_error(
                        TypeError(
                            f"[Local Testing] Unsupported conversion to_date of value {data} of VariantType"
                        )
                    )
            else:
                SnowparkLocalTestingException.raise_from_error(
                    TypeError(
                        f"[Local Testing] Unsupported conversion to_date of data type {type(column.sf_type.datatype).__name__}"
                    )
                )
        except BaseException as exc:
            if try_cast:
                return None
            else:
                SnowparkLocalTestingException.raise_from_error(exc)

    res = column.combine(fmt, convert_date)
    res.sf_type = ColumnType(DateType(), column.sf_type.nullable)
    return res


@patch("current_timestamp", pass_column_index=True)
def mock_current_timestamp(column_index):
    return ColumnEmulator(
        data=[datetime.datetime.now()] * len(column_index),
        sf_type=ColumnType(TimestampType(TimestampTimeZone.LTZ), False),
    )


@patch("current_date", pass_column_index=True)
def mock_current_date(column_index):
    now = datetime.datetime.now()
    return ColumnEmulator(
        data=[now.date()] * len(column_index), sf_type=ColumnType(DateType(), False)
    )


@patch("current_time", pass_column_index=True)
def mock_current_time(column_index):
    now = datetime.datetime.now()
    return ColumnEmulator(
        data=[now.time()] * len(column_index), sf_type=ColumnType(TimeType(), False)
    )


@patch("hour")
def mock_hour(expr):
    return ColumnEmulator(
        data=[None if value is None else value.hour for value in expr],
        sf_type=ColumnType(LongType(), False),
    )


@patch("minute")
def mock_minute(expr):
    return ColumnEmulator(
        data=[None if value is None else value.minute for value in expr],
        sf_type=ColumnType(LongType(), False),
    )


@patch("contains")
def mock_contains(expr1: ColumnEmulator, expr2: ColumnEmulator):
    if isinstance(expr1, str) and isinstance(expr2, str):
        return ColumnEmulator(data=[bool(str(expr2) in str(expr1))])
    if isinstance(expr1, ColumnEmulator) and isinstance(expr2, ColumnEmulator):
        res = [bool(str(item2) in str(item1)) for item1, item2 in zip(expr1, expr2)]
    elif isinstance(expr1, ColumnEmulator) and isinstance(expr2, str):
        res = [bool(str(expr2) in str(item)) for item in expr1]
    else:  # expr1 is string, while expr2 is column
        res = [bool(str(item) in str(expr1)) for item in expr2]
    return ColumnEmulator(
        data=res, sf_type=ColumnType(BooleanType(), expr1.sf_type.nullable)
    )


@patch("abs")
def mock_abs(expr):
    if isinstance(expr, ColumnEmulator):
        result = expr.abs()
        result.sf_type = expr.sf_type
        return result
    else:
        return abs(expr)


@patch("to_decimal")
def mock_to_decimal(
    e: ColumnEmulator,
    precision: Optional[int] = 38,
    scale: Optional[int] = 0,
    try_cast: bool = False,
):
    """
    [x] For NULL input, the result is NULL.

    [x] For fixed-point numbers:

        Numbers with different scales are converted by either adding zeros to the right (if the scale needs to be increased) or by reducing the number of fractional digits by rounding (if the scale needs to be decreased).

        Note that casts of fixed-point numbers to fixed-point numbers that increase scale might fail.

    [x] For floating-point numbers:

        Numbers are converted if they are within the representable range, given the scale.

        The conversion between binary and decimal fractional numbers is not precise. This might result in loss of precision or out-of-range errors.

        Values of infinity and NaN (not-a-number) result in conversion errors.

        For floating-point input, omitting the mantissa or exponent is allowed and is interpreted as 0. Thus, E is parsed as 0.

    [x] Strings are converted as decimal, integer, fractional, or floating-point numbers.

    [x] For fractional input, the precision is deduced as the number of digits after the point.

    For VARIANT input:

        [x] If the variant contains a fixed-point or a floating-point numeric value, an appropriate numeric conversion is performed.

        [x] If the variant contains a string, a string conversion is performed.

        [x] If the variant contains a Boolean value, the result is 0 or 1 (for false and true, correspondingly).

        [x] If the variant contains JSON null value, the output is NULL.
    """

    def is_str_int(s):
        if s[0] in ("-", "+"):
            return s[1:].isdigit()
        return s.isdigit()

    def cast_as_float_convert_to_decimal(x: Union[Decimal, float, str, bool]):
        # casting int of big value to float leads to precision loss
        # e.g. float(9223372036854775807) = 9.223372036854776e+18
        x = int(x) if is_str_int(str(x)) else float(x)
        if x in (math.inf, -math.inf, math.nan):
            SnowparkLocalTestingException.raise_from_error(
                ValueError("Values of infinity and NaN cannot be converted to decimal")
            )
        integer_part_len = 1 if abs(x) < 1 else math.ceil(math.log10(abs(x)))
        if integer_part_len > precision:
            raise SnowparkLocalTestingException(f"Numeric value '{x}' is out of range")
        remaining_decimal_len = min(precision - integer_part_len, scale)
        return Decimal(str(round(x, remaining_decimal_len)))

    if isinstance(e.sf_type.datatype, (_NumericType, BooleanType, NullType)):
        res = e.apply(
            lambda x: try_convert(cast_as_float_convert_to_decimal, try_cast, x)
        )
    elif isinstance(e.sf_type.datatype, (StringType, VariantType)):
        res = e.replace({"E": 0}).apply(
            lambda x: try_convert(cast_as_float_convert_to_decimal, try_cast, x)
        )
    else:
        SnowparkLocalTestingException.raise_from_error(
            TypeError(f"Invalid input type to TO_DECIMAL {e.sf_type.datatype}")
        )
    res.sf_type = ColumnType(
        DecimalType(precision, scale), nullable=e.sf_type.nullable or res.hasnans
    )
    return res


@patch("to_time")
def mock_to_time(
    column: ColumnEmulator,
    fmt: Optional[str] = None,
    try_cast: bool = False,
):
    """
    https://docs.snowflake.com/en/sql-reference/functions/to_time

    [x] For string_expr, the result of converting the string to a time.

    [x] For timestamp_expr, the time portion of the input value.

    [x] For 'integer' (a string containing an integer), the integer is treated as a number of seconds, milliseconds, microseconds, or nanoseconds after the start of the Unix epoch. See the Usage Notes below.

        [x] For this timestamp, the function gets the number of seconds after the start of the Unix epoch. The function performs a modulo operation to get the remainder from dividing this number by the number of seconds in a day (86400): number_of_seconds % 86400

    """
    import dateutil.parser

    def convert_int_string_to_time(d: str):
        return datetime.datetime.utcfromtimestamp(
            convert_numeric_string_value_to_float_seconds(d) % 86400
        ).time()

    def convert_string_to_time(_data: str, _time_format: str, _fractional_seconds: int):
        data_parts = _data.split(".")
        if len(data_parts) == 2:
            # there is a part of seconds
            seconds_part = data_parts[1]
            # find the idx that the seconds part ends
            idx = 0
            while idx < len(seconds_part) and seconds_part[idx].isdigit():
                idx += 1
            # truncate to precision
            seconds_part = (
                seconds_part[: min(idx, _fractional_seconds)] + seconds_part[idx:]
            )
            _data = f"{data_parts[0]}.{seconds_part}"

        # %f is optional if fractional seconds part doesn't show up in the input which means it is 0 nanoseconds
        if len(data_parts) == 1 and ".%f" in _time_format:
            _time_format = _time_format.replace(".%f", "")

        target_datetime = datetime.datetime.strptime(
            process_string_time_with_fractional_seconds(_data, _fractional_seconds),
            _time_format,
        )
        return target_datetime.time()

    if isinstance(column.sf_type.datatype, TimeType):
        return column.copy()

    res = []

    if not isinstance(fmt, ColumnEmulator):
        fmt = [fmt] * len(column)

    for data, _fmt in zip(column, fmt):
        if data is None:
            res.append(None)
            continue
        datatype = column.sf_type.datatype
        try:
            (
                time_fmt,
                fractional_seconds,
            ) = convert_snowflake_datetime_format(_fmt, default_format="%H:%M:%S")
            auto_detect = _fmt is None or str(_fmt).lower() == "auto"
            if isinstance(datatype, StringType):
                if data.isdigit():
                    res.append(convert_int_string_to_time(data))
                else:
                    res.append(
                        dateutil.parser.parse(data).time()
                        if auto_detect
                        else convert_string_to_time(data, time_fmt, fractional_seconds)
                    )
            elif isinstance(datatype, TimestampType):
                res.append(data.time())
            elif isinstance(datatype, VariantType):
                if isinstance(data, str):
                    if data.isdigit():
                        res.append(convert_int_string_to_time(data))
                    else:
                        # variant type does not support format input
                        res.append(dateutil.parser.parse(data).time())
                elif isinstance(data, datetime.time):
                    res.append(data)
                else:
                    SnowparkLocalTestingException.raise_from_error(
                        ValueError(
                            f"[Local Testing] Unsupported conversion to_time of value {data} of VariantType"
                        )
                    )
            else:
                SnowparkLocalTestingException.raise_from_error(
                    ValueError(
                        f"[Local Testing] Unsupported conversion to_time of data type {type(datatype).__name__}"
                    )
                )
        except BaseException as exc:
            if try_cast:
                data.append(None)
            else:
                SnowparkLocalTestingException.raise_from_error(exc)

    # TODO: TIME_OUTPUT_FORMAT is not supported, by default snowflake outputs time in the format HH24:MI:SS
    #  check https://snowflakecomputing.atlassian.net/browse/SNOW-1305979
    return ColumnEmulator(
        data=res, sf_type=ColumnType(TimeType(), column.sf_type.nullable)
    )


def _to_timestamp(
    column: ColumnEmulator,
    fmt: Optional[ColumnEmulator],
    try_cast: bool = False,
    add_timezone: bool = False,
    enforce_ltz=False,
):
    """
     https://docs.snowflake.com/en/sql-reference/functions/to_timestamp

    [x] For NULL input, the result will be NULL.

    [x] For string_expr: timestamp represented by a given string. If the string does not have a time component, midnight will be used.

    [x] For date_expr: timestamp representing midnight of a given day will be used, according to the specific timestamp flavor (NTZ/LTZ/TZ) semantics.

    [x] For timestamp_expr: a timestamp with possibly different flavor than the source timestamp.

    [x] For numeric_expr: a timestamp representing the number of seconds (or fractions of a second) provided by the user. Note, that UTC time is always used to build the result.

    For variant_expr:

        [x] If the variant contains JSON null value, the result will be NULL.

        [x] If the variant contains a timestamp value of the same kind as the result, this value will be preserved as is.

        [x] If the variant contains a timestamp value of the different kind, the conversion will be done in the same way as from timestamp_expr.

        [x] If the variant contains a string, conversion from a string value will be performed (using automatic format).

        [x] If the variant contains a number, conversion as if from numeric_expr will be performed.

    [x] If conversion is not possible, an error is returned.

    If the format of the input parameter is a string that contains an integer:

        After the string is converted to an integer, the integer is treated as a number of seconds, milliseconds, microseconds, or nanoseconds after the start of the Unix epoch (1970-01-01 00:00:00.000000000 UTC).

        [x] If the integer is less than 31536000000 (the number of milliseconds in a year), then the value is treated as a number of seconds.

        [x] If the value is greater than or equal to 31536000000 and less than 31536000000000, then the value is treated as milliseconds.

        [x] If the value is greater than or equal to 31536000000000 and less than 31536000000000000, then the value is treated as microseconds.

        [x] If the value is greater than or equal to 31536000000000000, then the value is treated as nanoseconds.
    """
    if len(column) == 0:
        return []

    import dateutil.parser

    fmt = (
        ColumnEmulator([fmt] * len(column), index=column.index)
        if not isinstance(fmt, ColumnEmulator)
        else fmt
    )

    def convert_timestamp(row):
        _fmt = fmt[row.name]
        data = row.iloc[0]
        auto_detect = _fmt is None or str(_fmt).lower() == "auto"
        default_format = "%Y-%m-%d %H:%M:%S.%f"

        if not isinstance(_fmt, numbers.Number):
            (
                timestamp_format,
                fractional_seconds,
            ) = convert_snowflake_datetime_format(_fmt, default_format=default_format)
        else:
            # if _fmt is a number, then snowflake expects <numeric_expr> + <scale>, format doesn't apply here
            timestamp_format, fractional_seconds = None, 0

        if data is None:
            return None
        try:

            datatype = column.sf_type.datatype
            if isinstance(datatype, TimestampType):
                # data is datetime.datetime type
                parsed = data
            elif isinstance(datatype, DateType):
                # data is datetime.date type
                parsed = datetime.datetime.combine(data, datetime.datetime.min.time())
            elif isinstance(datatype, StringType):
                # data is string type
                if data.isdigit() and auto_detect:
                    parsed = datetime.datetime.utcfromtimestamp(
                        convert_numeric_string_value_to_float_seconds(data)
                    )
                    # utc timestamps should be in utc timezone
                    if add_timezone:
                        parsed = parsed.replace(tzinfo=pytz.utc)
                else:
                    if auto_detect:
                        parsed = dateutil.parser.parse(data)
                    else:
                        parsed = datetime.datetime.strptime(
                            process_string_time_with_fractional_seconds(
                                data, fractional_seconds
                            ),
                            timestamp_format,
                        )
            elif isinstance(datatype, _NumericType):
                # handle scale
                scale = int(_fmt) if _fmt else 0
                data = data / 10**scale
                parsed = datetime.datetime.utcfromtimestamp(
                    convert_numeric_string_value_to_float_seconds(data)
                )
                # utc timestamps should be in utc timezone
                if add_timezone:
                    parsed = parsed.replace(tzinfo=pytz.utc)
            elif isinstance(datatype, VariantType):
                # An integer number of seconds or milliseconds.
                if isinstance(data, numbers.Number):
                    # check https://docs.snowflake.com/en/sql-reference/functions/to_timestamp#usage-notes
                    # "When an INTEGER value is cast directly to TIMESTAMP_NTZ ...
                    # However, if the INTEGER value is stored inside a VARIANT value,
                    # for example as shown below, then the conversion is indirect,
                    # and is affected by the local time zone, even though the final result is TIMESTAMP_NTZ:"
                    if enforce_ltz:
                        # local timestamp
                        local_now = datetime.datetime.now(LocalTimezone.LOCAL_TZ)
                        parsed = datetime.datetime.utcfromtimestamp(
                            data
                        ) + datetime.timedelta(
                            seconds=local_now.utcoffset().total_seconds()
                        )
                        return parsed
                    else:
                        parsed = datetime.datetime.utcfromtimestamp(data)
                    # utc timestamps should be in utc timezone
                    if add_timezone:
                        parsed = parsed.replace(tzinfo=pytz.utc)
                elif isinstance(data, str):
                    # A string containing an integer number of seconds or milliseconds.
                    if data.isdigit():
                        parsed = datetime.datetime.utcfromtimestamp(
                            convert_numeric_string_value_to_float_seconds(data)
                        )
                        # utc timestamps should be in utc timezone
                        if add_timezone:
                            parsed = parsed.replace(tzinfo=pytz.utc)
                    # A string from which to extract a timestamp.
                    else:
                        parsed = dateutil.parser.parse(data)
                # A timestamp.
                elif isinstance(data, datetime.datetime):
                    parsed = data
                else:
                    SnowparkLocalTestingException.raise_from_error(
                        TypeError(
                            f"[Local Testing] Unsupported conversion to_timestamp* of value {data} of VariantType"
                        )
                    )
            else:
                SnowparkLocalTestingException.raise_from_error(
                    TypeError(
                        f"[Local Testing] Unsupported conversion to_timestamp* of data type {type(column.sf_type.datatype).__name__}"
                    )
                )
            # Add the local timezone if tzinfo is missing and a tz is desired
            if parsed and add_timezone and parsed.tzinfo is None:
                parsed = LocalTimezone.replace_tz(parsed)
            return parsed
        except BaseException as exc:
            if try_cast:
                return None
            else:
                SnowparkLocalTestingException.raise_from_error(exc)

    res = column.to_frame().apply(convert_timestamp, axis=1).replace({pandas.NaT: None})
    return [
        x.to_pydatetime() if x is not None and hasattr(x, "to_pydatetime") else x
        for x in res
    ]


@patch("to_timestamp")
def mock_to_timestamp(
    column: ColumnEmulator,
    fmt: Optional[ColumnEmulator] = None,
    try_cast: bool = False,
):
    result = mock_to_timestamp_ntz(column, fmt, try_cast)

    result.sf_type = ColumnType(
        TimestampType(_TIMESTAMP_TYPE_TIMEZONE_MAPPING[_TIMESTAMP_TYPE_MAPPING]),
        column.sf_type.nullable,
    )
    return result


@patch("to_timestamp_ntz")
def mock_to_timestamp_ntz(
    column: ColumnEmulator,
    fmt: Optional[ColumnEmulator] = None,
    try_cast: bool = False,
):
    result = _to_timestamp(column, fmt, try_cast, enforce_ltz=True)
    # Cast to NTZ by removing tz data if present
    return ColumnEmulator(
        data=[
            try_convert(lambda x: x.replace(tzinfo=None), try_cast, x) for x in result
        ],
        sf_type=ColumnType(
            TimestampType(TimestampTimeZone.NTZ), column.sf_type.nullable
        ),
        dtype=object,
        index=column.index,
    )


@patch("to_timestamp_ltz")
def mock_to_timestamp_ltz(
    column: ColumnEmulator,
    fmt: Optional[ColumnEmulator] = None,
    try_cast: bool = False,
):
    result = _to_timestamp(column, fmt, try_cast, add_timezone=True)

    # Cast to ltz by providing an empty timezone when calling astimezone
    # datetime will populate with the local zone
    return ColumnEmulator(
        data=[LocalTimezone.to_local_timezone(x) for x in result],
        sf_type=ColumnType(
            TimestampType(TimestampTimeZone.LTZ), column.sf_type.nullable
        ),
        dtype=object,
        index=column.index,
    )


@patch("to_timestamp_tz")
def mock_to_timestamp_tz(
    column: ColumnEmulator,
    fmt: Optional[ColumnEmulator] = None,
    try_cast: bool = False,
):
    # _to_timestamp will use the tz present in the data.
    # Otherwise it adds an appropriate one by default.
    return ColumnEmulator(
        data=_to_timestamp(column, fmt, try_cast, add_timezone=True),
        sf_type=ColumnType(
            TimestampType(TimestampTimeZone.TZ), column.sf_type.nullable
        ),
        dtype=object,
        index=column.index,
    )


def try_convert(convert: Callable, try_cast: bool, val: Any):
    if val is None:
        return None
    try:
        return convert(val)
    except BaseException as exc:
        if try_cast:
            return None
        else:
            SnowparkLocalTestingException.raise_from_error(exc)


@patch("to_char")
def mock_to_char(
    column: ColumnEmulator,
    fmt: Optional[str] = None,
    try_cast: bool = False,
) -> ColumnEmulator:
    """
    https://docs.snowflake.com/en/sql-reference/functions/to_char
    [x] expr: An expression of any data type.
    [x] numeric_expr: A numeric expression.
        [ ] numeric_expr with format: not supported, check SNOW-1372863
    [x] date_or_time_expr: An expression of type DATE, TIME, or TIMESTAMP.
    [x] binary_expr: An expression of type BINARY or VARBINARY.

    """
    if isinstance(column.sf_type.datatype, StringType):
        return column.copy()

    source_datatype = column.sf_type.datatype

    if not isinstance(fmt, ColumnEmulator):
        fmt = ColumnEmulator([fmt] * len(column), index=column.index)

    def convert_char(data, _fmt):
        if isinstance(source_datatype, _NumericType):
            if _fmt:
                # SNOW-1372863 to support https://docs.snowflake.com/en/sql-reference/sql-format-models
                LocalTestOOBTelemetryService.get_instance().log_not_supported_error(
                    external_feature_name="Use format strings with Numeric types in TO_CHAR",
                    internal_feature_name="mock_to_char",
                    parameters_info={
                        "source_datatype": type(source_datatype).__name__,
                        "fmt": str(_fmt),
                    },
                    raise_error=NotImplementedError,
                )
            convert_numeric_to_str = (
                lambda x: "{:.{}f}".format(data, source_datatype.scale)
                if isinstance(source_datatype, DecimalType)
                else str(x)
            )
            return try_convert(convert_numeric_to_str, try_cast, data)
        elif isinstance(source_datatype, (DateType, TimeType)):
            default_format = _DEFAULT_OUTPUT_FORMAT.get(type(source_datatype))
            (format, _,) = convert_snowflake_datetime_format(
                _fmt, default_format=default_format, is_input_format=False
            )
            convert_date_time_to_str = (
                datetime.datetime.strftime
                if isinstance(source_datatype, DateType)
                else datetime.time.strftime
            )
            return try_convert(
                lambda x: convert_date_time_to_str(x, format), try_cast, data
            )
        elif isinstance(source_datatype, TimestampType):
            default_format = _DEFAULT_OUTPUT_FORMAT.get(TimestampType)
            (format, fractional_seconds,) = convert_snowflake_datetime_format(
                _fmt, default_format, is_input_format=False
            )
            # handle 3f, can use str index
            time_str = try_convert(
                lambda x: datetime.date.strftime(x, format), try_cast, data
            ).strip()
            # python doesn't offer a way to control digits in microseconds
            # when converting datatime into string using format %f, hence here we manually control the output fractional
            # microsecond parts
            # we find the beginning of 6 consecutive digits and manipulate the string
            # CAVEAT: this solution can not handle format like 'yyyymmddhhmmssff' in which case there are multiple
            # 6 digits occurrences, we cannot distinguish
            # whether the 6 digits are the part of fractional seconds or other parts of datetime
            if "%f" in format:
                pattern = r"\d{6}"
                if len(re.findall(pattern, time_str)) > 1:
                    # if we detect multiple consecutive 6 digits, it's a format we can't handle
                    LocalTestOOBTelemetryService.get_instance().log_not_supported_error(
                        external_feature_name=f"Use format string {_fmt} with TimestampType in TO_CHAR",
                        internal_feature_name="mock_to_char",
                        parameters_info={
                            "source_datatype": type(source_datatype).__name__,
                            "fmt": str(_fmt),
                        },
                        raise_error=NotImplementedError,
                    )
                start_idx = re.search(r"\d{6}", time_str).start()
                end_idx = start_idx + 6
                # truncate the microsecond string
                fractional_seconds_str = time_str[start_idx:end_idx][
                    0 : min(6, fractional_seconds)
                ]
                # concatenate the whole string
                time_str = (
                    time_str[:start_idx] + fractional_seconds_str + time_str[end_idx:]
                )
            return time_str
        elif isinstance(source_datatype, BinaryType):
            _fmt = (_fmt or "HEX").upper()
            fmt_decoder = {
                "HEX": binascii.hexlify,
                "BASE64": base64.b64encode,
                "UTF-8": lambda x: x,
            }.get(_fmt)

            if fmt_decoder is None:
                raise SnowparkLocalTestingException(f"Invalid binary format {fmt}")
            return try_convert(fmt_decoder, try_cast, data).decode()
        elif isinstance(source_datatype, BooleanType):
            return try_convert(lambda x: str(x).lower(), try_cast, data)
        elif isinstance(source_datatype, (VariantType, ArrayType, MapType)):
            from snowflake.snowpark.mock import CUSTOM_JSON_ENCODER

            # here we reuse CUSTOM_JSON_ENCODER to dump a python object to string
            # when handling string object, e.g., json.dumps("123"), by default json dumps added
            # double quotes to the output which we do not need in output, we strip the beginning and ending
            # double quote by calling strip('"'), this has no side effect to other input types.
            return try_convert(
                lambda x: json.dumps(
                    x,
                    cls=CUSTOM_JSON_ENCODER,
                    separators=(",", ":"),  # remove trailing space after the separators
                ).strip('"'),
                try_cast,
                data,
            )
        elif isinstance(source_datatype, (StringType, NullType)):
            return data
        else:
            LocalTestOOBTelemetryService.get_instance().log_not_supported_error(
                external_feature_name=f"Data type {type(source_datatype).__name__} in TO_CHAR",
                internal_feature_name="mock_to_char",
                parameters_info={
                    "source_datatype": type(source_datatype).__name__,
                    "fmt": str(_fmt),
                },
                raise_error=NotImplementedError,
            )

    res = column.combine(fmt, convert_char)
    res.sf_type = ColumnType(StringType(), column.sf_type.nullable)
    return res


@patch("to_varchar")
def mock_to_varchar(*args, **kwargs) -> ColumnEmulator:
    return mock_to_char(*args, **kwargs)


@patch("to_double")
def mock_to_double(
    column: ColumnEmulator, fmt: Optional[str] = None, try_cast: bool = False
) -> ColumnEmulator:
    """
        [x] Fixed-point numbers are converted to floating point; the conversion cannot fail, but might result in loss of precision.

        [x] Strings are converted as decimal integer or fractional numbers, scientific notation and special values (nan, inf, infinity) are accepted.

        For VARIANT input:

        [x] If the variant contains a fixed-point value, the numeric conversion will be performed.

        [x] If the variant contains a floating-point value, the value will be preserved unchanged.

        [x] If the variant contains a string, a string conversion will be performed.

        [x] If the variant contains a Boolean value, the result will be 0 or 1 (for false and true, correspondingly).

        [x] If the variant contains JSON null value (None in Python), the output will be NULL.

    Note that conversion of decimal fractions to binary and back is not precise (i.e. printing of a floating-point number converted from decimal representation might produce a slightly diffe
    """
    if isinstance(column.sf_type.datatype, (FloatType, DoubleType)):
        return column.copy()
    if fmt is not None:
        LocalTestOOBTelemetryService.get_instance().log_not_supported_error(
            external_feature_name="Using format strings in TO_DOUBLE",
            internal_feature_name="mock_to_double",
            parameters_info={"fmt": str(fmt)},
            raise_error=NotImplementedError,
        )
    if isinstance(
        column.sf_type.datatype, (_NumericType, StringType, VariantType, NullType)
    ):
        res = column.apply(lambda x: try_convert(float, try_cast, x))
        res.sf_type = ColumnType(DoubleType(), column.sf_type.nullable or res.hasnans)
        return res
    else:
        SnowparkLocalTestingException.raise_from_error(
            TypeError(
                f"[Local Testing] Invalid type {column.sf_type.datatype} for parameter 'TO_DOUBLE'"
            )
        )


@patch("to_boolean")
def mock_to_boolean(column: ColumnEmulator, try_cast: bool = False) -> ColumnEmulator:
    """
    [x] For a text expression, the string must be:

        'true', 't', 'yes', 'y', 'on', '1' return TRUE.

        'false', 'f', 'no', 'n', 'off', '0' return FALSE.

        All other strings return an error.

        Strings are case-insensitive.

    [x] For a numeric expression:

        0 returns FALSE.

        All non-zero numeric values return TRUE.

        When converting from the FLOAT data type, non-numeric values, such as ‘NaN’ (not a number) and ‘INF’ (infinity), cause an error.


    """
    if isinstance(column.sf_type.datatype, BooleanType):
        return column.copy()
    if isinstance(column.sf_type.datatype, StringType):

        def convert_str_to_bool(x: Optional[str]):
            if x is None:
                return None
            elif x.lower() in ("true", "t", "yes", "y", "on", "1"):
                return True
            elif x.lower() in ("false", "f", "no", "n", "off", "0"):
                return False
            raise SnowparkLocalTestingException(f"Boolean value {x} is not recognized")

        new_col = column.apply(lambda x: try_convert(convert_str_to_bool, try_cast, x))
        new_col.sf_type = ColumnType(BooleanType(), column.sf_type.nullable)
        return new_col
    elif isinstance(column.sf_type.datatype, _NumericType):

        def convert_num_to_bool(x: Optional[Real]):
            if x is None:
                return None
            elif math.isnan(x) or math.isinf(x):
                raise SnowparkLocalTestingException(
                    f"Invalid value {x} for parameter 'TO_BOOLEAN'"
                )
            else:
                return x != 0

        new_col = column.apply(lambda x: try_convert(convert_num_to_bool, try_cast, x))
        new_col.sf_type = ColumnType(BooleanType(), column.sf_type.nullable)
        return new_col
    elif isinstance(column.sf_type.datatype, VariantType):
        new_col = column.apply(lambda x: try_convert(bool, try_cast, x))
        new_col.sf_type = ColumnType(BooleanType(), column.sf_type.nullable)
        return new_col
    elif isinstance(column.sf_type.datatype, VariantType):
        new_col = column.apply(lambda x: try_convert(bool, try_cast, x))
        new_col.sf_type = ColumnType(BooleanType(), column.sf_type.nullable)
        return new_col
    else:
        raise SnowparkLocalTestingException(
            f"Invalid type {column.sf_type.datatype} for parameter 'TO_BOOLEAN'"
        )


@patch("to_binary")
def mock_to_binary(
    column: ColumnEmulator, fmt: str = None, try_cast: bool = False
) -> ColumnEmulator:
    """
    [x] TO_BINARY( <string_expr> [, '<format>'] )
    [x] TO_BINARY( <variant_expr> )
    """
    if isinstance(column.sf_type.datatype, BinaryType):
        return column.copy()

    fmt = fmt.upper() if fmt else "HEX"
    fmt_decoder = {
        "HEX": binascii.unhexlify,
        "BASE64": base64.b64decode,
        "UTF-8": lambda x: x.encode("utf-8"),
    }.get(fmt)

    if fmt is None:
        raise SnowparkLocalTestingException(f"Invalid binary format {fmt}")

    if isinstance(column.sf_type.datatype, (StringType, NullType, VariantType)):
        res = column.apply(lambda x: try_convert(fmt_decoder, try_cast, x))
        res.sf_type = ColumnType(BinaryType(), column.sf_type.nullable)
        return res
    else:
        raise SnowparkLocalTestingException(
            f"Invalid type {column.sf_type.datatype} for parameter 'TO_BINARY'"
        )


@patch("coalesce")
def mock_coalesce(*exprs):
    if len(exprs) < 2:
        raise SnowparkLocalTestingException(
            f"not enough arguments for function [COALESCE], got {len(exprs)}, expected at least two"
        )
    res = pandas.Series(
        exprs[0]
    )  # workaround because sf_type is not inherited properly
    for expr in exprs:
        res = res.combine_first(expr)
    return ColumnEmulator(data=res, sf_type=exprs[0].sf_type, dtype=object)


@patch("substring")
def mock_substring(
    base_expr: ColumnEmulator, start_expr: ColumnEmulator, length_expr: ColumnEmulator
):
    res = [
        None if string is None else string[start : start + length]
        for string, start, length in zip(
            base_expr, [max(0, s - 1) for s in start_expr], length_expr
        )
    ]
    res = ColumnEmulator(
        res, sf_type=ColumnType(StringType(), base_expr.sf_type.nullable), dtype=object
    )
    return res


@patch("startswith")
def mock_startswith(expr1: ColumnEmulator, expr2: ColumnEmulator):
    res = [x.startswith(y) if x is not None else None for x, y in zip(expr1, expr2)]
    res = ColumnEmulator(
        res, sf_type=ColumnType(BooleanType(), expr1.sf_type.nullable), dtype=bool
    )
    return res


@patch("endswith")
def mock_endswith(expr1: ColumnEmulator, expr2: ColumnEmulator):
    res = [x.endswith(y) if x is not None else None for x, y in zip(expr1, expr2)]
    res = ColumnEmulator(
        res, sf_type=ColumnType(BooleanType(), expr1.sf_type.nullable), dtype=bool
    )
    return res


@patch("row_number", pass_row_index=True)
def mock_row_number(row_index: int):
    res = ColumnEmulator(data=[row_index + 1], sf_type=ColumnType(LongType(), False))
    return res


@patch("parse_json")
def mock_parse_json(expr: ColumnEmulator):
    from snowflake.snowpark.mock import CUSTOM_JSON_DECODER

    if isinstance(expr.sf_type.datatype, StringType):
        res = expr.apply(
            lambda x: try_convert(
                partial(json.loads, cls=CUSTOM_JSON_DECODER), False, x
            )
        )
    else:
        res = expr.copy()
    res.sf_type = ColumnType(VariantType(), expr.sf_type.nullable)
    return res


@patch("to_array")
def mock_to_array(expr: ColumnEmulator):
    """
    [x] If the input is an ARRAY, or VARIANT containing an array value, the result is unchanged.

    [x] For NULL or a JSON null input, returns NULL.

    [x] For any other value, the result is a single-element array containing this value.
    """
    if isinstance(expr.sf_type.datatype, ArrayType):
        return expr.copy()
    if isinstance(expr.sf_type.datatype, VariantType):
        from snowflake.snowpark.mock import CUSTOM_JSON_DECODER

        def convert_variant_to_array(val):
            if type(val) is str:
                val = json.loads(val, cls=CUSTOM_JSON_DECODER)
            if val is None or type(val) is list:
                return val
            else:
                return [val]

        res = expr.apply(lambda x: try_convert(convert_variant_to_array, False, x))
    else:
        res = expr.apply(lambda x: try_convert(lambda y: [y], False, x))
    res.sf_type = ColumnType(ArrayType(), expr.sf_type.nullable)
    return res


@patch("strip_null_value")
def mock_strip_null_value(expr: ColumnEmulator):
    return ColumnEmulator(
        [None if x == "null" else x for x in expr],
        sf_type=ColumnType(expr.sf_type.datatype, True),
    )


@patch("to_object")
def mock_to_object(expr: ColumnEmulator):
    """
    [x] For a VARIANT value containing an OBJECT, returns the OBJECT.

    [x] For NULL input, or for a VARIANT value containing only JSON null, returns NULL.

    [x] For an OBJECT, returns the OBJECT itself.

    [x] For all other input values, reports an error.
    """
    if isinstance(expr.sf_type.datatype, (MapType, NullType)):
        res = expr.copy()
    elif isinstance(expr.sf_type.datatype, VariantType):
        from snowflake.snowpark.mock import CUSTOM_JSON_DECODER

        def convert_variant_to_object(val):
            if type(val) is str:
                val = json.loads(val, cls=CUSTOM_JSON_DECODER)
            if val is None or type(val) is dict:
                return val
            raise SnowparkLocalTestingException(
                f"Invalid object of type {type(val)} passed to 'TO_OBJECT'"
            )

        res = expr.apply(lambda x: try_convert(convert_variant_to_object, False, x))
    else:
        raise SnowparkLocalTestingException(
            f"Invalid type {type(expr.sf_type.datatype)} parameter 'TO_OBJECT'"
        )

    res.sf_type = ColumnType(MapType(), expr.sf_type.nullable)
    return res


@patch("to_variant")
def mock_to_variant(expr: ColumnEmulator):
    res = expr.copy()
    res.sf_type = ColumnType(VariantType(), expr.sf_type.nullable)
    return res


def _object_construct(exprs, drop_nulls):
    expr_count = len(exprs)
    if expr_count % 2 != 0:
        SnowparkLocalTestingException.raise_from_error(
            TypeError(
                f"Cannot construct an object from an odd number ({expr_count}) of values."
            )
        )

    if expr_count == 0:
        return ColumnEmulator(data=[dict()])

    def construct_dict(x):
        return {
            x[i]: x[i + 1]
            for i in range(0, expr_count, 2)
            if x[i] is not None and not (drop_nulls and x[i + 1] is None)
        }

    combined = pandas.concat(exprs, axis=1, ignore_index=True)
    return combined.apply(construct_dict, axis=1)


@patch("object_construct")
def mock_object_construct(*exprs: ColumnEmulator) -> ColumnEmulator:
    result = _object_construct(exprs, True)
    result.sf_type = ColumnType(MapType(StringType(), StringType()), False)
    return result


@patch("object_construct_keep_null")
def mock_object_construct_keep_null(*exprs: ColumnEmulator) -> ColumnEmulator:
    result = _object_construct(exprs, False)
    result.sf_type = ColumnType(MapType(StringType(), StringType()), True)
    return result


def cast_to_datetime(date):
    if isinstance(date, datetime.datetime):
        return date
    return datetime.datetime.fromordinal(date.toordinal())


def add_years(date, duration):
    return date.replace(year=date.year + duration)


def add_months(scalar, date, duration):
    res = (
        pandas.to_datetime(date) + pandas.DateOffset(months=scalar * duration)
    ).to_pydatetime()

    if not isinstance(date, datetime.datetime):
        res = res.date()

    return res


def add_timedelta(unit, date, duration, scalar=1):
    if date is None:
        return date
    return date + datetime.timedelta(**{f"{unit}s": float(duration) * scalar})


@patch("dateadd")
def mock_dateadd(
    part: str, value_expr: ColumnEmulator, datetime_expr: ColumnEmulator
) -> ColumnEmulator:
    # Extract a standardized name
    part = unalias_datetime_part(part)
    sf_type = datetime_expr.sf_type
    ts_type = ColumnType(
        TimestampType(TimestampTimeZone.NTZ), datetime_expr.sf_type.nullable
    )

    def nop(x):
        return x

    cast = nop

    # Create a lambda that applies the transformation
    # If the time unit is smaller than a day date types will be cast to datetime types
    if part == "year":
        func = add_years
    elif part == "quarter" or part == "month":
        scalar = 3 if part == "quarter" else 1
        func = partial(add_months, scalar)
    elif part in {"day", "week"}:
        func = partial(add_timedelta, part)
    elif part in {"second", "microsecond", "millisecond", "minute", "hour"}:
        func = partial(add_timedelta, part)
        cast = cast_to_datetime
        sf_type = ts_type
    elif part == "nanosecond":
        func = partial(add_timedelta, "microsecond", scalar=1 / 1000)
        cast = cast_to_datetime
        sf_type = ts_type
    else:
        SnowparkLocalTestingException.raise_from_error(
            ValueError(f"{part} is not a recognized date or time part.")
        )

    res = datetime_expr.combine(
        value_expr, lambda date, duration: func(cast(date), duration)
    )
    return ColumnEmulator(res, sf_type=sf_type)


@patch("date_part")
def mock_date_part(part: str, datetime_expr: ColumnEmulator):
    """
    SNOW-1183874: Add support for relevant session parameters.
    https://docs.snowflake.com/en/sql-reference/functions/date_part#usage-notes
    """
    unaliased = unalias_datetime_part(part)
    datatype = datetime_expr.sf_type.datatype

    # Year of week is another alias unique to date_part
    if unaliased == "yearofweek":
        unaliased = "year"

    if unaliased in {"year", "month", "day"} or (
        isinstance(datatype, TimestampType)
        and unaliased in {"hour", "minute", "second", "microsecond"}
    ):
        res = datetime_expr.apply(lambda x: getattr(x, unaliased, None))
    elif unaliased in {"week", "weekiso"}:
        res = pandas.to_datetime(datetime_expr).dt.isocalendar().week
    elif unaliased == "yearofweekiso":
        res = pandas.to_datetime(datetime_expr).dt.isocalendar().year
    elif unaliased in {"quarter", "dayofyear"}:
        res = getattr(pandas.to_datetime(datetime_expr).dt, unaliased, None)
    elif unaliased in {"dayofweek", "dayofweekiso"}:
        # Pandas has Monday as 0 while Snowflake uses Sunday as 0
        res = (pandas.to_datetime(datetime_expr).dt.dayofweek + 1) % 7
    elif unaliased == "nanosecond" and isinstance(datatype, TimestampType):
        res = datetime_expr.apply(lambda x: None if x is None else x.microsecond * 1000)
    elif unaliased in {
        "epoch_second",
        "epoch_millisecond",
        "epoch_microsecond",
        "epoch_nanosecond",
    }:
        if isinstance(datatype, DateType):
            datetime_expr = datetime_expr.apply(cast_to_datetime)

        # datetime.datetime.timestamp assumes no tz means local time. Snowflake assumes no tz means UTC time
        if isinstance(datatype, TimestampType) and datatype.tz in {
            TimestampTimeZone.DEFAULT,
            TimestampTimeZone.NTZ,
        }:
            datetime_expr = datetime_expr.apply(
                lambda x: None if x is None else x.replace(tzinfo=pytz.UTC)
            )

        # Part of the conversion happens as floating point arithmetic. Going from microseconds to nanoseconds
        # introduces floating point precision instability so do the final part of the conversion after int conversion
        multiplier = 1
        post = 1
        if unaliased == "epoch_millisecond":
            multiplier = 1000
        elif unaliased == "epoch_microsecond":
            multiplier = 1000000
        elif unaliased == "epoch_nanosecond":
            multiplier = 1000000
            post = 1000

        res = datetime_expr.apply(
            lambda x: None if x is None else int(x.timestamp() * multiplier) * post
        )
    elif unaliased == "timezone_hour":
        res = datetime_expr.apply(
            lambda x: None if x is None else int((x.strftime("%z") or "0000")[:-2])
        )
    elif unaliased == "timezone_minute":
        res = datetime_expr.apply(
            lambda x: None if x is None else int((x.strftime("%z") or "0000")[-2:])
        )
    else:
        SnowparkLocalTestingException.raise_from_error(
            ValueError(
                f"{part} is an invalid date part for column of type {datatype.__class__.__name__}"
            )
        )
    return ColumnEmulator(res, sf_type=ColumnType(LongType(), nullable=True))


@patch("date_trunc")
def mock_date_trunc(part: str, datetime_expr: ColumnEmulator) -> ColumnEmulator:
    """
    SNOW-1183874: Add support for relevant session parameters.
    https://docs.snowflake.com/en/sql-reference/functions/date_part#usage-notes
    """
    # Map snowflake time unit to pandas rounding alias
    # Not all units have an alias so handle those with a special case
    SUPPORTED_UNITS = {
        "day": "D",
        "hour": "h",
        "microsecond": "us",
        "millisecond": "ms",
        "minute": "min",
        "month": None,
        "nanosecond": "ns",
        "quarter": None,
        "second": "s",
        "week": None,
        "year": None,
    }
    time_unit = unalias_datetime_part(part)
    pandas_unit = SUPPORTED_UNITS.get(time_unit)

    if pandas_unit is not None:
        truncated = pandas.to_datetime(datetime_expr).dt.floor(pandas_unit)
    elif time_unit == "month":
        truncated = datetime_expr.apply(
            lambda x: datetime.datetime(
                x.year, x.month, 1, tzinfo=getattr(x, "tzinfo", None)
            )
        )
    elif time_unit == "quarter":
        # Assuming quarters start in Jan/April/July/Oct
        quarter_map = {i: (((i - 1) // 3) * 3) + 1 for i in range(1, 13)}
        truncated = datetime_expr.apply(
            lambda x: datetime.datetime(
                x.year, quarter_map[x.month], 1, tzinfo=getattr(x, "tzinfo", None)
            )
        )
    elif time_unit == "week":
        truncated = pandas.to_datetime(datetime_expr)
        # Calculate offset from start of week
        offsets = pandas.to_timedelta(truncated.dt.dayofweek, unit="d")
        # Subtract off offset
        truncated = truncated.combine(offsets, operator.sub)
        # Trim data smaller than a day
        truncated = truncated.apply(
            lambda x: datetime.datetime(
                x.year, x.month, x.day, tzinfo=getattr(x, "tzinfo", None)
            )
        )
    elif time_unit == "year":
        truncated = datetime_expr.apply(
            lambda x: datetime.datetime(x.year, 1, 1, tzinfo=getattr(x, "tzinfo", None))
        )
    else:
        SnowparkLocalTestingException.raise_from_error(
            ValueError(f"{part} is not a supported time unit for date_trunc.")
        )

    if isinstance(datetime_expr.sf_type.datatype, DateType):
        truncated = truncated.dt.date

    return ColumnEmulator(truncated, sf_type=datetime_expr.sf_type)


@patch("datediff")
def mock_datediff(
    part: str, col1: ColumnEmulator, col2: ColumnEmulator
) -> ColumnEmulator:
    from dateutil import relativedelta

    time_unit = unalias_datetime_part(part)

    if time_unit in {
        "week",
        "day",
        "hour",
        "minute",
        "second",
        "millisecond",
        "microsecond",
    }:

        def func(x, y):
            return (y - x) // datetime.timedelta(**{f"{time_unit}s": 1})

    elif time_unit in {"year", "month"}:
        if time_unit == "year":
            denom = 12
        else:
            denom = 1

        def func(x, y):
            delta = relativedelta.relativedelta(y, x)
            return ((delta.years * 12) + delta.months) // denom

    else:
        raise SnowparkLocalTestingException(
            f"Specified part {part} is not supported by local testing datediff."
        )

    data = []
    for x, y in zip(col1, col2):
        data.append(None if x is None or y is None else func(x, y))

    return ColumnEmulator(
        pandas.Series(data, dtype=object),
        sf_type=ColumnType(LongType(), col1.sf_type.nullable and col2.sf_type.nullable),
    )


CompareType = TypeVar("CompareType")


def _compare(x: CompareType, y: Any) -> Tuple[CompareType, CompareType]:
    """
    Compares two values based on the rules described for greatest/least
    https://docs.snowflake.com/en/sql-reference/functions/least#usage-notes

    SNOW-1065554: For now this only handles basic numeric and string coercions.
    """
    if x is None or y is None:
        return (None, None)

    _x = x
    if isinstance(x, str):
        try:
            _x = float(x)
        except ValueError:
            pass

    _y = y if type(_x) is type(y) else type(_x)(y)

    if _x > _y:
        return (_x, _y)
    else:
        return (_y, _x)


def _least(x: CompareType, y: Any) -> Union[CompareType, float]:
    return _compare(x, y)[1]


def _greatest(x: CompareType, y: Any) -> Union[CompareType, float]:
    return _compare(x, y)[0]


@patch("greatest")
def mock_greatest(*exprs: ColumnEmulator):
    result = reduce(lambda x, y: x.combine(y, _greatest), exprs)
    result.sf_type = exprs[0].sf_type
    return result


@patch("least")
def mock_least(*exprs: ColumnEmulator):
    result = reduce(lambda x, y: x.combine(y, _least), exprs)
    result.sf_type = exprs[0].sf_type
    return result


@patch("upper")
def mock_upper(expr: ColumnEmulator):
    return expr.str.upper()


@patch("lower")
def mock_lower(expr: ColumnEmulator):
    return expr.str.lower()


@patch("length")
def mock_length(expr: ColumnEmulator):
    result = expr.str.len()
    result.sf_type = ColumnType(LongType(), nullable=expr.sf_type.nullable)
    return result


# See https://docs.snowflake.com/en/sql-reference/functions/initcap for list of delimiters
DEFAULT_INITCAP_DELIMITERS = set('!?@"^#$&~_,.:;+-*%/|\\[](){}<>' + string.whitespace)


def _initcap(value: Optional[str], delimiters: Optional[str]) -> str:
    if value is None:
        return None

    delims = DEFAULT_INITCAP_DELIMITERS if delimiters is None else set(delimiters)

    result = ""
    cap = True
    for char in value:
        if cap:
            result += char.upper()
        else:
            result += char.lower()
        cap = char in delims
    return result


@patch("initcap")
def mock_initcap(values: ColumnEmulator, delimiters: ColumnEmulator):
    result = values.combine(delimiters, _initcap)
    result.sf_type = values.sf_type
    return result


@patch("convert_timezone")
def mock_convert_timezone(
    *args: ColumnEmulator,
) -> ColumnEmulator:
    """Converts the given source_time to the target timezone.

    For timezone information, refer to the `Snowflake SQL convert_timezone notes <https://docs.snowflake.com/en/sql-reference/functions/convert_timezone.html#usage-notes>`_
    """
    import dateutil

    # mock_convert_timezone matches the sql function call semantics.
    # It has different parameters when called with 2 or 3 args.
    # When called with two args, the third will be replaced with None.
    if args[2] is None:
        target_timezone, source_time, _ = args
        source_timezone = pandas.Series([None] * len(source_time))
        return_type = TimestampTimeZone.TZ
    else:
        source_timezone, target_timezone, source_time = args
        return_type = TimestampTimeZone.NTZ
        if source_time.sf_type.datatype.tz is not TimestampTimeZone.NTZ:
            raise ValueError(
                "[Local Testing] convert_timezone can only convert NTZ timestamps when source_timezone is specified."
            )

    combined = pandas.concat(
        [source_timezone, target_timezone, source_time], axis=1, ignore_index=True
    )

    def _convert(row):
        source_timezone, target_timezone, source_time = row
        if source_time is None:
            return None

        if source_timezone is not None:
            # Using dateutil because it uses iana timezones while pytz would use Olson tzdb.
            source_time = source_time.replace(tzinfo=dateutil.tz.gettz(source_timezone))

        if source_time.tzinfo is None:
            source_time = LocalTimezone.replace_tz(source_time)

        result = source_time.astimezone(dateutil.tz.gettz(target_timezone))

        if return_type == TimestampTimeZone.NTZ:
            result = result.replace(tzinfo=None)

        return result

    res = combined.apply(_convert, axis=1)

    return ColumnEmulator(
        res,
        sf_type=ColumnType(
            TimestampType(return_type), nullable=source_time.sf_type.nullable
        ),
        dtype=object,
    )


@patch("current_session", pass_column_index=True)
def mock_current_session(column_index):
    session = snowflake.snowpark.session._get_active_session()
    return ColumnEmulator(
        data=[str(hash(session))] * len(column_index),
        sf_type=ColumnType(StringType(), False),
    )


@patch("current_database", pass_column_index=True)
def mock_current_database(column_index):
    session = snowflake.snowpark.session._get_active_session()
    return ColumnEmulator(
        data=[session.get_current_database()] * len(column_index),
        sf_type=ColumnType(StringType(), False),
    )


@patch("get")
def mock_get(
    column_expression: ColumnEmulator, value_expression: ColumnEmulator
) -> ColumnEmulator:
    def get(obj, key):
        try:
            if isinstance(obj, list) and key < len(obj):
                return obj[key]
            elif isinstance(obj, dict):
                return obj.get(key, None)
            else:
                return None
        except KeyError:
            return None

    # pandas.Series.combine does not work here because it will not allow Nones in int columns
    result = []
    for exp, k in zip(column_expression, value_expression):
        result.append(get(exp, k))

    return ColumnEmulator(
        result,
        sf_type=ColumnType(column_expression.sf_type.datatype, True),
        dtype=object,
    )


@patch("concat")
def mock_concat(*columns: ColumnEmulator) -> ColumnEmulator:
    if len(columns) < 1:
        SnowparkLocalTestingException.raise_from_error(
            ValueError("concat expects one or more column(s) to be passed in.")
        )
    pdf = pandas.concat(columns, axis=1).reset_index(drop=True)
    result = pdf.T.apply(
        lambda c: None if c.isnull().values.any() else c.astype(str).str.cat()
    )
    result.sf_type = ColumnType(StringType(), result.hasnans)
    return result


@patch("concat_ws")
def mock_concat_ws(*columns: ColumnEmulator) -> ColumnEmulator:
    if len(columns) < 2:
        SnowparkLocalTestingException.raise_from_error(
            ValueError(
                "concat_ws expects a seperator column and one or more value column(s) to be passed in."
            )
        )
    pdf = pandas.concat(columns, axis=1).reset_index(drop=True)
    result = pdf.T.apply(
        lambda c: None
        if c.isnull().values.any()
        else c[1:].astype(str).str.cat(sep=c[0])
    )
    result.sf_type = ColumnType(StringType(), result.hasnans)
    return result


def cast_column_to(
    col: ColumnEmulator, target_column_type: ColumnType, try_cast: bool = False
) -> Optional[ColumnEmulator]:
    # col.sf_type.nullable = target_column_type.nullable
    target_data_type = target_column_type.datatype
    if col.sf_type == target_column_type:
        return col
    if isinstance(target_data_type, DateType):
        return mock_to_date(col, try_cast=try_cast)
    if isinstance(target_data_type, TimeType):
        return mock_to_time(col, try_cast=try_cast)
    if isinstance(target_data_type, TimestampType):
        if target_data_type.tz is TimestampTimeZone.LTZ:
            return mock_to_timestamp_ltz(col, try_cast=try_cast)
        elif target_data_type.tz is TimestampTimeZone.NTZ:
            return mock_to_timestamp_ntz(col, try_cast=try_cast)
        elif target_data_type.tz is TimestampTimeZone.TZ:
            return mock_to_timestamp_tz(col, try_cast=try_cast)
        else:
            return mock_to_timestamp(col, try_cast=try_cast)
    if isinstance(target_data_type, DecimalType):
        return mock_to_decimal(
            col,
            precision=target_data_type.precision,
            scale=target_data_type.scale,
            try_cast=try_cast,
        )
    if isinstance(
        target_data_type, _IntegralType
    ):  # includes ByteType, ShortType, IntegerType, LongType
        res = mock_to_decimal(col, try_cast=try_cast)
        res.sf_type = ColumnType(target_data_type, nullable=True)
        return res
    if isinstance(target_data_type, BinaryType):
        return mock_to_binary(col, try_cast=try_cast)
    if isinstance(target_data_type, BooleanType):
        return mock_to_boolean(col, try_cast=try_cast)
    if isinstance(target_data_type, StringType):
        return mock_to_char(col, try_cast=try_cast)
    if isinstance(target_data_type, _FractionalType):
        return mock_to_double(col, try_cast=try_cast)
    if isinstance(target_data_type, MapType):
        return mock_to_object(col)
    if isinstance(target_data_type, ArrayType):
        return mock_to_array(col)
    if isinstance(target_data_type, VariantType):
        return mock_to_variant(col)
    return None


@patch("iff")
def mock_iff(condition: ColumnEmulator, expr1: ColumnEmulator, expr2: ColumnEmulator):
    assert isinstance(condition.sf_type.datatype, BooleanType)

    coerce_result = get_coerce_result_type(expr1.sf_type, expr2.sf_type)
    if all(condition) or all(~condition) or coerce_result is not None:
        res = ColumnEmulator(data=[None] * len(condition), dtype=object)
        expr1 = cast_column_to(expr1, coerce_result)
        expr2 = cast_column_to(expr2, coerce_result)
        res.where(condition, other=expr2, inplace=True)
        res.where([not x for x in condition], other=expr1, inplace=True)
        res.sf_type = coerce_result
        return res
    else:
        raise SnowparkLocalTestingException(
            f"[Local Testing] expr1 and expr2 have conflicting datatypes that cannot be coerced: {expr1.sf_type} <-> {expr2.sf_type}"
        )


@patch("random", pass_column_index=True)
def mock_random(seed: Optional[int] = None, column_index=None) -> ColumnEmulator:
    rand_min = -(2**63)
    rand_max = 2**63 - 1
    seed = seed if seed is not None else randint(rand_min, rand_max)
    gen = numpy.random.Generator(numpy.random.MT19937(abs(seed)))
    return ColumnEmulator(
        data=[gen.integers(rand_min, rand_max) for _ in range(len(column_index))],
        sf_type=ColumnType(LongType(), False),
    )


def _rank(raw_input, dense=False):
    """
    Returns a series containing the rank of a row within an ordered TableEmulator.
    Args:
        raw_input: The TableEmulator to apply the rank to.
        dense: When dense is false ranks are skipped when there are repeated values. When set to true
            ranks are not skipped. eg.
            -----------------------------------
            |"VALUE"  |"RANK"  |"DENSE_RANK"  |
            -----------------------------------
            |1        |1       |1             |
            |1        |1       |1             |
            |2        |3       |2             |
            |3        |4       |3             |
            -----------------------------------
    """
    final_values = []
    rank = 0
    index = 0
    previous = None
    for value in raw_input[raw_input.sorted_by].apply(tuple, 1):
        index += 1
        if value != previous:
            if dense:
                rank = rank + 1
            else:
                rank = index
        previous = value
        final_values.append(rank)

    return pandas.Series(final_values, index=raw_input.index)


@patch("rank", pass_input_data=True, pass_row_index=True)
def mock_rank(raw_input: ColumnEmulator, row_index: int) -> ColumnEmulator:
    rank = _rank(raw_input)
    return ColumnEmulator(
        data=rank.iloc[row_index], sf_type=ColumnType(LongType(), False)
    )


@patch("dense_rank", pass_input_data=True, pass_row_index=True)
def mock_dense_rank(raw_input: ColumnEmulator, row_index: int) -> ColumnEmulator:
    rank = _rank(raw_input, True)
    return ColumnEmulator(
        data=rank.iloc[row_index], sf_type=ColumnType(LongType(), False)
    )


@patch("percent_rank", pass_input_data=True, pass_row_index=True)
def mock_percent_rank(raw_input: ColumnEmulator, row_index: int) -> ColumnEmulator:
    length = len(raw_input) - 1
    rank = _rank(raw_input).apply(lambda x: (x - 1.0) / length)
    return ColumnEmulator(
        data=rank.iloc[row_index], sf_type=ColumnType(DoubleType(), False)
    )


@patch("cume_dist", pass_input_data=True, pass_row_index=True)
def mock_cume_dist(raw_input: ColumnEmulator, row_index: int) -> ColumnEmulator:
    # Calculate dense rank
    rank = _rank(raw_input, True)

    # Get distribution of values
    agged = rank.value_counts().sort_index()

    # Calculate probability distribution
    pdf = agged.apply(lambda x: x / rank.size)

    # Compute cumulative probability
    cdf = pdf.cumsum()

    # Map cumulative probability back to rank
    cume_dist = rank.map(cdf)

    return ColumnEmulator(
        cume_dist.iloc[row_index], sf_type=ColumnType(DoubleType(), False)
    )


@patch("ntile", pass_input_data=True, pass_row_index=True)
def mock_ntile(ntile: int, raw_input: ColumnEmulator, row_index: int) -> ColumnEmulator:
    current_ntile = ntile.iloc[row_index]
    if current_ntile <= 0:
        raise SnowparkLocalTestingException("NTILE argument must be at least 1")

    num_rows = raw_input.shape[0]
    if num_rows <= current_ntile:
        bucket = row_index + 1
    else:
        bucket = math.floor(row_index * current_ntile / num_rows) + 1

    return ColumnEmulator([bucket], sf_type=ColumnType(LongType(), False))


@patch("any_value")
def mock_any_value(col: ColumnEmulator):
    return ColumnEmulator(
        col.sample(1),
        sf_type=col.sf_type,
    )
