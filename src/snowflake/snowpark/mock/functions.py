#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import base64
import binascii
import datetime
import json
import math
from copy import copy
from decimal import Decimal
from functools import partial
from numbers import Real
from typing import Callable, Optional, Union

from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.mock.snowflake_data_type import (
    ColumnEmulator,
    ColumnType,
    TableEmulator,
)
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    MapType,
    NullType,
    StringType,
    TimestampType,
    TimeType,
    VariantType,
    _NumericType,
)

from .util import (
    convert_snowflake_datetime_format,
    process_numeric_time,
    process_string_time_with_fractional_seconds,
)

RETURN_TYPE = Union[ColumnEmulator, TableEmulator]

_MOCK_FUNCTION_IMPLEMENTATION_MAP = {}


def _register_func_implementation(
    snowpark_func: Union[str, Callable], func_implementation: Callable
):
    try:
        _MOCK_FUNCTION_IMPLEMENTATION_MAP[snowpark_func.__name__] = func_implementation
    except AttributeError:
        _MOCK_FUNCTION_IMPLEMENTATION_MAP[snowpark_func] = func_implementation


def _unregister_func_implementation(snowpark_func: Union[str, Callable]):
    try:
        try:
            del _MOCK_FUNCTION_IMPLEMENTATION_MAP[snowpark_func.__name__]
        except AttributeError:
            del _MOCK_FUNCTION_IMPLEMENTATION_MAP[snowpark_func]
    except KeyError:
        pass


def try_convert(convert, try_cast, val):
    if val is None:
        return None
    try:
        return convert(val)
    except BaseException:
        if try_cast:
            return None
        else:
            raise


def patch(function):
    def decorator(mocking_function):
        _register_func_implementation(function, mocking_function)

        def wrapper(*args, **kwargs):
            mocking_function(*args, **kwargs)

        return wrapper

    return decorator


@patch("min")
def mock_min(column: ColumnEmulator) -> ColumnEmulator:
    if isinstance(
        column.sf_type.datatype, _NumericType
    ):  # TODO: figure out where 5 is coming from
        return ColumnEmulator(data=round(column.min(), 5), sf_type=column.sf_type)
    res = ColumnEmulator(data=column.dropna().min(), sf_type=column.sf_type)
    try:
        if math.isnan(res[0]):
            return ColumnEmulator(data=[None], sf_type=column.sf_type)
        return ColumnEmulator(data=res, sf_type=column.sf_type)
    except TypeError:  # math.isnan throws TypeError if res[0] is not a number
        return ColumnEmulator(data=res, sf_type=column.sf_type)


@patch("max")
def mock_max(column: ColumnEmulator) -> ColumnEmulator:
    if isinstance(column.sf_type.datatype, _NumericType):
        return ColumnEmulator(data=round(column.max(), 5), sf_type=column.sf_type)
    res = ColumnEmulator(data=column.dropna().max(), sf_type=column.sf_type)
    try:
        if math.isnan(res[0]):
            return ColumnEmulator(data=[None], sf_type=column.sf_type)
        return ColumnEmulator(data=res, sf_type=column.sf_type)
    except TypeError:
        return ColumnEmulator(data=res, sf_type=column.sf_type)


@patch("sum")
def mock_sum(column: ColumnEmulator) -> ColumnEmulator:
    all_item_is_none = True
    res = 0
    for data in column:
        if data is not None:
            try:
                if math.isnan(data):
                    continue
            except TypeError:
                pass
            all_item_is_none = False
            try:
                res += float(data)
            except ValueError:
                raise SnowparkSQLException(f"Numeric value '{data}' is not recognized.")
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


@patch("avg")
def mock_avg(column: ColumnEmulator) -> ColumnEmulator:
    all_item_is_none = True
    ret = 0
    cnt = 0
    for data in column:
        if data is not None:
            all_item_is_none = False
            ret += float(data)
            cnt += 1

    ret = (
        ColumnEmulator(data=[round((ret / cnt), 5)])
        if not all_item_is_none
        else ColumnEmulator(data=[None])
    )
    ret.sf_type = column.sf_type
    return ret


@patch("count")
def mock_count(column: ColumnEmulator) -> ColumnEmulator:
    count_column = column.count()
    if isinstance(count_column, ColumnEmulator):
        count_column.sf_type = ColumnType(LongType(), False)
    return ColumnEmulator(
        data=round(count_column, 5), sf_type=ColumnType(LongType(), False)
    )


@patch("count_distinct")
def mock_count_distinct(*cols: ColumnEmulator) -> ColumnEmulator:
    """
    Snowflake does not count rows that contain NULL values, in the mocking implementation
    we iterate over each row and then each col to check if there exists NULL value, if the col is NULL,
    we do not count that row.
    """
    dict_data = {}
    for i in range(len(cols)):
        dict_data[f"temp_col_{i}"] = cols[i]
    rows = len(cols[0])
    temp_table = TableEmulator(dict_data, index=[i for i in range(len(cols[0]))])
    temp_table = temp_table.reset_index()
    to_drop_index = set()
    for col in cols:
        for i in range(rows):
            if col[col.index[i]] is None:
                to_drop_index.add(i)
                break
    temp_table = temp_table.drop(index=list(to_drop_index))
    temp_table = temp_table.drop_duplicates(subset=list(dict_data.keys()))
    count_column = temp_table.count()
    if isinstance(count_column, ColumnEmulator):
        count_column.sf_type = ColumnType(LongType(), False)
    return ColumnEmulator(
        data=round(count_column, 5), sf_type=ColumnType(LongType(), False)
    )


@patch("median")
def mock_median(column: ColumnEmulator) -> ColumnEmulator:
    if isinstance(column.sf_type.datatype, DecimalType):
        return_type = DecimalType(
            column.sf_type.datatype.precision + 3, column.sf_type.datatype.scale + 3
        )
    else:
        return_type = column.sf_type.datatype
    return ColumnEmulator(
        data=round(column.median(), 5),
        sf_type=ColumnType(return_type, column.sf_type.nullable),
    )


@patch("covar_pop")
def mock_covar_pop(column1: ColumnEmulator, column2: ColumnEmulator) -> ColumnEmulator:
    non_nan_cnt = 0
    x_sum, y_sum, x_times_y_sum = 0, 0, 0
    for x, y in zip(column1, column2):
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


@patch("listagg")
def mock_listagg(column: ColumnEmulator, delimiter, is_distinct):
    columns_data = ColumnEmulator(column.unique()) if is_distinct else column
    return ColumnEmulator(
        data=delimiter.join([str(v) for v in columns_data.dropna()]),
        sf_type=ColumnType(ArrayType(), column.sf_type.nullable),
    )


@patch("to_date")
def mock_to_date(
    column: ColumnEmulator,
    fmt: Union[ColumnEmulator, str] = None,
    try_cast: bool = False,
):
    res = []
    auto_detect = bool(not fmt)

    date_format, _, _ = convert_snowflake_datetime_format(
        fmt, default_format="%Y-%m-%d"
    )

    for data in column:
        if data is None:
            res.append(None)
            continue
        try:
            if auto_detect and data.isnumeric():
                res.append(
                    datetime.datetime.utcfromtimestamp(
                        process_numeric_time(data)
                    ).date()
                )
            else:
                res.append(datetime.datetime.strptime(data, date_format).date())
        except BaseException:
            if try_cast:
                res.append(None)
            else:
                raise
    return ColumnEmulator(
        data=res, sf_type=ColumnType(DateType(), column.sf_type.nullable)
    )


@patch("contains")
def mock_contains(expr1: ColumnEmulator, expr2: Union[str, ColumnEmulator]):
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
    res = []

    for data in e:
        if data is None:
            res.append(data)
            continue
        try:
            try:
                float(data)
            except ValueError:
                raise SnowparkSQLException(f"Numeric value '{data}' is not recognized.")

            integer_part = round(float(data))
            integer_part_str = str(integer_part)
            len_integer_part = (
                len(integer_part_str) - 1
                if integer_part_str[0] == "-"
                else len(integer_part_str)
            )
            if len_integer_part > precision:
                raise SnowparkSQLException(f"Numeric value '{data}' is out of range")
            remaining_decimal_len = min(precision - len(str(integer_part)), scale)
            res.append(Decimal(str(round(float(data), remaining_decimal_len))))
        except BaseException:
            if try_cast:
                res.append(None)
            else:
                raise

    return ColumnEmulator(
        data=res,
        sf_type=ColumnType(DecimalType(precision, scale), nullable=e.sf_type.nullable),
    )


@patch("to_time")
def mock_to_time(
    column: ColumnEmulator,
    fmt: Union[ColumnEmulator, str] = None,
    try_cast: bool = False,
):
    res = []

    auto_detect = bool(not fmt)

    time_fmt, hour_delta, fractional_seconds = convert_snowflake_datetime_format(
        fmt, default_format="%H:%M:%S"
    )
    for data in column:
        try:
            if data is None:
                res.append(None)
                continue
            if auto_detect and data.isnumeric():
                res.append(
                    datetime.datetime.utcfromtimestamp(
                        process_numeric_time(data)
                    ).time()
                )
            else:
                # handle seconds fraction
                data_parts = data.split(".")
                if len(data_parts) == 2:
                    # there is a part of seconds
                    seconds_part = data_parts[1]
                    # find the idx that the seconds part ends
                    idx = 0
                    while seconds_part[idx].isdigit():
                        idx += 1
                    # truncate to precision
                    seconds_part = (
                        seconds_part[: min(idx, fractional_seconds)]
                        + seconds_part[idx:]
                    )
                    data = f"{data_parts[0]}.{seconds_part}"
                res.append(
                    (
                        datetime.datetime.strptime(
                            process_string_time_with_fractional_seconds(
                                data, fractional_seconds
                            ),
                            time_fmt,
                        )
                        + datetime.timedelta(hours=hour_delta)
                    ).time()
                )
        except BaseException:
            if try_cast:
                data.append(None)
            else:
                raise

    return ColumnEmulator(
        data=res, sf_type=ColumnType(TimeType(), column.sf_type.nullable)
    )


@patch("to_timestamp")
def mock_to_timestamp(
    column: ColumnEmulator,
    fmt: Union[ColumnEmulator, str] = None,
    try_cast: bool = False,
):
    res = []
    auto_detect = bool(not fmt)

    (
        timestamp_format,
        hour_delta,
        fractional_seconds,
    ) = convert_snowflake_datetime_format(fmt, default_format="%Y-%m-%d %H:%M:%S.%f")

    for data in column:
        try:
            if data is None:
                res.append(None)
                continue
            if auto_detect and data.isnumeric():
                res.append(
                    datetime.datetime.utcfromtimestamp(process_numeric_time(data))
                )
            else:
                # handle seconds fraction
                res.append(
                    datetime.datetime.strptime(
                        process_string_time_with_fractional_seconds(
                            data, fractional_seconds
                        ),
                        timestamp_format,
                    )
                    + datetime.timedelta(hours=hour_delta)
                )
        except BaseException:
            if try_cast:
                res.append(None)
            else:
                raise

    return ColumnEmulator(
        data=res,
        sf_type=ColumnType(TimestampType(), column.sf_type.nullable),
        dtype=object,
    )


@patch("to_char")
def mock_to_char(
    column: ColumnEmulator,
    fmt: Union[ColumnEmulator, str] = None,
    try_cast: bool = False,
) -> ColumnEmulator:  # TODO: support more input types
    source_datatype = column.sf_type.datatype

    if isinstance(source_datatype, DateType):
        date_format, _, _ = convert_snowflake_datetime_format(
            fmt, default_format="%Y-%m-%d"
        )
        func = partial(
            try_convert, lambda x: datetime.datetime.strftime(x, date_format), try_cast
        )
    elif isinstance(source_datatype, TimeType):
        raise NotImplementedError(
            "[Local Testing] Use TO_CHAR on Time data is not supported yet"
        )
    elif isinstance(source_datatype, (DateType, TimeType, TimestampType)):
        raise NotImplementedError(
            "[Local Testing] Use TO_CHAR on Timestamp data is not supported yet"
        )
    elif isinstance(source_datatype, _NumericType):
        if fmt:
            raise NotImplementedError(
                "[Local Testing] Use format strings with Numeric types in TO_CHAR is not supported yet."
            )
        func = partial(try_convert, lambda x: str(x), try_cast)
    else:
        func = partial(try_convert, lambda x: str(x), try_cast)
    new_col = column.apply(func)
    new_col.sf_type = ColumnType(StringType(), column.sf_type.nullable)
    return new_col


@patch("to_double")
def mock_to_double(
    column: ColumnEmulator, fmt: Optional[str] = None, try_cast: bool = False
) -> ColumnEmulator:
    if fmt:
        raise NotImplementedError(
            "[Local Testing] Using format strings in to_double is not supported yet"
        )
    if isinstance(column.sf_type.datatype, (_NumericType, StringType)):
        res = column.apply(lambda x: try_convert(float, try_cast, x))
        res.sf_type = ColumnType(DoubleType(), column.sf_type.nullable)
        return res
    elif isinstance(column.sf_type.datatype, VariantType):
        raise NotImplementedError("[Local Testing] Variant is not supported yet")
    else:
        raise NotImplementedError(
            f"[Local Testing[ Invalid type {column.sf_type.datatype} for parameter 'TO_DOUBLE'"
        )


@patch("to_boolean")
def mock_to_boolean(column: ColumnEmulator, try_cast: bool = False) -> ColumnEmulator:
    if isinstance(column.sf_type, StringType):

        def convert_str_to_bool(x: Optional[str]):
            if x is None:
                return None
            elif x.lower() in ("true", "t", "yes", "y", "on", "1"):
                return True
            elif x.lower() in ("false", "f", "no", "n", "off", "0"):
                return False
            raise SnowparkSQLException(f"Boolean value {x} is not recognized")

        new_col = column.apply(lambda x: try_convert(convert_str_to_bool, try_cast, x))
        new_col.sf_type = ColumnType(BooleanType(), column.sf_type.nullable)
        return new_col
    elif isinstance(column.sf_type, _NumericType):

        def convert_num_to_bool(x: Optional[Real]):
            if x is None:
                return None
            elif math.isnan(x) or math.isinf(x):
                raise SnowparkSQLException(
                    f"Invalid value {x} for parameter 'TO_BOOLEAN'"
                )
            else:
                return x != 0

        new_col = column.apply(lambda x: try_convert(convert_num_to_bool, try_cast, x))
        new_col.sf_type = ColumnType(BooleanType(), column.sf_type.nullable)
        return new_col
    else:
        raise SnowparkSQLException(
            f"Invalid type {column.sf_type.datatype} for parameter 'TO_BOOLEAN'"
        )


@patch("to_binary")
def mock_to_binary(
    column: ColumnEmulator, fmt: str = None, try_cast: bool = False
) -> ColumnEmulator:
    if isinstance(column.sf_type.datatype, (StringType, NullType)):
        fmt = fmt.upper() if fmt else "HEX"
        if fmt == "HEX":
            res = column.apply(lambda x: try_convert(binascii.unhexlify, try_cast, x))
        elif fmt == "BASE64":
            res = column.apply(lambda x: try_convert(base64.b64decode, try_cast, x))
        elif fmt == "UTF-8":
            res = column.apply(
                lambda x: try_convert(lambda y: y.encode("utf-8"), try_cast, x)
            )
        else:
            raise SnowparkSQLException(f"Invalid binary format {fmt}")
        res.sf_type = ColumnType(BinaryType(), column.sf_type.nullable)
        return res
    else:
        raise SnowparkSQLException(
            f"Invalid type {column.sf_type.datatype} for parameter 'TO_BINARY'"
        )


@patch("iff")
def mock_iff(condition: ColumnEmulator, expr1: ColumnEmulator, expr2: ColumnEmulator):
    assert isinstance(condition.sf_type.datatype, BooleanType)
    condition = condition.array
    res = ColumnEmulator(data=[None] * len(condition), dtype=object)
    if not all(condition) and expr1.sf_type != expr2.sf_type:
        raise SnowparkSQLException(
            f"iff expr1 and expr2 have conflicting data types: {expr1.sf_type} != {expr2.sf_type}"
        )
    res.sf_type = expr1.sf_type if any(condition) else expr2.sf_type
    res.where(condition, other=expr2, inplace=True)
    res.where([not x for x in condition], other=expr1, inplace=True)
    return res


@patch("coalesce")
def mock_coalesce(*exprs):
    import pandas

    if len(exprs) < 2:
        raise SnowparkSQLException(
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
    return base_expr.str.slice(start=start_expr - 1, stop=start_expr - 1 + length_expr)


@patch("startswith")
def mock_startswith(expr1: ColumnEmulator, expr2: ColumnEmulator):
    res = expr1.str.startswith(expr2)
    res.sf_type = ColumnType(StringType(), expr1.sf_type.nullable)
    return res


@patch("endswith")
def mock_endswith(expr1: ColumnEmulator, expr2: ColumnEmulator):
    res = expr1.str.endswith(expr2)
    res.sf_type = ColumnType(StringType(), expr1.sf_type.nullable)
    return res


@patch("row_number")
def mock_row_number(window: TableEmulator, row_idx: int):
    return ColumnEmulator(data=[row_idx + 1], sf_type=ColumnType(LongType(), False))


@patch("upper")
def mock_upper(expr: ColumnEmulator):
    res = expr.apply(lambda x: x.upper())
    res.sf_type = ColumnType(StringType(), expr.sf_type.nullable)
    return res


@patch("parse_json")
def mock_parse_json(expr: ColumnEmulator):
    if isinstance(expr.sf_type.datatype, StringType):
        res = expr.apply(lambda x: try_convert(json.loads, False, x))
    else:
        res = copy(expr)
    res.sf_type = ColumnType(VariantType(), expr.sf_type.nullable)
    return res


@patch("to_array")
def mock_to_array(expr: ColumnEmulator):
    if isinstance(expr.sf_type.datatype, ArrayType):
        res = copy(expr)
    elif isinstance(expr.sf_type.datatype, VariantType):
        res = expr.apply(
            lambda x: try_convert(lambda y: y if isinstance(y, list) else [y], False, x)
        )
    else:
        res = expr.apply(lambda x: try_convert(lambda y: [y], False, x))
    res.sf_type = ColumnType(ArrayType(), expr.sf_type.nullable)
    return res


@patch("to_object")
def mock_to_object(expr: ColumnEmulator):
    if isinstance(expr.sf_type.datatype, (MapType,)):
        res = res = copy(expr)
    elif isinstance(expr.sf_type.datatype, VariantType):
        res = expr.apply(
            lambda x: try_convert(lambda y: y if isinstance(y, dict) else {y}, False, x)
        )
    else:

        def raise_exc():
            raise SnowparkSQLException(
                f"Invalid type {type(expr.sf_type.datatype)} parameter 'TO_OBJECT'"
            )

        res = expr.apply(lambda x: try_convert(raise_exc, False, x))
    res.sf_type = ColumnType(MapType(), expr.sf_type.nullable)
    return res


@patch("to_variant")
def mock_to_variant(expr: ColumnEmulator):
    res = copy(expr)
    res.sf_type = ColumnType(VariantType(), expr.sf_type.nullable)
    return res
