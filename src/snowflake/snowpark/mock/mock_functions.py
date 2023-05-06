#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import datetime
import math
from typing import Callable, Union

from snowflake.snowpark.mock.snowflake_data_type import ColumnEmulator, TableEmulator
from snowflake.snowpark.types import _NumericType

RETURN_TYPE = Union[ColumnEmulator, TableEmulator]

MOCK_FUNCTION_IMPLEMENTATION_MAP = {}


def register_func_implementation(
    snowpark_func: Union[str, Callable], func_implementation: Callable
):
    try:
        MOCK_FUNCTION_IMPLEMENTATION_MAP[snowpark_func.__name__] = func_implementation
    except AttributeError:
        MOCK_FUNCTION_IMPLEMENTATION_MAP[snowpark_func] = func_implementation


def unregister_func_implementation(snowpark_func: Union[str, Callable]):
    try:
        try:
            del MOCK_FUNCTION_IMPLEMENTATION_MAP[snowpark_func.__name__]
        except AttributeError:
            del MOCK_FUNCTION_IMPLEMENTATION_MAP[snowpark_func]
    except KeyError:
        pass


def mock_min(column: ColumnEmulator) -> ColumnEmulator:
    if isinstance(column.sf_type, _NumericType):
        return ColumnEmulator(data=round(column.min(), 5))
    res = ColumnEmulator(data=column.dropna().min())
    try:
        if math.isnan(res[0]):
            return ColumnEmulator(data=[None])
        return ColumnEmulator(data=res)
    except TypeError:
        return ColumnEmulator(data=res)


def mock_max(column: ColumnEmulator) -> ColumnEmulator:
    if isinstance(column.sf_type, _NumericType):
        return ColumnEmulator(data=round(column.max(), 5))
    res = ColumnEmulator(data=column.dropna().max())
    try:
        if math.isnan(res[0]):
            return ColumnEmulator(data=[None])
        return ColumnEmulator(data=res)
    except TypeError:
        return ColumnEmulator(data=res)


def mock_sum(column: ColumnEmulator) -> ColumnEmulator:
    return ColumnEmulator(data=column.sum())


def mock_avg(column: ColumnEmulator) -> ColumnEmulator:
    return ColumnEmulator(data=round(column.mean(), 5))


def mock_count(column: ColumnEmulator) -> ColumnEmulator:
    return ColumnEmulator(data=round(column.count(), 5))


def mock_median(column: ColumnEmulator) -> ColumnEmulator:
    return ColumnEmulator(data=round(column.median(), 5))


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
    return ColumnEmulator(data=data)


def mock_listagg(column: ColumnEmulator, delimiter, is_distinct):
    columns_data = ColumnEmulator(column.unique()) if is_distinct else column
    return ColumnEmulator(data=delimiter.join([v for v in columns_data.dropna()]))


def mock_to_date(column: ColumnEmulator, fmt: Union[ColumnEmulator, str] = None):
    auto_detect = bool(not fmt)
    date_fmt = fmt or "%Y-%m-%d"
    date_fmt = date_fmt.replace("YYYY", "%Y")
    date_fmt = date_fmt.replace("MM", "%m")
    date_fmt = date_fmt.replace("MON", "%b")
    date_fmt = date_fmt.replace("DD", "%d")
    res = []
    for data in column:
        if auto_detect and data.isnumeric():
            # spec here: https://docs.snowflake.com/en/sql-reference/functions/to_date#usage-notes
            timestamp_values = int(data)
            if 31536000000000 <= timestamp_values < 31536000000000:  # milliseconds
                timestamp_values = timestamp_values / 1000
            elif timestamp_values >= 31536000000000:
                # nanoseconds
                timestamp_values = timestamp_values / 1000000
            # timestamp_values <  31536000000 are treated as seconds
            res.append(datetime.datetime.utcfromtimestamp(int(timestamp_values)).date())
        else:
            res.append(datetime.datetime.strptime(data, date_fmt).date())
    return ColumnEmulator(data=res)


def mock_contains(expr1: ColumnEmulator, expr2: Union[str, ColumnEmulator]):
    if isinstance(expr1, str) and isinstance(expr2, str):
        return ColumnEmulator(data=[bool(str(expr2) in str(expr1))])
    if isinstance(expr1, ColumnEmulator) and isinstance(expr2, ColumnEmulator):
        res = [bool(str(item2) in str(item1)) for item1, item2 in zip(expr1, expr2)]
    elif isinstance(expr1, ColumnEmulator) and isinstance(expr2, str):
        res = [bool(str(expr2) in str(item)) for item in expr1]
    else:  # expr1 is string, while expr2 is column
        res = [bool(str(item) in str(expr1)) for item in expr2]
    return ColumnEmulator(data=res)


def mock_abs(expr):
    if isinstance(expr, ColumnEmulator):
        return expr.abs()
    else:
        return abs(expr)


MOCK_FUNCTION_IMPLEMENTATION_MAP["min"] = mock_min
MOCK_FUNCTION_IMPLEMENTATION_MAP["max"] = mock_max
MOCK_FUNCTION_IMPLEMENTATION_MAP["sum"] = mock_sum
MOCK_FUNCTION_IMPLEMENTATION_MAP["median"] = mock_median
MOCK_FUNCTION_IMPLEMENTATION_MAP["mean"] = mock_avg
MOCK_FUNCTION_IMPLEMENTATION_MAP["avg"] = mock_avg
MOCK_FUNCTION_IMPLEMENTATION_MAP["count"] = mock_count
MOCK_FUNCTION_IMPLEMENTATION_MAP["covar_pop"] = mock_covar_pop
MOCK_FUNCTION_IMPLEMENTATION_MAP["listagg"] = mock_listagg
MOCK_FUNCTION_IMPLEMENTATION_MAP["to_date"] = MOCK_FUNCTION_IMPLEMENTATION_MAP[
    "date_format"
] = mock_to_date
MOCK_FUNCTION_IMPLEMENTATION_MAP["contains"] = mock_contains
MOCK_FUNCTION_IMPLEMENTATION_MAP["abs"] = mock_abs
