#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import datetime
import math
from typing import Callable, Union

from snowflake.snowpark.mock.snowflake_data_type import ColumnEmulator, TableEmulator
from snowflake.snowpark.types import _NumericType

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


def patch(function):
    def decorator(mocking_function):
        _register_func_implementation(function, mocking_function)

        def wrapper(*args, **kwargs):
            mocking_function(*args, **kwargs)

        return wrapper

    return decorator


@patch("min")
def mock_min(column: ColumnEmulator) -> ColumnEmulator:
    if isinstance(column.sf_type, _NumericType):
        return ColumnEmulator(data=round(column.min(), 5))
    res = ColumnEmulator(data=column.dropna().min())
    try:
        if math.isnan(res[0]):
            return ColumnEmulator(data=[None])
        return ColumnEmulator(data=res)
    except TypeError:  # math.isnan throws TypeError if res[0] is not a number
        return ColumnEmulator(data=res)


@patch("max")
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


@patch("sum")
def mock_sum(column: ColumnEmulator) -> ColumnEmulator:
    all_item_is_none = True
    ret = 0
    for data in column:
        if data is not None:
            try:
                if math.isnan(data):
                    continue
            except TypeError:
                pass
            all_item_is_none = False
            ret += float(data)
    return (
        ColumnEmulator(data=[ret])
        if not all_item_is_none
        else ColumnEmulator(data=[None])
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
    # round to 5 according to snowflake spec
    return (
        ColumnEmulator(data=[round((ret / cnt), 5)])
        if not all_item_is_none
        else ColumnEmulator(data=[None])
    )


@patch("count")
def mock_count(column: ColumnEmulator) -> ColumnEmulator:
    return ColumnEmulator(data=round(column.count(), 5))


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
    return ColumnEmulator(data=round(temp_table.count(), 5))


@patch("median")
def mock_median(column: ColumnEmulator) -> ColumnEmulator:
    return ColumnEmulator(data=round(column.median(), 5))


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
    return ColumnEmulator(data=data)


@patch("listagg")
def mock_listagg(column: ColumnEmulator, delimiter, is_distinct):
    columns_data = ColumnEmulator(column.unique()) if is_distinct else column
    return ColumnEmulator(data=delimiter.join([str(v) for v in columns_data.dropna()]))


@patch("to_date")
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
    return ColumnEmulator(data=res)


@patch("abs")
def mock_abs(expr):
    if isinstance(expr, ColumnEmulator):
        return expr.abs()
    else:
        return abs(expr)
