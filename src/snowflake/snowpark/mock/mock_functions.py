#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import math
from typing import Callable, List, Union

from .snowflake_data_type import ColumnEmulator, TableEmulator

RETURN_TYPE = Union[ColumnEmulator, TableEmulator]

MOCK_FUNCTION_IMPLEMENTATION_MAP = {}


def register_func_implementation(
    snowpark_func_name: str, func_implementation: Callable
):
    MOCK_FUNCTION_IMPLEMENTATION_MAP[snowpark_func_name] = func_implementation


def mock_min(columns: List[ColumnEmulator], **kwargs) -> ColumnEmulator:
    return ColumnEmulator(data=round(columns[0].min(), 5))


def mock_max(columns: List[ColumnEmulator], **kwargs) -> ColumnEmulator:
    return ColumnEmulator(data=round(columns[0].max(), 5))


def mock_sum(columns: List[ColumnEmulator], **kwargs) -> ColumnEmulator:
    return ColumnEmulator(data=round(columns[0].sum(), 5))


def mock_avg(columns: List[ColumnEmulator], **kwargs) -> ColumnEmulator:
    return ColumnEmulator(data=round(columns[0].mean(), 5))


def mock_count(columns: List[ColumnEmulator], **kwargs) -> ColumnEmulator:
    return ColumnEmulator(data=round(columns[0].count(), 5))


def mock_median(columns: List[ColumnEmulator], **kwargs) -> ColumnEmulator:
    return ColumnEmulator(data=round(columns[0].median(), 5))


def mock_covar_pop(columns: List[ColumnEmulator], **kwargs) -> ColumnEmulator:
    non_nan_cnt = 0
    x_sum, y_sum, x_times_y_sum = 0, 0, 0
    for x, y in zip(columns[0], columns[1]):
        if x is not None and y is not None and not math.isnan(x) and not math.isnan(y):
            non_nan_cnt += 1
            x_times_y_sum += x * y
            x_sum += x
            y_sum += y
    data = (x_times_y_sum - x_sum * y_sum / non_nan_cnt) / non_nan_cnt
    return ColumnEmulator(data=data)


def mock_listagg(columns: List[ColumnEmulator], **kwargs):
    delimiter = kwargs.get("delimiter")
    is_distinct = kwargs.get("is_distinct")
    columns_data = ColumnEmulator(columns[0].unique()) if is_distinct else columns[0]
    return ColumnEmulator(data=delimiter.join([v for v in columns_data.dropna()]))


MOCK_FUNCTION_IMPLEMENTATION_MAP["min"] = mock_min
MOCK_FUNCTION_IMPLEMENTATION_MAP["max"] = mock_max
MOCK_FUNCTION_IMPLEMENTATION_MAP["sum"] = mock_sum
MOCK_FUNCTION_IMPLEMENTATION_MAP["median"] = mock_median
MOCK_FUNCTION_IMPLEMENTATION_MAP["mean"] = mock_avg
MOCK_FUNCTION_IMPLEMENTATION_MAP["avg"] = mock_avg
MOCK_FUNCTION_IMPLEMENTATION_MAP["count"] = mock_count
MOCK_FUNCTION_IMPLEMENTATION_MAP["covar_pop"] = mock_covar_pop
MOCK_FUNCTION_IMPLEMENTATION_MAP["listagg"] = mock_listagg
