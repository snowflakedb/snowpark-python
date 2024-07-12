#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from typing import Any

import numpy as np
import pandas as native_pd
import pytest

from snowflake.snowpark.modin.plugin._internal.apply_utils import (
    convert_numpy_int_result_to_int,
    deduce_return_type_from_function,
    handle_missing_value_in_variant,
)
from snowflake.snowpark.types import (
    ArrayType,
    FloatType,
    LongType,
    MapType,
    StringType,
    VariantType,
)


def test_handle_missing_value_in_variant():
    assert handle_missing_value_in_variant(0) == 0
    assert handle_missing_value_in_variant(-1.1) == -1.1
    assert handle_missing_value_in_variant("") == ""
    assert handle_missing_value_in_variant(False) is False
    assert handle_missing_value_in_variant(None) is None
    assert handle_missing_value_in_variant(np.nan).is_sql_null
    assert handle_missing_value_in_variant(native_pd.NA).is_sql_null
    assert handle_missing_value_in_variant(native_pd.NaT).is_sql_null


# test functions, this could be expanded - relying on snowpark tests to capture this more widely.
# specifically test here the branches from deduce_return_type_from_function
def foo_int(x: int) -> int:
    return 42


def foo_float(x: int) -> float:
    return 42.1


def foo_str(x: int) -> str:
    return "test"


def foo_obj(x: int) -> object:
    return 42


def foo_any(x: Any) -> Any:
    return 42


@pytest.mark.parametrize(
    "func,datatype",
    [
        (foo_int, LongType),
        (foo_float, FloatType),
        (foo_str, StringType),
        ([1, 2, 3], ArrayType),
        ({10: 20}, MapType),
        (foo_obj, VariantType),
        (foo_any, None),
    ],
)
def test_deduce_return_type_from_function(func, datatype):
    if datatype is not None:
        # type could be deduced
        assert isinstance(deduce_return_type_from_function(func), datatype)
    else:
        # type could not be deduced
        assert deduce_return_type_from_function(func) is None


@pytest.mark.parametrize(
    "value,expected",
    [
        (3, 3),
        (np.int8(6), 6),
        (np.int16(7), 7),
        (np.int32(3), 3),
        (np.int64(4), 4),
        (np.uint8(5), 5),
        (np.float64(5), np.float64(5)),
        (["a", np.int64(1)], ["a", np.int64(1)]),
        ({np.int32(2): np.int64(3)}, {np.int32(2): np.int64(3)}),
    ],
)
def test_convert_numpy_ints_in_result_to_ints(value, expected):
    result = convert_numpy_int_result_to_int(value)
    assert type(result) == type(expected)
    assert result == expected
