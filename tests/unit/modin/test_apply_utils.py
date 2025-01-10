#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Any

import numpy as np
import pandas as native_pd
import pytest

from snowflake.snowpark.modin.plugin._internal.apply_utils import (
    convert_numpy_int_result_to_int,
    deduce_return_type_from_function,
    infer_return_type_using_dummy_data,
    handle_missing_value_in_variant,
)
from snowflake.snowpark.types import (
    BinaryType,
    BooleanType,
    ArrayType,
    FloatType,
    LongType,
    MapType,
    StringType,
    TimestampType,
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
        assert isinstance(deduce_return_type_from_function(func, None), datatype)
    else:
        # type could not be deduced
        assert deduce_return_type_from_function(func, None) is None


@pytest.mark.parametrize(
    "func, input_type, return_type",
    [
        (lambda x: x.decode("ascii"), BinaryType(), StringType()),
        (lambda x: x + 1, BooleanType(), LongType()),
        (lambda x: [x, x + 1], LongType(), ArrayType(LongType())),
        (lambda x: str(x).encode("utf-8"), LongType(), BinaryType()),
        (lambda x: x > 0, LongType(), BooleanType()),
        (lambda x: x + 1.8, LongType(), FloatType()),
        (lambda x: x + 1, LongType(), LongType()),
        (lambda x: str(x), LongType(), StringType()),
        (lambda x: 1 if x < 0 else "abc", LongType(), VariantType()),
        (lambda x: {x: x}, StringType(), MapType()),
        (lambda x: None, StringType(), None),  # failed to infer return type
        (lambda x: x.year, TimestampType(), LongType()),
        (lambda x: {x: str(x)}, LongType(), MapType(LongType(), StringType())),
        (
            lambda x: {x: x if x < 3 else str(x)},
            LongType(),
            MapType(LongType(), VariantType()),  # mixed return type in map values
        ),
    ],
    ids=[
        "binary_to_str",
        "bool_to_long",
        "long_to_array",
        "long_to_binary",
        "long_to_bool",
        "long_to_float",
        "long_to_long",
        "long_to_str",
        "long_to_variant",
        "str_to_map",
        "str_to_none",
        "timestamp_to_long",
        "long_to_map[long,str]",
        "long_to_map[long,variant]",
    ],
)
def test_infer_return_type_from_function(func, input_type, return_type):
    assert infer_return_type_using_dummy_data(func, input_type) == return_type


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
