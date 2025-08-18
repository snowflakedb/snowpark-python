#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import math
import operator
import random

import modin.pandas as pd
import numpy as np
import pandas
import pandas as native_pd
import pytest
from pandas.core.dtypes.common import is_object_dtype
from pandas.testing import assert_frame_equal, assert_series_equal

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.modin.plugin.extensions.utils import try_convert_index_to_native
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equal_to_pandas,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_dfs,
    create_test_series,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import running_on_public_ci


@pytest.mark.parametrize(
    "func",
    [
        # addition
        lambda x: x.add(1),
        lambda x: x.radd(-1.1),
        lambda x: x + 1,
        lambda x: -1.1 + x,
        # subtraction
        lambda x: x.sub(2),
        lambda x: x.rsub(-2.2),
        lambda x: x - 2,
        lambda x: -2.2 - x,
        # multiplication
        lambda x: x.mul(0),
        lambda x: x.rmul(-3.3),
        lambda x: x * 3,
        lambda x: 0 * x,
        # division
        lambda x: x.rdiv(-4.4),
        lambda x: x.truediv(4),
        lambda x: 0 / x,
        # floor division
        lambda x: x.rfloordiv(-5.5),
        lambda x: x // 5,
        lambda x: 0 // x,
        # pow
        lambda x: x.pow(0),
        lambda x: x.rpow(-7.7),
        lambda x: x**1,
        lambda x: 1.0**x,
    ],
)
@sql_count_checker(query_count=4)
def test_binary_arithmetic_method_number_scalar(func):
    # test both NULL and NaN in Snowflake
    data = [[-10, 1, 1.5], [100000, math.e, "NaN"], [-100000, math.pi, None]]

    snow_df = pd.DataFrame(data, dtype="Float64")
    native_df = native_pd.DataFrame(data, dtype="Float64")
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(snow_df, native_df, func)
    # test series
    for c in snow_df.columns:
        snow_series = snow_df[c]
        native_series = native_df[c]
        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snow_series, native_series, func, check_names=False
            )


@pytest.mark.parametrize(
    "func",
    [
        lambda x: x.div(0),
        lambda x: x.rtruediv(0),
        lambda x: x / 0,
        lambda x: x // 0,
        lambda x: x.floordiv(0),
        lambda x: x.rdiv(-4.4),
        lambda x: x.rfloordiv(-5.5),
    ],
)
@sql_count_checker(query_count=0)
def test_binary_arithmetic_method_number_scalar_negative(func):
    # test both NULL and NaN in Snowflake
    data = [[0, 1, 1.5], [-math.e, 0, "NaN"], [math.pi, None, 0]]
    snow_df = pd.DataFrame(data, dtype="Float64")

    # test dataframe
    with pytest.raises(SnowparkSQLException, match="Division by zero"):
        func(snow_df).to_pandas()  # eval with to_pandas, b.c. lazy

    # test series
    for c in snow_df.columns:
        with pytest.raises(SnowparkSQLException, match="Division by zero"):
            snow_series = snow_df[c]
            func(snow_series).to_pandas()  # eval with to_pandas, b.c. lazy


@sql_count_checker(query_count=2)
def test_binary_pow_scalar_different_from_pandas():
    data = [[0, 1, 1.5], [100000, math.e, np.nan], [-100000, math.pi, None]]
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)

    result1 = (snow_df**7).to_pandas()
    # this is different from pandas because originally this series is int64 type and
    # powering 7 still result in an integer, which will overflow. However, Snowflake's power
    # always creates a float
    assert_series_equal(
        result1[0],
        native_pd.Series([row[0] ** 7 for row in data]).astype(float),
        check_names=False,
        check_index_type=False,
    )
    # other parts are the same as pandas
    assert_frame_equal(
        result1.loc[:, 1:],
        native_df.loc[:, 1:] ** 7,
        check_dtype=False,
        check_index_type=False,
    )

    result2 = (0**snow_df).to_pandas()
    # this is different from pandas because pandas doesn't allow a ** b,
    # where a is an integer and b is a negative integer. However, Snowflake allows it and return
    # correct results
    assert_series_equal(
        result2[0],
        native_pd.Series([1.0, 0.0, np.inf]),
        check_names=False,
        check_index_type=False,
    )
    with pytest.raises(
        ValueError, match="Integers to negative integer powers are not allowed."
    ):
        0**native_df
    # other parts are the same as pandas
    assert_frame_equal(
        result2.loc[:, 1:],
        0 ** native_df.loc[:, 1:],
        check_dtype=False,
        check_index_type=False,
    )


@pytest.mark.parametrize(
    "func",
    [
        # concatenation
        lambda x: x.add("111"),
        lambda x: x.radd(""),
        lambda x: x + "NaN",
        lambda x: "@$#%'\"" + x,
        # repetition
        lambda x: x.mul(100000),
        lambda x: x.rmul(1),
        lambda x: x * 0,
        lambda x: -1 * x,
    ],
)
@sql_count_checker(query_count=3)
def test_binary_arithmetic_method_string_scalar(func):
    data = [["snowflake", ""], ["熊猫", "@$#%'\""]]
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(snow_df, native_df, func)
    # test series
    for i, c in enumerate(snow_df.columns):
        # TODO SNOW-862677: use snow_df[c] when __getitem__ is implemented
        snow_series = pd.Series([row[i] for row in data])
        native_series = native_df[c]
        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snow_series, native_series, func, check_names=False
            )


@pytest.mark.parametrize(
    "func",
    [
        lambda x: x + np.nan,
        lambda x: 0 + x,
        lambda x: x - np.nan,
        lambda x: 0.0 - x,
        lambda x: x * np.nan,
        lambda x: 0.0 * x,
        lambda x: np.nan / x,
        lambda x: x // np.nan,
        lambda x: 0 // x,
        lambda x: x % np.nan,
        lambda x: 0.0 % x,
        lambda x: x**0,
        lambda x: 0.0**x,
    ],
)
@sql_count_checker(query_count=1)
def test_binary_arithmetic_method_null_nan_scalar(func):
    snow_df = pd.DataFrame([None, "NaN"], dtype="Float64")
    native_df = native_pd.DataFrame([None, "NaN"], dtype="Float64")
    eval_snowpark_pandas_result(snow_df, native_df, func)


@pytest.mark.parametrize(
    "func",
    [
        lambda x: x * 1.1,
        lambda x: x - "s",
    ],
)
@sql_count_checker(query_count=0)
def test_binary_arithmetic_method_string_scalar_negative(func):
    data = ["snowflake"]
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        func,
        expect_exception=True,
        assert_exception_equal=False,
        expect_exception_type=SnowparkSQLException,
        expect_exception_match="Numeric value .+ is not recognized",
    )


@sql_count_checker(query_count=0)
def test_binary_arithmetic_method_array_scalar():
    snow_df = pd.DataFrame([[[1]]])
    # TODO SNOW-862613: Support array repetition in df.mul
    # This is working in pandas and will return [1, 1]
    with pytest.raises(
        SnowparkSQLException, match="Invalid argument types for function"
    ):
        (snow_df * 2).to_pandas()


@pytest.mark.parametrize(
    "data, scalars",
    [
        [
            [1, 2.5],
            [
                np.int8(1),
                np.int16(1),
                np.int32(1),
                np.int64(1),
                np.uint(1),
                np.longlong(1),
                np.half(7.4),
                np.float32(2.5),
                np.float64(2.5),
                np.double(2.5),
            ],
        ],
        [[True, False], [np.bool_(True)]],
        [
            [datetime.datetime(2021, 1, 1), datetime.datetime(2022, 12, 13)],
            [
                np.datetime64("2022-12-13"),
                native_pd.Timestamp(2021, 1, 1),
                pd.Timestamp(1513393355, unit="s", tz="US/Pacific"),
            ],
        ],
        [[1, 2, None, 4], [np.nan, pandas.NA, pandas.NaT]],
    ],
)
def test_binary_method_numpy_and_pandas_type_scalar(data, scalars):
    for scalar in scalars:
        with SqlCounter(query_count=1):
            snow_series = pd.Series(data)
            if pd.isna(scalar):
                # In Snowflake anything = NULL returns NULL
                assert (snow_series == scalar).to_pandas().tolist() == [None] * len(
                    data
                )
            else:
                native_series = native_pd.Series(data)
                eval_snowpark_pandas_result(
                    snow_series, native_series, lambda x, value=scalar: x == value
                )


@pytest.mark.parametrize(
    "data, scalars, expected_query_count",
    [
        [[1, 2, 4], [0, -1.1, 1e7], 3],
        [[100000, math.e, -100000], [0, -1.1, 1e7], 3],
        [["snowflake", "", "熊猫", "@$#%'\""], ["snowflake", "", "a", "z"], 4],
        [
            [
                datetime.datetime(2023, 1, 1),
                datetime.datetime(2023, 8, 19, 1, 2, 3),
            ],
            [datetime.datetime(2023, 1, 1), datetime.datetime(2023, 8, 19, 1, 2, 3)],
            2,
        ],
        [
            [
                datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(
                    2023,
                    8,
                    19,
                    1,
                    2,
                    3,
                    tzinfo=datetime.timezone(datetime.timedelta(hours=10)),
                ),
            ],
            [
                datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(
                    2023,
                    8,
                    19,
                    1,
                    2,
                    3,
                    tzinfo=datetime.timezone(datetime.timedelta(hours=10)),
                ),
            ],
            2,
        ],
        pytest.param(
            [pd.Timestamp(1, unit="ns")],
            [
                pd.Timestamp(0, unit="ns"),
                pd.Timestamp(1, unit="ns"),
                pd.Timestamp(2, unit="ns"),
            ],
            3,
            id="with_nanoseconds_no_timezone_SNOW_1628400",
        ),
        pytest.param(
            [pd.Timestamp(1, unit="ns", tz="Asia/Kolkata")],
            [
                pd.Timestamp(0, unit="ns", tz="Asia/Kolkata"),
                pd.Timestamp(1, unit="ns", tz="Asia/Kolkata"),
                pd.Timestamp(2, unit="ns", tz="Asia/Kolkata"),
            ],
            3,
            id="with_nanoseconds_and_timezone_SNOW_1628400",
        ),
    ],
)
@pytest.mark.parametrize(
    "op", [operator.eq, operator.ne, operator.gt, operator.ge, operator.lt, operator.le]
)
@pytest.mark.parametrize("obj_type", ["DataFrame", "Series"])
def test_binary_comparison_method_scalar(
    data, scalars, op, obj_type, expected_query_count
):
    if obj_type == "DataFrame":
        snow_obj = pd.DataFrame([data, data])
        native_obj = native_pd.DataFrame([data, data])
    else:
        snow_obj = pd.Series(data)
        native_obj = native_pd.Series(data)
    with SqlCounter(query_count=expected_query_count):
        for scalar in scalars:
            eval_snowpark_pandas_result(
                snow_obj, native_obj, lambda x, value=scalar: op(x, value)
            )


@sql_count_checker(query_count=0)
def test_multiple_comparison_negative():
    snow_series = pd.Series([1])
    native_series = native_pd.Series([1])
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda x: 0 < x <= 1,
        expect_exception=True,
        expect_exception_type=ValueError,
        assert_exception_equal=False,
        expect_exception_match="is ambiguous",
    )


@pytest.mark.parametrize(
    "data, scalar",
    [
        [[1], "s"],
        [[[bytes("s", "utf-8")]], "s"],
        [[["s"]], "s"],
        [[{"s": "s"}], "s"],
        [[datetime.datetime(2023, 1, 1)], datetime.datetime(2023, 1, 1).timestamp()],
    ],
)
@pytest.mark.parametrize("op", [operator.gt, operator.ge, operator.lt, operator.le])
@sql_count_checker(query_count=0)
def test_binary_comparison_between_non_supported_types(data, scalar, op):
    snow_series = pd.Series(data)
    native_series = native_pd.Series(data)
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda x: op(x, scalar),
        expect_exception=True,
        assert_exception_equal=False,
        expect_exception_type=SnowparkSQLException,
    )


def list_like_rhs_params(values):
    return [
        pytest.param(values, id="list"),
        # The ndarray is created with the float dtype to avoid raising TypeError for operations between
        # a float and NoneType.
        pytest.param(np.array(values, dtype=float), id="ndarray"),
        pytest.param(native_pd.Index(values), id="index"),
        pytest.param(native_pd.Index(values, name="some name"), id="index_with_name"),
    ]


@pytest.mark.parametrize("op", ["__or__", "__ror__", "__and__", "__rand__"])
@pytest.mark.parametrize(
    "rhs", list_like_rhs_params([True, False, True, False, False, False])
)
@sql_count_checker(query_count=1, join_count=1)
def test_binary_logic_operations_between_series_and_list_like(op, rhs):
    lhs = [True, True, False, False, True, False]
    eval_snowpark_pandas_result(
        *create_test_series(lhs), lambda df: getattr(df, op)(rhs)
    )


@pytest.mark.parametrize("op", ["__or__", "__ror__", "__and__", "__rand__"])
@pytest.mark.parametrize("rhs", list_like_rhs_params([True, False, True]))
@sql_count_checker(query_count=1)
def test_binary_logic_operations_between_df_and_list_like(op, rhs):
    # These operations are performed only on axis=1 for dataframes. There is no axis parameter for the op.
    lhs = [[True, True, False], [False, False, True]]
    eval_snowpark_pandas_result(*create_test_dfs(lhs), lambda df: getattr(df, op)(rhs))


@pytest.mark.parametrize(
    "op",
    [
        "eq",
        "ne",
        "gt",
        "lt",
        "ge",
        "le",
    ],
)
@pytest.mark.parametrize("rhs", list_like_rhs_params([0, 2, -11, -12, -99]))
@sql_count_checker(
    query_count=1, join_count=1, window_count=1
)  # before optimization, the window count is 5
def test_binary_comparison_between_series_and_list_like(op, rhs):
    lhs = [1, 2, -10, 3.14, -99]
    eval_snowpark_pandas_result(
        *create_test_series(lhs), lambda df: getattr(df, op)(rhs)
    )


@pytest.mark.parametrize(
    "op",
    [
        "eq",
        "ne",
        "gt",
        "lt",
        "ge",
        "le",
    ],
)
@pytest.mark.parametrize("rhs", list_like_rhs_params([1, 2, 3]))
@sql_count_checker(query_count=1, join_count=1, window_count=1)
def test_binary_comparison_between_df_and_list_like_on_axis_0(op, rhs):
    lhs = [[1, 2], [2, 3], [1, 3]]
    eval_snowpark_pandas_result(
        *create_test_dfs(lhs), lambda df: getattr(df, op)(rhs, axis=0)
    )


@pytest.mark.parametrize(
    "op",
    [
        "eq",
        "ne",
        "gt",
        "lt",
        "ge",
        "le",
    ],
)
@pytest.mark.parametrize("rhs", list_like_rhs_params([1, 2, 3]))
@sql_count_checker(query_count=1, join_count=0, window_count=0)
def test_binary_comparison_between_df_and_list_like_on_axis_1(op, rhs):
    lhs = [[1, 2, 2], [3, 1, 3]]
    eval_snowpark_pandas_result(
        *create_test_dfs(lhs), lambda df: getattr(df, op)(rhs, axis=1)
    )


TEST_DATA_FOR_BINARY_SERIES_NUMERIC = [
    native_pd.DataFrame([(1, 1), (1, -2), (0, -1)], columns=[0, 1]),
    native_pd.DataFrame([(1, 1.0), (1, -2.0), (0, -1.657)], columns=[0, 1]),
    native_pd.DataFrame([(10.01, 1.2), (1.3, -2.4), (0.1, -1.4)], columns=[0, 1]),
    native_pd.DataFrame([(1, None), (None, None), (None, -1), (42, 3)], columns=[0, 1]),
    native_pd.DataFrame(
        [(1, None), (None, None), (None, -2.0), (0, -1.657)], columns=[0, 1]
    ),
    native_pd.DataFrame(
        [(10.01, None), (None, None), (None, -2.4), (0.1, -1.4)], columns=[0, 1]
    ),
]

TEST_DATA_FOR_BINARY_SERIES_STRING = [
    native_pd.DataFrame([("abc", ""), ("s", "a"), ("", "-1.4")], columns=[0, 1]),
    native_pd.DataFrame(
        [("abc", None), (None, None), (None, "a"), ("", "-1.4")], columns=[0, 1]
    ),
]


@pytest.mark.parametrize(
    "native_df",
    TEST_DATA_FOR_BINARY_SERIES_NUMERIC + TEST_DATA_FOR_BINARY_SERIES_STRING,
)
@sql_count_checker(query_count=1, join_count=0)
def test_binary_add_between_series(native_df):
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df[0] + df[1],
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "testing_dfs_from_read_snowflake",
    [native_pd.DataFrame({"col0": [0]})],
    indirect=True,
)
def test_add_dataframe_from_read_snowflake_to_self_SNOW_2252101(
    testing_dfs_from_read_snowflake,
):
    eval_snowpark_pandas_result(
        *testing_dfs_from_read_snowflake,
        lambda df: repr(df + df),
        comparator=str.__eq__,
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "testing_dfs_from_read_snowflake",
    [native_pd.DataFrame({"col0": ["a"], "col1": ["b"]})],
    indirect=True,
)
def test_comparing_series_from_same_read_snowflake_result_does_not_require_join_SNOW_2250244(
    testing_dfs_from_read_snowflake,
):
    eval_snowpark_pandas_result(
        *testing_dfs_from_read_snowflake,
        lambda df: repr(df["col0"] == df["col1"]),
        comparator=str.__eq__,
    )


def _gen_random_int_list_with_nones(N: int) -> list[int]:
    assert N > 3
    arr = [random.randint(-1000000, 1000000) for _ in range(N)]

    indices = np.random.choice(list(range(N)), size=2, replace=False)

    # place at least one None
    arr[indices[0]] = None

    # place at least one 0
    arr[indices[1]] = 0

    return arr


def _gen_random_float_list_with_nones(N: int, scale=1.0) -> list[float]:
    assert N > 4
    arr = [np.random.normal(loc=0.0, scale=scale) for _ in range(N)]

    # add Nones, and various NaN
    indices = np.random.choice(list(range(N)), size=3, replace=False)
    arr[indices[0]] = np.nan
    arr[indices[1]] = pd.NA
    arr[indices[2]] = None

    return arr


# exclude S1/S2 boolean series for testing here because Snowflake doesn't support it.
S1 = native_pd.Series(
    [
        False,
        False,
        True,
        True,
        None,
        None,
        False,
        False,
        True,
        True,
        None,
        None,
        False,
        False,
        True,
        True,
        None,
        None,
    ]
)
S2 = native_pd.Series(
    [
        False,
        True,
        None,
        False,
        True,
        None,
        False,
        True,
        None,
        False,
        True,
        None,
        False,
        True,
        None,
        False,
        True,
        None,
    ]
)
S3 = _gen_random_int_list_with_nones(18)
S4 = _gen_random_int_list_with_nones(18)
S5 = _gen_random_float_list_with_nones(18)
S6 = _gen_random_float_list_with_nones(18, scale=0.002)
ALL_SNOWFLAKE_COMPATIBLE_NUMERIC_TEST_SERIES = [S3, S4, S5, S6]

all_supported_binary_ops = pytest.mark.parametrize(
    "op",
    [
        "truediv",
        "rtruediv",
        "floordiv",
        "rfloordiv",
        "mod",
        "rmod",
        "pow",
        "rpow",
        "__or__",
        "__ror__",
        "__and__",
        "__rand__",
        "add",
        "radd",
        "sub",
        "rsub",
        "mul",
        "rmul",
        "eq",
        "ne",
        "gt",
        "lt",
        "ge",
        "le",
    ],
)


@pytest.mark.parametrize("lhs", ALL_SNOWFLAKE_COMPATIBLE_NUMERIC_TEST_SERIES)
@pytest.mark.parametrize("rhs", ALL_SNOWFLAKE_COMPATIBLE_NUMERIC_TEST_SERIES)
@pytest.mark.parametrize("op", [operator.add, operator.sub, operator.mul])
@sql_count_checker(query_count=1, join_count=1)
def test_arithmetic_binary_ops_between_series_for_numeric_data(lhs, rhs, op):
    snow_ans = op(pd.Series(lhs), pd.Series(rhs))
    native_ans = op(native_pd.Series(lhs), native_pd.Series(rhs))

    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_ans, native_ans)


# The goal of tests below is to check whether fill_value is working as expected. Since we are testing all the
# arithmetic binary operations here:
# - 0 must be omitted from lhs and rhs series for division operators
# - mod is computed differently in Snowflake and native pandas when the lhs and rhs are of different signs.
# - np.nan and None are treated as the same value since fill_value is supposed to be a float.


@pytest.mark.parametrize(
    "op",
    [
        "add",
        "radd",
        "sub",
        "rsub",
        "mul",
        "rmul",
        "truediv",
        "rtruediv",
        "floordiv",
        "rfloordiv",
        "mod",
        "rmod",
        "pow",
        "rpow",
    ],
)
class TestFillValue:
    # This class tests the fill_value parameter with arithmetic operations.

    @pytest.mark.parametrize("fill_value", [3.14])
    @sql_count_checker(query_count=1, join_count=1)
    def test_binary_arithmetic_ops_between_series(self, op, fill_value):
        # Test whether fill_value param works for Series <op> Series.
        lhs = [-14, 16.8, np.nan, 175, np.nan]
        rhs = [-1.44, 99, 5, np.nan, np.nan]

        # fill_value is supposed to be used when either the lhs or rhs is NaN, not when both are NaN.
        def op_helper(ser):
            other = (
                pd.Series(rhs) if isinstance(ser, pd.Series) else native_pd.Series(rhs)
            )
            return getattr(ser, op)(other=other, fill_value=fill_value)

        eval_snowpark_pandas_result(*create_test_series(lhs), op_helper)

    @pytest.mark.parametrize("fill_value", [10.001])
    @sql_count_checker(query_count=1)
    def test_binary_arithmetic_ops_between_series_and_scalar(self, op, fill_value):
        # Test whether fill_value param works for Series <op> scalar.
        lhs = [14, 16.8, np.nan, 175, np.nan]
        rhs = 89
        # fill_value is supposed to be used when either the lhs or rhs is NaN, not when both are NaN.
        eval_snowpark_pandas_result(
            *create_test_series(lhs),
            lambda ser: getattr(ser, op)(rhs, fill_value=fill_value),
        )

    @pytest.mark.parametrize("fill_value", [10.001])
    @sql_count_checker(query_count=1)
    def test_binary_arithmetic_ops_between_series_and_scalar_nan(self, op, fill_value):
        # Test whether fill_value param works for Series <op> scalar.
        lhs = [14, 16.8, np.nan, 175, np.nan]
        rhs = np.nan

        # Snowpark pandas behavior
        # using fill_value as the rhs in the native pandas operation below
        expected_snowpark_pandas_result = getattr(native_pd.Series(lhs), op)(fill_value)
        actual_snowpark_pandas_result = getattr(pd.Series(lhs), op)(
            rhs, fill_value=fill_value
        )
        eval_snowpark_pandas_result(
            actual_snowpark_pandas_result,
            expected_snowpark_pandas_result,
            lambda ser: ser,
            atol=0.001,
        )

    @pytest.mark.parametrize("fill_value", [222])
    @sql_count_checker(query_count=1)
    def test_binary_arithmetic_ops_between_df_and_scalar(self, op, fill_value):
        # Test whether fill_value param works for DataFrame <op> scalar.
        lhs = [[14, 16.8], [np.nan, 175], [np.nan, math.e]]
        rhs = 89
        # fill_value is supposed to be used when either the lhs or rhs is NaN, not when both are NaN.
        eval_snowpark_pandas_result(
            *create_test_dfs(lhs),
            lambda ser: getattr(ser, op)(rhs, fill_value=fill_value),
        )

    @pytest.mark.parametrize("fill_value", [10.001])
    @sql_count_checker(query_count=1)
    def test_binary_arithmetic_ops_between_df_and_scalar_nan_behavior_deviates(
        self, op, fill_value
    ):
        # Test whether fill_value param works for DataFrame <op> scalar. Here, the behavior in Snowpark pandas and
        # native pandas is different. Snowpark pandas treats np.nan as a NULL value while native pandas uses it
        # as the rhs for all operations, therefore making the result np.nan.
        lhs = [[14, 16.8], [np.nan, 175], [np.nan, math.e]]
        rhs = np.nan

        # Native pandas behavior
        expected_native_pandas_result = native_pd.DataFrame([[np.nan, np.nan]] * 3)
        actual_native_pandas_result = getattr(native_pd.DataFrame(lhs), op)(
            rhs, fill_value=fill_value
        )
        assert_frame_equal(actual_native_pandas_result, expected_native_pandas_result)

        # Snowpark pandas behavior
        # using fill_value as the rhs in the native pandas operation below
        expected_snowpark_pandas_result = getattr(native_pd.DataFrame(lhs), op)(
            fill_value
        )
        actual_snowpark_pandas_result = getattr(pd.DataFrame(lhs), op)(
            rhs, fill_value=fill_value
        )
        eval_snowpark_pandas_result(
            actual_snowpark_pandas_result,
            expected_snowpark_pandas_result,
            lambda df: df,
            atol=0.001,
        )

    @pytest.mark.parametrize(
        "rhs", list_like_rhs_params([-14, np.nan, 24.4, 175, np.nan])
    )
    @pytest.mark.parametrize("fill_value", [24])
    @sql_count_checker(query_count=1, join_count=1)
    def test_binary_arithmetic_ops_between_series_and_list_like(
        self, op, rhs, fill_value
    ):
        # Test whether fill_value param works for Series <op> list-like object.
        lhs = [-14, 16.8, 175, np.nan, np.nan]

        # fill_value is supposed to be used when either the lhs or rhs is NaN, not when both are NaN.
        def op_helper(ser):
            other = rhs
            if isinstance(other, native_pd.Index) and isinstance(ser, native_pd.Series):
                # Native pandas does not support binary operations between a Series and list-like objects -
                # Series <op> list-like works as expected for all cases except when rhs is an Index object.
                index_as_list = other.tolist()
                # Since the behavior of all list-like objects is supposed to be the same, convert index to a list
                # for native pandas and compare it with the index version for Snowpark pandas.
                # The issue is that native pandas calls isna() directly on other and errors with:
                # ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()
                other = index_as_list
            return getattr(ser, op)(other=other, fill_value=fill_value)

        eval_snowpark_pandas_result(*create_test_series(lhs), op_helper)

    @pytest.mark.parametrize("rhs", list_like_rhs_params([2, np.nan, np.nan]))
    @pytest.mark.parametrize("fill_value", [10])
    @sql_count_checker(query_count=1, join_count=1)
    def test_binary_arithmetic_ops_between_df_and_list_like_axis_0(
        self, op, rhs, fill_value
    ):
        # Test whether fill_value param works for DataFrame <op> list-like object on axis=0.
        lhs = [
            [59, 189.5, 1.55, 15],
            [414, 67.8, np.nan, np.nan],
            [9, 9.14, np.nan, np.nan],
        ]

        # fill_value is supposed to be used when either the lhs or rhs is NaN, not when both are NaN.
        eval_snowpark_pandas_result(
            *create_test_dfs(lhs),
            lambda df: getattr(df, op)(other=rhs, fill_value=fill_value, axis=0),
        )

    @pytest.mark.parametrize("rhs", list_like_rhs_params([-55, -2, np.nan, np.nan]))
    @pytest.mark.parametrize("fill_value", [-5.5])
    @sql_count_checker(query_count=1)
    def test_binary_arithmetic_ops_between_df_and_list_like_axis_1(
        self, op, rhs, fill_value
    ):
        # Test whether fill_value param works for DataFrame <op> list-like object on axis=1.
        lhs = [[np.nan, -67.8, -35, np.nan], [-9, -3, np.nan, np.nan]]

        # fill_value is supposed to be used when either the lhs or rhs is NaN, not when both are NaN.
        eval_snowpark_pandas_result(
            *create_test_dfs(lhs),
            lambda df: getattr(df, op)(other=rhs, fill_value=fill_value, axis=1),
        )

    @pytest.mark.parametrize("fill_value", [native_pd.Series([1, 2, 3]), []])
    @sql_count_checker(query_count=0)
    def test_binary_arithmetic_ops_with_non_scalar_fill_value_negative(
        self, op, fill_value
    ):
        lhs = [-0.99, 16.8, np.nan, 175, np.nan]
        rhs = [-1.4, np.nan, 24.4, 2, np.nan]

        # Native pandas and Snowpark pandas have different error messages.
        with pytest.raises(
            ValueError, match="NumPy boolean array indexing assignment cannot assign"
        ):
            getattr(native_pd.Series(lhs), op)(other=rhs, fill_value=fill_value)
        with pytest.raises(ValueError, match="Only scalars can be used as fill_value."):
            getattr(pd.Series(lhs), op)(other=rhs, fill_value=fill_value)

    @sql_count_checker(query_count=1, join_count=1)
    def test_binary_arithmetic_ops_with_single_element_list_like_fill_value_negative(
        self, op
    ):
        lhs = [65, 16.8, np.nan, -23, np.nan]
        rhs = [2, np.nan, 24.4, -7, np.nan]

        # Native pandas allows users to pass in a single element list-like object and use this as the fill_value even
        # though fill_value is supposed to be a float. Snowpark pandas matches the pandas docstring and only allows
        # scalar values.
        fill_value = [1]

        # Same behavior in Snowpark pandas if fill_value is 1.
        def fill_value_helper(ser):
            _fill_value = 1 if isinstance(ser, pd.Series) else fill_value
            return getattr(ser, op)(other=rhs, fill_value=_fill_value)

        eval_snowpark_pandas_result(*create_test_series(lhs), fill_value_helper)

        # Snowpark pandas behavior with [1] fill_value.
        with pytest.raises(ValueError, match="Only scalars can be used as fill_value."):
            getattr(pd.Series(lhs), op)(other=rhs, fill_value=fill_value)

    @pytest.mark.parametrize("rhs", [55, np.nan])
    @sql_count_checker(query_count=1, join_count=1)
    def test_binary_op_between_series_of_different_lengths(self, op, rhs):
        lhs = [13, 56, 4.5, np.nan, 0.99]
        fill_value = 9.9

        def op_helper(ser):
            other = (
                pd.Series(rhs) if isinstance(ser, pd.Series) else native_pd.Series(rhs)
            )
            return getattr(ser, op)(other=other, fill_value=fill_value)

        eval_snowpark_pandas_result(*create_test_series(lhs), op_helper)

    @pytest.mark.parametrize(
        "rhs",
        list_like_rhs_params([55, np.nan]) + list_like_rhs_params([55, np.nan, 2, 3]),
    )
    @sql_count_checker(query_count=1, join_count=0)
    def test_binary_op_between_df_and_list_like_of_different_lengths_axis_1(
        self, op, rhs
    ):
        lhs = [[13, 56, 3], [4.5, 9, np.nan], [2.3, 0.99, 1]]
        fill_value = 9.9

        # Native pandas requires the number of elements in `other` to be same as the number of columns.
        def op_helper(df):
            other = rhs
            if isinstance(df, native_pd.DataFrame):
                other = rhs[:3]
                if len(other) < 3:
                    other = [55, np.nan, np.nan]
            return getattr(df, op)(other=other, fill_value=fill_value, axis=1)

        eval_snowpark_pandas_result(*create_test_dfs(lhs), op_helper)

    @pytest.mark.parametrize(
        "rhs",
        list_like_rhs_params([-55, np.nan])
        + list_like_rhs_params([-55, np.nan, -2, -3]),
    )
    @sql_count_checker(query_count=1, join_count=1)
    def test_binary_op_between_df_and_list_like_of_different_lengths_axis_0(
        self, op, rhs
    ):
        lhs = [[-13, -56, -3], [-4.5, -9, np.nan], [-2.3, -0.99, -1]]
        fill_value = -9.9

        # Native pandas requires the number of elements in `other` to be same as the number of columns.
        def op_helper(df):
            other = rhs
            if isinstance(df, native_pd.DataFrame):
                other = rhs[:3]
                if len(other) < 3:
                    other = [-55, np.nan, np.nan]
            return getattr(df, op)(other=other, fill_value=fill_value, axis=0)

        eval_snowpark_pandas_result(*create_test_dfs(lhs), op_helper)


@sql_count_checker(query_count=1, join_count=0)
def test_binary_string_add_between_series_and_scalar_with_fill_value():
    # Testing Series + scalar
    lhs = ["apple", "banana", None, None]
    rhs = "kitty-cat"
    fill_value = "fruit"
    eval_snowpark_pandas_result(
        *create_test_series(lhs), lambda ser: ser.add(other=rhs, fill_value=fill_value)
    )


@sql_count_checker(query_count=1, join_count=0)
def test_binary_string_add_between_df_and_scalar_with_fill_value():
    # Testing DataFrame + scalar
    lhs = [["apple", "banana", None], [None, "mango", None]]
    rhs = "kitty-cat"
    fill_value = "fruit"
    eval_snowpark_pandas_result(
        *create_test_dfs(lhs), lambda ser: ser.add(other=rhs, fill_value=fill_value)
    )


@sql_count_checker(query_count=1, join_count=1)
def test_binary_string_add_between_series_of_different_lengths_with_fill_value():
    lhs = ["apple", "banana", "kitty-cat", "mango"]
    rhs = ["potato", "orange"]
    fill_value = "fruit"

    def op_helper(ser):
        other = pd.Series(rhs) if isinstance(ser, pd.Series) else native_pd.Series(rhs)
        return ser.add(other=other, fill_value=fill_value)

    eval_snowpark_pandas_result(*create_test_series(lhs), op_helper)


@pytest.mark.parametrize(
    "op",
    ["add", "radd", "sub", "rsub", "mul", "rmul"],
)
@pytest.mark.parametrize("rhs", list_like_rhs_params([3.14, -7.888, 24, 67, 31]))
@sql_count_checker(query_count=1, join_count=1)
def test_binary_arithmetic_ops_between_series_and_list_like(op, rhs):
    lhs = [-0.32, 6.555, 1.34, 10, 0]
    eval_snowpark_pandas_result(
        *create_test_series(lhs), lambda df: getattr(df, op)(rhs)
    )


@pytest.mark.parametrize(
    "op",
    ["add", "radd", "sub", "rsub", "mul", "rmul"],
)
@pytest.mark.parametrize("rhs", list_like_rhs_params([3.14, -7.888, 24]))
@sql_count_checker(query_count=1, join_count=1)
def test_binary_arithmetic_ops_between_df_and_list_like_on_axis_0(op, rhs):
    lhs = [[-0.32, 6.555], [1.34, 10], [0, 1000]]
    eval_snowpark_pandas_result(
        *create_test_dfs(lhs), lambda df: getattr(df, op)(rhs, axis=0)
    )


@pytest.mark.parametrize(
    "op",
    ["add", "radd", "sub", "rsub", "mul", "rmul"],
)
@pytest.mark.parametrize("rhs", list_like_rhs_params([3.14, -7.888, 24]))
@sql_count_checker(query_count=1)
def test_binary_arithmetic_ops_between_df_and_list_like_on_axis_1(op, rhs):
    lhs = [[-0.32, 6.555, 1.34], [10, 0, 1000]]
    eval_snowpark_pandas_result(
        *create_test_dfs(lhs), lambda df: getattr(df, op)(rhs, axis=1)
    )


@pytest.mark.parametrize("op", ["truediv", "floordiv", "rtruediv", "rfloordiv"])
@pytest.mark.parametrize("rhs", list_like_rhs_params([12, -24, 7, -8, 24]))
@sql_count_checker(query_count=1, join_count=1)
def test_binary_div_between_series_and_list_like(op, rhs):
    lhs = [25, 2.5, 0.677, -3.33, -12]
    eval_snowpark_pandas_result(
        *create_test_series(lhs),
        lambda df: getattr(df, op)(try_convert_index_to_native(rhs)),
        atol=0.001,
    )


@pytest.mark.parametrize("op", ["truediv", "floordiv", "rtruediv", "rfloordiv"])
@pytest.mark.parametrize("rhs", list_like_rhs_params([67, -24, 7]))
@sql_count_checker(query_count=1, join_count=1)
def test_binary_div_between_df_and_list_like_on_axis_0(op, rhs):
    lhs = [[25, 2.5], [0.677, -3.33], [-12, 7.777]]
    eval_snowpark_pandas_result(
        *create_test_dfs(lhs), lambda df: getattr(df, op)(rhs, axis=0), atol=0.001
    )


@pytest.mark.parametrize("op", ["truediv", "floordiv", "rtruediv", "rfloordiv"])
@pytest.mark.parametrize("rhs", list_like_rhs_params([67, -24, 7]))
@sql_count_checker(query_count=1)
def test_binary_div_between_df_and_list_like_on_axis_1(op, rhs):
    lhs = [[25, 2.5, 0.677], [-3.33, -12, 7.777]]
    eval_snowpark_pandas_result(
        *create_test_dfs(lhs), lambda df: getattr(df, op)(rhs, axis=1), atol=0.001
    )


@pytest.mark.parametrize("lhs", ALL_SNOWFLAKE_COMPATIBLE_NUMERIC_TEST_SERIES)
@pytest.mark.parametrize("rhs", ALL_SNOWFLAKE_COMPATIBLE_NUMERIC_TEST_SERIES)
@pytest.mark.parametrize("op", [operator.truediv, operator.floordiv])
@sql_count_checker(query_count=1, join_count=1)
def test_arithmetic_binary_division_between_series(lhs, rhs, op):
    # truediv and floordiv have deviating behavior between Snowpark pandas and Snowflake. Use here Snowflake version.

    # div by zero causes both errors in Snowflake and pandas, test separately.
    # to avoid div by zero errors, replace 0 with 1
    rhs = [
        1 if el is not None and not pd.isna(el) and math.isclose(el, 0.0) else el
        for el in rhs
    ]

    snow_ans = op(pd.Series(lhs), pd.Series(rhs))

    native_rhs = native_pd.Series(rhs)

    # pandas will throw a bug in mask_zero_div_zero(...) due to zmask = y == 0 where y is a numpy array with dtype='object'.
    # For this reason, fix up rhs by replacing None with NaN for floordiv case where this happens.
    if op == operator.floordiv:
        native_rhs = native_rhs.fillna(np.nan)

    native_ans = op(native_pd.Series(lhs), native_rhs)

    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_ans, native_ans)


@pytest.mark.parametrize(
    "lhs", ALL_SNOWFLAKE_COMPATIBLE_NUMERIC_TEST_SERIES + [[42] * 18]
)
@pytest.mark.parametrize("rhs", [[0] * 18, [0.0] * 18, [0, None] * 9, [None, 0.0] * 9])
@pytest.mark.parametrize("op", [operator.truediv, operator.floordiv])
@sql_count_checker(query_count=0)
def test_arithmetic_binary_division_between_series_div_by_zero_negative(lhs, rhs, op):
    with pytest.raises(SnowparkSQLException, match="Division by zero"):
        op(
            pd.Series(lhs), pd.Series(rhs)
        ).to_pandas()  # call to_pandas because lazy else

    # avoid regressions when testing with pandas
    # note that pandas ONLY raises ZeroDivisonError when series type of either is object.
    # Else floating point logic (with Nan, inf, -inf) is used.
    native_lhs = native_pd.Series(lhs)
    native_rhs = native_pd.Series(rhs)
    if is_object_dtype(native_rhs.dtype) and is_object_dtype(native_lhs.dtype):
        pandas_match = (
            "float division by zero" if op == operator.truediv else "float divmod()"
        )
        with pytest.raises(ZeroDivisionError, match=pandas_match):
            op(native_lhs, native_rhs)


@pytest.mark.parametrize(
    "lhs", [S1]
)  # sufficient to test one series, because it's type based
@pytest.mark.parametrize("rhs", [S1, S3, S5])
@pytest.mark.parametrize(
    "op",
    [
        "add",
        "radd",
        "sub",
        "rsub",
        "mul",
        "rmul",
        "truediv",
        "rtruediv",
        "floordiv",
        "rfloordiv",
    ],
)
@sql_count_checker(query_count=0)
def test_arithmetic_binary_ops_between_series_with_booleans_negative(lhs, rhs, op):
    # negative test for operations involving boolean series, this fails in Snowflake. We match Snowflake semantics,
    # so fail here. In pandas this is supported.
    # do no test other way here, assume commutativity
    with pytest.raises(
        SnowparkSQLException, match="Invalid argument types for function"
    ):
        lhs_series = pd.Series(lhs)
        result = getattr(lhs_series, op)(pd.Series(rhs))
        result.to_pandas()


@all_supported_binary_ops
@sql_count_checker(query_count=0)
def test_other_with_native_pandas_object_raises(op):
    # native pandas object can not be passed as other arguments for binary operation
    with pytest.raises(
        TypeError, match="Please convert this to Snowpark pandas objects"
    ):
        lhs_series = pd.Series(S1)
        getattr(lhs_series, op)(S1)


@pytest.mark.parametrize(
    "lhs,rhs",
    [
        (
            native_pd.Series([4, 7, 4, 2], name="A"),
            native_pd.Series([1, 5, 3, 8], name="B"),
        ),  # matching index no duplicates
        (
            native_pd.Series([1, 2, 3], index=[0, 2, 1]),
            native_pd.Series([3, 2, 6], index=[0, 2, 1]),
        ),  # unsorted matching index, result should also be unsorted
        (
            native_pd.Series([1, 2, 3], index=[2, 1, 2]),
            native_pd.Series([3, 2, 6], index=[2, 1, 2]),
        ),  # matching index with duplicates
        (native_pd.Series([1, 2, 3]), native_pd.Series([3, 2])),  # unequal length
        (
            native_pd.Series([1, 2, 3]),
            native_pd.Series([3, 2, 6], index=[0, 2, 1]),
        ),  # overlapping index but different order
        (
            native_pd.Series([1, 2, 3], index=[2, 1, 2]),
            native_pd.Series([3, 2, 6], index=[2, 2, 1]),
        ),  # overlapping index but different order with duplicates
        (
            native_pd.Series([1, 2, 3], index=[2, 1, 2]),
            native_pd.Series([3, 2, 6, 3], index=[2, 1, 2, 1]),
        ),  # overlapping index but unequal length with duplicates
        (
            native_pd.Series([1, 2, 3], index=[5, 3, 4]),
            native_pd.Series([3, 2, 6], index=[3, 4, 2]),
        ),  # partially overlapping index
        (
            native_pd.Series([1, 2, 3], index=[7, 8, 9]),
            native_pd.Series([3, 2, 6], index=[0, 2, 1]),
        ),  # index with no overlap
        (
            native_pd.Series(
                [2, 3, 2], pd.MultiIndex.from_tuples([(1, 2), (0, 1), (0, 0)])
            ),
            native_pd.Series(
                [395, 23, -22], pd.MultiIndex.from_tuples([(0, 1), (0, 1), (0, 0)])
            ),
        ),  # Multi-index: partial overlap
        (
            native_pd.Series(
                [2, 3, 2],
                pd.MultiIndex.from_tuples([(1, 2), (0, 1), (0, 0)], names=["x", "y"]),
            ),
            native_pd.Series(
                [395, 23, -22],
                pd.MultiIndex.from_tuples([(0, 1), (0, 1), (0, 0)], names=["x", "y"]),
            ),
        ),  # Multi-index: partial overlap and matching index column names
        (
            native_pd.Series(
                [2, 3, 2],
                pd.MultiIndex.from_tuples([(1, 2), (0, 1), (0, 0)], names=["x", "z"]),
            ),
            native_pd.Series(
                [395, 23, -22],
                pd.MultiIndex.from_tuples([(0, 1), (0, 1), (0, 0)], names=["x", "y"]),
            ),
        ),  # Multi-index: partial overlap and non-matching index column names
        # same as above but will Nones in data
        (
            native_pd.Series([None, 2, None]),
            native_pd.Series([3, None, None], index=[0, 2, 1]),
        ),
        (
            native_pd.Series([None, 2, None], index=[7, 8, 9]),
            native_pd.Series([3, None, None], index=[0, 2, 1]),
        ),
        (
            native_pd.Series(
                [None, 3, None], pd.MultiIndex.from_tuples([(1, 2), (0, 1), (0, 0)])
            ),
            native_pd.Series(
                [395, None, None], pd.MultiIndex.from_tuples([(0, 1), (0, 1), (0, 0)])
            ),
        ),
        (
            native_pd.Series([None, 7, None, 2], name="A"),
            native_pd.Series([1, None, None, 8], name="B"),
        ),
        (
            native_pd.Series(
                [None, 3, None],
                pd.MultiIndex.from_tuples([(1, 2), (0, 1), (0, 0)], names=["x", "y"]),
            ),
            native_pd.Series(
                [None, None, -22],
                pd.MultiIndex.from_tuples([(0, 1), (0, 1), (0, 0)], names=["x", "y"]),
            ),
        ),
        (
            native_pd.Series(
                [None, 3, None],
                pd.MultiIndex.from_tuples([(1, 2), (0, 1), (0, 0)], names=["x", "z"]),
            ),
            native_pd.Series(
                [None, None, -22],
                pd.MultiIndex.from_tuples([(0, 1), (0, 1), (0, 0)], names=["x", "y"]),
            ),
        ),
        # None in index values
        (
            native_pd.Series([1, 2, 3], index=[None, 2, 1]),
            native_pd.Series([3, 2, 6], index=[None, 2, 1]),
        ),  # unsorted matching index, result should also be unsorted
        (
            native_pd.Series([1, 2, 3], index=[2, None, 2]),
            native_pd.Series([3, 2, 6], index=[2, None, 2]),
        ),  # matching index with duplicates
        (native_pd.Series([1, 2, 3]), native_pd.Series([3, 2])),  # unequal length
        (
            native_pd.Series([1, 2, 3], index=[0, None, 2]),
            native_pd.Series([3, 2, 6], index=[None, 2, 0]),
        ),  # overlapping index but different order
        (
            native_pd.Series([1, 2, 3], index=[2, None, 2]),
            native_pd.Series([3, 2, 6], index=[2, 2, None]),
        ),  # overlapping index but different order with duplicates
        (
            native_pd.Series([1, 2, 3], index=[2, None, 2]),
            native_pd.Series([3, 2, 6, 3], index=[2, None, 2, None]),
        ),  # overlapping index but unequal length with duplicates
        (
            native_pd.Series([1, 2, 3], index=[5, 3, None]),
            native_pd.Series([3, 2, 6], index=[3, None, 2]),
        ),  # partially overlapping index
        (
            native_pd.Series([1, 2, 3], index=[7, 8, None]),
            native_pd.Series([3, 2, 6], index=[0, 2, 1]),
        ),  # index with no overlap
    ],
)
@pytest.mark.parametrize("op", [operator.add])
def test_binary_add_between_series_for_index_alignment(lhs, rhs, op):
    def check_op(native_lhs, native_rhs, snow_lhs, snow_rhs):
        snow_ans = op(snow_lhs, snow_rhs)
        native_ans = op(native_lhs, native_rhs)
        # for one multi-index test case (marked with comment) the "inferred_type" doesn't match (Snowpark: float vs. pandas integer)
        with SqlCounter(query_count=1, join_count=1):
            eval_snowpark_pandas_result(
                snow_ans, native_ans, lambda s: s, check_index_type=False
            )

    snow_lhs, snow_rhs = pd.Series(lhs), pd.Series(rhs)
    check_op(lhs, rhs, snow_lhs, snow_rhs)
    # commute series
    check_op(rhs, lhs, snow_rhs, snow_lhs)


# MOD TESTS
TEST_DATA_THAT_MATCHES_FOR_DF_MOD = [
    native_pd.DataFrame([(1, None), (None, None), (None, -1), (42, 3)], columns=[0, 1]),
    native_pd.DataFrame(
        [(1, None), (None, None), (None, -2.0), (0, -1.657)], columns=[0, 1]
    ),
]

TEST_DATA_THAT_DEVIATES_FOR_DF_MOD = [
    [
        native_pd.DataFrame([(1, 1), (1, -2), (0, -1)], columns=[0, 1]),
        native_pd.Series([0, 1, 0]),
    ],
    [
        native_pd.DataFrame([(1, 1.0), (1, -2.0), (0, -1.657)], columns=[0, 1]),
        native_pd.Series([0.0, 1.0, 0.0]),
    ],
    [
        native_pd.DataFrame([(10.01, 1.2), (1.3, -2.4), (0.1, -1.4)], columns=[0, 1]),
        native_pd.Series([0.41000000000000014, 1.3, 0.1]),
    ],
    [
        native_pd.DataFrame(
            [(10.01, None), (None, None), (None, -2.4), (0.1, -1.4)], columns=[0, 1]
        ),
        native_pd.Series([np.nan, np.nan, np.nan, 0.1]),
    ],
]

TEST_DATA_THAT_DEVIATES_FOR_SERIES_TO_SERIES_MOD = [
    [
        [
            native_pd.Series([1.0, 0, 0.009, -1.08, -10000, -0.01, -10, 8.6]),
            native_pd.Series([0.0987, -3.3334, 5729, -19604, 0.2, 6.543, -0.342, 34.0]),
        ],
        native_pd.Series(
            [
                0.013000000000000012,
                0.0,
                0.009,
                -1.08,
                0.0,
                -0.01,
                -0.08199999999999896,
                8.6,
            ]
        ),
    ],
    [
        [
            native_pd.Series(list(range(-5, 5))),
            native_pd.Series(list(range(67, 77))),
        ],
        native_pd.Series([-5, -4, -3, -2, -1, 0, 1, 2, 3, 4]),
    ],
    [
        [native_pd.Series(list(range(40, 50))), native_pd.Series(list(range(-15, -5)))],
        native_pd.Series([10, 13, 3, 7, 0, 5, 1, 7, 6, 1]),
    ],
    [
        [native_pd.Series([7, 7, -7, -7]), native_pd.Series([5, -5, 5, -5])],
        native_pd.Series([2, 2, -2, -2]),
    ],
    [
        [
            native_pd.Series(list(range(23, 33))),
            native_pd.Series(list(range(-15, -5))),
        ],
        native_pd.Series([8, 10, 12, 2, 5, 8, 2, 6, 3, 2]),
    ],
    [
        [
            native_pd.Series([0.0008, -0.007, 10022, -8987, 67.450, 0, 71.0, -6.666]),
            native_pd.Series([1.0, 12.98, 10022, 0.035, -6.6667, -10000, -0.001, -77]),
        ],
        native_pd.Series(
            [
                0.0008,
                -0.007,
                0.0,
                -0.014999999999417923,
                0.7830000000000013,
                0.0,
                0.0,
                -6.666,
            ]
        ),
    ],
    [
        [
            native_pd.Series(
                [-10, 20, -30, 40, -50.356, 60.402, -70.25896, 80.28952, -90.22]
            ),
            native_pd.Series([3, -3, 0.89, -67.4, 6, -10000, 0.001, -77.348, 3.24]),
        ],
        native_pd.Series(
            [
                -1.0,
                2.0,
                -0.629999999999999,
                40.0,
                -2.3560000000000016,
                60.402,
                -0.0009600000000062892,
                2.941519999999997,
                -2.739999999999995,
            ]
        ),
    ],
    [
        [native_pd.Series(list(range(5))), native_pd.Series([1, -2, 3.5, -1.4, None])],
        native_pd.Series([0.0, 1.0, 2.0, 0.20000000000000018, np.nan]),
    ],
    [
        [
            native_pd.Series([1.0, 0, 0.009, -1.08, -10000, -0.01, -10, 8.6]),
            native_pd.Series([0.0987, -3.3334, -5729, 19604, 0, 6.543, -0.342, 34.0]),
        ],
        native_pd.Series(
            [
                0.013000000000000012,
                0.0,
                0.009,
                -1.08,
                np.nan,
                -0.01,
                -0.08199999999999896,
                8.6,
            ]
        ),
    ],
    [
        [
            native_pd.Series(list(range(23, 33))),
            native_pd.Series(list(range(-5, 5))),
        ],
        native_pd.Series([3.0, 0.0, 1.0, 0.0, 0.0, np.nan, 0.0, 0.0, 1.0, 0.0]),
    ],
    [
        [
            native_pd.Series([0.0008, -0.007, 10022, -8987, 67.450, 0, 71.0, -6.666]),
            native_pd.Series([1.0, 12.98, 0, 0.035, -6.6667, -10000, -0.001, -77]),
        ],
        native_pd.Series(
            [
                0.0008,
                -0.007,
                np.nan,
                -0.014999999999417923,
                0.7830000000000013,
                0.0,
                0.0,
                -6.666,
            ]
        ),
    ],
    [
        [native_pd.Series([1, -2, 3.5, -1.4, None]), native_pd.Series(list(range(5)))],
        native_pd.Series([np.nan, 0.0, 1.5, -1.4, np.nan]),
    ],
]

TEST_DATA_THAT_FAILS_RAISES_NOT_IMPLEMENTED_ERROR_MOD_NUMERIC = [
    [native_pd.Series([None] * 10), native_pd.Series(list(range(10)))],
    [native_pd.Series(list(range(10))), native_pd.Series([None] * 10)],
]

TEST_DATA_FOR_STR_SERIES_MOD_THAT_PASSES = [
    # Bug in native pandas and Snowpark - using the integer 0 with %d raises a weird error
    # but with %f it works fine
    # [native_pd.Series(['test %d'] * 3), native_pd.Series(list(range(3)))],
    [native_pd.Series(["test %d"] * 3), native_pd.Series([1, 2, 3])],
    [
        native_pd.Series(["test %d"] * 4),
        native_pd.Series([0.9999, 137572.345, -12846.1838, -1.3871]),
    ],
    [
        native_pd.Series(["test %.2f_"] * 7),
        native_pd.Series([1.111111, -9999999, -0.66666667, 3333.333333, 0, 12, -327]),
    ],
]

TEST_DATA_FOR_STR_SERIES_MOD_THAT_FAILS = [
    [native_pd.Series(["test %d", "test", "test %"]), native_pd.Series(list(range(3)))],
    [
        native_pd.Series(["test %d"] * 5),
        native_pd.Series([0.9999, None, -12846.1838, -1.3871, 0]),
    ],
    [
        native_pd.Series(["test %d", "test %", None]),
        native_pd.Series([0.9999, -12846.1838, -1.3871]),
    ],
]

TEST_DATA_FOR_STR_SERIES_MOD = (
    TEST_DATA_FOR_STR_SERIES_MOD_THAT_PASSES + TEST_DATA_FOR_STR_SERIES_MOD_THAT_FAILS
)

TEST_DATA_THAT_MATCHES_FOR_SERIES_MOD = [
    [native_pd.Series([0.0] * 10), native_pd.Series([0.0] * 10)],
    [native_pd.Series([0.0] * 10), native_pd.Series([0] * 10)],
    [native_pd.Series([0] * 10), native_pd.Series([0] * 10)],
    [native_pd.Series(list(range(40, 50))), native_pd.Series([0] * 10)],
]


@pytest.mark.parametrize("native_df", TEST_DATA_THAT_MATCHES_FOR_DF_MOD)
@sql_count_checker(query_count=1, join_count=0)
def test_binary_mod_matches_between_df(native_df):
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df[0] % df[1],
    )


# Snowpark mod with negative operands has different behavior/computation than
# traditional mod and native pandas mod.
@pytest.mark.parametrize("native_df, res", TEST_DATA_THAT_DEVIATES_FOR_DF_MOD)
@sql_count_checker(query_count=2, join_count=0)
def test_binary_mod_deviates_between_df(native_df, res):
    snow_df = pd.DataFrame(native_df)

    with pytest.raises(AssertionError):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df[0] % df[1],
        )

    snow_res = snow_df[0] % snow_df[1]
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_res, res)


# Snowpark mod with negative operands has different behavior/computation than
# traditional mod and native pandas mod.
@pytest.mark.parametrize("ser, res", TEST_DATA_THAT_DEVIATES_FOR_SERIES_TO_SERIES_MOD)
@sql_count_checker(query_count=2, join_count=2)
def test_binary_that_deviates_for_series_mod(ser, res):
    snowpark_res = pd.Series(ser[0]) % pd.Series(ser[1])
    native_res = ser[0] % ser[1]

    with pytest.raises(AssertionError):
        assert_series_equal(snowpark_res.to_pandas(), native_res, check_dtype=False)

    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snowpark_res, res)


@pytest.mark.parametrize(
    "ser", TEST_DATA_THAT_FAILS_RAISES_NOT_IMPLEMENTED_ERROR_MOD_NUMERIC
)
# The pandas None Series is treated as a StringType, therefore it's a
# NotImplementedError, not SnowparkSQLException.
@sql_count_checker(query_count=0)
def test_binary_fails_raises_not_implemented_error_for_series_mod(ser):
    with pytest.raises(NotImplementedError):
        pd.Series(ser[0]) % pd.Series(ser[1])


# Using % with str/str values. Should fail since the functionality is
# not implemented, behavior unknown.
@pytest.mark.parametrize("ser", TEST_DATA_FOR_STR_SERIES_MOD)
@sql_count_checker(query_count=0)
def test_binary_for_str_series_mod(ser):
    with pytest.raises(NotImplementedError):
        pd.Series(ser[0]) % pd.Series(ser[1])


# Should match for operands with non-negative values.
@pytest.mark.parametrize("ser", TEST_DATA_THAT_MATCHES_FOR_SERIES_MOD)
@sql_count_checker(query_count=1, join_count=1)
def test_binary_that_matches_for_series_mod(ser):
    snowpark_res = pd.Series(ser[0]) % pd.Series(ser[1])
    native_res = ser[0] % ser[1]

    assert_series_equal(
        snowpark_res.to_pandas(), native_res, check_dtype=False, check_index_type=False
    )


# The way mod is computed in Snowpark pandas and native pandas differs, therefore ensure that only numbers of the
# same sign are mod'd together. Snowflake and native pandas compute the mod of two numbers with opposite signs in
# different ways.
@pytest.mark.parametrize("op", ["mod", "rmod"])
@pytest.mark.parametrize("rhs", list_like_rhs_params([12, 10, -12, -9.8, 45]))
@sql_count_checker(query_count=1, join_count=1)
def test_binary_mod_between_series_and_list_like(op, rhs):
    lhs = [100, 283, -34, -56, 7]
    eval_snowpark_pandas_result(
        *create_test_series(lhs), lambda df: getattr(df, op)(rhs)
    )


@pytest.mark.parametrize("op", ["mod", "rmod"])
@pytest.mark.parametrize("rhs", list_like_rhs_params([-25, 17, 10]))
@sql_count_checker(query_count=1, join_count=1)
def test_binary_mod_between_df_and_list_like_on_axis_0(op, rhs):
    lhs = [[-73, -18], [7287, 159], [158, 267]]
    eval_snowpark_pandas_result(
        *create_test_dfs(lhs), lambda df: getattr(df, op)(rhs, axis=0)
    )


@pytest.mark.parametrize("op", ["mod", "rmod"])
@pytest.mark.parametrize("rhs", list_like_rhs_params([-25, 17, 10]))
@sql_count_checker(query_count=1)
def test_binary_mod_between_df_and_list_like_on_axis_1(op, rhs):
    lhs = [[-73, 18, 7287], [-159, 158, 267]]
    eval_snowpark_pandas_result(
        *create_test_dfs(lhs), lambda df: getattr(df, op)(rhs, axis=1)
    )


# The mod behavior in Snowpark pandas is different from native pandas if mod/rmod is performed on operands of
# different signs. Tests below highlight the difference in behavior for DataFrame/Series <op> list-like.
@pytest.mark.parametrize("op", ["mod", "rmod"])
@pytest.mark.parametrize("rhs", list_like_rhs_params([-12, 10, -12, -9.8, -45]))
@sql_count_checker(query_count=1, join_count=1)
def test_binary_mod_between_series_and_list_like_deviating_behavior(op, rhs):
    lhs = [-100, 283, 34, -56, 7]
    with pytest.raises(AssertionError, match="Series are different"):
        eval_snowpark_pandas_result(
            *create_test_series(lhs), lambda df: getattr(df, op)(rhs)
        )


@pytest.mark.parametrize("op", ["mod", "rmod"])
@pytest.mark.parametrize("rhs", list_like_rhs_params([-25, 17, -10]))
@sql_count_checker(query_count=1, join_count=1)
def test_binary_mod_between_df_and_list_like_on_axis_0_deviating_behavior(op, rhs):
    lhs = [[-73, -18], [7287, -159], [-158, 267]]
    # The error message points out the values at respective indices that are different.
    with pytest.raises(AssertionError, match="are different"):
        eval_snowpark_pandas_result(
            *create_test_dfs(lhs), lambda df: getattr(df, op)(rhs, axis=0)
        )


@pytest.mark.parametrize("op", ["mod", "rmod"])
@pytest.mark.parametrize("rhs", list_like_rhs_params([-25, 17, 10]))
@sql_count_checker(query_count=1)
def test_binary_mod_between_df_and_list_like_on_axis_1_deviating_behavior(op, rhs):
    lhs = [[-73, 18, 7287], [159, 158, 267]]
    # The error message points out the values at respective indices that are different.
    with pytest.raises(AssertionError, match="are different"):
        eval_snowpark_pandas_result(
            *create_test_dfs(lhs), lambda df: getattr(df, op)(rhs, axis=1)
        )


# POW TESTS
TEST_DATA_FOR_SERIES_POW = [
    [native_pd.Series(list(range(40, 50))), native_pd.Series([0] * 10)],
    [
        native_pd.Series([1.0, 0, 0.009, -1.08, -10000, -0.01, -10, 8.6]),
        native_pd.Series([0.0987, 3.3334, 5729, 19604, 0, 6.543, 0.342, 34.0]),
    ],
    [
        native_pd.Series([0.0008, -0.007, 10022, -8987, 67.450, 0, 71.0, -6.666]),
        native_pd.Series([1.0, 12.98, 0, 0.035, 6.6667, 10000, 0.001, 77]),
    ],
    [native_pd.Series([0] * 10), native_pd.Series([0] * 10)],
    [native_pd.Series([0.0] * 10), native_pd.Series([0.0] * 10)],
    [native_pd.Series([0.0] * 10), native_pd.Series([0] * 10)],
    [
        native_pd.Series([0.0008, -0.007, 10022, -8987, None, 0, 71.0, -6.666]),
        native_pd.Series([1.0, 12.98, None, 0.035, 6.6667, 10000, 0.001, 77]),
    ],
    [
        native_pd.Series([1.0, 0, 0.009, -1.08, -10000, -0.01, -10, 8.6]),
        native_pd.Series([0.0987, -3.3334, 5729, -19604, 0, 6.543, -0.342, 34.0]),
    ],
    [
        native_pd.Series([0.0008, -0.007, 10022, -8987, 67.450, 0, 71.0, -6.666]),
        native_pd.Series([1.0, 12.98, 0, 0.035, -6.6667, -10000, -0.001, -77]),
    ],
    [
        native_pd.Series([1.0, 0, 0.09, -1.08, -10000, -0.1, -10, 8.6]),
        native_pd.Series([-1, -3, -2, 0, -1, 3, -4, -3]),
    ],
    [
        native_pd.Series([0.0008, -0.07, 10022, -8987, 67.450, 10, 71.0, -6.666]),
        native_pd.Series([0, -3, -2, -1, -2, -5, -2, -34]),
    ],
    [
        native_pd.Series([100, 9, np.nan, -100.02, 3.15, 2.0]),
        native_pd.Series([-1.4, -0.99, -0.99, -0.2, -2.2, -3.4, -0.5]),
    ],
    [
        native_pd.Series(list(range(0, 11))),
        native_pd.Series(
            [-2.5, -0.5, -0.75, -9.9, -1.5, -8.5, -0.1, -0.01, -0.05, -0.12, -0.99]
        ),
    ],
    [
        native_pd.Series([34, -2, -7, 3, 7]),
        native_pd.Series([0.0008, -0.007, -10.1, -8987, 0]),
    ],
    [
        native_pd.Series([0.1, 2.3, -0.98, -32.5, 234.567, -765.456]),
        native_pd.Series([-0.2, -0.007, -10.1, -89.87, -99.033]),
    ],
    [
        native_pd.Series([34, -2, -7, 3, 7]),
        native_pd.Series([None, -0.007, -10.1, -8987, 0]),
    ],
    [
        native_pd.Series([0.1, 2.3, None, -32.5, 234.567, -765.456]),
        native_pd.Series([-0.2, -0.007, -10.1, -89.87, -99.033]),
    ],
    [native_pd.Series([None] * 10), native_pd.Series([None] * 10)],
]

TEST_DATA_THAT_DEVIATES_FOR_SERIES_POW = [
    [
        [
            native_pd.Series([1.0, 0, 0.009, -1.08, -10000, -0.01, -10, 8.6]),
            native_pd.Series([0.0987, -3.3334, 5729, -19604, 0.2, 6.543, -0.342, 34.0]),
        ],
        native_pd.Series(
            [1.0, float("inf"), 0.0, 0.0, np.nan, np.nan, np.nan, 5.9285343740780185e31]
        ),
    ],
    [
        [native_pd.Series([None] * 10), native_pd.Series(list(range(-5, 5)))],
        native_pd.Series(
            [
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                1.0,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
            ]
        ),
    ],
    [
        [native_pd.Series(list(range(-5, 5))), native_pd.Series([None] * 10)],
        native_pd.Series(
            [
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                1.0,
                np.nan,
                np.nan,
                np.nan,
            ]
        ),
    ],
]


# Native pandas raises a ValueError for 0 raised to any negative integer,
# Snowpark returns inf as the result, therefore excluding input data at index 0.
@pytest.mark.parametrize("native_df", TEST_DATA_FOR_BINARY_SERIES_NUMERIC[1:])
@sql_count_checker(query_count=1, join_count=0)
def test_binary_pow_between_series(native_df):
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df[0] ** df[1],
    )


# 1 ** None and None ** 0 yield 1.0 in Snowpark while native pandas produces np.nan.
# 0 ** any negative number produces inf in Snowpark while native pandas produces np.nan.
@pytest.mark.parametrize("ser, res", TEST_DATA_THAT_DEVIATES_FOR_SERIES_POW)
@sql_count_checker(query_count=1, join_count=1)
def test_binary_pow_deviating_behavior_series(ser, res):
    snowpark_res = pd.Series(ser[0]) ** pd.Series(ser[1])
    assert_snowpark_pandas_equal_to_pandas(snowpark_res, res)


@pytest.mark.parametrize("ser", TEST_DATA_FOR_SERIES_POW)
@sql_count_checker(query_count=1, join_count=1)
def test_binary_for_series_pow(ser):
    snowpark_res = pd.Series(ser[0]) ** pd.Series(ser[1])
    native_res = ser[0] ** ser[1]
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snowpark_res, native_res)


# Test pow and rpow separately because in native pandas integers cannot be raised to a negative power:
# rpow is performing pow in the reverse direction and therefore needs different inputs. In Snowpark pandas, it is valid
# to raise an integer to a negative power, which is tested in the tests above.
@pytest.mark.parametrize("rhs", list_like_rhs_params([3, 5, 2, 25, 16]))
@sql_count_checker(query_count=1, join_count=1)
def test_binary_pow_between_series_and_list_like(rhs):
    lhs = [29, 15, -36, 1, -12]
    eval_snowpark_pandas_result(*create_test_series(lhs), lambda df: df.pow(rhs))


@pytest.mark.parametrize("rhs", list_like_rhs_params([-25, 186, -1, 2, 6]))
@sql_count_checker(query_count=1, join_count=1)
def test_binary_rpow_between_series_and_list_like(rhs):
    lhs = [4, 1, 362, 62, 12]
    eval_snowpark_pandas_result(*create_test_series(lhs), lambda df: df.rpow(rhs))


@pytest.mark.parametrize("rhs", list_like_rhs_params([1, 2, 3]))
@sql_count_checker(query_count=1, join_count=1)
def test_binary_pow_between_df_and_list_like_on_axis_0(rhs):
    lhs = [[12, -36], [2, -8], [21, -78]]
    eval_snowpark_pandas_result(*create_test_dfs(lhs), lambda df: df.pow(rhs, axis=0))


@pytest.mark.parametrize("rhs", list_like_rhs_params([1, 2, 3]))
@sql_count_checker(query_count=1)
def test_binary_pow_between_df_and_list_like_on_axis_1(rhs):
    lhs = [[12, -36, 2], [-8, 21, -78]]
    eval_snowpark_pandas_result(*create_test_dfs(lhs), lambda df: df.pow(rhs, axis=1))


@pytest.mark.parametrize("rhs", list_like_rhs_params([72, -15, -62]))
@sql_count_checker(query_count=1, join_count=1)
def test_binary_rpow_between_df_and_list_like_on_axis_0(rhs):
    lhs = [[1, 2], [3, 4], [5, 6]]
    eval_snowpark_pandas_result(*create_test_dfs(lhs), lambda df: df.rpow(rhs, axis=0))


@pytest.mark.parametrize("rhs", list_like_rhs_params([72, -15, -4]))
@sql_count_checker(query_count=1)
def test_binary_rpow_between_df_and_list_like_on_axis_1(rhs):
    lhs = [[1, 2, 3], [4, 5, 6]]
    eval_snowpark_pandas_result(*create_test_dfs(lhs), lambda df: df.rpow(rhs, axis=1))


@pytest.mark.parametrize(
    "opname",
    [
        "add",
        "sub",
        "truediv",
        "rtruediv",
        "floordiv",
        "rfloordiv",
        "floordiv",
        "rfloordiv",
        "pow",
        "rpow",
        "add",
        "radd",
        "sub",
        "rsub",
        "mul",
        "rmul",
        "eq",
        "ne",
        "gt",
        "lt",
        "ge",
        "le",
        "mod",
        "rmod",
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_generated_docstring_examples(opname):
    # test for operators that correct examples are generated and match up with pandas.
    # if this test passes, this ensures that all the examples generated in utils.py will be correct.
    # Native pandas and Snowpark have different behavior for 1 % -2. Native pandas produces -1.0 while Snowpark
    # produces 1.0.

    # we differ here with the data from pandas docs, because pandas has different div by zero behavior
    # this here is the original pandas example data:
    # a_data = {'data':[1, 1, 1, np.nan], 'index':["a", "b", "c", "d"]}
    # b_data = {'data': [0, 1, 2, np.nan, 1], 'index' : ["a", "b", "c", "d", "f"]}
    a_data = {"data": [1, -2, 0, np.nan], "index": ["a", "b", "c", "d"]}
    b_data = {"data": [-2, 1, 3, np.nan, 1], "index": ["a", "b", "c", "d", "f"]}

    # to avoid div-by-zero, swap for rtruediv and rfloordiv a,b
    if opname in ["rtruediv", "rfloordiv"]:
        a_data, b_data = b_data, a_data

    # Snowpark and native pandas can have different behavior for negative operands in mod operations.
    if opname in ["mod", "rmod"]:
        a_data = {"data": [1, 2, 0, np.nan], "index": ["a", "b", "c", "d"]}
        b_data = {"data": [2, 1, 3, np.nan, 1], "index": ["a", "b", "c", "d", "f"]}

    def helper(dummy):
        nonlocal opname
        if isinstance(dummy, pd.Series):
            a = pd.Series(**a_data)
            b = pd.Series(**b_data)
        else:
            a = native_pd.Series(**a_data)
            b = native_pd.Series(**b_data)
        ans = getattr(a, opname)(b)
        if opname in ["eq", "ne", "ge", "le", "gt", "lt"] and isinstance(
            dummy, native_pd.Series
        ):
            ans = ans.astype(object)
            ans[["d", "f"]] = None
        return ans

    eval_snowpark_pandas_result(pd.Series(), native_pd.Series(), helper)


@pytest.mark.parametrize("lhs", ALL_SNOWFLAKE_COMPATIBLE_NUMERIC_TEST_SERIES)
@pytest.mark.parametrize("rhs", ALL_SNOWFLAKE_COMPATIBLE_NUMERIC_TEST_SERIES)
@pytest.mark.parametrize(
    "op", [operator.eq, operator.ne, operator.gt, operator.ge, operator.lt, operator.le]
)
@sql_count_checker(query_count=1, join_count=1)
def test_binary_comparison_method_between_series_numeric(lhs, rhs, op):
    # when compare NULL with other value, Snowflake always returns NULL
    # so we use pd.NA here to simulate ternary logic
    snow_ans = op(pd.Series(lhs), pd.Series(rhs))
    native_ans = op(
        native_pd.Series(lhs, dtype="Float64"), native_pd.Series(rhs, dtype="Float64")
    )
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_ans, native_ans)


@pytest.mark.parametrize(
    "op", [operator.eq, operator.ne, operator.gt, operator.ge, operator.lt, operator.le]
)
@sql_count_checker(query_count=0)
def test_binary_comparison_method_between_series_different_types(op):
    with pytest.raises(
        SnowparkSQLException, match="Numeric value .* is not recognized"
    ):
        op(pd.Series(["s", "n", "o", "w"]), pd.Series([1, 2, 3, 4])).to_pandas()


@pytest.mark.parametrize("lhs", [["s", 1, 2.2, True, ["b", "a"], None]])
@pytest.mark.parametrize("rhs", [["n", 3, 3.3, False, ["a", "b"], None]])
@pytest.mark.parametrize(
    "op", [operator.eq, operator.ne, operator.gt, operator.ge, operator.lt, operator.le]
)
@sql_count_checker(query_count=2, join_count=2)
def test_binary_comparison_method_between_series_variant(lhs, rhs, op):
    snow_ans = op(pd.Series(lhs), pd.Series(rhs))
    native_ans = op(native_pd.Series(lhs), native_pd.Series(rhs))
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_ans.iloc[:-1], native_ans.iloc[:-1]
    )
    assert snow_ans.iloc[-1] is None


@pytest.mark.parametrize(
    "dateoffset",
    [
        pd.DateOffset(),
        pd.DateOffset(5),
        pd.DateOffset(
            years=1,
            months=2,
            weeks=3,
            days=4,
            hours=5,
            minutes=6,
            seconds=7,
            microseconds=8,
        ),
        pd.DateOffset(nanoseconds=1),
    ],
)
def test_binary_method_datetime_with_dateoffset_timedelta(dateoffset):
    # Note: pandas 1.5.3 has a bug where creating a DateOffset with millisecond value results in a TypeError
    # Note 2: pandas drops nanoseconds when it is used in conjunction with days/minutes/hours/seconds for DateOffset
    # Note 3: Snowpark pandas converts DateOffset to INTERVAL, which can only be used on the RHS of an expression so
    # commutativity is not yet supported.
    native_series = native_pd.Series(
        [
            datetime.datetime(
                2021,
                1,
                2,
                3,
                4,
                5,
            ),
            datetime.datetime(2022, 12, 13),
        ]
    )
    snow_series = pd.Series(native_series)
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_series, native_series, lambda ser: ser + dateoffset
        )
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_series, native_series, lambda ser: ser - dateoffset
        )


@pytest.mark.parametrize(
    "dateoffset",
    [
        pd.DateOffset(year=1),
        pd.DateOffset(month=1),
        pd.DateOffset(day=1),
        pd.DateOffset(weekday=1),
        pd.DateOffset(hour=1),
        pd.DateOffset(minute=1),
        pd.DateOffset(second=1),
        pd.DateOffset(microsecond=1),
        pd.DateOffset(nanosecond=1),
        pd.DateOffset(years=2, day=1),
    ],
)
@sql_count_checker(query_count=0)
def test_binary_method_datetime_with_dateoffset_replacement(dateoffset):
    # TODO SNOW-1007629: Support DateOffset with replacement offset values
    snow_series = pd.Series(
        [
            datetime.datetime(
                2021,
                1,
                2,
                3,
                4,
                5,
            ),
            datetime.datetime(2022, 12, 13),
        ]
    )
    with pytest.raises(
        NotImplementedError,
        match="DateOffset with parameters that replace the offset value are not yet supported.",
    ):
        snow_series + dateoffset


@pytest.mark.parametrize(
    "df,s",
    [
        # example in docstring
        (
            native_pd.DataFrame({"A": [4, 6, None, 7], "B": [2, 5, 4, None]}),
            native_pd.Series([9, 10, 12], index=[3, 1, 4]),
        ),
        (
            native_pd.DataFrame({"A": [4, 6, None, 7], "B": [2, 5, 4, None]}),
            native_pd.Series([1, 4, 3, 8]),
        ),
    ],
)
def test_binary_add_dataframe_and_series_axis0(df, s):
    snow_df = pd.DataFrame(df)
    snow_s = pd.Series(s)

    # DataFrame <op> Series
    with SqlCounter(query_count=1):
        ans = df.add(s, axis=0)
        snow_ans = snow_df.add(snow_s, axis=0)

        eval_snowpark_pandas_result(snow_ans, ans, lambda x: x)

    # The other direction for axis=0 behaves like axis=1.
    with SqlCounter(
        query_count=2,
    ):
        snow_ans = snow_s.add(snow_df, axis=0)
        ans = s.add(df, axis=0)
        eval_snowpark_pandas_result(snow_ans, ans, lambda x: x)


# defer testing on all operations to daily Jenkins
@pytest.mark.parametrize(
    "opname",
    [
        "add",  # Captured in above test which will be always run
        # - can comment to save resources. Kept here to be exhaustive.
        "sub",
        "truediv",
        "rtruediv",
        "floordiv",
        "rfloordiv",
        "floordiv",
        "rfloordiv",
        "pow",
        "rpow",
        "add",
        "radd",
        "sub",
        "rsub",
        "mul",
        "rmul",
        # boolean operators deviate in result behavior. Other tests verify they work correctly.
        # "eq",
        # "ne",
        # "gt",
        # "lt",
        # "ge",
        # "le",
        "mod",
        "rmod",
    ],
)
@pytest.mark.parametrize(
    "df,s",
    [
        # example in docstring
        (
            native_pd.DataFrame({"A": [4, 6, None, 7], "B": [2, 5, 4, None]}),
            native_pd.Series([9, 10, 12], index=[3, 1, 4]),
        ),
        (
            native_pd.DataFrame({"A": [4, 6, None, 7], "B": [2, 5, 4, None]}),
            native_pd.Series([1, 4, 3, 8]),
        ),
    ],
)
def test_binary_op_between_dataframe_and_series_axis0(opname, df, s):
    snow_df = pd.DataFrame(df)
    snow_s = pd.Series(s)

    # DataFrame <op> Series
    with SqlCounter(query_count=1):
        ans = getattr(df, opname)(s, axis=0)
        snow_ans = getattr(snow_df, opname)(snow_s, axis=0)

        eval_snowpark_pandas_result(snow_ans, ans, lambda x: x)

    # The other direction for axis=0 behaves like axis=1 and works without fallback
    # using the logic implemented for the axis=1 case.
    # There are 3 queries issued:
    #   - Query to convert Series to native pandas Series.
    #   - One query checking whether result is_series_like, this is done in the frontend and could be avoided in the
    #     future by optimizing is_series_like.
    #   - One query to retrieve the result as a native pandas object.
    # Series <op> DataFrame
    with SqlCounter(
        query_count=2,
    ):
        snow_ans = getattr(snow_s, opname)(snow_df, axis=0)
        ans = getattr(s, opname)(df, axis=0)

        # Note: Snowpark pandas and pandas differ wrt to NULL handling. Snowpark pandas follows Snowflake,
        # for this reason if either operand is NULL, NULL will be returned. For some operators
        # pandas does not follow this, e.g., 1.0 ** np.nan is 1.0 but would be np.nan according to Snowpark pandas.
        # Simulate the Snowpark pandas behavior here by adjusting the result.
        # In the following adapted code snipper from original pandas, self is always the DataFrame.
        from pandas.core.ops import maybe_prepare_scalar_for_op

        # original pandas code to adapt (from _arith_method in pandas.core.frame):
        # axis: Literal[1] = 1  # only relevant for Series other case
        # other = ops.maybe_prepare_scalar_for_op(other, (self.shape[axis],))
        #
        # self, other = self._align_for_op(other, axis, flex=True, level=None)
        self, other = df.copy(), s.copy()
        other = maybe_prepare_scalar_for_op(other, (self.shape[1],))
        lhs, rhs = self.align(other, axis=1, level=None)
        mask = pd.isna(rhs) | pd.isna(lhs)
        # mask result ans from pandas with nan based on mask (wherever mask is True, replace with NaN).
        ans[mask] = np.nan

        eval_snowpark_pandas_result(snow_ans, ans, lambda x: x)


@pytest.mark.parametrize(
    "df,s",
    [
        (
            native_pd.DataFrame({"A": [4, 6, None, 7], "B": [2, 5, 4, None]}),
            native_pd.Series([1, 4, 3], index=["C", "B", "A"]),
        )
    ],
)
@sql_count_checker(query_count=0)
def test_binary_add_dataframe_and_series_axis0_with_type_mismatch_for_index_negative(
    df, s
):
    # Snowpark pandas does not support aligning two objects with different index types. This
    # test captures this behavior. To support this, cast index to variant type.
    snow_df = pd.DataFrame(df)
    snow_s = pd.Series(s)

    # DataFrame <op> Series
    # because the index of the Dataframe is of integer type, but the one of the Series is of string type
    # Snowflake will produce an error.
    with pytest.raises(
        SnowparkSQLException, match="Numeric value 'C' is not recognized"
    ):
        snow_df.add(snow_s, axis=0).to_pandas()


@sql_count_checker(query_count=0)
def test_binary_add_dataframe_and_series_axis0_with_fill_value_negative():

    snow_df = pd.DataFrame()
    snow_s = pd.Series()

    # fill value is not supported for axis=0
    with pytest.raises(NotImplementedError, match="fill_value 42 not supported."):
        snow_df.add(snow_s, axis=0, fill_value=42).to_pandas()

    # check same behavior with pandas to catch change if pandas decides to resolve TODO.
    with pytest.raises(NotImplementedError, match="fill_value 42 not supported."):
        native_pd.DataFrame().add(native_pd.Series(), axis=0, fill_value=42)


@pytest.mark.parametrize(
    "df,s",
    [
        (
            native_pd.DataFrame(
                [[1, None, 2, None], [None, None, None, None]], columns=[1, 3, 4, 5]
            ),
            native_pd.Series([None, 1, 2, None, 3, 4, -99]),
        ),
        (
            native_pd.DataFrame({"A": [4, 6, None, 7], 10: [2, 5, 4, None]}),
            native_pd.Series([1, 4, 3, 8]),
        ),
        (
            native_pd.DataFrame({"A": [4, 6, None, 7], "B": [2, 5, 4, None]}),
            native_pd.Series([1, 4, 3], index=["C", "B", "A"]),
        ),
        # test here with this configuration that name is dropped correctly.
        (
            native_pd.DataFrame(
                [[4, 6, None, 7], [2, 5, 4, None]],
                index=native_pd.Index([1, 2], name="test"),
            ),
            native_pd.Series([1, 4, 3, 8], name="other"),
        ),
        (
            native_pd.DataFrame(
                [[1], [2], [3], [4]], columns=["A"], index=[3, 5, 7, 9]
            ),
            native_pd.Series([-2, -4, 5, 7, 9], index=[-10, 0, 5, 3, 8]),
        ),
    ],
)
def test_binary_add_dataframe_sub_series_axis1(df, s):

    # Use sub (-) here as it is not commutative.
    # Other operators are tested exhausitvely in the CI test test_binary_op_between_dataframe_and_series_axis0 above,
    # as one case for axis=0 actually invokes axis=1.

    snow_df = pd.DataFrame(df)
    snow_s = pd.Series(s)

    # DataFrame <op> Series
    ans = df - s
    snow_ans = snow_df - snow_s

    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(snow_ans, ans, lambda x: x)

    # This case works in pandas, but is not supported technically.
    # Series <op> DataFrame
    snow_ans = snow_s - snow_df
    ans = s - df

    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(snow_ans, ans, lambda x: x)


@pytest.mark.parametrize("fill_value", [42])
def test_binary_op_with_fill_value_axis1_negative(fill_value):

    df = native_pd.DataFrame(
        [[1, None, 2, None], [None, None, None, None]], columns=[1, 3, 4, 5]
    )
    s = native_pd.Series([None, 1, 2, None, 3, 4, -99])

    snow_df = pd.DataFrame(df)
    snow_s = pd.Series(s)

    with SqlCounter(query_count=0):
        eval_snowpark_pandas_result(
            (snow_df, snow_s),
            (df, s),
            lambda t: t[0].add(t[1], axis=1, fill_value=fill_value),
            expect_exception=True,
            expect_exception_type=NotImplementedError,
            expect_exception_match=f"fill_value {fill_value} not supported.",
        )


@pytest.mark.parametrize(
    "df,s",
    [
        (
            native_pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=["A", "B", "A"]),
            native_pd.Series([2, 3]),
        ),
        (
            native_pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=["A", "B", "C"]),
            native_pd.Series([2, 3, 99], index=["A", "D", "A"]),
        ),
    ],
)
def test_binary_add_dataframe_and_series_duplicate_labels_negative(df, s):
    snow_df = pd.DataFrame(df)
    snow_s = pd.Series(s)

    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            (snow_df, snow_s),
            (df, s),
            lambda t: t[0].add(t[1], axis=1),
            expect_exception=True,
            expect_exception_type=ValueError,
            expect_exception_match="cannot reindex on an axis with duplicate labels",
        )


DATAFRAME_DATAFRAME_TEST_LIST = [
    (
        native_pd.DataFrame([[1, None, 3], [4, 5, 6]]),
        native_pd.DataFrame([[1, -2, 3], [6, -5, None]]),
    ),
    # test with np.Nan as well
    (
        native_pd.DataFrame([[np.nan, None, 3], [4, 5, 6]]),
        native_pd.DataFrame([[1, -2, 3], [6, -5, np.nan]]),
    ),
    # Test column alignment.
    (
        native_pd.DataFrame([[None, 2, 3], [4, 5, 6]], columns=["A", "B", "C"]),
        native_pd.DataFrame([[1, -2, 3], [None, -5, 4]], columns=["B", "C", "A"]),
    ),
    # Test index alignment.
    (
        native_pd.DataFrame([[1, 2, 3], [4, 5, None]], index=[0, 1]),
        native_pd.DataFrame([[1, None, -3], [6, -5, 4]], index=[1, 0]),
    ),
    # Test shape broadcast.
    (
        native_pd.DataFrame([[None, None], [4, 5]], columns=["A", "B"]),
        native_pd.DataFrame([[1, -2, 3], [None, -5, 4]], columns=["D", "A", "C"]),
    ),
]


@pytest.mark.parametrize("df1,df2", DATAFRAME_DATAFRAME_TEST_LIST)
def test_binary_sub_dataframe_and_dataframe(df1, df2):
    snow_df1 = pd.DataFrame(df1)
    snow_df2 = pd.DataFrame(df2)

    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            (snow_df1, snow_df2), (df1, df2), lambda t: t[0] - t[1]
        )

    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            (snow_df2, snow_df1), (df2, df1), lambda t: t[0] - t[1]
        )


@pytest.mark.parametrize("df1,df2", DATAFRAME_DATAFRAME_TEST_LIST)
@pytest.mark.parametrize("fill_value", [42, None])
def test_binary_sub_dataframe_and_dataframe_with_fill_value(df1, df2, fill_value):
    snow_df1 = pd.DataFrame(df1)
    snow_df2 = pd.DataFrame(df2)

    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            (snow_df1, snow_df2),
            (df1, df2),
            lambda t: t[0].sub(t[1], fill_value=fill_value),
        )

    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            (snow_df2, snow_df1),
            (df2, df1),
            lambda t: t[0].sub(t[1], fill_value=fill_value),
        )


@pytest.mark.parametrize(
    "df1,df2,expect_pandas_fail",
    [
        (
            native_pd.DataFrame([[1, 2, 3, 4]], columns=["A", "B", "A", "B"]),
            native_pd.DataFrame([[1, 2, 3, 4]], columns=["C", "B", "A", "D"]),
            True,
        ),
        (
            native_pd.DataFrame([[None, 2, 3], [3, 4, 5]], columns=["A", "B", "A"]),
            native_pd.DataFrame([[1, -2, 3]], columns=["B", "C", "A"]),
            False,
        ),
        (
            native_pd.DataFrame([[None, 2, 3]], columns=["A", "B", "C"]),
            native_pd.DataFrame([[1, -2, 3]], columns=["A", "A", "A"]),
            True,
        ),
        (
            native_pd.DataFrame([[None, 2, 3]], columns=["A", "B", "A"]),
            native_pd.DataFrame([[1, -2, 3]], columns=["B", "C", "C"]),
            False,
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_binary_sub_dataframe_and_dataframe_duplicate_labels_negative(
    df1, df2, expect_pandas_fail
):
    # pandas prints an assert error that is not useful to the user.
    # Deviate here from pandas by printing a meaningful ValueError instead.

    snow_df1 = pd.DataFrame(df1)
    snow_df2 = pd.DataFrame(df2)

    # Snowpark pandas provides a clear error, and does not support duplicate labels.
    with pytest.raises(
        ValueError, match="cannot reindex on an axis with duplicate labels"
    ):
        snow_df1.sub(snow_df2).to_pandas()

    # pandas has buggy behavior, manually check troublesome scenarios.
    if expect_pandas_fail:
        with pytest.raises(AssertionError, match="Gaps in blk ref_locs"):
            # ans = df1 - df2 works in pandas, however calling str on top of it
            # shows that internally the data has been corrupted.
            str(df1 - df2)


# defer testing on all operations to daily Jenkins
@pytest.mark.parametrize(
    "opname",
    [
        "add",  # Captured in above test which will be always run
        # - can comment to save resources. Kept here to be exhaustive.
        "sub",
        "truediv",
        "rtruediv",
        "floordiv",
        "rfloordiv",
        "floordiv",
        "rfloordiv",
        # does not work with negative values, comment here to allow reuse of dataset above.
        # "pow",
        # "rpow",
        "add",
        "radd",
        "sub",
        "rsub",
        "mul",
        "rmul",
        # These operators have deviating behavior, tested above.
        # "eq",
        # "ne",
        # "gt",
        # "lt",
        # "ge",
        # "le",
        # "mod",
        # "rmod",
    ],
)
@pytest.mark.parametrize("df1,df2", DATAFRAME_DATAFRAME_TEST_LIST)
@pytest.mark.skipif(running_on_public_ci(), reason="exhaustive and slow operation test")
@pytest.mark.parametrize("fill_value", [None, 42])
def test_binary_op_between_dataframe_and_dataframe_exhaustive(
    opname, df1, df2, fill_value
):
    snow_df1 = pd.DataFrame(df1)
    snow_df2 = pd.DataFrame(df2)

    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            (snow_df1, snow_df2),
            (df1, df2),
            lambda t: getattr(t[0], opname)(t[1], fill_value=fill_value),
        )

    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            (snow_df2, snow_df1),
            (df2, df1),
            lambda t: getattr(t[0], opname)(t[1], fill_value=fill_value),
        )


@pytest.mark.parametrize(
    "func,expected",
    [
        (
            lambda a, b: a & b,
            native_pd.DataFrame(
                [[True, None, False], [False, False, False], [False, None, None]]
            ),
        ),
        (
            lambda a, b: a | b,
            native_pd.DataFrame(
                [[True, True, True], [None, False, True], [None, True, None]]
            ),
        ),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.parametrize(
    "df1, df2",
    [
        (
            native_pd.DataFrame(
                [[True, None, False], [None, False, True], [False, True, None]]
            ),
            native_pd.DataFrame(
                [[True, True, True], [False, False, False], [None, None, None]]
            ),
        )
    ],
)
def test_binary_bitwise_op_on_df(df1, df2, func, expected):

    # Note: In Snowflake logical AND/OR are only defined for booleans (and NULL). We map directly here, and if users
    #       use different types let Snowflake's error propagate.

    snow_df1 = pd.DataFrame(df1)
    snow_df2 = pd.DataFrame(df2)

    # Snowpark pandas NULL behavior is different from pandas,
    # e.g., NULL OR FALSE = FALSE OR NULL = NULL
    #       NULL OR TRUE = TRUE OR NULL = TRUE
    #       NULL OR NULL = NULL
    #       NULL AND FALSE = FALSE AND NULL = FALSE
    #       NULL AND TRUE = TRUE AND NULL = NULL
    #       NULL AND NULL = NULL
    # use here explicit answers to run tests.

    snow_ans = func(snow_df1, snow_df2)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_ans, expected)


@sql_count_checker(query_count=2)
@pytest.mark.parametrize(
    "func",
    [
        lambda df: df + df[0],
        lambda df: df - df[0],
        lambda df: df[0] + df,
        lambda df: df[0] - df,
    ],
)
def test_binary_single_row_dataframe_and_series(func):
    native_df = native_pd.DataFrame([1])
    snow_df = pd.DataFrame([1])
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        func,
    )


@sql_count_checker(query_count=1, join_count=2)
def test_df_sub_series():

    series1 = native_pd.Series(np.random.randn(3), index=["a", "b", "c"])
    series2 = native_pd.Series(np.random.randn(4), index=["a", "b", "c", "d"])
    series3 = native_pd.Series(np.random.randn(3), index=["b", "c", "d"])

    native_df = native_pd.DataFrame(
        {
            "one": series1,
            "two": series2,
            "three": series3,
        }
    )
    snow_df = pd.DataFrame(
        {
            "one": pd.Series(series1),
            "two": pd.Series(series2),
            "three": pd.Series(series3),
        }
    )

    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.sub(df["two"], axis="index"), inplace=True
    )


@sql_count_checker(query_count=2, join_count=0)
def test_binary_op_multi_series_from_same_df():
    native_df = native_pd.DataFrame(
        {
            "A": [1, 2, 3],
            "B": [2, 3, 4],
            "C": [4, 5, 6],
            "D": [2, 2, 3],
        },
        index=["a", "b", "c"],
    )
    snow_df = pd.DataFrame(native_df)
    # ensure performing more than one binary operation for series coming from same
    # dataframe does not produce any join.
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df["A"] + df["B"] + df["C"]
    )
    # perform binary operations in different orders
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: (df["A"] + df["B"]) + (df["C"] + df["D"])
    )
