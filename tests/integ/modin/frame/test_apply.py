#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import udf
from snowflake.snowpark.modin.plugin._internal.apply_utils import (
    DEFAULT_UDTF_PARTITION_SIZE,
)
from snowflake.snowpark.types import DoubleType, PandasSeriesType
from tests.integ.modin.series.test_apply_and_map import (
    create_func_with_return_type_hint,
)
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equal_to_pandas,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import RUNNING_ON_GH, running_on_public_ci

# TODO SNOW-891796: replace native_pd with pd after allowing using snowpandas module/function in UDF

# test data which has a python type as return type that is not a pandas Series/pandas DataFrame/tuple/list
BASIC_DATA_FUNC_PYTHON_RETURN_TYPE_MAP = [
    [[[1.1, 2.2], [3, np.nan]], np.min, "float"],
    param(
        [[1.0, 2.2], [3, np.nan]],
        lambda x: native_pd.Timedelta(1),
        "native_pd.Timedelta",
        id="return_timedelta_scalar",
        marks=pytest.mark.xfail(
            strict=True, raises=AssertionError, reason="SNOW-1619940"
        ),
    ),
    [[[1.1, 2.2], [3, np.nan]], lambda x: x.sum(), "float"],
    [[[1.1, 2.2], [3, np.nan]], lambda x: x.size, "int"],
    [[[1.1, 2.2], [3, np.nan]], lambda x: "0" if x.sum() > 1 else 0, "object"],
    [[["snow", "flake"], ["data", "cloud"]], lambda x: x[0] + x[1], "str"],
    [[[True, False], [False, False]], lambda x: x[0] ^ x[1], "bool"],
    (
        [
            [bytes("snow", "utf-8"), bytes("flake", "utf-8")],
            [bytes("data", "utf-8"), bytes("cloud", "utf-8")],
        ],
        lambda x: (x[0] + x[1]).decode(),
        "str",
    ),
    (
        [[["a", "b"], ["c", "d"]], [["a", "b"], ["c", "d"]]],
        lambda x: x[0][1] + x[1][0],
        "str",
    ),
    (
        [[{"a": "b"}, {"c": "d"}], [{"c": "b"}, {"a": "d"}]],
        lambda x: str(x[0]) + str(x[1]),
        "str",
    ),
    param(
        [
            [native_pd.Timedelta(1), native_pd.Timedelta(2)],
            [native_pd.Timedelta(3), native_pd.Timedelta(4)],
        ],
        lambda row: row.sum().value,
        "int",
        id="apply_on_frame_with_timedelta_data_columns_returns_int",
        marks=pytest.mark.xfail(strict=True, raises=NotImplementedError),
    ),
]


@pytest.mark.parametrize(
    "data, func, return_type", BASIC_DATA_FUNC_PYTHON_RETURN_TYPE_MAP
)
@pytest.mark.modin_sp_precommit
def test_axis_1_basic_types_without_type_hints(data, func, return_type):
    # this test processes functions without type hints and invokes the UDTF solution.
    native_df = native_pd.DataFrame(data, columns=["A", "b"])
    snow_df = pd.DataFrame(data, columns=["A", "b"])
    # np.min is mapped to sql builtin function.
    with SqlCounter(query_count=1 if func == np.min else 5):
        eval_snowpark_pandas_result(snow_df, native_df, lambda x: x.apply(func, axis=1))


@pytest.mark.parametrize(
    "data, func, return_type", BASIC_DATA_FUNC_PYTHON_RETURN_TYPE_MAP
)
@pytest.mark.modin_sp_precommit
def test_axis_1_basic_types_with_type_hints(data, func, return_type):
    # create explicitly for supported python types UDF with type hints and process via vUDF.
    native_df = native_pd.DataFrame(data, columns=["A", "b"])
    snow_df = pd.DataFrame(data, columns=["A", "b"])
    func_with_type_hint = create_func_with_return_type_hint(func, return_type)
    #  Invoking a single UDF typically requires 3 queries (package management, code upload, UDF registration) upfront.
    with SqlCounter(query_count=4, join_count=0, udtf_count=0):
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda x: x.apply(func_with_type_hint, axis=1)
        )


@pytest.mark.parametrize(
    "df,row_label",
    [
        (
            native_pd.DataFrame(
                [[1, 2], [None, 3]], columns=["A", "b"], index=["A", "B"]
            ),
            "B",
        ),
        (
            native_pd.DataFrame(
                [[1, 2], [None, 3]],
                columns=["A", "b"],
                index=pd.MultiIndex.from_tuples([(1, 2), (1, 1)]),
            ),
            (1, 2),
        ),
    ],
)
def test_axis_1_index_passed_as_name(df, row_label):
    # when using apply(axis=1) the original index of the dataframe is passed as name.
    # test here for this for regular index and multi-index scenario.

    def foo(row) -> str:
        if row.name == row_label:
            return "MATCHING LABEL"
        else:
            return "NO MATCH"

    snow_df = pd.DataFrame(df)
    #  Invoking a single UDF typically requires 3 queries (package management, code upload, UDF registration) upfront.
    with SqlCounter(query_count=4, join_count=0, udtf_count=0):
        eval_snowpark_pandas_result(snow_df, df, lambda x: x.apply(foo, axis=1))


@pytest.mark.xfail(strict=True, raises=NotImplementedError)
@sql_count_checker(query_count=0)
def test_frame_with_timedelta_index():
    eval_snowpark_pandas_result(
        *create_test_dfs(
            native_pd.DataFrame([0], index=[native_pd.Timedelta(1)]),
        ),
        lambda df: df.apply(lambda row: row, axis=1)
    )


@pytest.mark.parametrize(
    "data, func, expected_result",
    [
        [
            [
                [datetime.date(2023, 1, 1), None],
                [datetime.date(2022, 12, 31), datetime.date(2021, 1, 9)],
            ],
            lambda x: x.dt.day,
            native_pd.DataFrame(
                [[1.0, np.nan], [31.0, 9.0]]
            ),  # expected dtypes become float64 since None cannot be presented in int.
        ],
        [
            [
                [datetime.date(2023, 1, 1), None],
                [datetime.date(2022, 12, 31), datetime.date(2021, 1, 9)],
            ],
            lambda x: x.astype(str),
            native_pd.DataFrame([["2023-01-01", "NaT"], ["2022-12-31", "2021-01-09"]]),
        ],
        [
            [
                [datetime.time(1, 2, 3), None],
                [datetime.time(1, 2, 3, 1), datetime.time(1)],
            ],
            lambda x: x.dt.seconds,
            native_pd.DataFrame(
                [[3723.0, np.nan], [3723.0, 3600]]
            ),  # expected dtypes become float64 since None cannot be presented in int.
        ],
        [
            [
                [datetime.time(1, 2, 3), None],
                [datetime.time(1, 2, 3, 1), datetime.time(1)],
            ],
            lambda x: x.astype(str),
            native_pd.DataFrame(
                [
                    ["0 days 01:02:03", "NaT"],
                    ["0 days 01:02:03.000001", "0 days 01:00:00"],
                ]
            ),
        ],
        [
            [
                [datetime.datetime(2023, 1, 1, 1, 2, 3), None],
                [
                    datetime.datetime(2022, 12, 31, 1, 2, 3, 1),
                    datetime.datetime(
                        2023, 1, 1, 1, 2, 3, tzinfo=datetime.timezone.utc
                    ),
                ],
            ],
            lambda x: native_pd.to_datetime(x, utc=True).dt.tz,
            native_pd.Series(["UTC", "UTC"]),
        ],
        [
            [
                [datetime.datetime(2023, 1, 1, 1, 2, 3), None],
                [
                    datetime.datetime(2022, 12, 31, 1, 2, 3, 1),
                    datetime.datetime(
                        2023, 1, 1, 1, 2, 3, tzinfo=datetime.timezone.utc
                    ),
                ],
            ],
            lambda x: x.astype(str),
            native_pd.DataFrame(
                [
                    ["2023-01-01 01:02:03", "None"],
                    ["2022-12-31 01:02:03.000001", "2023-01-01 01:02:03+00:00"],
                ]
            ),
        ],
    ],
)
@sql_count_checker(query_count=5, join_count=2, udtf_count=1)
def test_axis_1_date_time_timestamp_type(data, func, expected_result):
    snow_df = pd.DataFrame(data)
    result = snow_df.apply(func, axis=1)
    assert_snowpark_pandas_equal_to_pandas(result, expected_result)


@sql_count_checker(query_count=5, join_count=2, udtf_count=1)
def test_axis_1_return_list():
    snow_df = pd.DataFrame([[1, 2], [3, 4]])
    native_df = native_pd.DataFrame([[1, 2], [3, 4]])
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda x: x.apply(lambda x: [1, 2], axis=1)
    )


@sql_count_checker(query_count=5, join_count=2, udtf_count=1)
def test_axis_1_return_series():
    snow_df = pd.DataFrame([[1, 2], [3, 4]], columns=["A", "b"])
    native_df = native_pd.DataFrame([[1, 2], [3, 4]], columns=["A", "b"])
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda x: x.apply(lambda x: native_pd.Series([1, 2], index=["C", "d"]), axis=1),
    )


@sql_count_checker(query_count=5, join_count=2, udtf_count=1)
def test_axis_1_return_series_with_different_label_results():
    df = native_pd.DataFrame([[1, 2], [3, 4]], columns=["A", "b"])
    snow_df = pd.DataFrame(df)

    eval_snowpark_pandas_result(
        snow_df,
        df,
        lambda df: df.apply(
            lambda x: native_pd.Series([1, 2], index=["a", "b"])
            if x.sum() > 3
            else native_pd.Series([0, 1, 2], index=["c", "a", "b"]),
            axis=1,
        ),
    )


@pytest.mark.parametrize(
    "native_df, func",
    [
        (
            native_pd.DataFrame([[1, 2], [3, 4]], columns=["a", "b"]),
            lambda x: x["a"] + x["b"],
        ),
        (
            native_pd.DataFrame(
                [[1, 2, 3, 4], [5, 6, 7, 8]],
                columns=native_pd.MultiIndex.from_tuples(
                    [("baz", "A"), ("baz", "B"), ("zoo", "A"), ("zoo", "B")]
                ),
            ),
            lambda x: x["baz", "B"] * x["zoo", "A"],
        ),
    ],
)
@sql_count_checker(query_count=5, join_count=2, udtf_count=1)
def test_axis_1_column_labels(native_df, func):
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda x: x.apply(func, axis=1))


@sql_count_checker(query_count=5, join_count=2, udtf_count=1)
def test_axis_1_raw():
    snow_df = pd.DataFrame([[1, 2], [3, 4]])
    native_df = native_pd.DataFrame([[1, 2], [3, 4]])
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda x: x.apply(lambda x: str(type(x)), axis=1, raw=True)
    )


@sql_count_checker(query_count=6)
def test_axis_1_return_not_json_serializable_label():
    snow_df = pd.DataFrame([1])
    with pytest.raises(
        SnowparkSQLException, match="Object of type date is not JSON serializable"
    ):
        # label
        snow_df.apply(
            lambda x: native_pd.Series([1], index=[datetime.date.today()]), axis=1
        ).to_pandas()

    with pytest.raises(
        SnowparkSQLException, match="Object of type DataFrame is not serializable"
    ):
        # return value
        snow_df.apply(lambda x: native_pd.DataFrame([1, 2]), axis=1).to_pandas()


@pytest.mark.skipif(RUNNING_ON_GH, reason="Slow test")
def test_axis_1_apply_args_kwargs():
    def f(x, y, z=1) -> int:
        return x.sum() + y + z

    native_df = native_pd.DataFrame([[1, 2], [3, 4]])
    snow_df = pd.DataFrame([[1, 2], [3, 4]])

    with SqlCounter(query_count=3):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda x: x.apply(f, axis=1),
            expect_exception=True,
            expect_exception_type=SnowparkSQLException,
            expect_exception_match="missing 1 required positional argument",
            assert_exception_equal=False,
        )

    with SqlCounter(query_count=4):
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda x: x.apply(f, axis=1, args=(1,))
        )

    with SqlCounter(query_count=4):
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda x: x.apply(f, axis=1, args=(1,), z=2)
        )

    with SqlCounter(query_count=3):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda x: x.apply(f, axis=1, args=(1,), z=2, v=3),
            expect_exception=True,
            expect_exception_type=SnowparkSQLException,
            expect_exception_match="got an unexpected keyword argument",
            assert_exception_equal=False,
        )


class TestNotImplemented:
    @pytest.mark.parametrize("result_type", ["reduce", "expand", "broadcast"])
    @sql_count_checker(query_count=0)
    def test_result_type(self, result_type):
        snow_df = pd.DataFrame([[1, 2], [3, 4]])
        msg = "Snowpark pandas apply API doesn't yet support 'result_type' parameter"
        with pytest.raises(NotImplementedError, match=msg):
            snow_df.apply(lambda x: [1, 2], axis=1, result_type=result_type)

    @sql_count_checker(query_count=0)
    def test_axis_1_apply_args_kwargs_with_snowpandas_object(self):
        def f(x, y=None) -> native_pd.Series:
            return x + (y if y is not None else 0)

        snow_df = pd.DataFrame([[1, 2], [3, 4]])
        msg = "Snowpark pandas apply API doesn't yet support DataFrame or Series in 'args' or 'kwargs' of 'func'"
        with pytest.raises(NotImplementedError, match=msg):
            snow_df.apply(f, axis=1, args=(pd.Series([1, 2]),))
        with pytest.raises(NotImplementedError, match=msg):
            snow_df.apply(f, axis=1, y=pd.Series([1, 2]))


TEST_INDEX_1 = native_pd.MultiIndex.from_tuples(
    list(zip(*[["a", "b"], ["x", "y"]])),
    names=["first", "last"],
)


TEST_INDEX_WITH_NULL_1 = native_pd.MultiIndex.from_tuples(
    list(zip(*[[None, "b"], ["x", None]])),
    names=["first", "last"],
)


TEST_INDEX_2 = native_pd.MultiIndex.from_tuples(
    list(zip(*[["AA", "BB"], ["XX", "YY"]])),
    names=["FOO", "BAR"],
)


TEST_INDEX_WITH_NULL_2 = native_pd.MultiIndex.from_tuples(
    list(zip(*[[None, "BB"], ["XX", None]])),
    names=["FOO", "BAR"],
)


TEST_COLUMNS_1 = native_pd.MultiIndex.from_tuples(
    list(
        zip(
            *[
                ["car", "motorcycle", "bike", "bus"],
                ["blue", "green", "red", "yellow"],
            ]
        )
    ),
    names=["vehicle", "color"],
)


@pytest.mark.parametrize(
    "apply_func",
    [
        lambda x: -x,
        lambda x: native_pd.Series([1, 2], index=TEST_INDEX_1),
        lambda x: native_pd.Series([3, 4], index=TEST_INDEX_2),
        lambda x: native_pd.Series([1, 2], index=TEST_INDEX_WITH_NULL_1),
        lambda x: native_pd.Series([1, 2], index=TEST_INDEX_WITH_NULL_1),
    ],
)
@sql_count_checker(query_count=5, join_count=2, udtf_count=1)
def test_axis_1_multi_index_column_labels(apply_func):
    data = [[i + j for j in range(0, 4)] for i in range(0, 4)]

    native_df = native_pd.DataFrame(data, columns=TEST_COLUMNS_1)
    snow_df = pd.DataFrame(data, columns=TEST_COLUMNS_1)

    eval_snowpark_pandas_result(
        snow_df, native_df, lambda x: x.apply(apply_func, axis=1)
    )


@sql_count_checker(query_count=5, join_count=2, udtf_count=1)
def test_axis_1_multi_index_column_labels_with_different_results():
    data = [[i + j for j in range(0, 4)] for i in range(0, 4)]

    df = native_pd.DataFrame(data, columns=TEST_COLUMNS_1)
    snow_df = pd.DataFrame(df)

    apply_func = (
        lambda x: native_pd.Series([1, 2], index=TEST_INDEX_1)
        if min(x) == 0
        else native_pd.Series([3, 4], index=TEST_INDEX_2)
    )

    eval_snowpark_pandas_result(snow_df, df, lambda df: df.apply(apply_func, axis=1))


def test_axis_1_multi_index_column_labels_none_names():
    data = [[i + j for j in range(0, 4)] for i in range(0, 4)]

    cols_1 = TEST_COLUMNS_1.copy()
    cols_1.names = [None, None]

    native_df = native_pd.DataFrame(data, columns=cols_1)
    snow_df = pd.DataFrame(data, columns=cols_1)

    with SqlCounter(query_count=5, join_count=2, udtf_count=1):
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda x: x.apply(lambda x: x, axis=1)
        )

    test_index_1 = TEST_INDEX_1
    test_index_1.names = [None, None]

    with SqlCounter(query_count=5, join_count=2, udtf_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda x: x.apply(
                lambda x: native_pd.Series([1, 2], index=test_index_1), axis=1
            ),
        )


@sql_count_checker(query_count=5, join_count=2, udtf_count=1)
def test_axis_1_multi_index_column_labels_different_lengths():
    data = [[i + j for j in range(0, 4)] for i in range(0, 4)]

    df = native_pd.DataFrame(data, columns=TEST_COLUMNS_1)
    snow_df = pd.DataFrame(df)

    apply_func = (
        lambda x: native_pd.Series([5, 6, 7, 8], index=[0, 1, -1, -2])
        if min(x) == 0
        else native_pd.Series([9, 8, 7], index=[4, 5, 6])
    )

    eval_snowpark_pandas_result(snow_df, df, lambda df: df.apply(apply_func, axis=1))


@sql_count_checker(query_count=3)
def test_axis_1_multi_index_column_labels_different_levels_negative():
    data = [[i + j for j in range(0, 4)] for i in range(0, 4)]

    native_df = native_pd.DataFrame(data, columns=TEST_COLUMNS_1)
    snow_df = pd.DataFrame(data, columns=TEST_COLUMNS_1)

    test_index_1 = pd.MultiIndex.from_tuples(
        list(zip(*[["a", "b"], ["x", "y"]])),
    )

    test_index_2 = pd.MultiIndex.from_tuples(
        list(zip(*[["S", "T"], ["W", "X"], ["G", "H"]])),
    )

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda x: x.apply(
            lambda x: native_pd.Series([5, 6], index=test_index_1)
            if min(x) == 0
            else native_pd.Series([9, 8, 7], index=test_index_2),
            axis=1,
        ),
        expect_exception=True,
        expect_exception_type=SnowparkSQLException,
        assert_exception_equal=False,
    )


def test_apply_variant_json_null():
    # series -> scalar
    def f(v):
        x = v.sum()
        if x == 1:
            return None
        elif x == 2:
            return np.nan
        elif x == 3:
            return native_pd.NA
        else:
            return x

    with SqlCounter(query_count=5, join_count=2, udtf_count=1):
        df = pd.DataFrame([1, 2, 3, 4])
        assert df.apply(f, axis=1).isna().tolist() == [False, True, True, False]

    # series -> series
    def g(v):
        x = v.sum()
        if x == 1:
            return native_pd.Series([None])
        elif x == 2:
            return native_pd.Series(np.nan)
        elif x == 3:
            return native_pd.Series(native_pd.NA)
        else:
            return v

    with SqlCounter(query_count=5, join_count=2, udtf_count=1):
        assert df.apply(g, axis=1).isna().to_numpy().tolist() == [
            [False],
            [True],
            [True],
            [False],
        ]


@pytest.mark.xfail(
    strict=True,
    raises=SnowparkSQLException,
    reason="SNOW-1650918: Apply on dataframe data columns containing NULL fails with invalid arguments to udtf function",
)
@pytest.mark.parametrize(
    "data, apply_func",
    [
        [
            [[None, "abcd"]],
            lambda x: x + " are first 4 letters of alphabet" if x is not None else None,
        ],
        [
            [[123, None]],
            lambda x: x + 100 if x is not None else None,
        ],
    ],
)
def test_apply_bug_1650918(data, apply_func):
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda x: x.apply(apply_func, axis=1),
    )


TRANSFORM_TEST_MAP = [
    [[[0, 1, 2], [1, 2, 3]], lambda x: x + 1, 16],
    [[[0, 1, 2], [1, 2, 3]], np.exp, 1],
    [[[0, 1, 2], [1, 2, 3]], "exp", None],
    [[["Leonhard", "Jianzhun"]], lambda x: x + " is awesome!!", 11],
    [[[1.3, 2.5]], np.sqrt, 1],
    [[[1.3, 2.5]], "sqrt", None],
    [[[1.3, 2.5]], np.log, 1],
    [[[1.3, 2.5]], "log", None],
    [[[1.3, 2.5]], np.square, 1],
    [[[1.3, 2.5]], "square", None],
    [[[1.5, float("nan")]], lambda x: np.sqrt(x), 11],
]


@pytest.mark.modin_sp_precommit
@pytest.mark.parametrize("data, apply_func, expected_query_count", TRANSFORM_TEST_MAP)
def test_basic_dataframe_transform(data, apply_func, expected_query_count):
    if expected_query_count is None:
        msg = "Snowpark pandas apply API only supports callables func"
        with SqlCounter(query_count=0):
            with pytest.raises(NotImplementedError, match=msg):
                snow_df = pd.DataFrame(data)
                snow_df.transform(apply_func)
    else:
        msg = "SNOW-1650644 & SNOW-1345395: Avoid extra caching and repeatedly creating same temp function"
        native_df = native_pd.DataFrame(data)
        snow_df = pd.DataFrame(data)
        with SqlCounter(
            query_count=expected_query_count,
            high_count_expected=True,
            high_count_reason=msg,
        ):
            eval_snowpark_pandas_result(
                snow_df, native_df, lambda x: x.transform(apply_func)
            )


AGGREGATION_FUNCTIONS = [
    np.max,
    np.min,
    np.sum,
    np.mean,
    np.median,
    np.std,
    np.var,
]


@pytest.mark.parametrize("func", AGGREGATION_FUNCTIONS)
@sql_count_checker(query_count=0)
def test_dataframe_transform_aggregation_negative(func):
    snow_df = pd.DataFrame([[0, 1, 2], [1, 2, 3]])
    with pytest.raises(
        ValueError,
        match="Function did not transform",
    ):
        snow_df.transform(func)


@sql_count_checker(query_count=0)
def test_dataframe_transform_invalid_function_name_negative(session):
    snow_df = pd.DataFrame([[0, 1, 2], [1, 2, 3]])
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas apply API only supports callables func",
    ):
        snow_df.transform("mxyzptlk")


# check that we throw the same error as pandas for cases where the function type is
# invalid.
INVALID_TYPES_FOR_TRANSFORM = [
    1,
    2.5,
    True,
]


@pytest.mark.parametrize("func", INVALID_TYPES_FOR_TRANSFORM)
@sql_count_checker(query_count=0)
def test_dataframe_transform_invalid_types_negative(func):
    snow_df = pd.DataFrame([[0, 1, 2], [1, 2, 3]])
    with pytest.raises(
        TypeError,
        match="object is not callable",
    ):
        snow_df.transform(func)


@sql_count_checker(
    high_count_expected=True,
    high_count_reason="SNOW-1001470 test multiple apply",
    query_count=8,
    udtf_count=0,
    udf_count=2,
    join_count=0,
)
@pytest.mark.parametrize("is_sorted", [True, False])
def test_fix_1001470(is_sorted):
    test_df = pd.DataFrame({"income": [5000, 15000, 30000], "col": [3, 2, 1]})
    if is_sorted:
        # If it's originally sorted, then after apply, the order should still be maintained
        test_df = test_df.sort_values(by="col")

    def foo(row) -> float:
        income = row["income"]
        return income * 2

    # test calling apply twice and the result should be the same and no error raises
    test_df["A"] = test_df.apply(foo, axis=1)
    ans1 = test_df[test_df["A"] > 25000].values

    test_df["A"] = test_df.apply(foo, axis=1)
    ans2 = test_df[test_df["A"] > 25000].values

    assert np.array_equal(ans1, ans2)


def test_bug_SNOW_1172448():
    # This test case checks that reading a table + apply work together. Before SNOW-1172448 there
    # was a bug where the join within _apply_with_udtf_and_dynamic_pivot_along_axis_1 would fail
    # due to index identifiers being not disambiguous.
    df = pd.DataFrame(
        data=np.random.normal(size=(100, 3)), columns=["A", "AMT_INCOME_TOTAL", "C"]
    )
    temp_table_name = random_name_for_temp_object(TempObjectType.TABLE)

    df.to_snowflake(temp_table_name, index_label="index")
    df = pd.read_snowflake(temp_table_name)
    # Follow read_snowflake with a sort operation to ensure that ordering is stable and tests are not flaky.
    df = df.sort_values(df.columns.to_list())

    median_income = 187524.29
    std_income = 110086.85

    # UDF to apply row-wise
    def foo(row):
        income = row["AMT_INCOME_TOTAL"]
        return (income - median_income) / std_income

    with SqlCounter(query_count=6, join_count=5, udtf_count=1):
        df["pct_income"] = df.apply(foo, axis=1)
        # trigger computation here.
        ans = len(df[df["pct_income"] > 0.5]) / len(df)
        assert isinstance(ans, float)


@pytest.mark.parametrize(
    "data",
    [
        np.random.normal(loc=100, scale=30, size=(DEFAULT_UDTF_PARTITION_SIZE // 2, 3)),
        np.random.normal(loc=100, scale=30, size=(DEFAULT_UDTF_PARTITION_SIZE, 3)),
        np.random.normal(
            loc=100, scale=30, size=(int(DEFAULT_UDTF_PARTITION_SIZE * 10.7), 3)
        ),
    ],
)
@sql_count_checker(
    query_count=11,
    udtf_count=1,
    join_count=3,
    high_count_expected=True,
    high_count_reason="upload of larger data, udtf registration, additional temp table creation till bugfix in SNOW-1060191 is in",
)
def test_dataframe_relative_to_default_partition_size_with_apply_udtf(data):
    # test here that a Dataframe with size <, =, > than the default udtf partition size gets processed correctly.
    df = pd.DataFrame(data, columns=["A", "B", "C"])

    def foo(row):
        income = row["C"]
        return (income - 100) / 30

    df["normalized_C"] = df.apply(foo, axis=1)

    assert len(df) == len(data)


def test_with_duplicates_negative():
    df = native_pd.DataFrame([[1, 2], [3, 4]])
    snow_df = pd.DataFrame(df)

    def foo(x):
        return native_pd.Series(
            [1, 2, 3], index=["C", "A", "E"] if x.sum() > 3 else ["A", "E", "E"]
        )

    # Snowpark pandas and pandas deviate here wrt to the exception type thrown.
    # We expose this here by design, as we used to decide against an exception translation mechanism for UDFs.
    # I.e., Snowpark pandas will surface the error as SnowparkSQLException to allow to identify the error as being
    # server-side.
    with pytest.raises(
        ValueError, match="cannot reindex on an axis with duplicate labels"
    ):
        df.apply(foo, axis=1)

    with SqlCounter(query_count=3, join_count=0, udtf_count=0):
        with pytest.raises(
            SnowparkSQLException,
            match="cannot reindex on an axis with duplicate labels",
        ):
            snow_df.apply(foo, axis=1)


@pytest.mark.parametrize("partition_size", [1, 2])
@pytest.mark.parametrize("data", [{"a": [1], "b": [2]}, {"a": [2], "b": [3]}])
def test_apply_axis_1_with_if_where_duplicates_not_executed(partition_size, data):
    df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(df)

    def foo(x):
        return native_pd.Series(
            [1, 2, 3], index=["C", "A", "E"] if x.sum() > 3 else ["A", "E", "E"]
        )

    def helper(df):
        kwargs = {}
        if isinstance(df, pd.DataFrame):
            kwargs["snowpark_pandas_partition_size"] = partition_size

        return df.apply(foo, axis=1, **kwargs)

    with SqlCounter(query_count=5, join_count=2, udtf_count=1):
        eval_snowpark_pandas_result(snow_df, df, helper)


@sql_count_checker(query_count=5, udtf_count=1, join_count=2)
@pytest.mark.parametrize(
    "return_value",
    [
        native_pd.Series(["a", np.int64(3)]),
        param(
            ["a", np.int64(3)],
            marks=pytest.mark.xfail(
                strict=True,
                raises=SnowparkSQLException,
                reason="SNOW-1229760: un-json-serializable np.int64 is nested "
                + "inside the non-series return value, so we don't find "
                + "the np.int64 or convert it to int",
            ),
        ),
        np.int64(3),
    ],
)
def test_numpy_integers_in_return_values_snow_1227264(return_value):
    eval_snowpark_pandas_result(
        *create_test_dfs(["a"]), lambda df: df.apply(lambda row: return_value, axis=1)
    )


@pytest.mark.parametrize(
    "null_value",
    [
        param(
            None,
            marks=pytest.mark.xfail(
                strict=True, raises=SnowparkSQLException, reason="SNOW-1233832"
            ),
        ),
        np.nan,
    ],
)
@sql_count_checker(query_count=5, udtf_count=1, join_count=2)
def test_apply_axis_1_frame_with_column_of_all_nulls_snow_1233832(null_value):
    eval_snowpark_pandas_result(
        *create_test_dfs({"null_col": [null_value], "int_col": [1]}),
        lambda df: df.apply(lambda row: "a", axis=1)
    )


import scipy.stats  # noqa: E402


@pytest.mark.modin_sp_precommit
@pytest.mark.parametrize(
    "packages,expected_query_count",
    [
        (["scipy", "numpy"], 7),
        (["scipy>1.1", "numpy<2.0"], 7),
        # TODO: SNOW-1478188 Re-enable quarantined tests for 8.23
        # [scipy, np], 9),
    ],
)
def test_apply_axis1_with_3rd_party_libraries_and_decorator(
    packages, expected_query_count
):
    data = [[1, 2, 3, 4, 5], [7, -20, 4.0, 7.0, None]]

    with SqlCounter(
        query_count=expected_query_count,
        high_count_expected=True,
        high_count_reason="Snowpark package upload requires many queries.",
    ):
        try:
            pd.session.custom_package_usage_config["enabled"] = True
            df = pd.DataFrame(data)

            @udf(packages=packages, return_type=DoubleType())
            def func(row):
                return np.dot(row, scipy.stats.norm.pdf(row))

            snow_ans = df.apply(func, axis=1)
        finally:
            pd.session.clear_packages()
            pd.session.clear_imports()

        # same in native pandas:
        native_df = native_pd.DataFrame(data)
        native_ans = native_df.apply(func.func, axis=1)

        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_ans, native_ans)


@pytest.mark.parametrize(
    "packages,expected_query_count",
    [
        (["scipy", "numpy"], 7),
        (["scipy>1.1", "numpy<2.0"], 7),
        ([scipy, np], 9),
    ],
)
@pytest.mark.xfail(
    reason="TODO: SNOW-1261830 need to support PandasSeriesType annotation."
)
def test_apply_axis1_with_dynamic_pivot_and_with_3rd_party_libraries_and_decorator(
    packages, expected_query_count
):
    # This test checks the code path with dynamic pivot for axis=1.

    data = [[1, 2, 3, 4, 5], [7, -20, 4.0, 7.0, None]]

    with SqlCounter(query_count=expected_query_count):
        try:
            pd.session.custom_package_usage_config["enabled"] = True
            df = pd.DataFrame(data)

            @udf(packages=packages, return_type=PandasSeriesType(DoubleType()))
            def func(row):
                import pandas as pd

                x = np.dot(row, scipy.stats.norm.pdf(row))
                return pd.concat((row, pd.Series(x)))

            snow_ans = df.apply(func, axis=1)
        finally:
            pd.session.clear_packages()
            pd.session.clear_imports()

        # same in native pandas:
        native_df = native_pd.DataFrame(data)
        native_ans = native_df.apply(func.func, axis=1)

        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_ans, native_ans)


@pytest.mark.parametrize(
    "func", [np.sum, np.min, np.max, np.mean, np.median, np.std, np.var]
)
@sql_count_checker(query_count=1)
def test_apply_numpy_aggregate_functions(func):
    native_df = native_pd.DataFrame(
        {"A": [1, 2, 3, 4, 5], "B": [7, -20, 4.0, 7.0, None]}
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda x: x.apply(func))


@pytest.mark.parametrize(
    "func", [np.square, np.sin, np.sinh, np.sqrt, np.exp, np.log, np.log1p, np.absolute]
)
@sql_count_checker(query_count=1)
def test_apply_numpy_universal_functions(func):
    native_df = native_pd.DataFrame(
        {"A": [1, 2, 3, 4, 5], "B": [7, 20, 4.0, 7.0, None]}
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda x: x.apply(func))


@pytest.mark.skipif(running_on_public_ci(), reason="exhaustive UDF/UDTF test")
def test_udfs_and_udtfs_with_snowpark_object_error_msg():
    expected_error_msg = re.escape(
        "Snowpark pandas only allows native pandas and not Snowpark objects in `apply()`. "
        + "Instead, try calling `to_pandas()` on any DataFrame or Series objects passed to `apply()`. See Limitations"
        + "(https://docs.snowflake.com/developer-guide/snowpark/python/pandas-on-snowflake#limitations) section of"
        + "the Snowpark pandas documentation for more details."
    )
    snow_df = pd.DataFrame([7, 8, 9])
    with SqlCounter(
        query_count=16,
        high_count_expected=True,
        high_count_reason="Series.apply has high query count",
    ):
        with pytest.raises(ValueError, match=expected_error_msg):  # Series.apply
            snow_df[0].apply(lambda row: snow_df.iloc[0, 0])
    with SqlCounter(query_count=2):
        with pytest.raises(
            ValueError, match=expected_error_msg
        ):  # DataFrame.apply axis=0
            snow_df.apply(lambda row: snow_df.iloc[0, 0])
    with SqlCounter(query_count=2):
        with pytest.raises(
            ValueError, match=expected_error_msg
        ):  # DataFrame.apply axis=1
            snow_df.apply(lambda row: snow_df.iloc[0, 0], axis=1)
    with SqlCounter(query_count=2):
        with pytest.raises(ValueError, match=expected_error_msg):  # DataFrame.transform
            snow_df.transform(lambda row: snow_df.iloc[0, 0])
    with SqlCounter(
        query_count=16,
        high_count_expected=True,
        high_count_reason="DataFrame.map has high query count",
    ):
        with pytest.raises(ValueError, match=expected_error_msg):  # DataFrame.map
            snow_df.map(lambda row: snow_df.iloc[0, 0])
    with SqlCounter(
        query_count=16,
        high_count_expected=True,
        high_count_reason="Series.map has high query count",
    ):
        with pytest.raises(ValueError, match=expected_error_msg):  # Series.map
            snow_df[0].map(lambda row: snow_df.iloc[0, 0])
