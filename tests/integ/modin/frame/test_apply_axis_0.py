#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from packaging.version import Version
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
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
from tests.utils import RUNNING_ON_GH

pytestmark = [pytest.mark.udf]

# test data which has a python type as return type that is not a pandas Series/pandas DataFrame/tuple/list
BASIC_DATA_FUNC_PYTHON_RETURN_TYPE_MAP = [
    [[[1.0, 2.2], [3, np.nan]], np.min, "float"],
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
    [[[True, False], [False, False]], lambda x: True, "bool"],
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
        lambda column: column.sum().value,
        "int",
        id="apply_on_frame_with_timedelta_data_columns_returns_int",
        marks=pytest.mark.xfail(strict=True, raises=NotImplementedError),
    ),
]


@pytest.mark.parametrize(
    "data, func, return_type", BASIC_DATA_FUNC_PYTHON_RETURN_TYPE_MAP
)
@pytest.mark.modin_sp_precommit
def test_axis_0_basic_types_without_type_hints(data, func, return_type):
    # this test processes functions without type hints and invokes the UDTF solution.
    native_df = native_pd.DataFrame(data, columns=["A", "b"])
    snow_df = pd.DataFrame(data, columns=["A", "b"])
    # np.min is mapped to builtin function so no UDTF is required.
    with SqlCounter(
        query_count=1 if func == np.min else 11,
        join_count=0 if func == np.min else 2,
        udtf_count=0 if func == np.min else 2,
        high_count_expected=func != np.min,
        high_count_reason="SNOW-1650644 & SNOW-1345395: Avoid extra caching and repeatedly creating same temp function",
    ):
        eval_snowpark_pandas_result(snow_df, native_df, lambda x: x.apply(func, axis=0))


@pytest.mark.parametrize(
    "data, func, return_type", BASIC_DATA_FUNC_PYTHON_RETURN_TYPE_MAP
)
@pytest.mark.modin_sp_precommit
def test_axis_0_basic_types_with_type_hints(data, func, return_type):
    # create explicitly for supported python types UDF with type hints and process via vUDF.
    native_df = native_pd.DataFrame(data, columns=["A", "b"])
    snow_df = pd.DataFrame(data, columns=["A", "b"])
    func_with_type_hint = create_func_with_return_type_hint(func, return_type)
    #  Invoking a single UDF typically requires 3 queries (package management, code upload, UDF registration) upfront.
    with SqlCounter(
        query_count=11,
        join_count=2,
        udtf_count=2,
        high_count_expected=True,
        high_count_reason="SNOW-1650644 & SNOW-1345395: Avoid extra caching and repeatedly creating same temp function",
    ):
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda x: x.apply(func_with_type_hint, axis=0)
        )


@pytest.mark.xfail(strict=True, raises=NotImplementedError)
@sql_count_checker(query_count=0)
def test_frame_with_timedelta_index():
    eval_snowpark_pandas_result(
        *create_test_dfs(
            native_pd.DataFrame([0], index=[native_pd.Timedelta(1)]),
        ),
        lambda df: df.apply(lambda col: col)
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
def test_axis_0_index_passed_as_name(df, row_label):
    # when using apply(axis=1) the original index of the dataframe is passed as name.
    # test here for this for regular index and multi-index scenario.

    def foo(row) -> str:
        if row.name == row_label:
            return "MATCHING LABEL"
        else:
            return "NO MATCH"

    snow_df = pd.DataFrame(df)
    with SqlCounter(
        query_count=11,
        join_count=2,
        udtf_count=2,
        high_count_expected=True,
        high_count_reason="SNOW-1650644 & SNOW-1345395: Avoid extra caching and repeatedly creating same temp function",
    ):
        eval_snowpark_pandas_result(snow_df, df, lambda x: x.apply(foo, axis=0))


@sql_count_checker(
    query_count=11,
    join_count=3,
    udtf_count=2,
    high_count_expected=True,
    high_count_reason="SNOW-1650644 & SNOW-1345395: Avoid extra caching and repeatedly creating same temp function",
)
def test_axis_0_return_series():
    snow_df = pd.DataFrame([[1, 2], [3, 4]], columns=["A", "b"])
    native_df = native_pd.DataFrame([[1, 2], [3, 4]], columns=["A", "b"])
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda x: x.apply(lambda x: native_pd.Series([1, 2], index=["C", "d"]), axis=0),
    )


@sql_count_checker(
    query_count=11,
    join_count=3,
    udtf_count=2,
    high_count_expected=True,
    high_count_reason="SNOW-1650644 & SNOW-1345395: Avoid extra caching and repeatedly creating same temp function",
)
def test_axis_0_return_series_with_different_label_results():
    df = native_pd.DataFrame([[1, 2], [3, 4]], columns=["A", "b"])
    snow_df = pd.DataFrame(df)

    eval_snowpark_pandas_result(
        snow_df,
        df,
        lambda df: df.apply(
            lambda x: native_pd.Series([1, 2], index=["a", "b"])
            if x.sum() > 3
            else native_pd.Series([0, 1, 2], index=["c", "a", "b"]),
            axis=0,
        ),
    )


@sql_count_checker(query_count=6, join_count=1, udtf_count=1)
def test_axis_0_return_single_scalar_series():
    native_df = native_pd.DataFrame([1])
    snow_df = pd.DataFrame(native_df)

    def apply_func(x):
        return native_pd.Series([1], index=["xyz"])

    eval_snowpark_pandas_result(
        snow_df, native_df, lambda x: x.apply(apply_func, axis=0)
    )


@sql_count_checker(query_count=3)
def test_axis_0_return_dataframe_not_supported():
    snow_df = pd.DataFrame([1])

    # Note that pands returns failure "ValueError: If using all scalar values, you must pass an index" which
    # doesn't explain this isn't supported.  We go with the default returned by pandas in this case.
    if Version(native_pd.__version__) > Version("2.2.1"):
        expected_message = re.escape(
            "Data must be 1-dimensional, got ndarray of shape (2, 1) instead"
        )
    else:
        expected_message = "The truth value of a DataFrame is ambiguous."
    with pytest.raises(SnowparkSQLException, match=expected_message):
        # return value
        snow_df.apply(lambda x: native_pd.DataFrame([1, 2]), axis=0).to_pandas()


class TestNotImplemented:
    @pytest.mark.parametrize("result_type", ["reduce", "expand", "broadcast"])
    @sql_count_checker(query_count=0)
    def test_result_type(self, result_type):
        snow_df = pd.DataFrame([[1, 2], [3, 4]])
        msg = "Snowpark pandas apply API doesn't yet support 'result_type' parameter"
        with pytest.raises(NotImplementedError, match=msg):
            snow_df.apply(lambda x: [1, 2], axis=0, result_type=result_type)

    @sql_count_checker(query_count=0)
    def test_axis_1_apply_args_kwargs_with_snowpandas_object(self):
        def f(x, y=None) -> native_pd.Series:
            return x + (y if y is not None else 0)

        snow_df = pd.DataFrame([[1, 2], [3, 4]])
        msg = "Snowpark pandas apply API doesn't yet support DataFrame or Series in 'args' or 'kwargs' of 'func'"
        with pytest.raises(NotImplementedError, match=msg):
            snow_df.apply(f, axis=0, args=(pd.Series([1, 2]),))
        with pytest.raises(NotImplementedError, match=msg):
            snow_df.apply(f, axis=0, y=pd.Series([1, 2]))


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
    "apply_func, expected_join_count, expected_union_count",
    [
        [lambda x: [1, 2], 3, 0],
        [lambda x: x + 1 if x is not None else None, 3, 0],
        [lambda x: x.min(), 2, 1],
    ],
)
def test_axis_0_series_basic(apply_func, expected_join_count, expected_union_count):
    native_df = native_pd.DataFrame(
        [[1.1, 2.2], [3.0, None]], index=pd.Index([2, 3]), columns=["A", "b"]
    )
    snow_df = pd.DataFrame(native_df)
    with SqlCounter(
        query_count=11,
        join_count=expected_join_count,
        udtf_count=2,
        union_count=expected_union_count,
        high_count_expected=True,
        high_count_reason="SNOW-1650644 & SNOW-1345395: Avoid extra caching and repeatedly creating same temp function",
    ):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.apply(apply_func, axis=0),
        )


@sql_count_checker(query_count=4, join_count=1, udtf_count=1)
def test_groupby_apply_constant_output():
    native_df = native_pd.DataFrame([1, 2])
    native_df["fg"] = 0
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(by=["fg"], axis=0).apply(lambda x: [1, 2]),
        # Some calls to the native pandas function propagate attrs while some do not, depending on the values of its arguments.
        test_attrs=False,
    )


@sql_count_checker(
    query_count=11,
    join_count=3,
    udtf_count=2,
    high_count_expected=True,
    high_count_reason="SNOW-1650644 & SNOW-1345395: Avoid extra caching and repeatedly creating same temp function",
)
def test_axis_0_return_list():
    snow_df = pd.DataFrame([[1, 2], [3, 4]])
    native_df = native_pd.DataFrame([[1, 2], [3, 4]])
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda x: x.apply(lambda x: [1, 2], axis=0)
    )


@pytest.mark.skipif(RUNNING_ON_GH, reason="Slow test")
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
@sql_count_checker(
    query_count=21,
    join_count=7,
    udtf_count=4,
    high_count_expected=True,
    high_count_reason="SNOW-1650644 & SNOW-1345395: Avoid extra caching and repeatedly creating same temp function",
)
def test_axis_0_multi_index_column_labels(apply_func):
    data = [[i + j for j in range(0, 4)] for i in range(0, 4)]

    native_df = native_pd.DataFrame(data, columns=TEST_COLUMNS_1)
    snow_df = pd.DataFrame(data, columns=TEST_COLUMNS_1)

    eval_snowpark_pandas_result(
        snow_df, native_df, lambda x: x.apply(apply_func, axis=0)
    )


@pytest.mark.skipif(RUNNING_ON_GH, reason="Slow test")
@sql_count_checker(
    query_count=21,
    join_count=7,
    udtf_count=4,
    high_count_expected=True,
    high_count_reason="SNOW-1650644 & SNOW-1345395: Avoid extra caching and repeatedly creating same temp function",
)
def test_axis_0_multi_index_column_labels_with_different_results():
    data = [[i + j for j in range(0, 4)] for i in range(0, 4)]

    df = native_pd.DataFrame(data, columns=TEST_COLUMNS_1)
    snow_df = pd.DataFrame(df)

    apply_func = (
        lambda x: native_pd.Series([1, 2], index=TEST_INDEX_1)
        if min(x) == 0
        else native_pd.Series([3, 4], index=TEST_INDEX_2)
    )

    eval_snowpark_pandas_result(snow_df, df, lambda df: df.apply(apply_func, axis=0))


@pytest.mark.parametrize(
    "data, func, expected_result",
    [
        [
            [
                [datetime.date(2023, 1, 1), None],
                [datetime.date(2022, 12, 31), datetime.date(2021, 1, 9)],
            ],
            lambda x: x.dt.day,
            native_pd.DataFrame([[1, np.nan], [31, 9.0]]),
        ],
        [
            [
                [datetime.time(1, 2, 3), None],
                [datetime.time(1, 2, 3, 1), datetime.time(1)],
            ],
            lambda x: x.dt.seconds,
            native_pd.DataFrame([[3723, np.nan], [3723, 3600]]),
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
                    ["2023-01-01 01:02:03.000000", "NaT"],
                    ["2022-12-31 01:02:03.000001", "2023-01-01 01:02:03+00:00"],
                ]
            ),
        ],
    ],
)
@sql_count_checker(
    query_count=11,
    join_count=3,
    udtf_count=2,
    high_count_expected=True,
    high_count_reason="SNOW-1650644 & SNOW-1345395: Avoid extra caching and repeatedly creating same temp function",
)
def test_axis_0_date_time_timestamp_type(data, func, expected_result):
    snow_df = pd.DataFrame(data)
    result = snow_df.apply(func, axis=0)

    assert_snowpark_pandas_equal_to_pandas(result, expected_result)


@pytest.mark.parametrize(
    "native_df, func",
    [
        (
            native_pd.DataFrame([[1, 2], [3, 4]], index=["a", "b"]),
            lambda x: x["a"] + x["b"],
        ),
        (
            native_pd.DataFrame(
                [[1, 5], [2, 6], [3, 7], [4, 8]],
                index=native_pd.MultiIndex.from_tuples(
                    [("baz", "A"), ("baz", "B"), ("zoo", "A"), ("zoo", "B")]
                ),
            ),
            lambda x: x["baz", "B"] * x["zoo", "A"],
        ),
    ],
)
@sql_count_checker(
    query_count=11,
    join_count=2,
    udtf_count=2,
    union_count=1,
    high_count_expected=True,
    high_count_reason="SNOW-1650644 & SNOW-1345395: Avoid extra caching and repeatedly creating same temp function",
)
def test_axis_0_index_labels(native_df, func):
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda x: x.apply(func, axis=0))


@sql_count_checker(
    query_count=11,
    join_count=2,
    udtf_count=2,
    union_count=1,
    high_count_expected=True,
    high_count_reason="SNOW-1650644 & SNOW-1345395: Avoid extra caching and repeatedly creating same temp function",
)
def test_axis_0_raw():
    snow_df = pd.DataFrame([[1, 2], [3, 4]])
    native_df = native_pd.DataFrame([[1, 2], [3, 4]])
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda x: x.apply(lambda x: str(type(x)), axis=0, raw=True)
    )


@pytest.mark.skipif(RUNNING_ON_GH, reason="Slow test")
def test_axis_0_apply_args_kwargs():
    def f(x, y, z=1) -> int:
        return x.sum() + y + z

    native_df = native_pd.DataFrame([[1, 2], [3, 4]])
    snow_df = pd.DataFrame([[1, 2], [3, 4]])

    with SqlCounter(query_count=3):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda x: x.apply(f, axis=0),
            expect_exception=True,
            expect_exception_type=SnowparkSQLException,
            expect_exception_match="missing 1 required positional argument",
            assert_exception_equal=False,
        )

    with SqlCounter(
        query_count=11,
        high_count_expected=True,
        high_count_reason="SNOW-1650644 & SNOW-1345395: Avoid extra caching and repeatedly creating same temp function",
    ):
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda x: x.apply(f, axis=0, args=(1,))
        )

    with SqlCounter(
        query_count=11,
        high_count_expected=True,
        high_count_reason="SNOW-1650644 & SNOW-1345395: Avoid extra caching and repeatedly creating same temp function",
    ):
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda x: x.apply(f, axis=0, args=(1,), z=2)
        )

    with SqlCounter(query_count=3):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda x: x.apply(f, axis=0, args=(1,), z=2, v=3),
            expect_exception=True,
            expect_exception_type=SnowparkSQLException,
            expect_exception_match="got an unexpected keyword argument",
            assert_exception_equal=False,
        )


@pytest.mark.parametrize("data", [{"a": [1], "b": [2]}, {"a": [2], "b": [3]}])
def test_apply_axis_0_with_if_where_duplicates_not_executed(data):
    df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(df)

    def foo(x):
        return native_pd.Series(
            [1, 2, 3], index=["C", "A", "E"] if x.sum() > 3 else ["A", "E", "E"]
        )

    with SqlCounter(
        query_count=11,
        join_count=3,
        udtf_count=2,
        high_count_expected=True,
        high_count_reason="SNOW-1650644 & SNOW-1345395: Avoid extra caching and repeatedly creating same temp function",
    ):
        eval_snowpark_pandas_result(snow_df, df, lambda x: x.apply(foo, axis=0))


@pytest.mark.parametrize(
    "return_value",
    [
        native_pd.Series(["a", np.int64(3)]),
        ["a", np.int64(3)],
        np.int64(3),
    ],
)
@sql_count_checker(query_count=6, join_count=1, udtf_count=1)
def test_numpy_integers_in_return_values_snow_1227264(return_value):
    eval_snowpark_pandas_result(
        *create_test_dfs(["a"]), lambda df: df.apply(lambda row: return_value, axis=0)
    )


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
def test_apply_axis_0_bug_1650918(data, apply_func):
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda x: x.apply(apply_func, axis=0),
    )


def test_apply_nested_series_negative():
    snow_df = pd.DataFrame([[1, 2], [3, 4]])

    with SqlCounter(
        query_count=10,
        join_count=2,
        udtf_count=2,
        high_count_expected=True,
        high_count_reason="SNOW-1650644 & SNOW-1345395: Avoid extra caching and repeatedly creating same temp function",
    ):
        with pytest.raises(
            NotImplementedError,
            match=r"Nested pd.Series in result is not supported in DataFrame.apply\(axis=0\)",
        ):
            snow_df.apply(
                lambda ser: 99 if ser.sum() == 4 else native_pd.Series([1, 2]), axis=0
            ).to_pandas()

    snow_df2 = pd.DataFrame([[1, 2, 3]])

    with SqlCounter(
        query_count=15,
        join_count=3,
        udtf_count=3,
        high_count_expected=True,
        high_count_reason="SNOW-1650644 & SNOW-1345395: Avoid extra caching and repeatedly creating same temp function",
    ):
        with pytest.raises(
            NotImplementedError,
            match=r"Nested pd.Series in result is not supported in DataFrame.apply\(axis=0\)",
        ):
            snow_df2.apply(
                lambda ser: 99
                if ser.sum() == 2
                else native_pd.Series([100], index=["a"]),
                axis=0,
            ).to_pandas()


import scipy.stats  # noqa: E402


@pytest.mark.skipif(RUNNING_ON_GH, reason="Slow test")
@pytest.mark.parametrize(
    "packages,expected_query_count",
    [
        (["scipy", "numpy"], 26),
        # TODO: SNOW-2217451 Re-enable scipy>1.1, numpy<2.0 test case after NumPy 2.x compatibility issue is resolved
        # TODO: SNOW-1478188 Re-enable quarantined tests for 8.23
        # [scipy, np], 9),
    ],
)
def test_apply_axis0_with_3rd_party_libraries_and_decorator(
    packages, expected_query_count
):
    data = [[1, 2, 3, 4, 5], [7, -20, 4.0, 7.0, None]]

    with SqlCounter(
        query_count=expected_query_count,
        high_count_expected=True,
        high_count_reason="SNOW-1650644 & SNOW-1345395: Avoid extra caching and repeatedly creating same temp function",
    ):
        try:
            pd.session.custom_package_usage_config["enabled"] = True
            pd.session.add_packages(packages)

            df = pd.DataFrame(data)

            def func(row):
                return np.dot(row, scipy.stats.norm.pdf(row))

            snow_ans = df.apply(func, axis=0)
        finally:
            pd.session.clear_packages()
            pd.session.clear_imports()

        # same in native pandas:
        native_df = native_pd.DataFrame(data)
        native_ans = native_df.apply(func, axis=0)

        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_ans, native_ans)
