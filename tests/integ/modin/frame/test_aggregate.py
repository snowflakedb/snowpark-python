#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import functools
import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas.testing import assert_series_equal
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.types import VariantType
from tests.integ.modin.utils import (
    ColumnSchema,
    assert_snowpark_pandas_equal_to_pandas,
    create_snow_df_with_table_and_data,
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


def sensitive_function_name(col: native_pd.Series) -> int:
    return col.sum()


@pytest.fixture(scope="function")
def numeric_native_df() -> native_pd.DataFrame:
    native_df = native_pd.DataFrame(
        {"A": range(5), "B": range(0, 10, 2), "C": [3.6, 2.0, 7.2, 4.1, 8.2]}
    )
    return native_df


@pytest.fixture(scope="function")
def native_df_multiindex() -> native_pd.DataFrame:
    tuples = list(
        zip(
            *[
                ["bar", "bar", "baz", "baz", "foo", "foo", "qux", "qux"],
                ["one", "two", "one", "two", "one", "two", "one", "two"],
            ]
        )
    )
    index = native_pd.MultiIndex.from_tuples(tuples)
    columns = native_pd.MultiIndex.from_tuples(
        [("A", "cat"), ("B", "dog"), ("B", "cat"), ("A", "dog")]
    )
    data = np.random.randn(8, 4)
    native_df = native_pd.DataFrame(data, index=index, columns=columns)
    return native_df


@pytest.mark.parametrize(
    "func, expected_union_count",
    [
        (lambda df: df.aggregate(["min"]), 0),
        (lambda df: df.aggregate(["min", np.max]), 1),
        (
            lambda df: df.aggregate({"B": ["count"], "A": "sum", "C": ["max", "min"]}),
            3,
        ),
        (lambda df: df.aggregate({"A": ["count", "max"], "B": [max, "min"]}), 2),
        (
            lambda df: df.aggregate(
                x=pd.NamedAgg("A", "max"), y=("B", "min"), c=("A", "count")
            ),
            2,
        ),
        (lambda df: df.aggregate(x=("A", "max")), 0),
        (lambda df: df.aggregate(x=("A", "max"), y=pd.NamedAgg("A", "max")), 1),
        (lambda df: df.aggregate(x=("A", "max"), y=("C", "min"), z=("A", "min")), 2),
        (lambda df: df.aggregate(x=("B", "all"), y=("B", "any")), 1),
        # note following aggregation requires transpose
        (lambda df: df.aggregate(max), 0),
        (lambda df: df.min(), 0),
        (lambda df: df.min(axis=1), 0),
        (lambda df: df.max(), 0),
        (lambda df: df.max(axis=1), 0),
        (lambda df: df.count(), 0),
        (lambda df: df.count(axis=1), 0),
        (lambda df: df.sum(), 0),
        (lambda df: df.sum(axis=1), 0),
        (lambda df: df.mean(), 0),
        (lambda df: df.median(), 0),
        (lambda df: df.skew(), 0),
        (lambda df: df.std(), 0),
        (lambda df: df.var(), 0),
        (lambda df: df.quantile(), 0),
        (lambda df: df.corr(), 2),
        (lambda df: df.aggregate(["idxmin"]), 0),
        (
            lambda df: df.aggregate(
                {"B": ["idxmax"], "A": "sum", "C": ["max", "idxmin"]}
            ),
            3,
        ),
        (lambda df: df.aggregate({"B": "idxmax", "A": "sum"}), 0),
    ],
)
def test_agg_basic(numeric_native_df, func, expected_union_count):
    snow_df = pd.DataFrame(numeric_native_df)
    with SqlCounter(query_count=1, union_count=expected_union_count):
        eval_snowpark_pandas_result(snow_df, numeric_native_df, func)


@pytest.mark.parametrize("min_periods", [None, -1, 0, 1, 2, 3, 4])
@sql_count_checker(query_count=1, union_count=2)
def test_corr_min_periods(min_periods):
    snow_df, pandas_df = create_test_dfs(
        {"a": [None, 1, 2], "b": [3, 4, 5], "c": [6, 7, 8]}
    )
    eval_snowpark_pandas_result(
        snow_df, pandas_df, lambda df: df.corr(min_periods=min_periods)
    )


@pytest.mark.parametrize(
    "method",
    [
        "kendall",
        "spearman",
        lambda x, y: np.minimum(x, y).sum().round(decimals=1),
    ],
)
@sql_count_checker(query_count=0)
def test_corr_negative(numeric_native_df, method):
    snow_df = pd.DataFrame(numeric_native_df)
    with pytest.raises(NotImplementedError):
        snow_df.corr(method=method)


@pytest.mark.parametrize(
    "data",
    [
        param(
            {"col0": ["a", "b", "c"], "col1": [0, 1, 2]},
            id="string_and_integer_column",
        ),
        param(
            {"col0": ["a", "b", "c"], "col1": ["d", "e", "f"]},
            id="only_string_columns",
        ),
    ],
)
@pytest.mark.parametrize(
    "numeric_only_kwargs",
    [
        param({}, id="no_numeric_only_kwarg"),
        param({"numeric_only": False}, id="numeric_only_False"),
        param({"numeric_only": True}, id="numeric_only_True"),
        param({"numeric_only": None}, id="numeric_only_None"),
    ],
)
@sql_count_checker(query_count=1)
def test_string_sum(data, numeric_only_kwargs):
    eval_snowpark_pandas_result(
        *create_test_dfs(data),
        lambda df: df.sum(**numeric_only_kwargs),
        # pandas doesn't propagate attrs if the frame is empty after type filtering,
        # which happens if numeric_only=True and all columns are strings, but Snowpark pandas does.
        test_attrs=not (
            numeric_only_kwargs.get("numeric_only", False)
            and isinstance(data["col1"][0], str)
        ),
    )


@sql_count_checker(query_count=1)
def test_string_sum_of_reversed_df():
    # check that we get the string concatenation right even when the dataframe
    # is not in its original order.
    eval_snowpark_pandas_result(
        *create_test_dfs(["a", "b", "c"]),
        lambda df: df.iloc[::-1].sum(),
    )


@sql_count_checker(query_count=1)
def test_string_sum_with_nulls():
    """
    pandas raises TypeError('can only concatenate str (not \"int\") to str') but
    we instead drop the null values without an exception because checking for
    nulls requires at least one extra query. one possible solution is to do the
    sum and then compare the length of the result to the length of the df, but
    right now that requires an extra length query."""
    snow_df, pandas_df = create_test_dfs(["a", None, "b"])
    with pytest.raises(TypeError):
        pandas_df.sum(numeric_only=False)
    snow_result = snow_df.sum(numeric_only=False)
    assert_series_equal(snow_result.to_pandas(), native_pd.Series(["ab"]))


class TestTimedelta:
    """Test aggregating dataframes containing timedelta columns."""

    @pytest.mark.parametrize(
        "func, union_count",
        [
            param(
                lambda df: df.aggregate(["min"]),
                0,
                id="aggregate_list_with_one_element",
            ),
            param(lambda df: df.aggregate(x=("A", "max")), 0, id="single_named_agg"),
            # this works since all results are timedelta and we don't need to do any concats.
            param(
                lambda df: df.aggregate({"B": "mean", "A": "sum"}),
                0,
                id="dict_producing_two_timedeltas",
            ),
            # this works since even though we need to do concats, all the results are non-timdelta.
            param(
                lambda df: df.aggregate(x=("B", "all"), y=("B", "any")),
                1,
                id="named_agg_producing_two_bools",
            ),
            # note following aggregation requires transpose
            param(lambda df: df.aggregate(max), 0, id="aggregate_max"),
            param(lambda df: df.min(), 0, id="min"),
            param(lambda df: df.max(), 0, id="max"),
            param(lambda df: df.count(), 0, id="count"),
            param(lambda df: df.sum(), 0, id="sum"),
            param(lambda df: df.mean(), 0, id="mean"),
            param(lambda df: df.median(), 0, id="median"),
            param(lambda df: df.std(), 0, id="std"),
            param(lambda df: df.quantile(), 0, id="single_quantile"),
            param(lambda df: df.quantile([0.01, 0.99]), 1, id="two_quantiles"),
        ],
    )
    def test_supported_axis_0(self, func, union_count, timedelta_native_df):
        with SqlCounter(query_count=1, union_count=union_count):
            eval_snowpark_pandas_result(
                *create_test_dfs(timedelta_native_df),
                func,
            )

    @sql_count_checker(query_count=0)
    @pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="SNOW-1653126")
    def test_axis_1(self, timedelta_native_df):
        eval_snowpark_pandas_result(
            *create_test_dfs(timedelta_native_df), lambda df: df.sum(axis=1)
        )

    @sql_count_checker(query_count=0)
    def test_var_invalid(self, timedelta_native_df):
        eval_snowpark_pandas_result(
            *create_test_dfs(timedelta_native_df),
            lambda df: df.var(),
            expect_exception=True,
            expect_exception_type=TypeError,
            assert_exception_equal=False,
            expect_exception_match=re.escape(
                "timedelta64 type does not support var operations"
            ),
        )

    @sql_count_checker(query_count=0)
    @pytest.mark.xfail(
        strict=True,
        raises=NotImplementedError,
        reason="requires concat(), which we cannot do with Timedelta.",
    )
    @pytest.mark.parametrize(
        "operation",
        [
            lambda df: df.aggregate({"A": ["count", "max"], "B": [max, "min"]}),
            lambda df: df.aggregate({"B": ["count"], "A": "sum", "C": ["max", "min"]}),
            lambda df: df.aggregate(
                x=pd.NamedAgg("A", "max"), y=("B", "min"), c=("A", "count")
            ),
            lambda df: df.aggregate(["min", np.max]),
            lambda df: df.aggregate(x=("A", "max"), y=("C", "min"), z=("A", "min")),
            lambda df: df.aggregate(x=("A", "max"), y=pd.NamedAgg("A", "max")),
            lambda df: df.aggregate(
                {"B": ["idxmax"], "A": "sum", "C": ["max", "idxmin"]}
            ),
        ],
    )
    def test_agg_requires_concat_with_timedelta(self, timedelta_native_df, operation):
        eval_snowpark_pandas_result(*create_test_dfs(timedelta_native_df), operation)

    @sql_count_checker(query_count=0)
    @pytest.mark.xfail(
        strict=True,
        raises=NotImplementedError,
        reason="requires transposing a one-row frame with integer and timedelta.",
    )
    def test_agg_produces_timedelta_and_non_timedelta_type(self, timedelta_native_df):
        eval_snowpark_pandas_result(
            *create_test_dfs(timedelta_native_df),
            lambda df: df.aggregate({"B": "idxmax", "A": "sum"}),
        )


@pytest.mark.parametrize(
    "func, expected_union_count",
    [
        (lambda df: df.aggregate(["min"]), 0),
        (lambda df: df.aggregate(["min", "max"]), 1),
        (lambda df: df.aggregate(x=("B", "min")), 0),
        (lambda df: df.aggregate(x=("B", "min"), y=("B", "max")), 1),
    ],
)
def test_agg_dup_col(numeric_native_df, func, expected_union_count):
    # rename to have duplicated column
    numeric_native_df.columns = ["A", "B", "A"]
    snow_df = pd.DataFrame(numeric_native_df)

    with SqlCounter(query_count=1, union_count=expected_union_count):
        eval_snowpark_pandas_result(snow_df, numeric_native_df, func)


class TestNamedAggDupColPandasFails:
    def test_single_agg_on_dup_column(self, numeric_native_df):
        # rename to have duplicated column
        numeric_native_df.columns = ["A", "B", "A"]
        snow_df = pd.DataFrame(numeric_native_df)

        with pytest.raises(
            ValueError,
            match=re.escape(
                "Data must be 1-dimensional, got ndarray of shape (1, 2) instead"
            ),
        ):
            numeric_native_df.agg(x=("A", "min"))

        result_df = native_pd.DataFrame(
            [[0, np.nan], [np.nan, 2.0]], columns=["A", "A"], index=["x", "x"]
        )

        with SqlCounter(query_count=1, union_count=1):
            assert_snowpark_pandas_equal_to_pandas(
                snow_df.agg(x=("A", "min")), result_df
            )

    @pytest.mark.parametrize("order", [True, False], ids=["dup_first", "dup_last"])
    def test_single_agg_on_dup_and_non_dup_columns(self, numeric_native_df, order):
        # rename to have duplicated column
        numeric_native_df.columns = ["A", "B", "A"]
        snow_df = pd.DataFrame(numeric_native_df)
        with pytest.raises(
            ValueError,
            match=re.escape(
                "Data must be 1-dimensional, got ndarray of shape (2, 2) instead"
            ),
        ):
            numeric_native_df.agg(x=("A", "min"), y=("B", "min"))

        if order:
            kwargs = {"x": ("A", "min"), "y": ("B", "min")}
        else:
            kwargs = {"x": ("B", "min"), "y": ("A", "min")}
        result_df = native_pd.DataFrame(
            [[0, np.nan, np.nan], [np.nan, 2.0, np.nan], [np.nan, np.nan, 0]],
            columns=["A", "A", "B"],
            index=["x", "x", "y"] if order else ["y", "y", "x"],
        )
        if not order:
            b_col = result_df.pop("B")
            result_df.insert(0, "B", b_col)
            result_df = result_df.sort_index()

        with SqlCounter(query_count=1, union_count=2):
            assert_snowpark_pandas_equal_to_pandas(
                snow_df.agg(**kwargs),
                result_df,
            )

    def test_multiple_agg_on_dup_and_single_agg_on_non_dup_columns(
        self, numeric_native_df
    ):
        # rename to have duplicated column
        numeric_native_df.columns = ["A", "B", "A"]
        snow_df = pd.DataFrame(numeric_native_df)
        with pytest.raises(
            ValueError,
            match=re.escape(
                "Data must be 1-dimensional, got ndarray of shape (3, 2) instead"
            ),
        ):
            numeric_native_df.agg(x=("A", "min"), y=("B", "min"), z=("A", "max"))

        result_df = native_pd.DataFrame(
            [
                [0, np.nan, np.nan],
                [np.nan, 2.0, np.nan],
                [np.nan, np.nan, 0],
                [4, np.nan, np.nan],
                [np.nan, 8.2, np.nan],
            ],
            columns=["A", "A", "B"],
            index=["x", "x", "y", "z", "z"],
        )

        with SqlCounter(query_count=1, union_count=4):
            assert_snowpark_pandas_equal_to_pandas(
                snow_df.agg(x=("A", "min"), y=("B", "min"), z=("A", "max")),
                result_df,
            )

    def test_multiple_agg_on_only_dup_columns(self, numeric_native_df):
        # rename to have duplicated column
        numeric_native_df.columns = ["A", "A", "A"]
        snow_df = pd.DataFrame(numeric_native_df)

        with pytest.raises(
            ValueError,
            match=re.escape(
                "Data must be 1-dimensional, got ndarray of shape (2, 3) instead"
            ),
        ):
            numeric_native_df.agg(x=("A", "min"), y=("A", "max"))

        result_df = native_pd.DataFrame(
            [
                [0, np.nan, np.nan],
                [np.nan, 0, np.nan],
                [np.nan, np.nan, 2.0],
                [4, np.nan, np.nan],
                [np.nan, 8, np.nan],
                [np.nan, np.nan, 8.2],
            ],
            columns=["A", "A", "A"],
            index=["x", "x", "x", "y", "y", "y"],
        )

        with SqlCounter(query_count=1, union_count=5):
            assert_snowpark_pandas_equal_to_pandas(
                snow_df.agg(x=("A", "min"), y=("A", "max")),
                result_df,
            )


@pytest.mark.parametrize(
    "func, expected_union_count",
    [
        (lambda df: df.aggregate(["min"]), 0),
        (lambda df: df.aggregate([max, "min", "count"]), 2),
        (lambda df: df.min(), 0),
        (lambda df: df.max(), 0),
        (lambda df: df.count(), 0),
        (lambda df: df.mean(numeric_only=True), 0),
        (lambda df: df.median(numeric_only=True), 0),
        (lambda df: df.std(numeric_only=True), 0),
        (lambda df: df.var(numeric_only=True), 0),
        (lambda df: df.corr(numeric_only=True), 1),
        (lambda df: df.aggregate("max"), 0),
    ],
)
def test_agg_with_all_missing(func, expected_union_count):
    missing_df = native_pd.DataFrame(
        {
            "nan": [np.nan, np.nan, np.nan, np.nan],
            "na": [native_pd.NA, native_pd.NA, native_pd.NA, native_pd.NA],
            "nat": [native_pd.NaT, native_pd.NaT, native_pd.NaT, native_pd.NaT],
            "none": [None, None, None, None],
            "values": [1, 2, 3, 4],
        }
    )
    snow_missing_df = pd.DataFrame(missing_df)
    with SqlCounter(query_count=1, union_count=expected_union_count):
        eval_snowpark_pandas_result(snow_missing_df, missing_df, func)


# For some reason, this aggregation causes pandas to fail.
# This ticket: https://github.com/pandas-dev/pandas/issues/58810
# has been opened on the pandas repo.
def test_agg_with_all_missing_pandas_bug_58810():
    missing_df = native_pd.DataFrame(
        {
            "nan": [np.nan, np.nan, np.nan, np.nan],
            "na": [native_pd.NA, native_pd.NA, native_pd.NA, native_pd.NA],
            "nat": [native_pd.NaT, native_pd.NaT, native_pd.NaT, native_pd.NaT],
            "none": [None, None, None, None],
            "values": [1, 2, 3, 4],
        }
    )
    snow_missing_df = pd.DataFrame(missing_df)
    with pytest.raises(IndexError, match="positional indexers are out-of-bounds"):
        missing_df.aggregate(x=("nan", "min"), y=("na", "min"), z=("values", "sum"))

    result_df = native_pd.DataFrame(
        [[None, None, np.nan], [None, None, np.nan], [None, None, 10.0]],
        index=["x", "y", "z"],
        columns=["nan", "na", "values"],
    )
    with SqlCounter(query_count=1, union_count=2):
        snow_df = snow_missing_df.aggregate(
            x=("nan", "min"), y=("na", "min"), z=("values", "sum")
        )
        assert_snowpark_pandas_equal_to_pandas(snow_df, result_df)


@pytest.mark.parametrize(
    "interpolation",
    [
        None,
        "linear",
        pytest.param(
            "nearest",
            marks=pytest.mark.xfail(
                # strict=False because this passes on the sample data with the default quantile (0.5)
                strict=False,
                reason="SNOW-1062839: PERCENTILE_DISC does not give desired behavior",
            ),
        ),
    ],
)
@pytest.mark.parametrize(
    "q",
    [
        None,
        0.1,
        pytest.param(
            [0.25, 0.5, 0.75],
            marks=pytest.mark.xfail(
                strict=True,
                reason='SNOW-1062878: agg("quantile") with list-like q fails',
            ),
        ),
    ],
)
@sql_count_checker(query_count=1)
def test_agg_quantile_kwargs(interpolation, q):
    data = {
        "with_nans": [np.nan, 25, 0, 75, 50, 100, np.nan],
        "no_nans": [0.1, -10.0, 100, 9.2, 999, -999, 20.2],
    }
    kwargs = {}
    if interpolation is not None:
        kwargs["interpolation"] = interpolation
    if q is not None:
        kwargs["q"] = q
    eval_snowpark_pandas_result(
        *create_test_dfs(data), lambda df: df.agg("quantile", **kwargs)
    )


@sql_count_checker(query_count=1, union_count=2)
def test_agg_with_variant():
    native_df = native_pd.DataFrame(
        {
            "nat": [native_pd.NaT, native_pd.NaT, native_pd.NaT, native_pd.NaT],
            "nan": [np.nan, np.nan, np.nan, np.nan],
            "str": ["a", "b", "c", "d"],
            "none": [None, None, None, None],
            "reg_value": [1.1, 2.3, 4.5, 6],
        }
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.aggregate([min, "count", "max"])
    )


@sql_count_checker(query_count=1)
def test_general_agg_numeric_only(agg_method):
    native_df = native_pd.DataFrame(
        {
            "int": [4, 2, 0, 8],
            "str": ["female", "male", "male", "female"],
            "float": [32.1, 28.92, 37.88, 15.6],
            "variant": [(1, 1), (2, 3), (4, 5), (2, 2)],
            "NaN mix": [3, None, 2.5, None],
            "timestamp": [
                native_pd.Timestamp(1513393355.5, unit="s"),
                native_pd.Timestamp(1513393375.5, unit="s"),
                native_pd.NaT,
                native_pd.Timestamp(1513393355.5, unit="s"),
            ],
        }
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: getattr(df, agg_method)(numeric_only=True),
    )


AGG_MULTIINDEX_FAIL_REASON = (
    "SNOW-1010307: MultiIndex aggregation on axis=1 not supported"
)


@pytest.mark.parametrize(
    "func, expected_union_count",
    [
        (lambda df: df.aggregate(["min"]), 0),
        (lambda df: df.aggregate([max, np.min, "count"]), 2),
        (
            lambda df: df.aggregate(
                x=(("A", "cat"), "min"),
                y=(("B", "dog"), "min"),
                z=(("B", "cat"), "sum"),
            ),
            2,
        ),
        (
            lambda df: df.aggregate(
                x=(("A", "cat"), "min"),
                y=(("B", "dog"), np.min),
                z=(("B", "cat"), "sum"),
            ),
            2,
        ),
        (
            lambda df: df.aggregate(
                x=(("A", "cat"), "min"),
                y=(("A", "cat"), "max"),
                z=(("A", "cat"), "sum"),
            ),
            2,
        ),
        (lambda df: df.aggregate(min), 0),
        (lambda df: df.max(), 0),
    ],
)
def test_agg_with_multiindex(native_df_multiindex, func, expected_union_count):
    snow_df = pd.DataFrame(native_df_multiindex)
    with SqlCounter(query_count=1, union_count=expected_union_count):
        eval_snowpark_pandas_result(snow_df, native_df_multiindex, func)


@pytest.mark.parametrize(
    "func",
    [
        pytest.param(
            lambda df: df.aggregate([max, np.min, "count"], axis=1),
            marks=pytest.mark.xfail(strict=True, reason=AGG_MULTIINDEX_FAIL_REASON),
        ),
        pytest.param(
            lambda df: df.aggregate(min, axis=1),
            marks=pytest.mark.xfail(strict=True, reason=AGG_MULTIINDEX_FAIL_REASON),
        ),
        pytest.param(
            lambda df: df.max(axis=1),
            marks=pytest.mark.xfail(strict=True, reason=AGG_MULTIINDEX_FAIL_REASON),
        ),
    ],
)
@sql_count_checker(query_count=1)
def test_agg_with_multiindex_negative(native_df_multiindex, func):
    snow_df = pd.DataFrame(native_df_multiindex)
    eval_snowpark_pandas_result(snow_df, native_df_multiindex, func)


@pytest.mark.parametrize(
    "func",
    [
        lambda df: df.aggregate(["min"]),
        lambda df: df.aggregate([max, np.min, "count"]),
        lambda df: df.aggregate(x=("A", "min"), y=("B", "min"), z=("C", "sum")),
        lambda df: df.aggregate(x=("A", "min"), y=("A", "max"), z=("A", "sum")),
        lambda df: df.aggregate(min),
        lambda df: df.aggregate({"A": [min], "B": [np.min, max]}),
    ],
)
@sql_count_checker(query_count=1, union_count=1)
def test_agg_with_empty_df_no_row(func):
    native_df = native_pd.DataFrame([], columns=["A", "B", "C"], dtype="int64")
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.aggregate([min, max]))


@pytest.mark.parametrize(
    "pandas_df",
    [
        native_pd.DataFrame([]),
        native_pd.DataFrame([], index=native_pd.Index([1, 2, 3])),
    ],
)
@sql_count_checker(query_count=0)
def test_agg_with_no_column_raises(pandas_df):
    snow_df = pd.DataFrame(pandas_df)
    eval_snowpark_pandas_result(
        snow_df,
        pandas_df,
        lambda df: df.aggregate([min, max]),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="No column to aggregate on",
        assert_exception_equal=False,
    )


@pytest.mark.parametrize(
    "func, test_attrs",
    [
        (lambda df: df.aggregate(min), True),
        # This is a bug in pandas - the attrs are not propagated for
        # size, but are propagated for other functions.
        (lambda df: df.aggregate("size"), False),
        (lambda df: df.aggregate(len), False),
        (lambda df: df.max(), True),
        (lambda df: df.count(), True),
        (lambda df: df.corr(), True),
        (lambda df: df.aggregate(x=("A", "min")), True),
    ],
)
@sql_count_checker(query_count=1)
def test_agg_with_single_col(func, test_attrs):
    native_df = native_pd.DataFrame({"A": [1, 2, 3]})
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, func, test_attrs=test_attrs)


@pytest.mark.parametrize(
    "func, expected_union_count",
    [
        (lambda df: df.aggregate(min), 0),
        (lambda df: df.max(), 0),
        (lambda df: df.count(), 0),
        (lambda df: df.corr(), 1),
        (lambda df: df.aggregate(x=("A", "min")), 0),
    ],
)
def test_agg_with_multi_col(func, expected_union_count):
    native_df = native_pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    snow_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=1, union_count=expected_union_count):
        eval_snowpark_pandas_result(snow_df, native_df, func)


@pytest.mark.parametrize(
    "func, expected_union_count",
    [
        (lambda df: df.aggregate(["min"]), 0),
        (lambda df: df.aggregate([max, np.min, "count"]), 2),
        (lambda df: df.aggregate(x=("A", "min")), 0),
        (lambda df: df.aggregate(x=("A", "min"), y=("A", "max"), z=("A", "sum")), 2),
    ],
)
def test_agg_with_single_col_mult_agg(func, expected_union_count):
    native_df = native_pd.DataFrame({"A": [1, 2, 3]})
    snow_df = pd.DataFrame(native_df)

    with SqlCounter(query_count=1, union_count=expected_union_count):
        eval_snowpark_pandas_result(snow_df, native_df, func)


@pytest.mark.parametrize(
    "func",
    [
        lambda df: df.std(ddof=0),
        lambda df: df.var(ddof=0),
        lambda df: df.std(ddof=1),
        lambda df: df.var(ddof=1),
    ],
)
@sql_count_checker(query_count=1)
def test_ddof(numeric_native_df, func):
    snow_df = pd.DataFrame(numeric_native_df)

    eval_snowpark_pandas_result(snow_df, numeric_native_df, func)


@pytest.mark.parametrize(
    "func",
    [
        "std",
        "var",
    ],
)
@sql_count_checker(query_count=0)
def test_ddof_fallback_negative(numeric_native_df, func):
    # TODO: SNOW-892532 support ddof that is not 0 or 1 for var/std
    snow_df = pd.DataFrame(numeric_native_df)
    with pytest.raises(
        AttributeError, match=f"'{func}' is not a valid function for 'Series' object"
    ):
        eval_snowpark_pandas_result(
            snow_df, numeric_native_df, lambda df: getattr(df, func)(ddof=2)
        )


@pytest.mark.parametrize("skipna", [True, False])
@sql_count_checker(query_count=1)
def test_general_agg_skipna(skipna_agg_method, skipna):
    native_df = native_pd.DataFrame(
        {
            "int": [1, 2, 4, 5],
            "float": [32.1, 28.92, 37.88, 15.6],
            "float_nan": [2.0, np.nan, 3.0, 4.0],
            "nan": [np.nan, np.nan, np.nan, np.nan],
        }
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: getattr(df, skipna_agg_method)(skipna=skipna)
    )


@pytest.mark.parametrize("min_count", [-1, 0, 1, 3, 4, 8, 40])
@pytest.mark.parametrize("axis", [0, 1], ids=["axis0", "axis1"])
@sql_count_checker(query_count=1)
def test_sum_min_count(min_count, axis):
    native_df = native_pd.DataFrame(
        {
            "ts_na": [4.0, np.nan, 4.0, 5.0, np.nan, np.nan],
            "ts": [3, 4, 8, 2, 0, 5],
        }
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.sum(min_count=min_count, axis=axis)
    )


@sql_count_checker(query_count=2, union_count=4)
def test_agg_valid_variant_col(session, test_table_name):
    pandas_df = native_pd.DataFrame(
        {
            "COL_0": [2, 3, 6, 5, 4, 10],
            "COL_1": [2.0, None, 1.1, 5, None, 3.5],
            "COL_2": ["aa", "ac", "dc", "ee", "bb", "de"],
            "COL_3": [5, None, 1.4, 5.2, 4, None],
        }
    )
    column_schema = [
        ColumnSchema("COL_0", VariantType()),
        ColumnSchema("COL_1", VariantType()),
        ColumnSchema("COL_2", VariantType()),
        ColumnSchema("COL_3", VariantType()),
    ]
    snowpark_pandas_df = create_snow_df_with_table_and_data(
        session, test_table_name, column_schema, pandas_df.values.tolist()
    )
    # Follow read_snowflake with a sort operation to ensure that ordering is stable and tests are not flaky.
    snowpark_pandas_df = snowpark_pandas_df.sort_values(
        snowpark_pandas_df.columns.to_list()
    )
    pandas_df = pandas_df.sort_values(pandas_df.columns.to_list())

    # 2 queries are executed, one for df.agg, another one is triggered during to_pandas,
    # which tries to convert the variant columns back to the right format.

    eval_snowpark_pandas_result(
        snowpark_pandas_df,
        pandas_df,
        lambda df: df.agg(
            {
                "COL_0": ["median"],
                "COL_1": ["sum"],
                "COL_2": ["min", "max"],
                "COL_3": ["max", "std"],
            }
        ),
    )


@pytest.mark.parametrize(
    "agg_func",
    [
        "min",
        "max",
        "count",
        "sum",
        np.min,
        np.max,
        np.sum,
        "size",
        len,
        ["max", "min", "count", "sum"],
        ["min"],
        ["idxmax", "max", "idxmin", "min"],
    ],
)
@sql_count_checker(query_count=1)
def test_agg_axis_1_simple(agg_func):
    data = [[1, 2, 3], [2, 4, -1], [3, 0, 6]]
    native_df = native_pd.DataFrame(data)
    df = pd.DataFrame(data)
    eval_snowpark_pandas_result(
        df,
        native_df,
        lambda df: df.agg(agg_func, axis=1),
        test_attrs=agg_func not in ("size", len),
    )  # native pandas does not propagate attrs for size, but snowpark pandas does


@pytest.mark.parametrize(
    "data",
    [
        [["a", "b", "c"], [None, None, "d"], ["e", None, "g"]],
        [[np.nan, 1, 2], [3, np.nan, 4], [5, np.nan, 6]],
    ],
)
@sql_count_checker(query_count=1)
def test_count_axis_1(data):
    native_df = native_pd.DataFrame(data)
    df = pd.DataFrame(data)
    eval_snowpark_pandas_result(df, native_df, lambda df: df.count(axis=1))


@pytest.mark.parametrize("axis", [0, 1], ids=["axis0", "axis1"])
@pytest.mark.parametrize(
    "agg_func",
    [
        {0: "min", 2: "max", 1: "min"},  # result is a single unnamed series
        {0: "min", 1: ["min", "max"]},  # result is a DF with 2 columns ("min", "max")
        {0: ["min"], 1: "min"},  # result is a DF with 1 column ("min")
        {
            2: ["count", "min", "max"],
            0: ["max", "min"],
            1: ["min", "count", "max"],
        },  # order of result columns is determined by order of appearance of keys
        {1: "max", 0: "min"},  # order of rows is determined by order of keys
    ],
)
def test_agg_dict(agg_func, axis):
    data = [[1, 2, 3], [2, 4, -1], [3, 0, 6]]
    native_df = native_pd.DataFrame(data)
    df = pd.DataFrame(data)
    if axis == 1:
        # With axis=1, we always generate each row of aggregations separately (one for each row label
        # in the input dict) and UNION ALL together at the end. This is true even if the output is a Series.
        expected_union_count = len(agg_func) - 1
    else:
        # With axis=0, we generate exactly one row for each aggregation function in the input dict,
        # which are then combined via UNION ALL. This means when the output is a Series, we
        # do not perform UNION ALL.
        num_unique_agg_funcs = len(
            functools.reduce(
                lambda acc, value: acc | set(value),
                (
                    value if isinstance(value, list) else [value]
                    for value in agg_func.values()
                ),
                set(),
            )
        )
        expected_union_count = (
            0
            if all(not isinstance(value, list) for value in agg_func.values())
            else num_unique_agg_funcs - 1
        )
    with SqlCounter(query_count=1, union_count=expected_union_count):
        eval_snowpark_pandas_result(
            df, native_df, lambda df: df.agg(agg_func, axis=axis)
        )


@pytest.mark.parametrize(
    "agg_func",
    [
        "min",
        ["max", "min", "sum"],
        {1: "max", 0: "min"},
        {1: "idxmax", 0: "idxmin"},
        {1: "idxmax", 0: "max"},
    ],
)
@pytest.mark.parametrize(
    "data",
    [
        [[1, 0, np.nan], [1, np.nan, np.nan], [np.nan, np.nan, np.nan]],
        [[np.nan], [1]],
    ],
)
@pytest.mark.parametrize("skipna", [True, False], ids=lambda skipna: f"skipna={skipna}")
def test_agg_axis_1_with_nan(agg_func, data, skipna):
    native_df = native_pd.DataFrame(data)
    df = pd.DataFrame(data)

    expected_union_count = 1 if isinstance(agg_func, dict) else 0
    with SqlCounter(query_count=1, union_count=expected_union_count):
        eval_snowpark_pandas_result(
            df, native_df, lambda df: df.agg(agg_func, axis=1, skipna=skipna)
        )


@sql_count_checker(query_count=1)
def test_agg_axis_1_col_agg_name():
    # Tests running an aggregation on a dataframe where the columns have names "min" and "max".
    # This ensures that we do not error out due to a collision between the names of columns in the
    # original frame and names in the aggregation result.
    data = [[0, 1], [1, 0]]
    native_df = native_pd.DataFrame(data, columns=["min", "max"])
    df = pd.DataFrame(data, columns=["min", "max"])
    eval_snowpark_pandas_result(
        df, native_df, lambda df: df.agg(["max", "min"], axis=1)
    )


@pytest.mark.parametrize("axis", [0, 1], ids=["axis0", "axis1"])
@sql_count_checker(query_count=0)
def test_agg_empty_func_negative(axis):
    # Error out if a row/column label had no aggregations provided
    agg_func = {0: [], 1: "min"}
    data = [[0, 1], [1, 0]]
    native_df = native_pd.DataFrame(data)
    df = pd.DataFrame(data)
    eval_snowpark_pandas_result(
        df,
        native_df,
        lambda df: df.agg(agg_func, axis=axis),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="No objects to concatenate",
        assert_exception_equal=True,
    )


@sql_count_checker(query_count=1, union_count=1)
def test_agg_axis_1_missing_label():
    # Like our implementation of df.loc and similar methods, if a label specified in the
    # aggregation dictionary does not exist in the DF, we simply ignore that label.
    # This differs from pandas, which would raise an error.
    data = {"a": [0, 1], "b": [2, 3]}
    agg_func = {0: ["max"], 2: ["min"]}
    native_df = native_pd.DataFrame(data)
    df = pd.DataFrame(data)
    with pytest.raises(KeyError):
        native_df.agg(agg_func, axis=1)
    assert_snowpark_pandas_equal_to_pandas(
        df.agg(agg_func, axis=1),
        native_pd.DataFrame({"max": [2], "min": [None]}),
    )


@sql_count_checker(query_count=1, union_count=4)
def test_agg_preserve_cols_order():
    pandas_df = native_pd.DataFrame(
        {
            "COL_0": [2, 3, 6, 5, 4, 10],
            "COL_1": [2.0, None, 1.1, 5, None, 3.5],
            "COL_2": ["aa", "ac", "dc", "ee", "bb", "de"],
            "COL_3": [5, None, 1.4, 5.2, 4, None],
        }
    )
    snowpark_pandas_df = pd.DataFrame(pandas_df)

    eval_snowpark_pandas_result(
        snowpark_pandas_df,
        pandas_df,
        lambda df: df.agg(
            {
                "COL_0": ["median"],
                "COL_1": ["sum"],
                "COL_2": ["min", "max"],
                "COL_3": ["sum", "std"],
            }
        ),
    )


@sql_count_checker(query_count=0)
def test_agg_numeric_only_negative(numeric_native_df, agg_method):
    snow_df = pd.DataFrame(numeric_native_df)
    with pytest.raises(
        ValueError, match=re.escape('For argument "numeric_only" expected type bool')
    ):
        getattr(snow_df, agg_method)(numeric_only="TEST")


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("skipna", ["TEST", None])
def test_agg_skipna_negative(numeric_native_df, skipna_agg_method, skipna):
    snow_df = pd.DataFrame(numeric_native_df)
    eval_snowpark_pandas_result(
        snow_df,
        numeric_native_df,
        lambda df: getattr(df, skipna_agg_method)(skipna=skipna),
        expect_exception=True,
        expect_exception_type=ValueError,
    )


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("min_count", ["TEST", 5.3, 5.0])
def test_agg_min_count_negative(numeric_native_df, min_count):
    snow_df = pd.DataFrame(numeric_native_df)
    # This behavior is different compare with Native pandas.
    with pytest.raises(ValueError, match=re.escape("min_count must be an integer")):
        snow_df.sum(min_count=min_count)


@sql_count_checker(query_count=0)
def test_named_agg_not_supported_axis_1(numeric_native_df):
    snow_df = pd.DataFrame(numeric_native_df)
    # This behavior is different compared to native pandas
    # which raises a different type of error depending
    # on whether the index values specified in the
    # NamedAgg appear in the DataFrame's index or not.
    with pytest.raises(
        ValueError,
        match="`func` must not be `None` when `axis=1`. Named aggregations are not supported with `axis=1`.",
    ):
        snow_df.agg(x=("A", "min"), axis=1)


@pytest.mark.parametrize(
    "func, kwargs, error_pattern",
    # note that we expect some functions like `any` to become strings like
    # 'any' in the error message because of preprocessing in the modin API
    # layer in [1]. That's okay.
    # [1] https://github.com/snowflakedb/snowpark-python/blob/7c854cb30df2383042d7899526d5237a44f9fdaf/src/snowflake/snowpark/modin/pandas/utils.py#L633
    [
        param(
            sensitive_function_name,
            {},
            "Callable is not a valid function for 'Series' object",
            id="user_defined_function",
        ),
        param(
            [sensitive_function_name, "size"],
            {},
            "Callable is not a valid function for 'Series' object",
            id="list_with_user_defined_function_and_string",
        ),
        param(
            (sensitive_function_name, "size"),
            {},
            "Callable is not a valid function for 'Series' object",
            id="tuple_with_user_defined_function_and_string",
        ),
        param(
            {sensitive_function_name, "size"},
            {},
            "Callable is not a valid function for 'Series' object",
            id="set_with_user_defined_function_and_string",
        ),
        param(
            (all, any, len, list, min, max, set, str, tuple, native_pd.Series.sum),
            {},
            "list is not a valid function for 'Series' object",
            id="tuple_with_builtins_and_native_pandas_function",
        ),
        param(
            {"A": sensitive_function_name, "B": sum, "C": [np.mean, "size"]},
            {},
            "Callable is not a valid function for 'Series' object",
            id="dict",
        ),
        param(
            None,
            {
                "x": ("A", np.exp),
                "y": pd.NamedAgg("C", sum),
            },
            "np.exp is not a valid function for 'Series' object",
            id="named_agg",
        ),
        param(
            None,
            {
                "x": ("A", "first"),
                "y": pd.NamedAgg("C", sum),
            },
            "'first' is not a valid function for 'Series' object",
            id="named_agg",
        ),
        param(
            None,
            {
                "x": ("A", "last"),
                "y": pd.NamedAgg("C", sum),
            },
            "'last' is not a valid function for 'Series' object",
            id="named_agg",
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_aggregate_unsupported_aggregation_SNOW_1526422(
    numeric_native_df, func, kwargs, error_pattern
):
    snow_df = pd.DataFrame(numeric_native_df)
    with pytest.raises(
        AttributeError,
        match=error_pattern,
    ):
        snow_df.agg(func, **kwargs)


@sql_count_checker(query_count=0)
def test_named_agg_missing_key(numeric_native_df):
    snow_df = pd.DataFrame(numeric_native_df)
    eval_snowpark_pandas_result(
        snow_df,
        numeric_native_df,
        lambda df: df.agg(x=("x", "min")),
        expect_exception=True,
        assert_exception_equal=True,
        expect_exception_match=re.escape("Column(s) ['x'] do not exist"),
        expect_exception_type=KeyError,
    )


@sql_count_checker(query_count=1, union_count=1)
def test_named_agg_passed_in_via_star_kwargs(numeric_native_df):
    snow_df = pd.DataFrame(numeric_native_df)
    kwargs = {"x": ("A", "min"), "y": pd.NamedAgg("B", "max")}
    eval_snowpark_pandas_result(snow_df, numeric_native_df, lambda df: df.agg(**kwargs))
