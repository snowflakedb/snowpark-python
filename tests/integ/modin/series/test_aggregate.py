#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas._testing import assert_almost_equal
from pandas.errors import SpecificationError
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import (
    ARRAY_DATA_AND_TYPE,
    MAP_DATA_AND_TYPE,
    MIXED_NUMERIC_STR_DATA_AND_TYPE,
    TIMESTAMP_DATA_AND_TYPE,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_series,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.fixture(scope="function")
def native_series() -> native_pd.Series:
    index = native_pd.Index(["a", "b", "b", "a", "c"], name="index")
    native_series = native_pd.Series([3.5, 1.2, 4.3, 2.0, 1.8], index=index)
    return native_series


def validate_scalar_result(res1, res2):
    if native_pd.isna(res1):
        # in python nan != nan, here we do a check for nan specially
        assert native_pd.isna(res2)
    else:
        assert_almost_equal(res1, res2)


@pytest.mark.parametrize(
    "func, is_scalar, has_count_aggregate, expected_union_count",
    [
        (lambda df: df.aggregate(["min"]), False, False, 0),
        (lambda df: df.aggregate(x="min"), False, False, 0),
        (lambda df: df.aggregate(["min", np.max]), False, False, 1),
        (lambda df: df.aggregate(x="min", y=np.max), False, False, 1),
        (
            lambda df: df.aggregate(y="min", x=np.max),
            False,
            False,
            1,
        ),  # Test order of index is correct.
        (lambda df: df.aggregate(["min", np.max, "count"]), False, True, 2),
        (lambda df: df.aggregate(x="min", y=np.max, z="count"), False, True, 2),
        (lambda df: df.aggregate(min), True, False, 0),
        (lambda df: df.max(), True, False, 0),
        (lambda df: df.max(skipna=False), True, False, 0),
        (lambda df: df.count(), True, False, 0),
        (lambda df: df.aggregate("size"), True, False, 0),
        (lambda df: df.aggregate(len), True, False, 0),
        (lambda df: df.agg({"x": "min", "y": "max"}), False, False, 1),
        (lambda df: df.agg({"x": "min"}, y="max"), False, False, 0),
    ],
)
@pytest.mark.parametrize(
    "data, dtype",
    [
        ([3.5, 1.2, 4.3, 2.0, 1.8], "float32"),
        ([3.5, native_pd.NA, 4.3, native_pd.NA, 1.8], "Float32"),
        ([2, native_pd.NA, native_pd.NA, native_pd.NA, 10], "Int64"),
        ([None, "c", None, "d"], "string"),
        (
            [
                native_pd.Timestamp(1513393355.5, unit="s"),
                native_pd.NaT,
                native_pd.NaT,
                native_pd.Timestamp(1513393355.5, unit="s"),
            ],
            "datetime64[ns]",
        ),
    ],
)
def test_agg_series(
    func, is_scalar, has_count_aggregate, data, dtype, expected_union_count
):
    native_series = native_pd.Series(data)
    native_series = native_series.astype(dtype)
    snow_series = pd.Series(native_series)

    with SqlCounter(query_count=1, union_count=expected_union_count):
        if is_scalar:
            eval_snowpark_pandas_result(
                snow_series, native_series, func, comparator=validate_scalar_result
            )
        else:
            eval_snowpark_pandas_result(snow_series, native_series, func)


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "numeric_only_kwargs",
    [
        param({}, id="no_numeric_only_kwarg"),
        param({"numeric_only": False}, id="numeric_only_False"),
        param({"numeric_only": None}, id="numeric_only_None"),
    ],
)
def test_string_sum_not_numeric_only(numeric_only_kwargs):
    eval_snowpark_pandas_result(
        *create_test_series(["a", "b", "c"]),
        lambda df: df.sum(**numeric_only_kwargs),
        comparator=validate_scalar_result,
        expect_exception=numeric_only_kwargs.get("numeric_only", False),
    )


@sql_count_checker(query_count=0)
@pytest.mark.parametrize(
    "method", ["sum", "var", "mean", "std", "min", "max", "median"]
)
def test_string_aggregation_numeric_only(method):
    # pandas.Series.sum does not support truthy numeric_only:
    # https://github.com/pandas-dev/pandas/blob/2e218d10984e9919f0296931d92ea851c6a6faf5/pandas/core/series.py#L4801-L4814
    eval_snowpark_pandas_result(
        *create_test_series(["a", "b", "c"]),
        lambda df: getattr(df, method)(numeric_only=True),
        expect_exception=True,
    )


@sql_count_checker(query_count=1)
def test_string_sum_with_nulls():
    """
    pandas raises TypeError('can only concatenate str (not \"int\") to str') but
    we instead drop the null values without an exception because checking for
    nulls requires at least one extra query. one possible solution is to do the
    sum and then compare the length of the result to the length of the df, but
    right now that requires an extra length query."""
    snow_series, pandas_series = create_test_series(["a", None, "b"])
    with pytest.raises(TypeError):
        pandas_series.sum()
    snow_result = snow_series.sum()
    validate_scalar_result(snow_result, "ab")


@pytest.mark.parametrize(
    "data, dtype",
    [
        ([3.5, 1.2, 4.3, 2.0, 1.8], "float"),  # no missing
        ([3.5, native_pd.NA, 4.3, native_pd.NA, 1.8], "Float64"),  # 2 missing
        ([2, native_pd.NA, native_pd.NA, native_pd.NA, 10], "Int64"),  # 3 missing
    ],
)
@pytest.mark.parametrize("min_count", [-1, 0, 1, 3, 4, 8, 20])
@sql_count_checker(query_count=1)
def test_series_sum_min_count(data, dtype, min_count):
    native_series = native_pd.Series(data)
    native_series = native_series.astype(dtype)
    snow_series = pd.Series(native_series)

    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda se: se.sum(min_count=min_count),
        comparator=validate_scalar_result,
    )


@pytest.mark.parametrize(
    "data, dtype",
    [
        ([3.5, 1.2, 4.3, 2.0, 1.8], "float32"),  # empty series
        ([3.5, native_pd.NA, 4.3, native_pd.NA, 1.8], "Float32"),
        ([2, native_pd.NA, native_pd.NA, native_pd.NA, 10], "Int64"),
        ([False, native_pd.NA, native_pd.NA, native_pd.NA, True], "boolean"),
        ([], "float"),
    ],
)
@pytest.mark.parametrize("skipna", [True, False])
@sql_count_checker(query_count=1)
def test_general_agg_numeric_series(skipna_agg_method, data, dtype, skipna):
    native_series = native_pd.Series(data)
    native_series = native_series.astype(dtype)
    snow_series = pd.Series(native_series)

    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda grp: getattr(grp, skipna_agg_method)(skipna=skipna),
        comparator=validate_scalar_result,
    )


@pytest.mark.parametrize(
    "func, is_scalar, expected_union_count",
    [
        (lambda df: df.aggregate(["min"]), False, 0),
        (lambda df: df.aggregate(["min", np.max]), False, 1),
        (lambda df: df.aggregate(min), True, 0),
        (lambda df: df.aggregate(y="min", z=np.max), False, 1),
        (lambda df: df.aggregate(x=min), False, 0),
        (lambda df: df.count(), True, 0),
    ],
)
def test_agg_on_empty_series(func, is_scalar, expected_union_count):
    native_empty_series = native_pd.Series([], dtype="float64")
    snow_series = pd.Series(native_empty_series)
    with SqlCounter(query_count=1, union_count=expected_union_count):
        if is_scalar:
            eval_snowpark_pandas_result(
                snow_series,
                native_empty_series,
                func,
                comparator=validate_scalar_result,
            )
        else:
            eval_snowpark_pandas_result(snow_series, native_empty_series, func)


@sql_count_checker(query_count=2)
def test_min_max_with_mixed_str_numeric_type():
    mixed_data, _ = MIXED_NUMERIC_STR_DATA_AND_TYPE
    native_series = native_pd.Series(mixed_data)
    snow_series = pd.Series(native_series)
    result_max = snow_series.max()
    # This behavior is different compare with native pandas, in native pandas
    # min/max comparison between string and numeric value is invalid. However,
    # it is valid with snowflake.
    assert result_max == "A"

    result_min = snow_series.min()
    assert result_min == 1


@pytest.mark.parametrize(
    "func, message, exception_type, same_as_pandas",
    [
        (
            lambda se: se.aggregate({"index": {"index": min}}),
            "nested renamer is not supported",
            SpecificationError,
            True,
        ),
        (
            lambda se: se.max(axis=1),
            "No axis named 1 for object type Series",
            ValueError,
            True,
        ),
        (
            lambda se: se.aggregate([min, max], axis=1),
            "No axis named 1 for object type Series",
            ValueError,
            True,
        ),
        (
            lambda se: se.aggregate([min, max], axis="columns"),
            "No axis named columns for object type Series",
            ValueError,
            True,
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_invalid_arg_raise(
    native_series, func, message, exception_type, same_as_pandas
):
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        func,
        expect_exception=True,
        expect_exception_type=exception_type,
        expect_exception_match=message,
        assert_exception_equal=same_as_pandas,
    )


@sql_count_checker(query_count=0)
def test_duplicate_agg_funcs_raise(native_series):
    snow_series = pd.Series(native_series)
    # native pandas raises an exception for duplicated aggregation function with dataframe,
    # but not for series. Snowpark pandas raise error for duplicated aggregation functions
    # in general for consistency.
    with pytest.raises(SpecificationError, match="Function names must be unique"):
        snow_series.aggregate([min, max, min])


@sql_count_checker(query_count=1, union_count=1)
def test_duplicate_named_agg_funcs_succeeds(native_series):
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda df: df.agg(x="min", y="min")
    )


@pytest.mark.parametrize("agg_func", ["sum", "std", "var", "mean", "median"])
@pytest.mark.parametrize(
    "data, data_type",
    [
        ARRAY_DATA_AND_TYPE,
        ARRAY_DATA_AND_TYPE,
        TIMESTAMP_DATA_AND_TYPE,
        MIXED_NUMERIC_STR_DATA_AND_TYPE,
    ],
)
@sql_count_checker(query_count=0)
def test_agg_invalid_dtype_raise(agg_func, data, data_type):
    snow_series = pd.Series(data)
    message = "Invalid argument types for function"
    # Sum on string and array type is valid in pandas, but becomes invalid with Snowflake
    # TODO (SNOW-899808): support sum with string and array type
    if data_type == "Variant":
        message = "Failed to cast variant value"
    elif agg_func == "median":
        message = "incompatible types"
    with pytest.raises(SnowparkSQLException, match=message):
        getattr(snow_series, agg_func)()


@pytest.mark.parametrize("agg_func", ["min", "max"])
@pytest.mark.parametrize(
    "data, data_type",
    [ARRAY_DATA_AND_TYPE, MAP_DATA_AND_TYPE],
)
@sql_count_checker(query_count=0)
def test_agg_invalid_min_max_dtype_raise(agg_func, data, data_type):
    snow_series = pd.Series(data)
    # min/max on array type is valid in pandas, but not valid with Snowflake.
    message = f"Function {agg_func.upper()} does not support {data_type.upper()} argument type"
    with pytest.raises(SnowparkSQLException, match=message):
        getattr(snow_series, agg_func)()


# skew is a little different, skipna is ignored in pandas.df.skew
@sql_count_checker(query_count=2)
def test_skew_series():
    native_df = native_pd.DataFrame(np.array([1, 2, 3]), columns=["A"])
    snow_df = pd.DataFrame(native_df)
    assert snow_df["A"].skew() == native_df["A"].skew()
    native_df = native_pd.DataFrame(np.array([1, 2, 1]), columns=["A"])
    snow_df = pd.DataFrame(native_df)
    assert round(snow_df["A"].skew(), 4) == round(native_df["A"].skew(), 4)


@sql_count_checker(query_count=0)
@pytest.mark.parametrize(
    "agg_kwargs",
    [{"x": ("y", "min")}, {"x": pd.NamedAgg("y", "min")}],
    ids=["2-tuple", "NamedAgg"],
)
def test_2_tuple_named_agg_errors_for_series(native_series, agg_kwargs):
    eval_snowpark_pandas_result(
        pd.Series(native_series),
        native_series,
        lambda series: series.agg(**agg_kwargs),
        expect_exception=True,
        expect_exception_match="nested renamer is not supported",
        expect_exception_type=SpecificationError,
        assert_exception_equal=True,
    )


class TestTimedelta:
    """Test aggregating a timedelta series."""

    @pytest.mark.parametrize(
        "func, union_count, is_scalar",
        [
            pytest.param(*v, id=str(i))
            for i, v in enumerate(
                [
                    (lambda series: series.aggregate(["min"]), 0, False),
                    (lambda series: series.aggregate({"A": "max"}), 0, False),
                    # this works since even though we need to do concats, all the results are non-timdelta.
                    (lambda df: df.aggregate(["all", "any", "count"]), 2, False),
                    # note following aggregation requires transpose
                    (lambda df: df.aggregate(max), 0, True),
                    (lambda df: df.min(), 0, True),
                    (lambda df: df.max(), 0, True),
                    (lambda df: df.count(), 0, True),
                    (lambda df: df.sum(), 0, True),
                    (lambda df: df.mean(), 0, True),
                    (lambda df: df.median(), 0, True),
                    (lambda df: df.std(), 0, True),
                    (lambda df: df.quantile(), 0, True),
                    (lambda df: df.quantile([0.01, 0.99]), 0, False),
                ]
            )
        ],
    )
    def test_supported(self, func, union_count, timedelta_native_df, is_scalar):
        with SqlCounter(query_count=1, union_count=union_count):
            eval_snowpark_pandas_result(
                *create_test_series(timedelta_native_df["A"]),
                func,
                comparator=validate_scalar_result
                if is_scalar
                else assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
                # Some calls to the native pandas function propagate attrs while some do not, depending on the values of its arguments.
                test_attrs=False,
            )

    @sql_count_checker(query_count=0)
    def test_var_invalid(self, timedelta_native_df):
        eval_snowpark_pandas_result(
            *create_test_series(timedelta_native_df["A"]),
            lambda series: series.var(),
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
    def test_unsupported_due_to_concat(self, timedelta_native_df):
        eval_snowpark_pandas_result(
            *create_test_series(timedelta_native_df["A"]),
            lambda df: df.agg(["count", "max"]),
        )
