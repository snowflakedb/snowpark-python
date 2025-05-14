#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re
from typing import Sequence

import modin.pandas as pd
import pandas as native_pd
import pytest
from modin.pandas import Series
from pandas.errors import SpecificationError

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import (
    ARRAY_DATA_AND_TYPE,
    MAP_DATA_AND_TYPE,
    MIXED_NUMERIC_STR_DATA_AND_TYPE,
    TIMESTAMP_DATA_AND_TYPE,
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize(
    "invalid_by",
    [
        ["col1"],
        None,
        [],
        "non_exist_by",
        ["col2", "non_exist_by"],
        ("col2", "col3"),
    ],
)
def test_invalid_by(invalid_by) -> None:
    pandas_df = native_pd.DataFrame(
        {
            "col1": [0, 1, 1, 0],
            "col2": [4, 5, 36, 7],
            "col3": [3, 8, 12, 10],
            "col4": [-1, 3, -1, 3],
        }
    )
    # rename the columns to have duplicated column names
    pandas_df.columns = ["col1", "col2", "col3", "col1"]
    snowpark_pandas_df = pd.DataFrame(pandas_df)

    with SqlCounter(query_count=0):
        eval_snowpark_pandas_result(
            snowpark_pandas_df,
            pandas_df,
            lambda df: df.groupby(by=invalid_by),
            expect_exception=True,
        )


@sql_count_checker(query_count=1)
def test_invalid_none_label():
    snowpark_pandas_df = pd.DataFrame(
        {
            "col1": [0, 1, 1, 0],
            "col2": [4, 5, 36, 7],
            "col3": [3, 8, 12, 10],
        }
    )
    # In snowpark pandas, we recognize the index with no name as None also, which
    # is different compare with pandas. In pandas, index with name None seems
    # not recognized as None, investigate with (SNOW-888586).
    snowpark_pandas_df = snowpark_pandas_df.set_index(["col1"])
    eval_snowpark_pandas_result(
        snowpark_pandas_df,
        snowpark_pandas_df.to_pandas(),
        lambda df: df.groupby(by=["col1", None]),
        expect_exception=True,
    )


@sql_count_checker(query_count=1)
def test_pandas_series_by(basic_snowpark_pandas_df):
    pandas_df = basic_snowpark_pandas_df.to_pandas()

    # create pandas series for groupby
    series_data = [0, 1, 0, 1]
    pandas_series = native_pd.Series(series_data)
    snowpark_pandas_series = Series(series_data)

    # verify TypeError is raised when native pandas series
    # is used as by columns, this is a snowflake specific error,
    # which is different compare with native pandas
    with pytest.raises(TypeError):
        basic_snowpark_pandas_df.groupby(by=pandas_series)
    with pytest.raises(TypeError):
        basic_snowpark_pandas_df.groupby(by=pandas_df["col1"])
    with pytest.raises(TypeError):
        basic_snowpark_pandas_df.groupby(by=[pandas_series, snowpark_pandas_series])


@pytest.mark.parametrize(
    "agg_func, func_name",
    [
        (lambda df: df.groupby("grp_col").sum(numeric_only=False), "sum"),
        (lambda df: df.groupby("grp_col").mean(numeric_only=False), "mean"),
        (lambda df: df.groupby("grp_col").median(numeric_only=False), "median"),
        (lambda df: df.groupby("grp_col").std(numeric_only=False), "stddev"),
        (lambda df: df.groupby("grp_col").var(numeric_only=False), "variance"),
        (
            lambda df: df.groupby("grp_col").std(numeric_only=False, ddof=0),
            "stddev_pop",
        ),
        (lambda df: df.groupby("grp_col").var(numeric_only=False, ddof=0), "var_pop"),
    ],
)
@pytest.mark.parametrize(
    "data, type_name",
    [
        TIMESTAMP_DATA_AND_TYPE,
        ARRAY_DATA_AND_TYPE,
        MAP_DATA_AND_TYPE,
        MIXED_NUMERIC_STR_DATA_AND_TYPE,
    ],
)
@sql_count_checker(query_count=0)
def test_groupby_numeric_only_func_invalid_non_numeric_column(
    agg_func, func_name, data, type_name
):
    df = pd.DataFrame({"grp_col": ["M", "M", "W", "W"], "val_col": data})

    # Sum array type is valid in pandas, but becomes invalid with Snowflake
    # TODO (SNOW-899808): support sum with string and array type
    message = "Invalid argument types for function"
    if type_name == "Variant":
        message = "Failed to cast variant value"
    elif func_name == "median":
        message = "incompatible types"

    # native pandas typically raise type error for invalid aggregation, Snowpark SQL exception
    # is raised with Snowpark pandas.
    with pytest.raises(SnowparkSQLException, match=message):
        # call to_pandas to trigger the evaluation of the operation
        agg_func(df).to_pandas()


@pytest.mark.parametrize(
    "agg_func, func_name",
    [
        (lambda df: df.groupby("grp_col").min(numeric_only=False), "min"),
        (lambda df: df.groupby("grp_col").max(numeric_only=False), "max"),
    ],
)
@sql_count_checker(query_count=0)
@pytest.mark.parametrize("data, type_name", [MAP_DATA_AND_TYPE, ARRAY_DATA_AND_TYPE])
@sql_count_checker(query_count=0)
def test_groupby_min_max_invalid_non_numeric_column(
    agg_func, func_name, data, type_name
):
    df = pd.DataFrame({"grp_col": ["M", "M", "W", "W"], "val_col": data})
    # min/max on array type is valid in pandas, but not valid with Snowflake.
    message = f"Function {func_name.upper()} does not support {type_name.upper()} argument type"
    with pytest.raises(SnowparkSQLException, match=message):
        agg_func(df).to_pandas()


@sql_count_checker(query_count=1, join_count=1)
def test_groupby_series_numeric_only_true(series_str):
    message = "SeriesGroupBy does not implement numeric_only"
    eval_snowpark_pandas_result(
        series_str,
        series_str.to_pandas(),
        lambda se: se.groupby(by="grp_col").max(numeric_only=True),
        expect_exception=True,
        expect_exception_match=message,
        assert_exception_equal=False,
    )


@sql_count_checker(query_count=1, join_count=1)
def test_groupby_as_index_raises(series_str):
    eval_snowpark_pandas_result(
        series_str,
        series_str.to_pandas(),
        lambda se: se.groupby(by="grp_col", as_index=False),
        expect_exception=True,
    )


@pytest.mark.parametrize("level", [[0, 0], [], ["level", 0]])
@sql_count_checker(query_count=0)
def test_groupby_singleindex_invalid_level(level):
    native_df = native_pd.DataFrame({"col1": [4, 5, 36, 7]}, index=[0, 1, 1, 0])
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(level=level),
        expect_exception=True,
    )


@pytest.mark.parametrize("level", [[0, 0], [], ["level", 0]])
@sql_count_checker(query_count=0)
def test_groupby_singleindex_invalid_level_name(level):
    native_df = native_pd.DataFrame({"col1": [4, 5, 36, 7]}, index=[0, 1, 1, 0])
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(level=level),
        expect_exception=True,
    )


@pytest.mark.parametrize("level", [10, [-6, 1], [0, 3], [], "level"])
@sql_count_checker(query_count=1)
def test_groupby_multiindex_invalid_level(df_multi, level):
    eval_snowpark_pandas_result(
        df_multi,
        df_multi.to_pandas(),
        lambda se: se.groupby(level=level),
        expect_exception=True,
    )


@pytest.mark.parametrize("level", [["level", 0], ["level", "A"]])
@sql_count_checker(query_count=1)
def test_groupby_multiindex_invalid_level_name_list(df_multi, level):
    # when there are invalid level name in a list, Snowpark pandas raises KeyError, but native pandas
    # raises AssertionError, we stay with KeyError which seems fits better in the context, and
    # consistent across different APIs.
    eval_snowpark_pandas_result(
        df_multi,
        df_multi.to_pandas(),
        lambda se: se.groupby(level=level),
        expect_exception=True,
        expect_exception_type=KeyError,
        expect_exception_match="Level level not found",
        assert_exception_equal=False,
    )


@sql_count_checker(query_count=1)
def test_groupby_as_index_false_axis_1_raises(df_multi):
    eval_snowpark_pandas_result(
        df_multi,
        df_multi.to_pandas(),
        lambda se: se.groupby(level=0, axis=1, as_index=False),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="as_index=False only valid for axis=0",
        assert_exception_equal=True,
    )


@sql_count_checker(query_count=1, join_count=1)
def test_groupby_series_agg_dict_like_input_raise(series_str):
    eval_snowpark_pandas_result(
        series_str,
        series_str.to_pandas(),
        lambda se: se.groupby(level=0).aggregate({"col2": max}),
        expect_exception=True,
        expect_exception_type=SpecificationError,
        expect_exception_match="nested renamer is not supported",
        assert_exception_equal=True,
    )


@sql_count_checker(query_count=1)
def test_groupby_agg_dict_like_input_nested_renamer_raises(basic_snowpark_pandas_df):
    eval_snowpark_pandas_result(
        basic_snowpark_pandas_df,
        basic_snowpark_pandas_df.to_pandas(),
        lambda df: df.groupby(by="col1").aggregate(
            {"col2": max, "col3": {"col3": "min"}}
        ),
        expect_exception=True,
        expect_exception_type=SpecificationError,
        expect_exception_match="Value for func argument with nested dict format is not allowed.",
        assert_exception_equal=False,
    )


@sql_count_checker(query_count=1)
def test_groupby_agg_dict_like_input_invalid_column_raises(basic_snowpark_pandas_df):
    eval_snowpark_pandas_result(
        basic_snowpark_pandas_df,
        basic_snowpark_pandas_df.to_pandas(),
        lambda df: df.groupby(by="col1").aggregate(
            {"col2": max, "col_invalid": ["min"]}
        ),
        expect_exception=True,
        expect_exception_type=KeyError,
        expect_exception_match=re.escape("Column(s) ['col_invalid'] do not exist"),
    )


@sql_count_checker(query_count=1)
def test_groupby_named_agg_like_input_invalid_column_raises(basic_snowpark_pandas_df):
    eval_snowpark_pandas_result(
        basic_snowpark_pandas_df,
        basic_snowpark_pandas_df.to_pandas(),
        lambda df: df.groupby(by="col1").aggregate(
            new_col=("col2", max), new_col1=("col_invalid", "min")
        ),
        expect_exception=True,
        expect_exception_type=KeyError,
        expect_exception_match=re.escape("Column(s) ['col_invalid'] do not exist"),
    )


@sql_count_checker(query_count=1)
def test_groupby_as_index_false_with_dup(basic_snowpark_pandas_df) -> None:
    by = ["col1", "col1"]
    eval_snowpark_pandas_result(
        basic_snowpark_pandas_df,
        basic_snowpark_pandas_df.to_pandas(),
        lambda df: df.groupby(by=by, as_index=False).max(),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match=re.escape("cannot insert col1, already exists"),
    )


# periods need to be of type integer - so this test raises a type error.
@sql_count_checker(query_count=0)
@pytest.mark.parametrize("periods", [-5.5, 5.5, "mxyzptlk", -2.0, 2.0])
def test_groupby_shift_non_integer_period_negative(periods):
    pandas_df = native_pd.DataFrame(
        data=[
            [1, 2, 3, "Lois", 42.42],
            [1, 5, 6, "Lana", 55.55],
            [2, 5, 8, "Luma", 76.76],
            [2, 6, 9, "Lyla", 90099.95],
            [3, 7, 10, "Cat", 888.88],
        ],
        columns=["LEXLUTHOR", "LOBO", "DARKSEID", "FRIENDS", "RATING"],
        index=["tuna", "salmon", "catfish", "goldfish", "shark"],
    )
    snow_df = pd.DataFrame(pandas_df)
    if isinstance(periods, Sequence):
        # Technically sequence of str isn't supported by pandas (sequence of int is),
        # but we only check if the argument is a sequence
        with pytest.raises(NotImplementedError):
            snow_df.groupby("LEXLUTHOR").shift(periods=periods)
    else:
        eval_snowpark_pandas_result(
            snow_df,
            pandas_df,
            lambda df: df.groupby("LEXLUTHOR").shift(periods=periods),
            expect_exception=True,
            expect_exception_type=TypeError,
            expect_exception_match="Periods must be integer",
        )


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("periods", [0, 1, 2, 3, -3, -2, -1])
@pytest.mark.parametrize("by", ["LEXLUTHOR"])
@pytest.mark.parametrize("freq", ["D"])
def test_groupby_shift_freq_negative(periods, by, freq):
    pandas_df = native_pd.DataFrame(
        data=[
            [1, 2, 3, "Lois", 42.42],
            [1, 5, 6, "Lana", 55.55],
            [2, 5, 8, "Luma", 76.76],
            [2, 6, 9, "Lyla", 90099.95],
            [3, 7, 10, "Cat", 888.88],
        ],
        columns=["LEXLUTHOR", "LOBO", "DARKSEID", "FRIENDS", "RATING"],
        index=["tuna", "salmon", "catfish", "goldfish", "shark"],
    )
    snow_df = pd.DataFrame(pandas_df)
    with pytest.raises(NotImplementedError):
        eval_snowpark_pandas_result(
            snow_df,
            pandas_df,
            lambda df: df.groupby(by).shift(periods=periods, freq=freq),
        )


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("periods", [0, 1, 2, 3, -3, -2, -1])
@pytest.mark.parametrize(
    "by",
    [
        ["LEXLUTHOR"],
    ],
)
@pytest.mark.parametrize(
    "fill_value",
    [
        4242,
    ],
)
def test_groupby_shift_with_fill_multiindex_negative(periods, by, fill_value):
    pandas_df = native_pd.DataFrame(
        data=[
            ["Lois", 42],
            ["Lois", 42],
            ["Lana", 76],
            ["Lana", 76],
            ["Lima", 888],
        ],
        columns=["LEXLUTHOR", "RATING"],
        index=pd.MultiIndex.from_tuples(
            [(1, "tuna"), (2, "salmon"), (3, "catfish"), (0, "goldfish"), (1, "shark")],
            names=["x", "y"],
        ),
    )
    snow_df = pd.DataFrame(pandas_df)
    with pytest.raises(NotImplementedError):
        eval_snowpark_pandas_result(
            snow_df,
            pandas_df,
            lambda df: df.groupby(by).shift(periods=periods, fill_value=fill_value),
        )


@sql_count_checker(query_count=0)
def test_groupby_shift_with_by_and_level_negative():
    pandas_df = native_pd.DataFrame(
        data=[
            ["Lois", 42],
            ["Lois", 42],
            ["Lana", 76],
            ["Lana", 76],
            ["Lima", 888],
        ],
        columns=["LEXLUTHOR", "RATING"],
        index=["tuna", "salmon", "catfish", "goldfish", "shark"],
    )
    snow_df = pd.DataFrame(pandas_df)
    with pytest.raises(NotImplementedError):
        eval_snowpark_pandas_result(
            snow_df,
            pandas_df,
            lambda df: df.groupby(by=["LEXLUTHOR"], level=0).shift(periods=1),
        )


@sql_count_checker(query_count=0)
def test_groupby_shift_with_external_by_negative():
    pandas_df = native_pd.DataFrame(
        data=[
            ["Lois", 42],
            ["Lois", 42],
            ["Lana", 76],
            ["Lana", 76],
            ["Lima", 888],
        ],
        columns=["LEXLUTHOR", "RATING"],
        index=["tuna", "salmon", "catfish", "goldfish", "shark"],
    )
    snow_df = pd.DataFrame(pandas_df)
    with pytest.raises(NotImplementedError):
        eval_snowpark_pandas_result(
            snow_df,
            pandas_df,
            lambda df: df.groupby(
                by=["Lois", "Lois", "Lana", "Lana", "Lima"], level=0
            ).shift(periods=1),
        )


@sql_count_checker(query_count=0)
@pytest.mark.parametrize(
    "by",
    [
        ["X"],
    ],
)
def test_groupby_shift_with_by_index_negative(by):
    pandas_df = native_pd.DataFrame(
        data=[
            ["Lois", 42],
            ["Lois", 42],
            ["Lana", 76],
            ["Lana", 76],
            ["Lima", 888],
        ],
        columns=["LEXLUTHOR", "RATING"],
        index=pd.MultiIndex.from_tuples(
            [(1, "tuna"), (2, "salmon"), (3, "catfish"), (0, "goldfish"), (1, "shark")],
            names=["X", "Y"],
        ),
    )
    snow_df = pd.DataFrame(pandas_df)
    with pytest.raises(NotImplementedError):
        eval_snowpark_pandas_result(
            snow_df,
            pandas_df,
            lambda df: df.groupby(by).shift(periods=1),
        )


@pytest.mark.parametrize(
    "params",
    [
        {"periods": [1, 2, 3]},  # sequence periods is unsupported
        {"periods": [1], "suffix": "_suffix"},  # suffix is unsupported
    ],
)
@sql_count_checker(query_count=0)
def test_groupby_shift_unsupported_args_negative(params):
    pandas_df = native_pd.DataFrame(
        data=[
            ["Lois", 42],
            ["Lois", 42],
            ["Lana", 76],
            ["Lana", 76],
            ["Lima", 888],
        ],
        columns=["LEXLUTHOR", "RATING"],
        index=["tuna", "salmon", "catfish", "goldfish", "shark"],
    )
    snow_df = pd.DataFrame(pandas_df)
    with pytest.raises(NotImplementedError):
        eval_snowpark_pandas_result(
            snow_df, pandas_df, lambda df: df.groupby("RATING").shift(**params)
        )


@pytest.mark.parametrize(
    "agg_method_name", ["min", "max", "std", "var", "sum", "mean", "median"]
)
@pytest.mark.parametrize("numeric_only", ["TEST", 5])
@sql_count_checker(query_count=0)
def test_groupby_agg_invalid_numeric_only(
    basic_snowpark_pandas_df, agg_method_name, numeric_only
):
    # This behavior is different compare with Native pandas. In Native pandas, if the value
    # given for numeric_only is a non-boolean value, no error is thrown, and numeric_only is
    # treated as True. This behavior is confusing to customer, in Snowpark pandas, we do an
    # explicit type check, an errors it out if an invalid value is given.
    with pytest.raises(
        ValueError,
        match=re.escape(
            f"GroupBy aggregations like 'sum' take a 'numeric_only' argument that needs to be a bool, but a {type(numeric_only).__name__} value was passed in."
        ),
    ):
        getattr(basic_snowpark_pandas_df.groupby("col1"), agg_method_name)(
            numeric_only=numeric_only
        )


@pytest.mark.parametrize("min_count", ["TEST", 5.8, 3.0])
@sql_count_checker(query_count=0)
def test_groupby_agg_invalid_min_count(
    basic_snowpark_pandas_df, min_count_method, min_count
):
    # This behavior is different compare with Native pandas. In Native pandas, if min_count value
    # is not an integer, a warning about numeric_only is thrown, and the result dataframe is a
    # groupby with no aggregation. This is really confusing, and fixed in pandas 2.x, where an
    # error message is thrown.
    # In Snowpark pandas, we do an explict check to throw an error message.
    with pytest.raises(ValueError, match=re.escape("min_count must be an integer")):
        getattr(basic_snowpark_pandas_df.groupby("col1"), min_count_method)(
            min_count=min_count
        )


@sql_count_checker(query_count=0)
def test_timedelta_var_invalid():
    eval_snowpark_pandas_result(
        *create_test_dfs(
            [["key0", pd.Timedelta(1)]],
        ),
        lambda df: df.groupby(0).var(),
        expect_exception=True,
        expect_exception_type=TypeError,
    )
