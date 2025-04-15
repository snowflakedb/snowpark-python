#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
from typing import Any

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from modin.pandas import DataFrame
from modin.pandas.groupby import DataFrameGroupBy as SnowparkPandasDFGroupBy
from pandas import NA, NaT, Timestamp
from pandas.core.groupby.generic import DataFrameGroupBy as PandasDFGroupBy

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.types import IntegerType, StringType, VariantType
from tests.integ.modin.utils import (
    TEST_DF_DATA,
    ColumnSchema,
    assert_frame_equal,
    assert_snowpark_pandas_equal_to_pandas,
    assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_snow_df_with_table_and_data,
    create_test_dfs,
    eval_snowpark_pandas_result as _eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import Utils


def eval_groupby_result(
    snowpark_pandas_df: DataFrame,
    by: Any,
    dropna: bool = True,
    sort: bool = True,
    as_index: bool = True,
) -> tuple[SnowparkPandasDFGroupBy, PandasDFGroupBy]:
    """
    apply groupby on the snowpark pandas dataframe using the by, and performs checks against
    the groupby result on native pandas dataframe converted from the snowpark pandas dataframe.

    Returns:
        SnowparkPandasDFGroupBy: the snowpark pandas groupby result
        PandasDFGroupBy: the pandas groupby result
    """
    pandas_df = snowpark_pandas_df.to_pandas()

    snowpark_pandas_groupby = snowpark_pandas_df.groupby(
        by=by, sort=sort, dropna=dropna, as_index=as_index
    )
    pandas_groupby = pandas_df.groupby(
        by=by, sort=sort, dropna=dropna, as_index=as_index
    )

    return snowpark_pandas_groupby, pandas_groupby


def eval_snowpark_pandas_result(*args, **kwargs):
    # Some calls to the native pandas function propagate attrs while some do not, depending on the values of its arguments.
    return _eval_snowpark_pandas_result(*args, test_attrs=False, **kwargs)


@pytest.mark.parametrize("by", ["col1", ["col3"], ["col5"]])
@sql_count_checker(query_count=2)
def test_basic_single_group_row_groupby(
    result_compatible_agg_method, basic_snowpark_pandas_df, by
) -> None:
    snowpark_pandas_groupby, pandas_groupby = eval_groupby_result(
        basic_snowpark_pandas_df, by
    )
    eval_snowpark_pandas_result(
        snowpark_pandas_groupby,
        pandas_groupby,
        result_compatible_agg_method,
    )


@pytest.mark.parametrize(
    "by_col_data, expected_groupby_col",
    [
        (["g1", 1, 1, "g1", 3.2, 3.2], [1, 3.2, "g1"]),
        (
            [(1, 1), (1, 2), (1, 1), (2, 2), (2, 1), (1, 2)],
            [(1, 1), (1, 2), (2, 1), (2, 2)],
        ),
    ],
)
@sql_count_checker(query_count=4)
def test_single_group_row_groupby_with_variant(
    session,
    test_table_name,
    result_compatible_agg_method,
    by_col_data,
    expected_groupby_col,
) -> None:
    # this test uses dataframe created by directly reading from a snowflake table, because write_pandas
    # in python connector can not handle columns with mixed data type correctly (SNOW-841827).
    pandas_df = native_pd.DataFrame(
        {
            "COL_0": by_col_data,
            "COL_1": np.arange(6, dtype="float64"),
            "COL_2": [2, 3, 1, 5, 4, 10],
        }
    )
    column_schema = [
        ColumnSchema("COL_0", VariantType()),
        ColumnSchema("COL_1", IntegerType()),
        ColumnSchema("COL_2", IntegerType()),
    ]
    snowpark_pandas_df = create_snow_df_with_table_and_data(
        session, test_table_name, column_schema, pandas_df.values.tolist()
    )

    by = "COL_0"
    with SqlCounter(query_count=1):
        snowpark_pandas_groupby = snowpark_pandas_df.groupby(by=by)
        pandas_groupby = pandas_df.groupby(by=by)

        eval_snowpark_pandas_result(
            snowpark_pandas_groupby,
            pandas_groupby,
            result_compatible_agg_method,
        )


@sql_count_checker(query_count=9)
def test_groupby_agg_with_decimal_dtype(session, agg_method) -> None:
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(
        session, table_name, "COL_G string, COL_D decimal(38, 1)", is_temporary=True
    )
    session.sql(f"insert into {table_name} values ('A', 1)").collect()
    session.sql(f"insert into {table_name} values ('B', 2)").collect()
    session.sql(f"insert into {table_name} values ('A', 3)").collect()
    session.sql(f"insert into {table_name} values ('B', 5)").collect()

    snowpark_pandas_df = pd.read_snowflake(table_name)
    pandas_df = snowpark_pandas_df.to_pandas()

    by = "COL_G"
    with SqlCounter(query_count=1):
        snowpark_pandas_groupby = snowpark_pandas_df.groupby(by=by)
        pandas_groupby = pandas_df.groupby(by=by)
        eval_snowpark_pandas_result(snowpark_pandas_groupby, pandas_groupby, agg_method)


@sql_count_checker(query_count=9)
def test_groupby_agg_with_decimal_dtype_named_agg(session) -> None:
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(
        session, table_name, "COL_G string, COL_D decimal(38, 1)", is_temporary=True
    )
    session.sql(f"insert into {table_name} values ('A', 1)").collect()
    session.sql(f"insert into {table_name} values ('B', 2)").collect()
    session.sql(f"insert into {table_name} values ('A', 3)").collect()
    session.sql(f"insert into {table_name} values ('B', 5)").collect()

    snowpark_pandas_df = pd.read_snowflake(table_name)
    pandas_df = snowpark_pandas_df.to_pandas()

    by = "COL_G"
    with SqlCounter(query_count=1):
        snowpark_pandas_groupby = snowpark_pandas_df.groupby(by=by)
        pandas_groupby = pandas_df.groupby(by=by)
        eval_snowpark_pandas_result(
            snowpark_pandas_groupby,
            pandas_groupby,
            lambda gr: gr.agg(
                new_col=pd.NamedAgg("COL_D", "max"), new_col1=("COL_D", np.std)
            ),
        )


@sql_count_checker(query_count=2)
def test_groupby_agg_with_float_dtypes(agg_method) -> None:
    snowpark_pandas_df = pd.DataFrame(
        {
            "col1_grp": ["g1", "g2", "g0", "g0", "g2", "g3", "g0", "g2", "g3"],
            "col2_float16": np.arange(9, dtype="float16") // 3,
            "col3_float64": np.arange(9, dtype="float64") // 4,
            "col4_float32": np.arange(9, dtype="float32") // 5,
            "col5_mixed": np.concatenate(
                [
                    np.arange(3, dtype="int64"),
                    np.arange(3, dtype="float32"),
                    np.arange(3, dtype="float64"),
                ]
            ),
            "col6_float_identical": [3.0] * 9,
            "col7_float_missing": [
                3.0,
                2.0,
                np.nan,
                1.0,
                np.nan,
                4.0,
                np.nan,
                np.nan,
                7.0,
            ],
            "col8_mix_missing": np.concatenate(
                [
                    np.arange(2, dtype="int64"),
                    [np.nan, np.nan],
                    np.arange(2, dtype="float32"),
                    [np.nan],
                    np.arange(2, dtype="float64"),
                ]
            ),
        }
    )

    by = "col1_grp"
    snowpark_pandas_groupby, pandas_groupby = eval_groupby_result(
        snowpark_pandas_df, by
    )
    eval_snowpark_pandas_result(
        snowpark_pandas_groupby,
        pandas_groupby,
        agg_method,
    )


@sql_count_checker(query_count=2)
def test_groupby_agg_with_float_dtypes_named_agg() -> None:
    snowpark_pandas_df = pd.DataFrame(
        {
            "col1_grp": ["g1", "g2", "g0", "g0", "g2", "g3", "g0", "g2", "g3"],
            "col2_float16": np.arange(9, dtype="float16") // 3,
            "col3_float64": np.arange(9, dtype="float64") // 4,
            "col4_float32": np.arange(9, dtype="float32") // 5,
            "col5_mixed": np.concatenate(
                [
                    np.arange(3, dtype="int64"),
                    np.arange(3, dtype="float32"),
                    np.arange(3, dtype="float64"),
                ]
            ),
            "col6_float_identical": [3.0] * 9,
            "col7_float_missing": [
                3.0,
                2.0,
                np.nan,
                1.0,
                np.nan,
                4.0,
                np.nan,
                np.nan,
                7.0,
            ],
            "col8_mix_missing": np.concatenate(
                [
                    np.arange(2, dtype="int64"),
                    [np.nan, np.nan],
                    np.arange(2, dtype="float32"),
                    [np.nan],
                    np.arange(2, dtype="float64"),
                ]
            ),
        }
    )

    by = "col1_grp"
    snowpark_pandas_groupby, pandas_groupby = eval_groupby_result(
        snowpark_pandas_df, by
    )
    eval_snowpark_pandas_result(
        snowpark_pandas_groupby,
        pandas_groupby,
        lambda gr: gr.agg(
            new_col1=pd.NamedAgg("col2_float16", max),
            new_col2=("col3_float64", min),
            new_col3=("col4_float32", np.std),
            new_col4=("col5_mixed", max),
            new_col5=("col6_float_identical", np.std),
            new_col6=("col7_float_missing", max),
            new_col7=("col8_mix_missing", min),
        ),
    )


@pytest.mark.parametrize(
    "grpby_fn",
    [
        lambda gr: gr.quantile(),
        lambda gr: gr.quantile(q=0.3),
    ],
)
@sql_count_checker(query_count=1)
def test_groupby_agg_quantile_with_int_dtypes(grpby_fn) -> None:
    native_df = native_pd.DataFrame(
        {
            "col1_grp": ["g1", "g2", "g0", "g0", "g2", "g3", "g0", "g2", "g3"],
            "col2_int64": np.arange(9, dtype="int64") // 3,
            "col3_int_identical": [2] * 9,
            "col4_int32": np.arange(9, dtype="int32") // 4,
            "col5_int16": np.arange(9, dtype="int16") // 3,
            "col6_mixed": np.concatenate(
                [
                    np.arange(3, dtype="int64") // 3,
                    np.arange(3, dtype="int32") // 3,
                    np.arange(3, dtype="int16") // 3,
                ]
            ),
            "col7_int_missing": [5, 6, np.nan, 2, 1, np.nan, 5, np.nan, np.nan],
            "col8_mixed_missing": np.concatenate(
                [
                    np.arange(2, dtype="int64") // 3,
                    [np.nan],
                    np.arange(2, dtype="int32") // 3,
                    [np.nan],
                    np.arange(2, dtype="int16") // 3,
                    [np.nan],
                ]
            ),
        }
    )
    snowpark_pandas_df = pd.DataFrame(native_df)
    by = "col1_grp"
    snowpark_pandas_groupby = snowpark_pandas_df.groupby(by=by)
    pandas_groupby = native_df.groupby(by=by)
    eval_snowpark_pandas_result(
        snowpark_pandas_groupby,
        pandas_groupby,
        grpby_fn,
        comparator=assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64,
    )


@sql_count_checker(query_count=2)
def test_groupby_agg_with_int_dtypes(int_to_decimal_float_agg_method) -> None:
    snowpark_pandas_df = pd.DataFrame(
        {
            "col1_grp": ["g1", "g2", "g0", "g0", "g2", "g3", "g0", "g2", "g3"],
            "col2_int64": np.arange(9, dtype="int64") // 3,
            "col3_int_identical": [2] * 9,
            "col4_int32": np.arange(9, dtype="int32") // 4,
            "col5_int16": np.arange(9, dtype="int16") // 3,
            "col6_mixed": np.concatenate(
                [
                    np.arange(3, dtype="int64") // 3,
                    np.arange(3, dtype="int32") // 3,
                    np.arange(3, dtype="int16") // 3,
                ]
            ),
            "col7_bool": [True] * 5 + [False] * 4,
            "col8_bool_missing": [
                True,
                None,
                False,
                False,
                None,
                None,
                True,
                False,
                None,
            ],
            "col9_int_missing": [5, 6, np.nan, 2, 1, np.nan, 5, np.nan, np.nan],
            "col10_mixed_missing": np.concatenate(
                [
                    np.arange(2, dtype="int64") // 3,
                    [np.nan],
                    np.arange(2, dtype="int32") // 3,
                    [np.nan],
                    np.arange(2, dtype="int16") // 3,
                    [np.nan],
                ]
            ),
        }
    )

    # Snowflake boolean column is always the nullable, cast the col8_bool_missing column in native pandas
    # to nullable boolean dtype for result comparison
    native_df = snowpark_pandas_df.to_pandas().astype({"col8_bool_missing": "boolean"})
    by = "col1_grp"
    snowpark_pandas_groupby = snowpark_pandas_df.groupby(by=by)
    pandas_groupby = native_df.groupby(by=by)
    eval_snowpark_pandas_result(
        snowpark_pandas_groupby,
        pandas_groupby,
        int_to_decimal_float_agg_method,
        comparator=assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64,
    )


@pytest.mark.parametrize(
    "agg_func",
    [
        "max",
        "min",
        np.max,
        np.min,
        min,
        sum,
        np.std,
        "var",
        {"col2": "sum"},
        {"col2": [np.sum]},
        {"col2": [np.sum, max, "min"]},
        {"col2": "max", "col4": ["sum", np.max], "col5": min},
    ],
)
@pytest.mark.parametrize("as_index", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@sql_count_checker(query_count=2)
def test_single_group_row_groupby_agg(
    basic_snowpark_pandas_df, agg_func, as_index, sort
) -> None:
    eval_snowpark_pandas_result(
        basic_snowpark_pandas_df,
        basic_snowpark_pandas_df.to_pandas(),
        lambda df: df.groupby(by="col1", sort=sort, as_index=as_index).agg(agg_func),
    )


@pytest.mark.parametrize(
    "data",
    [
        {
            "key_col": [0, 1, 1, 2, 3],
            "string_col": ["a", "b", "c", "d", "e"],
            "float_col": [0.5, 1.5, 2.5, 3.5, 4.5],
        },
        {
            "key_col": [0, 1, 1, 2, 3],
            "string_col": ["a", "b", None, "d", "e"],
            "float_col": [0.5, 1.5, 2.5, 3.5, 4.5],
        },
    ],
)
@sql_count_checker(query_count=1)
def test_string_sum(data):
    eval_snowpark_pandas_result(
        *create_test_dfs(data),
        lambda df: df.groupby("key_col").sum(numeric_only=False),
    )


@sql_count_checker(query_count=1)
def test_string_sum_with_all_nulls_in_group_produces_empty_string():
    """
    pandas groupby.sum() gives 0 for groups where the aggregated column is all
    null. snowpark pandas returns an empty string, matching the behavior of
    snowflake LISTAGG(). Returning 0 requires an extra query to check for a
    group of all nulls and, if there is one, cast the intermediate aggregation
    results to VARIANT so that the final column can contain both 0 and strings.
    Instead just follow snowpark behavior.

    It's possible that the pandas behavior here is wrong and the sum should be
    null if all the string values are null. Ongoing discussion is here:
    https://github.com/pandas-dev/pandas/issues/53568#issuecomment-1904973950
    """
    snow_df, pandas_df = create_test_dfs({"key_col": [0, 1], "string_col": [None, "a"]})
    snow_result = snow_df.groupby("key_col").sum()
    pandas_result = pandas_df.groupby("key_col").sum()
    assert_frame_equal(
        pandas_result,
        native_pd.DataFrame(
            {"string_col": [0, "a"]}, index=native_pd.Index([0, 1], name="key_col")
        ),
    )
    assert_snowpark_pandas_equal_to_pandas(
        snow_result,
        native_pd.DataFrame(
            {"string_col": ["", "a"]}, index=native_pd.Index([0, 1], name="key_col")
        ),
    )


@sql_count_checker(query_count=1)
def test_string_sum_on_reversed_df():
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {
                "key_col": [0, 1, 1, 2, 3],
                "string_col": ["a", "b", "c", "d", "e"],
                "float_col": [0.5, 1.5, 2.5, 3.5, 4.5],
            },
        ),
        lambda df: df[::-1].groupby("key_col").sum(numeric_only=False),
    )


@pytest.mark.parametrize(
    "by", ["col1", ["col1", "col2", "col3"], ["col1", "col1", "col2"]]
)
@pytest.mark.parametrize("as_index", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@sql_count_checker(query_count=2)
def test_groupby_agg_on_groupby_columns(
    basic_snowpark_pandas_df, by, as_index, sort
) -> None:
    agg_func = {"col1": [min, max], "col2": "count"}
    native_pandas = basic_snowpark_pandas_df.to_pandas()
    eval_snowpark_pandas_result(
        basic_snowpark_pandas_df,
        native_pandas,
        lambda df: df.groupby(by=by, sort=sort, as_index=as_index).agg(agg_func),
    )


@pytest.mark.parametrize("as_index", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@sql_count_checker(query_count=1)
def test_groupby_agg_with_incorrect_func(as_index, sort) -> None:
    basic_snowpark_pandas_df = pd.DataFrame(
        data=8 * [range(3)], columns=["a", "b", "c"]
    )
    native_pandas = basic_snowpark_pandas_df.to_pandas()
    eval_snowpark_pandas_result(
        basic_snowpark_pandas_df,
        native_pandas,
        lambda df: df.groupby(by="a", sort=sort, as_index=as_index).agg(
            {"b": sum, "c": "COUNT"}
        ),
        expect_exception=True,
        expect_exception_match="'SeriesGroupBy' object has no attribute 'COUNT'",
    )


@pytest.mark.parametrize("as_index", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@sql_count_checker(query_count=1)
def test_groupby_agg_with_multiple_incorrect_funcs(as_index, sort) -> None:
    basic_snowpark_pandas_df = pd.DataFrame(
        data=8 * [range(3)], columns=["a", "b", "c"]
    )
    native_pandas = basic_snowpark_pandas_df.to_pandas()
    eval_snowpark_pandas_result(
        basic_snowpark_pandas_df,
        native_pandas,
        lambda df: df.groupby(by="a", sort=sort, as_index=as_index).agg(
            {"b": "COUNT", "c": "RANDOM_FUNC"}
        ),
        expect_exception=True,
        expect_exception_match="'SeriesGroupBy' object has no attribute 'COUNT'",
    )


@pytest.mark.parametrize(
    "by", ["col1", ["col1", "col2", "col3"], ["col1", "col1", "col2"]]
)
@pytest.mark.parametrize("as_index", [True, False])
@pytest.mark.parametrize("sort", [True, False])
def test_groupby_agg_on_groupby_columns_named_agg(
    basic_snowpark_pandas_df, by, as_index, sort
) -> None:
    query_count = 2
    kwargs = {}
    # https://github.com/pandas-dev/pandas/issues/58446
    # pandas (and Snowpark pandas) fail when duplicate columns are specified for
    # by and `as_index` is False and `pd.NamedAgg`s are
    # used for aggregation functions, but not when a dictionary
    # is passed in.
    if by == ["col1", "col1", "col2"] and not as_index:
        kwargs = {
            "expect_exception": True,
            "expect_exception_type": ValueError,
            "expect_exception_match": "cannot insert col1, already exists",
            "assert_exception_equal": True,
        }
        query_count = 1
    with SqlCounter(query_count=query_count):
        native_pandas = basic_snowpark_pandas_df.to_pandas()
        eval_snowpark_pandas_result(
            basic_snowpark_pandas_df,
            native_pandas,
            lambda df: df.groupby(by=by, sort=sort, as_index=as_index).agg(
                new_col=pd.NamedAgg("col2", sum),
                new_col1=("col4", sum),
                new_col3=("col3", "min"),
            ),
            **kwargs,
        )


@pytest.mark.parametrize(
    "agg_func",
    [
        ["max", "min"],
        ("max", "min"),
        ["min", "max", min],
    ],
)
@pytest.mark.parametrize("as_index", [True, False])
@sql_count_checker(query_count=2)
def test_single_group_row_groupby_agg_list(
    basic_snowpark_pandas_df, agg_func, as_index
) -> None:
    native_df = basic_snowpark_pandas_df.to_pandas()
    eval_snowpark_pandas_result(
        basic_snowpark_pandas_df,
        native_df,
        lambda df: df.groupby(by="col1", as_index=as_index).agg(agg_func),
    )


@pytest.mark.parametrize(
    "group_data",
    [
        ["A", "B", "A", "B"],
        ["A", np.nan, "A", np.nan],
        ["A", np.nan, "A", "B"],
        [np.nan, np.nan, np.nan, np.nan],
    ],
)
@pytest.mark.parametrize("dropna", [True, False])
@pytest.mark.parametrize("as_index", [True, False])
@sql_count_checker(query_count=1)
def test_groupby_dropna_single_index(group_data, dropna, as_index) -> None:
    pandas_df = native_pd.DataFrame(
        {"grp_col": group_data, "value": [123.23, 13.0, 12.3, 1.0]}
    )
    snow_df = pd.DataFrame(pandas_df)

    eval_snowpark_pandas_result(
        snow_df,
        pandas_df,
        lambda df: df.groupby(by="grp_col", dropna=dropna, as_index=as_index).max(),
    )


@pytest.mark.parametrize(
    "group_data",
    [
        ["A", "B", "A", "B"],
        ["A", np.nan, "A", np.nan],
        ["A", np.nan, "A", "B"],
        [np.nan, np.nan, np.nan, np.nan],
    ],
)
@pytest.mark.parametrize("dropna", [True, False])
@pytest.mark.parametrize("as_index", [True, False])
@sql_count_checker(query_count=1)
def test_groupby_dropna_single_index_named_agg(group_data, dropna, as_index) -> None:
    pandas_df = native_pd.DataFrame(
        {"grp_col": group_data, "value": [123.23, 13.0, 12.3, 1.0]}
    )
    snow_df = pd.DataFrame(pandas_df)

    eval_snowpark_pandas_result(
        snow_df,
        pandas_df,
        lambda df: df.groupby(by="grp_col", dropna=dropna, as_index=as_index).agg(
            new_col=("value", max)
        ),
    )


@pytest.mark.parametrize(
    "group_index, expected_index_dropna_false",
    [
        (
            pd.MultiIndex.from_tuples(
                [("foo", "one"), ("bar", "two"), ("foo", "one"), ("bar", "one")],
                name=["A", "B"],
            ),
            pd.MultiIndex.from_tuples(
                [("bar", "one"), ("bar", "two"), ("foo", "one")], name=["A", "B"]
            ),
        ),
        (
            pd.MultiIndex.from_tuples(
                [("foo", "one"), ("bar", "two"), ("foo", np.nan), ("bar", np.nan)],
                name=["A", "B"],
            ),
            pd.MultiIndex.from_tuples(
                [("bar", "two"), ("bar", np.nan), ("foo", "one"), ("foo", np.nan)],
                name=["A", "B"],
            ),
        ),
        (
            pd.MultiIndex.from_tuples(
                [("foo", np.nan), ("bar", "two"), ("foo", np.nan), (np.nan, "one")],
                name=["A", "B"],
            ),
            pd.MultiIndex.from_tuples(
                [("bar", "two"), ("foo", np.nan), (np.nan, "one")], name=["A", "B"]
            ),
        ),
        (
            pd.MultiIndex.from_tuples(
                [("foo", "one"), (np.nan, np.nan), ("foo", "one"), (np.nan, np.nan)],
                name=["A", "B"],
            ),
            pd.MultiIndex.from_tuples(
                [("foo", "one"), (np.nan, np.nan)], name=["A", "B"]
            ),
        ),
        (
            pd.MultiIndex.from_tuples(
                [
                    (np.nan, np.nan),
                    (np.nan, np.nan),
                    (np.nan, np.nan),
                    (np.nan, np.nan),
                ],
                name=["A", "B"],
            ),
            pd.MultiIndex.from_tuples([(np.nan, np.nan)], name=["A", "B"]),
        ),
    ],
)
@pytest.mark.parametrize("dropna", [True, False])
@sql_count_checker(query_count=1)
def test_groupby_dropna_multi_index(
    group_index, expected_index_dropna_false, dropna
) -> None:
    pandas_df = native_pd.DataFrame(
        {"value": [123.23, 13.0, 12.3, 1.0]}, index=group_index
    )
    snow_df = pd.DataFrame(pandas_df)

    snow_res = snow_df.groupby(by=["A", "B"], dropna=dropna).sum()
    pandas_res = pandas_df.groupby(by=["A", "B"], dropna=dropna).sum()
    if not dropna:
        # when dropna is false, we manually reset the expected multiindex for comparison. This is due to
        # https://github.com/pandas-dev/pandas/issues/29111. That bug is caused by the difference
        # of unique labels used at levels when np.nan is involved, where the native pandas groupby result uses np.nan
        # as a unique label when create the final multiindex, but the default pandas multiindex creation doesn't treat
        # np.nan as a unique label, but uses -1 to refer to np.nan, which is the mechanism used by Snowpark pandas.
        # However, these two representation yields the same index values. For example:
        # pd.MultiIndex(levels=[['bar', 'foo', np.nan], ['one', 'two', np.nan]], codes=[[0, 1, 2], [1, 2, 0]])
        # and pd.MultiIndex(levels=[['bar', 'foo'], ['one', 'two']], codes=[[0, 1, -1], [1, -1, 0]]) produces the same
        # multiindex result as following:
        # MultiIndex([('bar', 'two'),
        #             ('foo',   nan),
        #             (  nan, 'one')],)
        pandas_res.index = expected_index_dropna_false
    assert_frame_equal(snow_res, pandas_res)


@pytest.mark.parametrize("dropna", [True, False])
@sql_count_checker(query_count=2)
def test_groupby_with_dropna_random(agg_method, dropna: bool) -> None:
    snowpark_pandas_df = pd.DataFrame(TEST_DF_DATA["float_nan_data"])
    pandas_df = snowpark_pandas_df.to_pandas()

    by = ["col2"]

    snowpark_pandas_groupby = snowpark_pandas_df.groupby(by=by, dropna=dropna)
    pandas_groupby = pandas_df.groupby(by=by, dropna=dropna)

    eval_snowpark_pandas_result(
        snowpark_pandas_groupby,
        pandas_groupby,
        agg_method,
    )


@pytest.mark.parametrize("dropna", [True, False])
@sql_count_checker(query_count=2)
def test_groupby_with_dropna_random_named_agg(dropna: bool) -> None:
    snowpark_pandas_df = pd.DataFrame(TEST_DF_DATA["float_nan_data"])
    pandas_df = snowpark_pandas_df.to_pandas()

    by = ["col2"]

    agg_funcs = {}
    for i, c in enumerate(snowpark_pandas_df.columns):
        if c not in by:
            agg_funcs[f"new_col{i}"] = (c, np.std)

    snowpark_pandas_groupby = snowpark_pandas_df.groupby(by=by, dropna=dropna)
    pandas_groupby = pandas_df.groupby(by=by, dropna=dropna)

    eval_snowpark_pandas_result(
        snowpark_pandas_groupby,
        pandas_groupby,
        lambda gr: gr.agg(**agg_funcs),
    )


@pytest.mark.parametrize(
    "by", ["col1_str", "col2_int", "col3_float", ["col3_float", "col1_str"]]
)
@pytest.mark.parametrize("sort", [True, False])
@sql_count_checker(query_count=2)
def test_groupby_with_sort(by, sort) -> None:
    snowpark_pandas_df = pd.DataFrame(
        {
            "col1_str": ["g1", "g2", "g0", "g0", "g2", "g3", "g0", "g2"],
            "col2_int": [2, 6, 4, 9, 4, 2, 1, 6],
            "col3_float": [3.5, 1.42, 3.72, 2.0, 3.72, 1.42, 2.0, 3.5],
        }
    )

    snowpark_pandas_groupby, pandas_groupby = eval_groupby_result(
        snowpark_pandas_df, by=by, sort=sort
    )
    eval_snowpark_pandas_result(
        snowpark_pandas_groupby,
        pandas_groupby,
        lambda gr: gr.sum(numeric_only=True),
    )


@pytest.mark.parametrize(
    "by", ["col1", "col2", ["col5", "col1"], ["col5", "col2", "col1"]]
)
@sql_count_checker(query_count=2)
def test_groupby_with_index_columns(basic_snowpark_pandas_df, by) -> None:
    snow_df = basic_snowpark_pandas_df.set_index(["col1", "col5"])
    snowpark_pandas_groupby, pandas_groupby = eval_groupby_result(snow_df, by)
    eval_snowpark_pandas_result(
        snowpark_pandas_groupby,
        pandas_groupby,
        lambda gr: gr.sum(),
    )


@pytest.mark.parametrize("by", [["col1", "col5"], ["col5", "col1"], ["col1", "col1"]])
@sql_count_checker(query_count=2)
def test_multi_group_row_groupby(basic_snowpark_pandas_df, by) -> None:
    snowpark_pandas_groupby, pandas_groupby = eval_groupby_result(
        basic_snowpark_pandas_df, by
    )
    eval_snowpark_pandas_result(
        snowpark_pandas_groupby,
        pandas_groupby,
        lambda gr: gr.sum(),
    )


@pytest.mark.parametrize("by", [[None, "col1"], [None, None]])
@sql_count_checker(query_count=2)
def test_groupby_with_none_label(basic_snowpark_pandas_df, by) -> None:
    snow_df = basic_snowpark_pandas_df.set_index(["col1"])
    snow_df.columns = ["col2", None, "col4", "col5"]
    eval_snowpark_pandas_result(
        snow_df,
        snow_df.to_pandas(),
        lambda df: df.groupby(by=by).max(),
    )


@pytest.mark.parametrize(
    "grp_func", [lambda grp: grp.max(), lambda grp: grp.sum(), lambda grp: grp.mean()]
)
@sql_count_checker(query_count=2)
def test_groupby_with_series(basic_snowpark_pandas_df, grp_func):
    pandas_df = basic_snowpark_pandas_df.to_pandas()

    # verify Series from the Dataframe can be handled
    snowpark_pandas_groupby = basic_snowpark_pandas_df.groupby(
        by=basic_snowpark_pandas_df["col1"]
    )
    pandas_groupby = pandas_df.groupby(by=pandas_df["col1"])

    # evaluate the basic aggregation result
    eval_snowpark_pandas_result(
        snowpark_pandas_groupby,
        pandas_groupby,
        grp_func,
    )


@pytest.mark.parametrize("by", ["A", ["A", "B"]])
@pytest.mark.parametrize(
    "op", [lambda x: x.sum(numeric_only=False), lambda x: x.median(numeric_only=False)]
)
@sql_count_checker(query_count=2)
def test_groupby_multiple_columns(df_multi, by, op):
    snowpark_pandas_groupby, pandas_groupby = eval_groupby_result(df_multi, by)
    eval_snowpark_pandas_result(snowpark_pandas_groupby, pandas_groupby, op)


@pytest.mark.parametrize("numeric_only", [True, False])
@sql_count_checker(query_count=2)
def test_groupby_agg_of_booleans(result_compatible_agg_method, numeric_only):
    df = pd.DataFrame(
        {
            "groupby_col": [2] * 10 + [1] * 10,
            "bool": [True] * 5 + [False] * 5 + [True] * 5 + [False] * 5,
        }
    )
    snowpark_pandas_groupby, pandas_groupby = eval_groupby_result(df, "groupby_col")
    eval_snowpark_pandas_result(
        snowpark_pandas_groupby,
        pandas_groupby,
        result_compatible_agg_method,
    )


@pytest.mark.parametrize(
    "grp_func",
    [
        lambda grp: grp.max(numeric_only=True),
        lambda grp: grp.min(numeric_only=True),
        lambda grp: grp.count(),
        lambda grp: grp.sum(numeric_only=True),
        lambda grp: grp.mean(numeric_only=True),
        lambda grp: grp.std(numeric_only=True),
        lambda grp: grp.var(numeric_only=True),
    ],
)
@sql_count_checker(query_count=2)
def test_groupby_agg_invalid_numeric_columns(grp_func):
    df = pd.DataFrame(
        [
            ["foo", 1, True, {"a": 1, "b": 2}],
            ["bar", 3, False, {"c": 1, "d": 2}],
            ["foo", 2, False, {"c": 2, "e": 2}],
            ["bar", 4, True, {"d": 1, "b": 3}],
        ],
        columns=["g_col", "int_col", "bool_col", "map_col"],
    )

    snowpark_pandas_groupby, pandas_groupby = eval_groupby_result(df, "g_col")
    eval_snowpark_pandas_result(
        snowpark_pandas_groupby,
        pandas_groupby,
        grp_func,
    )


@pytest.mark.parametrize("numeric_only", [True, False])
@sql_count_checker(query_count=1)
def test_groupby_empty_multi_column(numeric_only):
    df = pd.DataFrame(data=[], columns=["A", "B", "C"])
    gb = df.groupby(["A", "B"])
    result = gb.sum(numeric_only=numeric_only)
    index = native_pd.MultiIndex([[], []], [[], []], names=["A", "B"])
    columns = ["C"] if not numeric_only else []
    expected = native_pd.DataFrame([], columns=columns, index=index)

    assert_frame_equal(result, expected, check_dtype=False)


@pytest.mark.parametrize("sort", [True, False])
@sql_count_checker(query_count=2)
def test_groupby_multi_column_no_agg_colums(sort):
    snow_df = pd.DataFrame(
        data=[["foo", "bar"], ["foo", "zoo"], ["bar", "baz"], ["bar", "baz"]],
        columns=["A", "B"],
    )
    eval_snowpark_pandas_result(
        snow_df, snow_df.to_pandas(), lambda df: df.groupby(["A", "B"], sort=sort).sum()
    )


@sql_count_checker(query_count=2)
def test_groupby_timestamp_nat():
    snow_df = pd.DataFrame(
        {
            "values": np.random.randn(8),
            "dt": [
                np.nan,
                Timestamp("2013-01-01"),
                NaT,
                Timestamp("2013-02-01"),
                np.nan,
                Timestamp("2013-02-01"),
                NaT,
                Timestamp("2013-01-01"),
            ],
        }
    )
    eval_snowpark_pandas_result(
        snow_df, snow_df.to_pandas(), lambda df: df.groupby("dt").median()
    )


@pytest.mark.parametrize(
    "grp_func",
    [
        lambda grp: grp.max(numeric_only=True),
        lambda grp: grp.min(numeric_only=True),
        lambda grp: grp.count(),
        lambda grp: grp.sum(numeric_only=True),
        lambda grp: grp.mean(numeric_only=True),
        lambda grp: grp.median(numeric_only=True),
        lambda grp: grp.std(numeric_only=True),
        lambda grp: grp.var(numeric_only=True),
    ],
)
@pytest.mark.parametrize("by", ["nan", "na", "nat", "none"])
@sql_count_checker(query_count=2)
def test_groupby_all_missing(grp_func, by):
    missing_df = pd.DataFrame(
        {
            "nan": [np.nan, np.nan, np.nan, np.nan],
            "na": [NA, NA, NA, NA],
            "nat": [NaT, NaT, NaT, NaT],
            "none": [None, None, None, None],
            "values": [1, 2, 3, 4],
        }
    )
    snowpark_pandas_groupby, pandas_groupby = eval_groupby_result(missing_df, by)
    eval_snowpark_pandas_result(
        snowpark_pandas_groupby,
        pandas_groupby,
        grp_func,
    )


@sql_count_checker(query_count=0)
def test_engine_kwargs_warning(basic_snowpark_pandas_df, caplog):
    engine_msg = (
        "The argument `engine` of `groupby_max` has been ignored by Snowpark pandas API"
    )
    engine_kwargs_msg = "The argument `engine_kwargs` of `groupby_max` has been ignored by Snowpark pandas API"
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        basic_snowpark_pandas_df.groupby("col1").max()
    assert engine_msg not in caplog.text
    assert engine_kwargs_msg not in caplog.text

    caplog.clear()
    with caplog.at_level(logging.WARNING):
        basic_snowpark_pandas_df.groupby("col1").max(
            engine="cython", engine_kwargs={"test": True}
        )
    assert engine_msg in caplog.text
    assert engine_kwargs_msg in caplog.text


@sql_count_checker(query_count=0)
def test_groupby_with_observed_warns(basic_snowpark_pandas_df, caplog):
    msg = "CategoricalDType is not yet supported with Snowpark pandas API, the observed parameter is ignored."
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        basic_snowpark_pandas_df.groupby("col1", observed=True).max()
    assert msg in caplog.text


@pytest.mark.parametrize(
    "level", [0, "B", [1, 1], [1, 0], ["A", "B"], [0, "A"], [-1, 0]]
)
@sql_count_checker(query_count=2)
def test_groupby_with_level(df_multi, level):
    df_multi_native = df_multi.to_pandas()
    eval_snowpark_pandas_result(
        df_multi,
        df_multi_native,
        lambda df: df.groupby(level=level).sum(),
    )


@sql_count_checker(query_count=1)
def test_groupby_with_hier_columns():
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
    snow_df = pd.DataFrame(data, index=index, columns=columns)
    native_df = native_pd.DataFrame(data, index=index, columns=columns)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(level=0).count(),
    )


@pytest.mark.parametrize(
    "by",
    [
        "C",  # single data column
        ["C", "D"],  # all data columns
        ["A"],  # single index column
        ["A", "B"],  # all index columns
        ["A", "D"],  # mix of index and data columns
        ["A", "B", "C", "D"],  # all columns
    ],
)
@sql_count_checker(query_count=2)
@pytest.mark.parametrize("as_index", [True, False])
@sql_count_checker(query_count=2)
def test_groupby_as_index(df_multi, by, as_index):
    native_df = df_multi.to_pandas()
    eval_snowpark_pandas_result(
        df_multi,
        native_df,
        lambda df: df.groupby(by=by, as_index=as_index).median(),
    )


@pytest.mark.parametrize("min_count", [-1, 0, 1, 2, 3, 40])
@sql_count_checker(query_count=1)
def test_groupby_min_count_methods_with_na(min_count_method, min_count):
    # create dataframe with 3 groups, and one column ts_na with groups contain missing value, and column ts with
    # no missing value
    native_df = native_pd.DataFrame(
        {
            "id": [2, 2, 2, 1, 3, 1],
            "ts_na": [4.0, np.nan, np.nan, 5.0, np.nan, np.nan],
            "ts": [3, 4, 8, 2, 0, 5],
        }
    )

    snow_df = pd.DataFrame(native_df)
    snowpark_pandas_groupby = snow_df.groupby("id")
    pandas_groupby = native_df.groupby("id")

    eval_snowpark_pandas_result(
        snowpark_pandas_groupby,
        pandas_groupby,
        lambda grp: getattr(grp, min_count_method)(min_count=min_count),
    )


@pytest.mark.parametrize("dtype", ["Int64", "Int32", "Float64", "Float32", "boolean"])
@pytest.mark.parametrize("min_count", [1, 2, 4, 50])
@sql_count_checker(query_count=1)
def test_groupby_min_count_methods_with_nullable_type(
    min_count_method, dtype, min_count
):
    if dtype == "boolean":
        # for boolean type the valid numeric value is 0 and 1, set the ts
        # value to 0 to ensure we got valid data for the column
        ts = 0
    else:
        ts = 4.0

    native_df = native_pd.DataFrame(
        {"id": [2, 2, 2, 1], "ts": [ts, pd.NA, ts + 1, pd.NA]}
    )
    native_df["ts"] = native_df["ts"].astype(dtype)
    snow_df = pd.DataFrame(native_df)

    snowpark_pandas_groupby = snow_df.groupby("id")
    pandas_groupby = native_df.groupby("id")
    eval_snowpark_pandas_result(
        snowpark_pandas_groupby,
        pandas_groupby,
        lambda grp: getattr(grp, min_count_method)(min_count=min_count),
    )


@sql_count_checker(query_count=4)
def test_groupby_agg_on_valid_variant_column(session, test_table_name):
    pandas_df = native_pd.DataFrame(
        {
            "COL_0": ["a", "b", "a", "a", "b", "c"],
            "COL_1": [2, 3, 1, 5, 4, 10],
            "COL_2": ["aa", "ac", "dc", "ee", "bb", "de"],
            "COL_3": [None, 3.2, None, None, 5.4, 7.0],
            "COL_MIX_NUMERIC": [5, None, 1.4, 5.2, 4, None],
        }
    )
    column_schema = [
        ColumnSchema("COL_0", StringType()),
        ColumnSchema("COL_1", VariantType()),
        ColumnSchema("COL_2", VariantType()),
        ColumnSchema("COL_3", VariantType()),
        ColumnSchema("COL_MIX_NUMERIC", VariantType()),
    ]
    snowpark_pandas_df = create_snow_df_with_table_and_data(
        session, test_table_name, column_schema, pandas_df.values.tolist()
    )

    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snowpark_pandas_df,
            pandas_df,
            lambda df: df.groupby(by="COL_0").agg(
                {
                    "COL_1": ["sum", "median"],
                    "COL_2": ["min", "max"],
                    "COL_3": "sum",
                    "COL_MIX_NUMERIC": ["sum", "min"],
                }
            ),
        )


@sql_count_checker(query_count=2)
def test_valid_func_valid_kwarg_should_work(basic_snowpark_pandas_df):
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        basic_snowpark_pandas_df.groupby("col1").agg(max, min_count=2),
        basic_snowpark_pandas_df.to_pandas().groupby("col1").max(min_count=2),
    )


@pytest.mark.parametrize("skipna", [True, False])
@pytest.mark.parametrize("op", ["first", "last"])
@sql_count_checker(query_count=1)
def test_groupby_agg_first_and_last(skipna, op):
    native_df = native_pd.DataFrame(
        {"grp_col": ["A", "A", "B", "B", "A"], "float_col": [np.nan, 2, 3, np.nan, 4]}
    )
    snow_df = pd.DataFrame(native_df)

    def comparator(snow_result, native_result):
        # When passing a list of aggregations, native pandas does not respect kwargs, so skipna
        # is always treated as the default value (true).
        # Massage the native results to match the expected behavior.
        if not skipna:
            if op == "first":
                assert native_result["float_col", "first"]["A"] == 2
                assert native_result["float_col", "first"]["B"] == 3
                native_result["float_col", "first"]["A"] = None
            else:
                assert native_result["float_col", "last"]["A"] == 4
                assert native_result["float_col", "last"]["B"] == 3
                native_result["float_col", "last"]["B"] = None
        assert_snowpark_pandas_equal_to_pandas(snow_result, native_result)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(by="grp_col").agg(
            {"float_col": ["quantile", op]}, skipna=skipna
        ),
        comparator=comparator,
    )


class TestTimedelta:
    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize(
        "method",
        [
            "count",
            "mean",
            "min",
            "max",
            "idxmax",
            "idxmin",
            "sum",
            "median",
            "std",
            "nunique",
        ],
    )
    @pytest.mark.parametrize("by", ["A", "B"])
    def test_aggregation_methods(self, method, by):
        eval_snowpark_pandas_result(
            *create_test_dfs(
                {
                    "A": native_pd.to_timedelta(
                        ["1 days 06:05:01.00003", "16us", "nan", "16us"]
                    ),
                    "B": [8, 8, 12, 10],
                }
            ),
            lambda df: getattr(df.groupby(by), method)(),
        )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize(
        "operation",
        [
            lambda df: df.groupby("A").agg({"B": ["sum", "median"], "C": "min"}),
            lambda df: df.groupby("B").agg({"A": ["sum", "median"], "C": "min"}),
            lambda df: df.groupby("B").agg({"A": ["sum", "count"], "C": "median"}),
            lambda df: df.groupby("B").agg(["mean", "std"]),
            lambda df: df.groupby("B").agg({"A": ["count", np.sum]}),
            lambda df: df.groupby("B").agg({"A": "sum"}),
        ],
    )
    def test_agg(self, operation):
        eval_snowpark_pandas_result(
            *create_test_dfs(
                native_pd.DataFrame(
                    {
                        "A": native_pd.to_timedelta(
                            ["1 days 06:05:01.00003", "16us", "nan", "16us"]
                        ),
                        "B": [8, 8, 12, 10],
                        "C": [True, False, False, True],
                    }
                )
            ),
            operation,
        )

    @sql_count_checker(query_count=1)
    def test_groupby_timedelta_var(self):
        """
        Test that we can group by a timedelta column and take var() of an integer column.

        Note that we can't take the groupby().var() of the timedelta column because
        var() is not defined for timedelta, in pandas or in Snowpark pandas.
        """
        eval_snowpark_pandas_result(
            *create_test_dfs(
                {
                    "A": native_pd.to_timedelta(
                        ["1 days 06:05:01.00003", "16us", "nan", "16us"]
                    ),
                    "B": [8, 8, 12, 10],
                }
            ),
            lambda df: df.groupby("A").var(),
        )
