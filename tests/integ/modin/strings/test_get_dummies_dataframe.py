#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from tests.integ.modin.utils import assert_snowpark_pandas_equal_to_pandas
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.fixture(scope="function")
def simple_pandas_df():
    pandas_df = native_pd.DataFrame(
        {"COL_0": [1, 1, 3], "COL_1": ["MANAGER", "MINION", "EMPLOYEE"]}
    )

    return pandas_df


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("prefix", ["Brenan", "Is", "A", "Manager", "1"])
@pytest.mark.parametrize("prefix_sep", ["_", "/"])
def test_get_dummies_madeup(simple_pandas_df, prefix, prefix_sep):
    snow_df = pd.DataFrame(simple_pandas_df)

    pandas_get_dummies = native_pd.get_dummies(
        simple_pandas_df, columns=["COL_1"], prefix=prefix, prefix_sep=prefix_sep
    )

    snow_get_dummies = pd.get_dummies(
        snow_df, columns=["COL_1"], prefix=prefix, prefix_sep=prefix_sep
    )

    assert_snowpark_pandas_equal_to_pandas(
        snow_get_dummies, pandas_get_dummies, check_dtype=False
    )


@sql_count_checker(query_count=1)
def test_get_dummies_prefix_and_column_same(simple_pandas_df):
    snow_df = pd.DataFrame(simple_pandas_df)

    pandas_get_dummies = native_pd.get_dummies(
        simple_pandas_df, columns=["COL_1"], prefix="COL_1"
    )

    snow_get_dummies = pd.get_dummies(snow_df, columns=["COL_1"], prefix="COL_1")

    assert_snowpark_pandas_equal_to_pandas(
        snow_get_dummies, pandas_get_dummies, check_dtype=False
    )


@sql_count_checker(query_count=1)
def test_get_dummies_no_prefix_column(simple_pandas_df):
    snow_df = pd.DataFrame(simple_pandas_df)

    pandas_get_dummies = native_pd.get_dummies(simple_pandas_df)

    snow_get_dummies = pd.get_dummies(snow_df)

    assert_snowpark_pandas_equal_to_pandas(
        snow_get_dummies, pandas_get_dummies, check_dtype=False
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("prefix", ["2", "10", "A", "Manager", "1"])
@pytest.mark.parametrize("prefix_sep", ["_", "/"])
def test_get_dummies_with_numeric_column_names(prefix, prefix_sep):
    pandas_df = native_pd.DataFrame(
        {1: ["Brenan", "Bala", "John"], 10000: ["MANAGER", "MINION", "EMPLOYEE"]}
    )
    snow_df = pd.DataFrame(pandas_df)

    pandas_get_dummies = native_pd.get_dummies(
        pandas_df, columns=[1], prefix=prefix, prefix_sep=prefix_sep
    )

    snow_get_dummies = pd.get_dummies(
        snow_df, columns=[1], prefix=prefix, prefix_sep=prefix_sep
    )

    assert_snowpark_pandas_equal_to_pandas(
        snow_get_dummies, pandas_get_dummies, check_dtype=False
    )


@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.parametrize("prefix_sep", ["_", "/"])
def test_get_dummies_pandas(prefix_sep):

    pandas_df = native_pd.DataFrame(
        {"A": ["a", "b", "a"], "B": ["b", "a", "c"], "C": [1, 2, 3]}
    )

    snow_df = pd.DataFrame(pandas_df)

    pandas_get_dummies = native_pd.get_dummies(
        pandas_df, prefix=["col1", "col2"], prefix_sep=prefix_sep
    )

    snow_get_dummies = pd.get_dummies(
        snow_df, prefix=["col1", "col2"], prefix_sep=prefix_sep
    )

    assert_snowpark_pandas_equal_to_pandas(
        snow_get_dummies, pandas_get_dummies, check_dtype=False
    )


@pytest.mark.parametrize("sort_column", ["A", "C", "D"])
@sql_count_checker(query_count=1, join_count=1)
def test_get_dummies_pandas_no_row_pos_col(sort_column):
    data = {"A": ["a", "b", "a"], "B": ["b", "a", "c"], "C": [1, 2, 3]}
    if sort_column == "D":
        data["D"] = [1, 2, 3]
        pandas_df = native_pd.DataFrame(data).sort_values("D", ascending=False)[
            ["A", "B", "C"]
        ]
        snow_df = pd.DataFrame(data).sort_values("D", ascending=False)[["A", "B", "C"]]
    else:
        pandas_df = native_pd.DataFrame(data).sort_values(sort_column, ascending=False)
        snow_df = pd.DataFrame(data).sort_values(sort_column, ascending=False)

    assert (
        snow_df._query_compiler._modin_frame.row_position_snowflake_quoted_identifier
        is None
    )

    pandas_get_dummies = native_pd.get_dummies(
        pandas_df,
        prefix=["col1", "col2"],
        prefix_sep="/",
    )

    snow_get_dummies = pd.get_dummies(
        snow_df,
        prefix=["col1", "col2"],
        prefix_sep="/",
    )

    assert_snowpark_pandas_equal_to_pandas(
        snow_get_dummies, pandas_get_dummies, check_dtype=False
    )


@pytest.mark.parametrize("sort_column", ["A", "C"])
@sql_count_checker(query_count=1, join_count=1)
def test_get_dummies_pandas_no_row_pos_col_duplicate_values(sort_column):
    pandas_df = native_pd.DataFrame(
        {"A": ["a", "b", "a"], "B": ["b", "a", "c"], "C": [1, 2, 2]}
    ).sort_values(sort_column, ascending=False)

    snow_df = pd.DataFrame(
        {"A": ["a", "b", "a"], "B": ["b", "a", "c"], "C": [1, 2, 2]}
    ).sort_values(sort_column, ascending=False)
    assert (
        snow_df._query_compiler._modin_frame.row_position_snowflake_quoted_identifier
        is None
    )

    pandas_get_dummies = native_pd.get_dummies(
        pandas_df,
        prefix=["col1", "col2"],
        prefix_sep="/",
    )

    snow_get_dummies = pd.get_dummies(
        snow_df,
        prefix=["col1", "col2"],
        prefix_sep="/",
    )

    assert_snowpark_pandas_equal_to_pandas(
        snow_get_dummies, pandas_get_dummies, check_dtype=False
    )


@sql_count_checker(query_count=1, join_count=2)
def test_get_dummies_multiple_columns():
    pandas_df = native_pd.DataFrame(
        {
            "A": ["a", "b", "a"],
            "B": ["b", "a", "c"],
            "C": [1, 2, 2],
            "D": ["e", "a", "a"],
        },
        index=native_pd.Index(["e", "f", "g"]),
    )
    snow_df = pd.DataFrame(pandas_df)

    snow_get_dummies = pd.get_dummies(
        snow_df,
        columns=["A", "B", "D"],
        prefix=["colA", "colB", "colD"],
        prefix_sep="_",
    )

    pandas_get_dummies = native_pd.get_dummies(
        pandas_df,
        columns=["A", "B", "D"],
        prefix=["colA", "colB", "colD"],
        prefix_sep="_",
    )
    assert_snowpark_pandas_equal_to_pandas(
        snow_get_dummies, pandas_get_dummies, check_dtype=False
    )


# https://snowflakecomputing.atlassian.net/browse/SNOW-1050112
# Customer issue: Calling get_dummies on the result of
# pd.read_snowflake directly results in a ValueError.
@sql_count_checker(query_count=3, join_count=2)
def test_get_dummies_pandas_after_read_snowflake(session):
    pandas_df = native_pd.DataFrame(
        {
            "A": ["a", "b", "a"],
            "B": ["b", "a", "c"],
            "C": ["e", "e", "a"],
            "D": [1, 2, 3],
        }
    )
    snowpark_df = session.create_dataframe(pandas_df)
    table_name = random_name_for_temp_object(TempObjectType.TABLE)
    snowpark_df.write.save_as_table(table_name, table_type="temp")
    snow_df = pd.read_snowflake(table_name)

    assert (
        snow_df._query_compiler._modin_frame.index_column_snowflake_quoted_identifiers
        == ['"__row_position__"']
        and "__index__"
        not in snow_df._query_compiler._modin_frame.ordered_dataframe.projected_column_snowflake_quoted_identifiers
    )

    pandas_get_dummies = native_pd.get_dummies(
        pandas_df,
        prefix=["col1", "col2", "col3"],
        prefix_sep="/",
    )

    snow_get_dummies = pd.get_dummies(
        snow_df,
        prefix=["col1", "col2", "col3"],
        prefix_sep="/",
    )

    assert_snowpark_pandas_equal_to_pandas(
        snow_get_dummies, pandas_get_dummies, check_dtype=False
    )


@sql_count_checker(query_count=0)
def test_get_dummies_pandas_negative():

    pandas_df = native_pd.DataFrame(
        {"A": ["a", "b", "a"], "B": ["b", "a", "c"], "C": [1, 2, 3]}
    )

    snow_df = pd.DataFrame(pandas_df)

    with pytest.raises(NotImplementedError):
        pd.get_dummies(
            snow_df,
            prefix=["col1", "col2"],
            dummy_na=True,
            drop_first=True,
            dtype=np.int32,
        )


@sql_count_checker(query_count=0)
def test_get_dummies_pandas_negative_duplicated_columns():
    pandas_df = native_pd.DataFrame(
        {"A": ["a", "b", "a"], "B": ["b", "a", "c"], "C": [1, 2, 3]}
    )
    pandas_df.columns = ["A", "A", "C"]
    snow_df = pd.DataFrame(pandas_df)
    with pytest.raises(NotImplementedError):
        pd.get_dummies(
            snow_df,
            columns=["A"],
            prefix=["col1", "col2"],
        )
