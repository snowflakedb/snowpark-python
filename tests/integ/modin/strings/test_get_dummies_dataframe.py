#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import re

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

    assert_snowpark_pandas_equal_to_pandas(snow_get_dummies, pandas_get_dummies)


@sql_count_checker(query_count=1)
def test_get_dummies_prefix_and_column_same(simple_pandas_df):
    snow_df = pd.DataFrame(simple_pandas_df)

    pandas_get_dummies = native_pd.get_dummies(
        simple_pandas_df, columns=["COL_1"], prefix="COL_1"
    )

    snow_get_dummies = pd.get_dummies(snow_df, columns=["COL_1"], prefix="COL_1")

    assert_snowpark_pandas_equal_to_pandas(snow_get_dummies, pandas_get_dummies)


@sql_count_checker(query_count=1)
def test_get_dummies_no_prefix_column(simple_pandas_df):
    snow_df = pd.DataFrame(simple_pandas_df)

    pandas_get_dummies = native_pd.get_dummies(simple_pandas_df)

    snow_get_dummies = pd.get_dummies(snow_df)

    assert_snowpark_pandas_equal_to_pandas(snow_get_dummies, pandas_get_dummies)


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

    assert_snowpark_pandas_equal_to_pandas(snow_get_dummies, pandas_get_dummies)


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

    assert_snowpark_pandas_equal_to_pandas(snow_get_dummies, pandas_get_dummies)


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

    assert_snowpark_pandas_equal_to_pandas(snow_get_dummies, pandas_get_dummies)


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

    assert_snowpark_pandas_equal_to_pandas(snow_get_dummies, pandas_get_dummies)


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
    assert_snowpark_pandas_equal_to_pandas(snow_get_dummies, pandas_get_dummies)


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
    # Follow read_snowflake with a sort operation to ensure that ordering is stable and tests are not flaky.
    snow_df = snow_df.sort_values(snow_df.columns.to_list())
    pandas_df = pandas_df.sort_values(pandas_df.columns.to_list())

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

    # The column D is of type int8 in snowpark and int64 in pandas.
    pandas_get_dummies["D"] = pandas_get_dummies["D"].astype(np.int8)
    assert_snowpark_pandas_equal_to_pandas(snow_get_dummies, pandas_get_dummies)


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


@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.parametrize("kwargs", [{"dummy_na": False}, {}])
@pytest.mark.parametrize(
    "data", [["a", "a", None, "c"], ["a", "a", "c", "c"], ["a", "NULL"], [None, "NULL"]]
)
def test_get_dummies_null_values(kwargs, data):
    df = native_pd.DataFrame({"col1": data, "col2": data})
    expected = native_pd.get_dummies(df, **kwargs)
    actual = pd.get_dummies(pd.DataFrame(df), **kwargs)
    assert_snowpark_pandas_equal_to_pandas(actual, expected)


@sql_count_checker(query_count=1)
def test_get_dummies_with_duplicate_column_names():
    # Bug fix for SNOW-1945131: get_dummies fails when pivot column names are duplicated
    # due to same value being present in both the columns.
    native_df = native_pd.DataFrame(
        {"col1": ["a", "b", "b"], "col2": ["a", "b", None], "col3": ["b", None, None]}
    )
    snow_df = pd.DataFrame(native_df)
    for col in native_df.columns:
        snow_df = pd.get_dummies(snow_df, columns=[col])
        native_df = native_pd.get_dummies(native_df, columns=[col])
    assert_snowpark_pandas_equal_to_pandas(snow_df, native_df)


@sql_count_checker(query_count=1)
def test_get_dummies_exclude_columns():
    native_df = native_pd.DataFrame({"col1": ["a", "b", "b"], "col2": ["a", "b", None]})
    snow_df = pd.DataFrame(native_df)
    snow_df = pd.get_dummies(snow_df, columns=["col2"])
    native_df = native_pd.get_dummies(native_df, columns=["col2"])
    assert_snowpark_pandas_equal_to_pandas(snow_df, native_df)


@sql_count_checker(query_count=0)
@pytest.mark.parametrize(
    "columns, prefix",
    [
        (["A", "B"], ["p1"]),
        (["A", "B"], ["p1", "p1", "p3"]),
        (["A", "B"], []),
        (None, []),
        (None, ["p1"]),
    ],
)
def test_get_dummies_prefix_length_mismatch_negative(columns, prefix):
    native_df = native_pd.DataFrame({"A": ["a", "b", "b"], "B": ["a", "b", None]})
    col_len = 2 if columns is None else len(columns)
    error_msg = re.escape(
        f"Length of 'prefix' ({len(prefix)}) did not match the length of the columns being encoded ({col_len})."
    )
    with pytest.raises(ValueError, match=error_msg):
        native_pd.get_dummies(native_df, columns=columns, prefix=prefix)

    snow_df = pd.DataFrame(native_df)
    with pytest.raises(ValueError, match=error_msg):
        pd.get_dummies(snow_df, columns=columns, prefix=prefix)
