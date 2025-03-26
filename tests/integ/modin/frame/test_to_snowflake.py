#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import re

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import VALID_PANDAS_LABELS, VALID_SNOWFLAKE_COLUMN_NAMES
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize("index", [True, False])
@pytest.mark.parametrize("index_labels", [None, ["my_index"]])
# one extra query to convert index to native pandas when creating the snowpark pandas dataframe
@sql_count_checker(query_count=3)
def test_to_snowflake_index(test_table_name, index, index_labels):
    df = pd.DataFrame(
        {"a": [1, 2, 3], "b": [4, 5, 6]}, index=pd.Index([2, 3, 4], name="index")
    )

    df.to_snowflake(
        test_table_name, if_exists="replace", index=index, index_label=index_labels
    )
    expected_columns = []
    if index:
        # if index is retained in the result, add it as the first expected column
        expected_index = ["index"]
        if index_labels:
            expected_index = index_labels
        expected_columns = expected_columns + expected_index
    # add the expected data columns
    expected_columns = expected_columns + ["a", "b"]
    verify_columns(test_table_name, expected_columns)


@sql_count_checker(query_count=2)
def test_to_snowflake_multiindex(test_table_name):
    index = native_pd.MultiIndex.from_arrays(
        [[1, 1, 2, 2], ["red", "blue", "red", "blue"]], names=("number", "color")
    )
    native_df = native_pd.DataFrame(
        [[1] * 2, [2] * 2, [3] * 2, [4] * 2], index=index, columns=["a", "b"]
    )
    snow_df = pd.DataFrame(native_df)
    snow_df.to_snowflake(test_table_name, if_exists="replace", index=True)
    verify_columns(test_table_name, ["number", "color", "a", "b"])

    with pytest.raises(
        ValueError, match="Length of 'index_label' should match number of levels"
    ):
        snow_df.to_snowflake(
            test_table_name, if_exists="replace", index=True, index_label=["a"]
        )


@pytest.mark.parametrize(
    "native_df, message",
    [
        (
            native_pd.DataFrame(
                [[1, 2], [4, 5]],
                index=native_pd.Index([4, 5], name="b"),
                columns=["a", "a"],
            ),
            re.escape(
                "Duplicated labels ['a'] found in index columns ['b'] and data columns ['a', 'a']."
            ),
        ),
        (
            native_pd.DataFrame(
                [1, 2, 3], index=native_pd.Index([4, 5, 6], name="a"), columns=["a"]
            ),
            re.escape(
                "Duplicated labels ['a'] found in index columns ['a'] and data columns ['a']."
            ),
        ),
    ],
)
@sql_count_checker(query_count=1)
def test_to_snowflake_index_duplicate_column_name_negative(
    test_table_name, native_df, message
):
    df = pd.DataFrame(native_df)
    with pytest.raises(ValueError, match=message):
        df.to_snowflake(test_table_name)


def test_to_snowflake_if_exists(session, test_table_name):
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    # Verify new table is created
    with SqlCounter(query_count=3):
        df.to_snowflake(test_table_name, if_exists="fail", index=False)
        verify_columns(test_table_name, ["a", "b"])

    # Verify attempt to write to existing table fails
    with SqlCounter(query_count=1):
        with pytest.raises(ValueError):
            df.to_snowflake(test_table_name, if_exists="fail", index=False)

    # Verify by default attempt to write to existing table fails
    with SqlCounter(query_count=1):
        with pytest.raises(ValueError):
            df.to_snowflake(test_table_name, index=False)

    # Verify existing table is replaced with new data
    df = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]})
    with SqlCounter(query_count=3):
        df.to_snowflake(test_table_name, if_exists="replace", index=False)
        verify_columns(test_table_name, ["a", "c"])
        verify_num_rows(session, test_table_name, 3)

    # Verify data is appended to existing table
    with SqlCounter(query_count=4):
        df.to_snowflake(test_table_name, if_exists="append", index=False)
        verify_columns(test_table_name, ["a", "c"])
        verify_num_rows(session, test_table_name, 6)

    # Verify pd.to_snowflake operates the same
    with SqlCounter(query_count=4):
        pd.to_snowflake(df, test_table_name, if_exists="append", index=False)
        verify_columns(test_table_name, ["a", "c"])
        verify_num_rows(session, test_table_name, 9)

    # Verify invalid 'if_exists' input.
    with SqlCounter(query_count=0):
        with pytest.raises(ValueError):
            df.to_snowflake(test_table_name, if_exists="abc")


@pytest.mark.parametrize("index_label", VALID_PANDAS_LABELS)
@sql_count_checker(query_count=2)
def test_to_snowflake_index_labels(index_label, test_table_name):
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    df.to_snowflake(
        test_table_name, if_exists="replace", index=True, index_label=index_label
    )
    verify_columns(test_table_name, [str(index_label), "a", "b"])


@pytest.mark.parametrize("col_name", VALID_PANDAS_LABELS)
@sql_count_checker(query_count=2)
def test_to_snowflake_column_names_from_panadas(col_name, test_table_name):
    df = pd.DataFrame({col_name: [1, 2, 3], "b": [4, 5, 6]})
    df.to_snowflake(test_table_name, if_exists="replace", index=False)
    verify_columns(test_table_name, [str(col_name), "b"])


@pytest.mark.parametrize("col_name", VALID_SNOWFLAKE_COLUMN_NAMES)
@pytest.mark.parametrize("if_exists", ["append", "replace"])
def test_column_names_with_read_snowflake_and_to_snowflake(
    col_name, if_exists, session
):
    with SqlCounter(query_count=7 if if_exists == "append" else 6):
        # Create a table
        session.sql(f"create or replace table t1 ({col_name} int)").collect()
        session.sql("insert into t1 values (1), (2), (3)").collect()
        data = session.sql(f"select {col_name} from t1").collect()
        assert len(data) == 3
        # Capture column names of origing table.
        expected_columns = session.table("t1").columns

        df = pd.read_snowflake("t1")
        df.to_snowflake("t1", if_exists=if_exists, index=False)
        # Verify column names are not updated here.
        assert expected_columns == session.table("t1").columns
        data = session.sql(f"select {col_name} from t1").collect()
        assert len(data) == (6 if if_exists == "append" else 3)


@sql_count_checker(query_count=2)
def test_to_snowflake_column_with_quotes(session, test_table_name):
    df = pd.DataFrame({'a"b': [1, 2, 3], 'a""b': [4, 5, 6]})
    df.to_snowflake(test_table_name, if_exists="replace", index=False)
    verify_columns(test_table_name, ['a"b', 'a""b'])


# one extra query to convert index to native pandas when creating the snowpark pandas dataframe
def test_to_snowflake_index_label_none(test_table_name):
    # no index
    with SqlCounter(query_count=2):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        df.to_snowflake(test_table_name, if_exists="replace")
        verify_columns(test_table_name, ["index", "a", "b"])

    # named index
    with SqlCounter(query_count=3):
        df = pd.DataFrame(
            {"a": [1, 2, 3], "b": [4, 5, 6]}, index=pd.Index([2, 3, 4], name="index")
        )
        df.to_snowflake(test_table_name, if_exists="replace", index_label=[None])
        verify_columns(test_table_name, ["index", "a", "b"])

    # nameless index
    with SqlCounter(query_count=3):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=pd.Index([2, 3, 4]))
        df.to_snowflake(test_table_name, if_exists="replace", index_label=[None])
        verify_columns(test_table_name, ["index", "a", "b"])


# one extra query to convert index to native pandas when creating the snowpark pandas dataframe
@sql_count_checker(query_count=6)
def test_to_snowflake_index_label_none_data_column_conflict(test_table_name):
    df = pd.DataFrame({"index": [1, 2, 3], "a": [4, 5, 6]})
    df.to_snowflake(test_table_name, if_exists="replace")
    # If the column name "index" is taken by one of the data columns,
    # then "level_0" is used instead for naming the index column.
    # This is based on the behavior of reset_index.
    verify_columns(test_table_name, ["level_0", "index", "a"])

    df = pd.DataFrame(
        {"index": [1, 2, 3], "a": [4, 5, 6]}, index=pd.Index([2, 3, 4], name="index")
    )
    # If the index already has a name, "index", then "level_0" is not used,
    # and a ValueError is raised instead.
    # This is based on the behavior of reset_index.
    with pytest.raises(ValueError):
        df.to_snowflake(test_table_name, if_exists="replace", index_label=[None])

    # nameless index
    df = pd.DataFrame({"index": [1, 2, 3], "a": [4, 5, 6]}, index=pd.Index([2, 3, 4]))
    df.to_snowflake(test_table_name, if_exists="replace", index_label=[None])
    verify_columns(test_table_name, ["level_0", "index", "a"])


# one extra query to convert index to native pandas when creating the snowpark pandas dataframe
@sql_count_checker(query_count=1)
def test_to_snowflake_data_label_none_raises(test_table_name):
    df = pd.DataFrame(
        {"a": [1, 2, 3], "b": [4, 5, 6]}, index=pd.Index([2, 3, 4], name="index")
    )
    df.columns = ["c", None]

    message = re.escape(
        "Label None is found in the data columns ['c', None], which is invalid in Snowflake."
    )
    with pytest.raises(ValueError, match=message):
        df.to_snowflake(test_table_name, if_exists="replace")


@sql_count_checker(query_count=2)
def test_to_snowflake_with_dropped_row_position():
    snow_df = pd.DataFrame({"a": [1, 2, 3], "b": [2, 4, 5]})
    snow_df = snow_df.groupby("a").count().reset_index()
    snow_df.to_snowflake("out", index=False, table_type="temporary")


def verify_columns(table_name: str, expected: list[str]) -> None:
    actual = pd.read_snowflake(table_name).columns
    assert actual.tolist() == expected


def verify_num_rows(session, table_name: str, expected: int) -> None:
    actual = session.table(table_name).count()
    assert actual == expected


@sql_count_checker(query_count=2)
def test_timedelta_to_snowflake_with_read_snowflake(test_table_name, caplog):
    with caplog.at_level(logging.WARNING):
        df = pd.DataFrame(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
                "t": native_pd.timedelta_range("1 days", periods=3),
            }
        )
        df.to_snowflake(test_table_name, index=False, if_exists="replace")
        df = pd.read_snowflake(test_table_name)
        assert df.dtypes[-1] == "int64"
        assert "`TimedeltaType` may be lost in `to_snowflake`'s result" in caplog.text
