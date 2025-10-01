#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
from random import Random
import re
import string

import modin.pandas as pd
import pandas as native_pd
import pytest
from modin.config import context as config_context

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    VALID_PANDAS_LABELS,
    VALID_SNOWFLAKE_COLUMN_NAMES,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from modin.config import PandasToSnowflakeParquetThresholdBytes

from tests.utils import Utils


def _generate_lower_case_table_name_unquoted() -> str:
    return "snowpark_temp_table_" + "".join(
        Random().choice(string.ascii_lowercase + string.digits) for _ in range(10)
    )


@pytest.fixture
def lower_case_table_name_unquoted(session):
    name = _generate_lower_case_table_name_unquoted()
    yield name
    Utils.drop_table(session, name)


@pytest.fixture
def lower_case_table_name_quoted(session):
    name = '"' + _generate_lower_case_table_name_unquoted() + '"'
    yield name
    Utils.drop_table(session, name)


@pytest.fixture
def upper_case_table_name_quoted(session):
    name = '"' + _generate_lower_case_table_name_unquoted().upper() + '"'
    yield name
    Utils.drop_table(session, name)


@pytest.fixture
def upper_case_table_name_with_space_quoted(session):
    name = '" ' + _generate_lower_case_table_name_unquoted().upper() + ' "'
    yield name
    Utils.drop_table(session, name)


@pytest.fixture
def valid_unquoted_identifier_table_name(session):
    name = _generate_lower_case_table_name_unquoted() + "$n"
    yield name
    Utils.drop_table(session, name)


@pytest.fixture(
    params=[
        ("pandas", 0),
        ("pandas", int(1e9)),
        ("snowflake", 0),
    ],
    autouse=True,
    ids=lambda param: f"backend_{param[0]}-parquet_threshold_{param[1]}",
)
def starting_backend_and_parquet_threshold(request):
    with config_context(
        Backend=request.param[0],
        PandasToSnowflakeParquetThresholdBytes=request.param[1],
    ):
        yield


def to_snowflake_counter(*, df: pd.DataFrame, if_exists: str) -> SqlCounter:
    if (
        df.get_backend() == "Pandas"
        and df.memory_usage(deep=False).sum()
        > PandasToSnowflakeParquetThresholdBytes.get()
    ):
        # In this case we are using a parquet file to load pandas data to
        # Snowflake.
        # if if_exists='fail', we issue a query to check whether the table
        # exists. In any case, we do issue more queries to make the parquet
        # file, stage it, and load it into a table, but the SqlCounter doesn't
        # track those queries.
        query_count = 1 if if_exists == "fail" else 0
    elif if_exists in ("append", "fail"):
        query_count = 2
    else:
        assert if_exists == "replace"
        # In this case, we can skip the query that checks whether the table
        # exists.
        query_count = 1
    return SqlCounter(query_count=query_count)


@pytest.mark.parametrize("index", [True, False])
@pytest.mark.parametrize("index_labels", [None, ["my_index"]])
def test_to_snowflake_index(test_table_name, index, index_labels):
    df = pd.DataFrame(
        {"a": [1, 2, 3], "b": [4, 5, 6]}, index=native_pd.Index([2, 3, 4], name="index")
    )

    if_exists = "replace"
    with to_snowflake_counter(df=df, if_exists=if_exists):
        df.to_snowflake(
            test_table_name, if_exists=if_exists, index=index, index_label=index_labels
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


def test_to_snowflake_multiindex(test_table_name):
    index = native_pd.MultiIndex.from_arrays(
        [[1, 1, 2, 2], ["red", "blue", "red", "blue"]], names=("number", "color")
    )
    native_df = native_pd.DataFrame(
        [[1] * 2, [2] * 2, [3] * 2, [4] * 2], index=index, columns=["a", "b"]
    )
    snow_df = pd.DataFrame(native_df)
    if_exists = "replace"
    with to_snowflake_counter(df=snow_df, if_exists=if_exists):
        snow_df.to_snowflake(test_table_name, if_exists=if_exists, index=True)
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
    if_exists = "fail"
    with to_snowflake_counter(df=df, if_exists=if_exists):
        df.to_snowflake(test_table_name, if_exists=if_exists, index=False)
    verify_columns(test_table_name, ["a", "b"])

    # Verify attempt to write to existing table fails.
    # Just one query to find that the table already exists.
    if_exists = "fail"
    with SqlCounter(query_count=1):
        with pytest.raises(ValueError):
            df.to_snowflake(test_table_name, if_exists=if_exists, index=False)

    # Verify by default attempt to write to existing table fails
    with SqlCounter(query_count=1):
        with pytest.raises(ValueError):
            df.to_snowflake(test_table_name, index=False)

    # Verify existing table is replaced with new data
    if_exists = "replace"
    df = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]})
    with to_snowflake_counter(df=df, if_exists=if_exists):
        df.to_snowflake(test_table_name, if_exists=if_exists, index=False)
    verify_columns(test_table_name, ["a", "c"])
    verify_num_rows(session, test_table_name, 3)

    # Verify data is appended to existing table
    if_exists = "append"
    with to_snowflake_counter(df=df, if_exists=if_exists):
        df.to_snowflake(test_table_name, if_exists=if_exists, index=False)
    verify_columns(test_table_name, ["a", "c"])
    verify_num_rows(session, test_table_name, 6)

    # Verify pd.to_snowflake operates the same
    if_exists = "append"
    with to_snowflake_counter(df=df, if_exists=if_exists):
        pd.to_snowflake(df, test_table_name, if_exists=if_exists, index=False)
    verify_columns(test_table_name, ["a", "c"])
    verify_num_rows(session, test_table_name, 9)

    # Verify invalid 'if_exists' input.
    with SqlCounter(query_count=0):
        with pytest.raises(ValueError):
            df.to_snowflake(test_table_name, if_exists="abc")


@pytest.mark.parametrize("index_label", VALID_PANDAS_LABELS)
def test_to_snowflake_index_labels(index_label, test_table_name):
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    if_exists = "replace"
    with to_snowflake_counter(df=df, if_exists=if_exists):
        df.to_snowflake(
            test_table_name, if_exists=if_exists, index=True, index_label=index_label
        )
    verify_columns(test_table_name, [str(index_label), "a", "b"])


@pytest.mark.parametrize("col_name", VALID_PANDAS_LABELS)
def test_to_snowflake_column_names_from_pandas(col_name, test_table_name):
    df = pd.DataFrame({col_name: [1, 2, 3], "b": [4, 5, 6]})
    if_exists = "replace"
    with to_snowflake_counter(df=df, if_exists=if_exists):
        df.to_snowflake(test_table_name, if_exists="replace", index=False)
    verify_columns(test_table_name, [str(col_name), "b"])


@pytest.mark.parametrize("col_name", VALID_SNOWFLAKE_COLUMN_NAMES)
@pytest.mark.parametrize("if_exists", ["append", "replace"])
def test_column_names_with_read_snowflake_and_to_snowflake(
    col_name, if_exists, session, test_table_name
):
    # Create a table
    session.sql(f"create or replace table {test_table_name} ({col_name} int)").collect()
    session.sql(f"insert into {test_table_name} values (1), (2), (3)").collect()
    data = session.sql(f"select {col_name} from {test_table_name}").collect()
    assert len(data) == 3

    # Capture column names of origing table.
    expected_columns = session.table(test_table_name).columns

    df = pd.read_snowflake(test_table_name)
    with to_snowflake_counter(df=df, if_exists=if_exists):
        df.to_snowflake(test_table_name, if_exists=if_exists, index=False)
    # Verify column names are not updated here.
    assert expected_columns == session.table(test_table_name).columns
    data = session.sql(f"select {col_name} from {test_table_name}").collect()
    assert len(data) == (6 if if_exists == "append" else 3)


def test_to_snowflake_column_with_quotes(session, test_table_name):
    df = pd.DataFrame({'a"b': [1, 2, 3], 'a""b': [4, 5, 6]})
    if_exists = "replace"
    with to_snowflake_counter(df=df, if_exists=if_exists):
        df.to_snowflake(test_table_name, if_exists=if_exists, index=False)
    verify_columns(test_table_name, ['a"b', 'a""b'])


# one extra query to convert index to native pandas when creating the snowpark pandas dataframe
def test_to_snowflake_index_label_none(test_table_name):
    # no index
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    if_exists = "replace"
    with to_snowflake_counter(df=df, if_exists=if_exists):
        df.to_snowflake(test_table_name, if_exists=if_exists)
    verify_columns(test_table_name, ["index", "a", "b"])

    # named index
    if_exists = "replace"
    df = pd.DataFrame(
        {"a": [1, 2, 3], "b": [4, 5, 6]}, index=pd.Index([2, 3, 4], name="index")
    )
    with to_snowflake_counter(df=df, if_exists=if_exists):
        df.to_snowflake(test_table_name, if_exists=if_exists, index_label=[None])
    verify_columns(test_table_name, ["index", "a", "b"])

    # nameless index
    if_exists = "replace"
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=pd.Index([2, 3, 4]))
    with to_snowflake_counter(df=df, if_exists=if_exists):
        df.to_snowflake(test_table_name, if_exists, index_label=[None])
    verify_columns(test_table_name, ["index", "a", "b"])


# one extra query to convert index to native pandas when creating the snowpark pandas dataframe
def test_to_snowflake_index_label_none_data_column_conflict(test_table_name):
    df = pd.DataFrame({"index": [1, 2, 3], "a": [4, 5, 6]})
    if_exists = "replace"
    with to_snowflake_counter(df=df, if_exists=if_exists):
        df.to_snowflake(test_table_name, if_exists=if_exists)
    # If the column name "index" is taken by one of the data columns,
    # then "level_0" is used instead for naming the index column.
    # This is based on the behavior of reset_index.
    verify_columns(test_table_name, ["level_0", "index", "a"])

    df = pd.DataFrame(
        {"index": [1, 2, 3], "a": [4, 5, 6]},
        index=native_pd.Index([2, 3, 4], name="index"),
    )
    # If the index already has a name, "index", then "level_0" is not used,
    # and a ValueError is raised instead.
    # This is based on the behavior of reset_index.
    with pytest.raises(ValueError):
        df.to_snowflake(test_table_name, if_exists="replace", index_label=[None])

    # nameless index
    df = pd.DataFrame({"index": [1, 2, 3], "a": [4, 5, 6]}, index=pd.Index([2, 3, 4]))
    if_exists = "replace"
    with to_snowflake_counter(df=df, if_exists=if_exists):
        df.to_snowflake(test_table_name, if_exists=if_exists, index_label=[None])
    verify_columns(test_table_name, ["level_0", "index", "a"])


@sql_count_checker(query_count=0)
def test_to_snowflake_data_label_none_raises(test_table_name):
    df = pd.DataFrame(
        {"a": [1, 2, 3], "b": [4, 5, 6]}, index=native_pd.Index([2, 3, 4], name="index")
    )
    df.columns = ["c", None]

    message = re.escape(
        "Label None is found in the data columns ['c', None], which is invalid in Snowflake."
    )
    with pytest.raises(ValueError, match=message):
        df.to_snowflake(test_table_name, if_exists="replace")


def test_to_snowflake_with_dropped_row_position(test_table_name):
    snow_df = pd.DataFrame({"a": [1, 2, 3], "b": [2, 4, 5]})
    snow_df = snow_df.groupby("a").count().reset_index()
    with to_snowflake_counter(df=snow_df, if_exists="fail"):
        snow_df.to_snowflake(test_table_name, index=False, table_type="temporary")


def verify_columns(table_name: str, expected: list[str]) -> None:
    actual = pd.read_snowflake(table_name).columns
    assert actual.tolist() == expected


def verify_num_rows(session, table_name: str, expected: int) -> None:
    actual = session.table(table_name).count()
    assert actual == expected


def test_timedelta_to_snowflake_with_read_snowflake(test_table_name, caplog):
    with caplog.at_level(logging.WARNING):
        df = pd.DataFrame(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
                "t": native_pd.timedelta_range("1 days", periods=3),
            }
        )
        if_exists = "replace"
        with to_snowflake_counter(df=df, if_exists=if_exists):
            df.to_snowflake(test_table_name, index=False, if_exists=if_exists)
        df = pd.read_snowflake(test_table_name)
        # dytpe may be float64 or int64 depending on how we wrote to snowflake,
        # but it's not timedelta either way.
        assert df.dtypes[-1] in ("float64", "int64")
        assert "`TimedeltaType` may be lost in `to_snowflake`'s result" in caplog.text


class TestTableName:
    def test_lower_case_unquoted(self, lower_case_table_name_unquoted):
        native_df = native_pd.DataFrame({"a": [1]})
        df = pd.DataFrame(native_df)
        if_exists = "replace"
        with to_snowflake_counter(df=df, if_exists=if_exists):
            df.to_snowflake(
                lower_case_table_name_unquoted, if_exists=if_exists, index=False
            )
        written = pd.read_snowflake(lower_case_table_name_unquoted)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            written, native_df.rename(str, axis=1)
        )

    def test_lower_case_quoted(self, lower_case_table_name_quoted):
        native_df = native_pd.DataFrame({"a": [1]})
        df = pd.DataFrame(native_df)
        if_exists = "replace"
        with to_snowflake_counter(df=df, if_exists=if_exists):
            df.to_snowflake(
                lower_case_table_name_quoted, if_exists=if_exists, index=False
            )
        written = pd.read_snowflake(lower_case_table_name_quoted)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            written, native_df.rename(str, axis=1)
        )

    def test_upper_case_quoted(self, upper_case_table_name_quoted):
        native_df = native_pd.DataFrame({"a": [1]})
        df = pd.DataFrame(native_df)
        if_exists = "replace"
        with to_snowflake_counter(df=df, if_exists=if_exists):
            df.to_snowflake(
                upper_case_table_name_quoted, if_exists=if_exists, index=False
            )
        written = pd.read_snowflake(upper_case_table_name_quoted)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            written, native_df.rename(str, axis=1)
        )

    def test_upper_case_with_space_quoted(
        self, upper_case_table_name_with_space_quoted
    ):
        native_df = native_pd.DataFrame({"a": [1]})
        df = pd.DataFrame(native_df)
        if_exists = "replace"
        with to_snowflake_counter(df=df, if_exists=if_exists):
            df.to_snowflake(
                upper_case_table_name_with_space_quoted,
                if_exists=if_exists,
                index=False,
            )
        written = pd.read_snowflake(upper_case_table_name_with_space_quoted)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            written, native_df.rename(str, axis=1)
        )

    def test_special_chars_unquoted(self, valid_unquoted_identifier_table_name):
        native_df = native_pd.DataFrame({"a": [1]})
        df = pd.DataFrame(native_df)
        if_exists = "replace"
        with to_snowflake_counter(df=df, if_exists=if_exists):
            df.to_snowflake(
                valid_unquoted_identifier_table_name, if_exists=if_exists, index=False
            )
        written = pd.read_snowflake(valid_unquoted_identifier_table_name)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            written, native_df.rename(str, axis=1)
        )
