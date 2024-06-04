#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import logging
import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark._internal.analyzer.analyzer_utils import quote_name
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.modin.plugin._internal.utils import (
    READ_ONLY_TABLE_SUFFIX,
    extract_pandas_label_from_snowflake_quoted_identifier,
)
from snowflake.snowpark.modin.plugin.utils.exceptions import (
    SnowparkPandasErrorCode,
    SnowparkPandasException,
)
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import (
    BASIC_TYPE_DATA1,
    BASIC_TYPE_DATA2,
    SEMI_STRUCTURED_TYPE_DATA,
    VALID_SNOWFLAKE_COLUMN_NAMES,
    assert_frame_equal,
    assert_index_equal,
    assert_series_equal,
    create_table_with_type,
)
from tests.utils import Utils


def call_read_snowflake(table_name: str, as_query: bool, **kwargs) -> pd.DataFrame:
    """
    Helper method to call `read_snowflake`, either with the table name directly, or with `SELECT * FROM {table_name}`.

    Args:
        table_name: The name of the table to call.
        as_query: Whether to call `read_snowflake` with a query or the table name.
        kwargs: Keyword arguments to pass to `read_snowflake`.

    Returns:
        The resulting Snowpark pandas DataFrame.
    """
    if as_query:
        return pd.read_snowflake(f"SELECT * FROM {table_name}", **kwargs)
    return pd.read_snowflake(table_name, **kwargs)


@sql_count_checker(query_count=5)
@pytest.mark.precommit
@pytest.mark.parametrize(
    "as_query", [True, False], ids=["read_with_select_*", "read_with_table_name"]
)
def test_read_snowflake_basic(session, as_query):
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    fully_qualified_name = [
        session.get_current_database(),
        session.get_current_schema(),
        table_name,
    ]
    session.create_dataframe([BASIC_TYPE_DATA1, BASIC_TYPE_DATA2]).write.save_as_table(
        table_name, table_type="temp"
    )
    if as_query:
        names_list = [table_name, ".".join(fully_qualified_name)]
    else:
        names_list = [table_name, fully_qualified_name]
    # create snowpark pandas dataframe
    for name in names_list:
        df = call_read_snowflake(name, as_query)

        # test if the snapshot is created
        # the table name should match the following reg expression
        # "^SNOWPARK_TEMP_TABLE_[0-9A-Z]+$")
        sql = df._query_compiler._modin_frame.ordered_dataframe.queries["queries"][-1]
        temp_table_pattern = ".*SNOWPARK_TEMP_TABLE_[0-9A-Z]+.*$"
        assert re.match(temp_table_pattern, sql) is not None
        assert READ_ONLY_TABLE_SUFFIX in sql

        # check the row position snowflake quoted identifier is set
        assert (
            df._query_compiler._modin_frame.row_position_snowflake_quoted_identifier
            is not None
        )

        pdf = df.to_pandas()
        assert pdf.values[0].tolist() == BASIC_TYPE_DATA1
        assert pdf.values[1].tolist() == BASIC_TYPE_DATA2


@sql_count_checker(query_count=3)
@pytest.mark.parametrize(
    "as_query", [True, False], ids=["read_with_select_*", "read_with_table_name"]
)
def test_read_snowflake_semi_structured_types(session, as_query):
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe([SEMI_STRUCTURED_TYPE_DATA]).write.save_as_table(
        table_name, table_type="temp"
    )

    # create snowpark pandas dataframe
    df = call_read_snowflake(table_name, as_query)

    pdf = df.to_pandas()
    for res, expected_res in zip(pdf.values[0].tolist(), SEMI_STRUCTURED_TYPE_DATA):
        assert res == expected_res


@sql_count_checker(query_count=3)
@pytest.mark.parametrize(
    "as_query", [True, False], ids=["read_with_select_*", "read_with_table_name"]
)
def test_read_snowflake_none_nan(session, as_query):
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe([None, float("nan")]).write.save_as_table(
        table_name, table_type="temp"
    )

    # create snowpark pandas dataframe
    df = call_read_snowflake(table_name, as_query)

    pdf = df.to_pandas()
    assert np.isnan(pdf.values[0][0])
    assert np.isnan(pdf.values[1][0])


@pytest.mark.parametrize("col_name", VALID_SNOWFLAKE_COLUMN_NAMES)
@sql_count_checker(query_count=3)
@pytest.mark.parametrize(
    "as_query", [True, False], ids=["read_with_select_*", "read_with_table_name"]
)
def test_read_snowflake_column_names(session, col_name, as_query):
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, f"{col_name} int", is_temporary=True)

    # create snowpark pandas dataframe
    df = call_read_snowflake(table_name, as_query)

    pdf = df.to_pandas()
    assert pdf.index.dtype == np.int64
    assert pdf.columns[0] == extract_pandas_label_from_snowflake_quoted_identifier(
        quote_name(col_name)
    )


@pytest.mark.parametrize(
    "col_name1, col_name2",
    list(zip(VALID_SNOWFLAKE_COLUMN_NAMES, VALID_SNOWFLAKE_COLUMN_NAMES[::-1])),
)
@pytest.mark.parametrize(
    "as_query", [True, False], ids=["read_with_select_*", "read_with_table_name"]
)
@sql_count_checker(query_count=3)
def test_read_snowflake_index_col(session, col_name1, col_name2, as_query):
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(
        session, table_name, f"{col_name1} int, {col_name2} text", is_temporary=True
    )

    col_label1 = extract_pandas_label_from_snowflake_quoted_identifier(
        quote_name(col_name1)
    )
    col_label2 = extract_pandas_label_from_snowflake_quoted_identifier(
        quote_name(col_name2)
    )

    # create snowpark pandas dataframe
    df = call_read_snowflake(table_name, as_query, index_col=col_label1)

    pdf = df.to_pandas()
    assert pdf.index.name == col_label1
    assert len(pdf.columns) == 1
    assert pdf.columns[0] == col_label2


@sql_count_checker(query_count=4)
@pytest.mark.parametrize(
    "as_query", [True, False], ids=["read_with_select_*", "read_with_table_name"]
)
def test_read_snowflake_index_col_multiindex(session, as_query):
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(
        session,
        table_name,
        "col1 text, col2 text, col3 text, col4 text",
        is_temporary=True,
    )
    session.sql(f"insert into {table_name} values ('A', 'B', 'C', 'D')").collect()

    # create snowpark pandas dataframe
    df = call_read_snowflake(table_name, as_query, index_col=["COL1", "COL2", "COL3"])

    assert_index_equal(
        df.index,
        pd.MultiIndex.from_tuples([("A", "B", "C")], names=["COL1", "COL2", "COL3"]),
    )


@pytest.mark.parametrize(
    # non_existing_index_col latter doesn't exist in the table because we require
    # it equals to extract_pandas_label_from_snowflake_quoted_identifier(quote_name(col_name))
    # in read_snowflake
    "col_name, non_existing_index_col",
    (
        ("col", "test"),
        ("col", "col"),
        ("COL", "col"),
        ('"col"', "COL"),
        ('"COL"', "col"),
    ),
)
@pytest.mark.parametrize("index_col_or_columns", [True, False])
@sql_count_checker(query_count=2)
def test_read_snowflake_non_existing(
    session, col_name, non_existing_index_col, index_col_or_columns
):
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, f"{col_name} int", is_temporary=True)
    with pytest.raises(
        KeyError,
        match="is not in existing snowflake columns",
    ):
        if index_col_or_columns:
            pd.read_snowflake(table_name, index_col=non_existing_index_col)
        else:
            pd.read_snowflake(table_name, columns=[non_existing_index_col])


@pytest.mark.parametrize("col_name", VALID_SNOWFLAKE_COLUMN_NAMES)
@sql_count_checker(query_count=3)
def test_read_snowflake_columns(session, col_name):
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(
        session, table_name, f"{col_name} int, s text", is_temporary=True
    )

    col_label = extract_pandas_label_from_snowflake_quoted_identifier(
        quote_name(col_name)
    )

    # create snowpark pandas dataframe
    df = pd.read_snowflake(table_name, columns=[col_label])

    pdf = df.to_pandas()
    assert pdf.columns[0] == col_label


@sql_count_checker(query_count=3)
def test_read_snowflake_both_index_col_columns(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(
        session,
        table_name,
        "X int, Y int, Z int",
        is_temporary=True,
    )

    df = pd.read_snowflake(table_name, index_col="X", columns=["Y"])
    pdf = df.to_pandas()
    assert pdf.index.name == "X"
    assert pdf.columns[0] == "Y"


@sql_count_checker(query_count=8)
def test_read_snowflake_duplicate_columns(session):
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, '"X" int, Y int', is_temporary=True)

    df = pd.read_snowflake(table_name, columns=["X", "X"])
    assert df.columns.tolist() == ["X", "X"]

    df = pd.read_snowflake(table_name, index_col=["X", "X"])
    assert df.index.names == ["X", "X"]

    df = pd.read_snowflake(table_name, index_col="X", columns=["X", "Y"])
    assert df.index.names == ["X"]
    assert df.columns.tolist() == ["X", "Y"]

    df = pd.read_snowflake(table_name, index_col=["X", "Y"], columns=["X", "Y"])
    assert df.index.names == ["X", "Y"]
    assert df.columns.tolist() == ["X", "Y"]


@sql_count_checker(query_count=0)
def test_read_snowflake_table_not_exist_negative(session) -> None:
    table_name = "non_exist_table_error"

    with pytest.raises(SnowparkPandasException) as ex:
        pd.read_snowflake(table_name)

    assert ex.value.error_code == SnowparkPandasErrorCode.GENERAL_SQL_EXCEPTION.value


@sql_count_checker(query_count=1)
def test_read_snowflake_column_not_list_raises(session) -> None:
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    col_name = "TEST"
    Utils.create_table(
        session, table_name, f"{col_name} int, s text", is_temporary=True
    )

    with pytest.raises(ValueError, match="columns must be provided as list"):
        pd.read_snowflake(table_name, columns=col_name)


@pytest.mark.precommit
@pytest.mark.parametrize(
    "table_type",
    [
        "",
        "temporary",
        "transient",
        "view",
        "SECURE VIEW",
        "TEMP VIEW",
    ],
)
@pytest.mark.parametrize(
    "as_query", [True, False], ids=["read_with_select_*", "read_with_table_name"]
)
def test_read_snowflake_with_views(
    session, test_table_name, table_type, caplog, as_query
) -> None:
    # create a temporary test table
    expected_query_count = 6
    original_table_type = "temporary"
    if table_type in ["", "temporary", "transient"]:
        original_table_type = table_type
        expected_query_count = 3
    elif table_type == "MATERIALIZED VIEW":
        original_table_type = ""
    with SqlCounter(query_count=expected_query_count):
        create_table_with_type(
            session, test_table_name, "col1 int, s text", table_type=original_table_type
        )
        session.sql(f"insert into {test_table_name} values (1, 'ok')").collect()
        # create a view out of the temporary test table
        table_name = test_table_name
        view_name = None
        try:
            if table_type in ["view", "SECURE VIEW", "TEMP VIEW"]:
                view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
                session.sql(
                    f"create or replace {table_type} {view_name} (col1, s) as select * from {test_table_name}"
                ).collect()
                table_name = view_name
            caplog.clear()
            with caplog.at_level(logging.WARNING):
                df = call_read_snowflake(table_name, as_query)
            assert df.columns.tolist() == ["COL1", "S"]
            materialize_log = f"Data from source table/view '{table_name}' is being copied into a new temporary table"
            if table_type in ["view", "SECURE VIEW", "TEMP VIEW"]:
                # verify temporary table is materialized for view, secure view and temp view
                assert materialize_log in caplog.text
            else:
                # verify no temporary table is materialized for regular table
                assert not (materialize_log in caplog.text)
        finally:
            if view_name:
                Utils.drop_view(session, view_name)


@pytest.mark.precommit
def test_read_snowflake_row_access_policy_table(
    session,
    test_table_name,
) -> None:
    Utils.create_table(session, test_table_name, "col1 int, s text", is_temporary=True)
    session.sql(f"insert into {test_table_name} values (1, 'ok')").collect()
    # create row access policy that there is no access to the row
    session.sql(
        "create or replace row access policy no_access_policy as (c1 int) returns boolean -> False"
    ).collect()
    # add the row access policy to the table on column col1
    session.sql(
        f"alter table {test_table_name} add row access policy no_access_policy on (col1)"
    ).collect()

    with SqlCounter(query_count=3):
        df = pd.read_snowflake(test_table_name)

        assert df.columns.tolist() == ["COL1", "S"]
        assert len(df) == 0

    with SqlCounter(query_count=3):
        df = pd.read_snowflake(f"SELECT * FROM {test_table_name}")

        assert df.columns.tolist() == ["COL1", "S"]
        assert len(df) == 0


@pytest.mark.parametrize(
    "input_data, snowflake_type_string, logical_dtype, actual_dtype",
    [
        ([1.1, 2.2, 3.3], "decimal(3, 1)", np.dtype("float64"), np.dtype("float64")),
        ([1.1, 2.2, 3.3], "decimal(38, 1)", np.dtype("float64"), np.dtype("float64")),
        ([1.0, 2.0, 3.0], "decimal(3, 0)", np.dtype("int64"), np.dtype("int8")),
        ([1.0, 2.0, 3.0], "decimal(38, 0)", np.dtype("int64"), np.dtype("int8")),
        (
            [1 << 20, 2 << 20, 3 << 20],
            "decimal(15, 0)",
            np.dtype("int64"),
            np.dtype("int32"),
        ),
        # The example below is questionable. Arguably to_pandas should be returning objects with decimals.
        (
            [1 << 65, 2 << 65, 3 << 65],
            "decimal(38, 0)",
            np.dtype("int64"),
            np.dtype("float64"),
        ),
    ],
)
@pytest.mark.parametrize(
    "as_query", [True, False], ids=["read_with_select_*", "read_with_table_name"]
)
@sql_count_checker(query_count=5)
def test_decimal(
    input_data,
    snowflake_type_string,
    logical_dtype,
    actual_dtype,
    session,
    test_table_name,
    as_query,
) -> None:
    colname = "D"
    values_string = ",".join(f"({i})" for i in input_data)
    Utils.create_table(
        session, test_table_name, f"d {snowflake_type_string}", is_temporary=True
    )
    session.sql(f"insert into {test_table_name} values {values_string}").collect()
    # create row access policy that there is no access to the row
    df = call_read_snowflake(test_table_name, as_query)

    assert_series_equal(df.dtypes, native_pd.Series([logical_dtype], index=[colname]))
    pandas_df = df.to_pandas()
    assert_series_equal(
        pandas_df.dtypes, native_pd.Series([actual_dtype], index=[colname])
    )
    assert_frame_equal(df, pandas_df, check_dtype=False)


@sql_count_checker(query_count=9)
@pytest.mark.precommit
@pytest.mark.parametrize(
    "as_query", [True, False], ids=["read_with_select_*", "read_with_table_name"]
)
def test_read_snowflake_with_table_in_different_db(session, caplog, as_query) -> None:
    db_name = f"testdb_snowpandas_{Utils.random_alphanumeric_str(4)}"
    schema_name = f"testschema_snowpandas_{Utils.random_alphanumeric_str(4)}"
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    origin_db = session.get_current_database()
    origin_schema = session.get_current_schema()
    try:
        # create a different database and schema for testing compare with the
        # current database and schema used in current session.
        session.sql(f"create database {db_name}").collect()
        session.sql(f"create schema {db_name}.{schema_name}").collect()

        # create table
        Utils.create_table(
            session,
            f"{db_name}.{schema_name}.{table_name}",
            '"X" int, Y int',
            is_temporary=True,
        )

        caplog.clear()
        with caplog.at_level(logging.DEBUG):
            df = call_read_snowflake(table_name, as_query)
        # verify no temporary table is materialized for regular table
        assert not ("Materialize temporary table" in caplog.text)
        assert df.columns.tolist() == ["X", "Y"]
    finally:
        # drop the created temp object
        Utils.drop_table(session, f"{db_name}.{schema_name}.{table_name}")
        Utils.drop_schema(session, f"{db_name}.{schema_name}")
        Utils.drop_database(session, db_name)
        # recover the origin db and schema
        session.use_database(origin_db)
        session.use_schema(origin_schema)
