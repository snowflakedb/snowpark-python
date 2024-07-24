#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
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
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import (
    BASIC_TYPE_DATA1,
    BASIC_TYPE_DATA2,
    VALID_SNOWFLAKE_COLUMN_NAMES_AND_ALIASES,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
)
from tests.utils import Utils


@sql_count_checker(query_count=4)
def test_read_snowflake_basic_query_with_weird_formatting(session):
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe([BASIC_TYPE_DATA1, BASIC_TYPE_DATA2]).write.save_as_table(
        table_name, table_type="temp"
    )
    df = pd.read_snowflake(f"(((SELECT * FROM {table_name})))")

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


@sql_count_checker(query_count=5)
def test_read_snowflake_basic_query_with_comment_preceding_sql_inline_string(session):
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe([BASIC_TYPE_DATA1, BASIC_TYPE_DATA2]).write.save_as_table(
        table_name, table_type="temp"
    )
    df = pd.read_snowflake(f"--SQL Comment\nSELECT * FROM {table_name}")

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


@sql_count_checker(query_count=5)
def test_read_snowflake_basic_query_with_comment_preceding_sql_multiline_string(
    session,
):
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe([BASIC_TYPE_DATA1, BASIC_TYPE_DATA2]).write.save_as_table(
        table_name, table_type="temp"
    )
    df = pd.read_snowflake(
        f"""--SQL Comment 1
                           -- SQL Comment 2
                           SELECT * FROM {table_name}
                            -- SQL Comment 3"""
    )

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


@sql_count_checker(query_count=4)
@pytest.mark.parametrize("only_nulls", [True, False])
def test_read_snowflake_query_none_nan_condition(session, only_nulls):
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame(
            [[1, 2, None], [3, 4, 5], [6, 7, float("nan")]], columns=["A", "B", "C"]
        )
    ).write.save_as_table(table_name, table_type="temp")

    # crate snowpark pandas dataframe
    df = pd.read_snowflake(
        f"SELECT * FROM {table_name} WHERE C IS {'NOT' if not only_nulls else ''} NULL"
    )
    if not only_nulls:
        pdf = native_pd.DataFrame([[3, 4, 5]], columns=["A", "B", "C"])
    else:
        pdf = native_pd.DataFrame(
            [[1, 2, None], [6, 7, float("nan")]], columns=["A", "B", "C"]
        )
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(df, pdf)


@pytest.mark.parametrize(
    "col_name_and_alias_tuple", VALID_SNOWFLAKE_COLUMN_NAMES_AND_ALIASES
)
@sql_count_checker(query_count=4)
def test_read_snowflake_query_aliased_columns(session, col_name_and_alias_tuple):
    # create table
    col_name, alias = col_name_and_alias_tuple
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, f"{col_name} int", is_temporary=True)

    # create snowpark pandas dataframe
    df = pd.read_snowflake(f"SELECT {col_name} AS {alias} FROM {table_name}")
    pdf = df.to_pandas()
    assert pdf.index.dtype == np.int64
    assert pdf.columns[0] == extract_pandas_label_from_snowflake_quoted_identifier(
        quote_name(alias)
    )


@pytest.mark.parametrize(
    "col_name_and_alias_tuple", VALID_SNOWFLAKE_COLUMN_NAMES_AND_ALIASES
)
@sql_count_checker(query_count=4)
def test_read_snowflake_query_aliased_columns_and_columns_kwarg_specified(
    session, col_name_and_alias_tuple
):
    # create table
    col_name, alias = col_name_and_alias_tuple
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, f"{col_name} int", is_temporary=True)

    # create snowpark pandas dataframe
    pandas_col_name = extract_pandas_label_from_snowflake_quoted_identifier(
        quote_name(alias)
    )
    df = pd.read_snowflake(
        f"SELECT {col_name} AS {alias}, {col_name} FROM {table_name}",
        columns=[pandas_col_name],
    )
    pdf = df.to_pandas()
    assert pdf.index.dtype == np.int64
    assert pdf.columns[0] == pandas_col_name
    assert len(pdf.columns) == 1


@sql_count_checker(query_count=3)
def test_read_snowflake_query_with_columns(session):
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, '"col0" int, "col1" int', is_temporary=True)

    # create snowpark pandas dataframe
    df = pd.read_snowflake(f"SELECT * FROM {table_name}", columns=["col0"])
    pdf = df.to_pandas()
    assert pdf.index.dtype == np.int64
    assert len(pdf.columns) == 1
    assert pdf.columns[0] == "col0"


@sql_count_checker(query_count=3)
def test_read_snowflake_query_with_index_col_and_columns(session):
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(
        session,
        table_name,
        '"index_col" int, "col0" int, "col1" int',
        is_temporary=True,
    )

    # create snowpark pandas dataframe
    df = pd.read_snowflake(
        f"SELECT * FROM {table_name}", columns=["col0"], index_col="index_col"
    )
    pdf = df.to_pandas()
    assert pdf.index.dtype == np.int64
    assert len(pdf.columns) == 1
    assert pdf.columns[0] == "col0"
    assert pdf.index.name == "index_col"


@sql_count_checker(query_count=3)
def test_read_snowflake_query_with_index_col_and_columns_overlap(session):
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(
        session,
        table_name,
        '"index_col" int, "col0" int, "col1" int',
        is_temporary=True,
    )

    # create snowpark pandas dataframe
    df = pd.read_snowflake(
        f"SELECT * FROM {table_name}",
        columns=["col0", "index_col"],
        index_col="index_col",
    )
    pdf = df.to_pandas()
    assert pdf.index.dtype == np.int64
    assert pdf.columns.equals(native_pd.Index(["col0", "index_col"]))
    assert pdf.index.name == "index_col"


@sql_count_checker(query_count=4)
def test_read_snowflake_query_additional_derived_column(session):
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[1, 2], [3, 4], [6, 7]], columns=["A", "B"])
    ).write.save_as_table(table_name, table_type="temp")

    df = pd.read_snowflake(
        f"SELECT A, B, SQUARE(A) + SQUARE(B) as C FROM {table_name}", index_col="C"
    )
    pdf = native_pd.DataFrame(
        [[1, 2], [3, 4], [6, 7]],
        index=native_pd.Index([5, 25, 85], name="C"),
        columns=["A", "B"],
    )
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        df, pdf, check_index_type=False
    )


@pytest.mark.parametrize(
    # non_existing_index_col latter doesn't exist in the table because we require
    # it equals to extract_pandas_label_from_snowflake_quoted_identifier(quote_name(col_name))
    # in read_snowflake_query
    "col_name, non_existing_index_col",
    (
        ("col", "test"),
        ("col", "col"),
        ("COL", "col"),
        ('"col"', "COL"),
        ('"COL"', "col"),
    ),
)
@sql_count_checker(query_count=2)
def test_read_snowflake_query_non_existing(
    session,
    col_name,
    non_existing_index_col,
):
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, f"{col_name} int", is_temporary=True)
    with pytest.raises(
        KeyError,
        match="is not in existing snowflake columns",
    ):
        pd.read_snowflake(
            f"SELECT * FROM {table_name}", index_col=non_existing_index_col
        )


@sql_count_checker(query_count=5)
def test_read_snowflake_query_duplicate_columns(session):
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, '"X" int, Y int', is_temporary=True)

    df = pd.read_snowflake(f"SELECT * FROM {table_name}", index_col=["X", "X"])
    assert df.index.names == ["X", "X"]
    assert df.columns.tolist() == ["Y"]

    df = pd.read_snowflake(f"SELECT * FROM {table_name}", index_col=["X", "Y"])
    assert df.index.names == ["X", "Y"]
    assert df.columns.tolist() == []


@sql_count_checker(query_count=0)
def test_read_snowflake_query_table_not_exist_negative() -> None:
    table_name = "non_exist_table_error"

    with pytest.raises(SnowparkPandasException) as ex:
        pd.read_snowflake(f"SELECT * FROM {table_name}")

    assert ex.value.error_code == SnowparkPandasErrorCode.GENERAL_SQL_EXCEPTION.value


@sql_count_checker(query_count=0)
@pytest.mark.parametrize(
    "bad_sql", ["SELET * FROM A", "WITH T1 as (SELECT * FROM A), SELECT * FROM T1"]
)
def test_read_snowflake_query_table_bad_sql_negative(bad_sql) -> None:
    with pytest.raises(SnowparkPandasException) as ex:
        pd.read_snowflake(bad_sql)

    assert ex.value.error_code == SnowparkPandasErrorCode.GENERAL_SQL_EXCEPTION.value


@sql_count_checker(query_count=5, join_count=1)
def test_read_snowflake_query_complex_query_with_join(session):
    # create table
    table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame(
            [[10, "car"], [3, "bus"], [6, "train"]],
            columns=["price to consumer", "mode of transportation"],
        )
    ).write.save_as_table(table_name1, table_type="temp")
    table_name2 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame(
            [[5, "car"], [0.5, "bus"], [2, "train"]],
            columns=["cost to operator", "mode of transportation"],
        )
    ).write.save_as_table(table_name2, table_type="temp")
    df = pd.read_snowflake(
        f"""SELECT "price to consumer" - "cost to operator" as "profit",
        "mode of transportation" FROM {table_name1} NATURAL JOIN {table_name2}"""
    )
    pdf = native_pd.DataFrame(
        [[5, "car"], [2.5, "bus"], [4, "train"]],
        columns=["profit", "mode of transportation"],
    )
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(df, pdf)


@sql_count_checker(query_count=6)
def test_read_snowflake_query_connect_by(session):
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.sql(
        f"CREATE OR REPLACE TABLE {table_name} (title VARCHAR, employee_ID INTEGER, manager_ID INTEGER)"
    ).collect()
    session.sql(
        f"""INSERT INTO {table_name} (title, employee_ID, manager_ID) VALUES
                    ('President', 1, NULL),  -- The President has no manager.
                        ('Vice President Engineering', 10, 1),
                            ('Programmer', 100, 10),
                            ('QA Engineer', 101, 10),
                        ('Vice President HR', 20, 1),
                            ('Health Insurance Analyst', 200, 20)"""
    ).collect()
    SQL_QUERY = f"""SELECT employee_ID, manager_ID, title
                    FROM {table_name}
                        START WITH title = 'President'
                        CONNECT BY
                        manager_ID = PRIOR employee_id
                    ORDER BY employee_ID"""
    native_df = native_pd.DataFrame(session.sql(SQL_QUERY).collect())
    snow_df = (
        pd.read_snowflake(SQL_QUERY).sort_values("EMPLOYEE_ID").reset_index(drop=True)
    )
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, native_df)
