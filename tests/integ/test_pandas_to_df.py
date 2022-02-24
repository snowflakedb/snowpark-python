#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from datetime import datetime, timedelta, timezone

import pytest
from pandas import DataFrame as PandasDF, to_datetime
from pandas.testing import assert_frame_equal

from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import SnowparkPandasException
from tests.utils import Utils

# TODO enable this when SNOW-507647 is fixed and ENABLE_PARQUET_TIMESTAMP_NEW_LOGICAL_TYPE is out
# @pytest.fixture(scope="module", autouse=True)
# def setup(session):
#     session._run_query(
#         "alter session set ENABLE_PARQUET_TIMESTAMP_NEW_LOGICAL_TYPE=true"
#     )
#     yield
#     session._run_query("alter session unset ENABLE_PARQUET_TIMESTAMP_NEW_LOGICAL_TYPE")


@pytest.fixture(scope="module")
def tmp_table_basic(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(
        session, table_name, "id integer, foot_size float, shoe_model varchar"
    )
    try:
        yield table_name
    finally:
        Utils.drop_table(session, table_name)


@pytest.fixture(scope="module")
def tmp_table_complex(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(
        session,
        table_name,
        "id integer, foot_size float, shoe_model varchar, "
        "received boolean, date_purchased timestamp_ntz",
    )
    try:
        yield table_name
    finally:
        Utils.drop_table(session, table_name)


def test_write_pandas(session, tmp_table_basic):
    pd = PandasDF(
        [
            (1, 4.5, "t1"),
            (2, 7.5, "t2"),
            (3, 10.5, "t3"),
        ],
        columns=["id".upper(), "foot_size".upper(), "shoe_model".upper()],
    )

    df = session.write_pandas(pd, tmp_table_basic)
    results = df.to_pandas()
    assert_frame_equal(results, pd, check_dtype=False)

    # Auto create a new table
    session._run_query('drop table if exists "tmp_table_basic"')
    df = session.write_pandas(pd, "tmp_table_basic", auto_create_table=True)
    table_info = session.sql(f"show tables like 'tmp_table_basic'").collect()
    assert table_info[0]["kind"] == "TABLE"
    results = df.to_pandas()
    assert_frame_equal(results, pd, check_dtype=False)

    # Try to auto create a table that already exists (should NOT throw an error)
    # and upload data again. We use distinct to compare results since we are
    # uploading data twice
    df = session.write_pandas(pd, "tmp_table_basic", auto_create_table=True)
    results = df.distinct().to_pandas()
    assert_frame_equal(results, pd, check_dtype=False)

    # TODO enable this when SNOW-507647 is fixed and ENABLE_PARQUET_TIMESTAMP_NEW_LOGICAL_TYPE is out
    # # Do a more complex case where we create the table
    # pd = PandasDF(
    #     [
    #         (1, 4.5, "t1", True, datetime.now()),
    #         (2, 7.5, "t2", False, datetime.now()),
    #         (3, 10.5, "t3", True, datetime.now()),
    #     ],
    #     columns=[
    #         "id".upper(),
    #         "foot_size".upper(),
    #         "shoe_model".upper(),
    #         "received".upper(),
    #         "date_ordered".upper(),
    #     ],
    # )
    # session._run_query('drop table if exists "tmp_table_complex"')
    # df = session.write_pandas(pd, "tmp_table_complex", auto_create_table=True)
    # results = df.distinct().to_pandas()
    # assert_frame_equal(results, pd, check_dtype=False)

    with pytest.raises(SnowparkPandasException) as ex_info:
        df = session.write_pandas(pd, "tmp_table")
    assert (
        'Cannot write pandas DataFrame to table "tmp_table" because it does not exist. '
        "Create table before trying to write a pandas DataFrame" in str(ex_info)
    )

    # Drop tables that were created for this test
    session._run_query('drop table if exists "tmp_table_basic"')
    session._run_query('drop table if exists "tmp_table_complex"')


def test_create_dataframe_from_pandas(session):
    pd = PandasDF(
        [
            (1, 4.5, "t1", True),
            (2, 7.5, "t2", False),
            (3, 10.5, "t3", True),
        ],
        columns=[
            "id".upper(),
            "foot_size".upper(),
            "shoe_model".upper(),
            "received".upper(),
        ],
    )

    df = session.create_dataframe(pd)
    results = df.to_pandas()
    assert_frame_equal(results, pd, check_dtype=False)

    # TODO enable this when SNOW-507647 is fixed and ENABLE_PARQUET_TIMESTAMP_NEW_LOGICAL_TYPE is out
    # pd = PandasDF(
    #     [
    #         (1, 4.5, "t1", True, datetime.now()),
    #         (2, 7.5, "t2", False, datetime.now()),
    #         (3, 10.5, "t3", True, datetime.now()),
    #     ],
    #     columns=[
    #         "id".upper(),
    #         "foot_size".upper(),
    #         "shoe_model".upper(),
    #         "received".upper(),
    #         "date_ordered".upper(),
    #     ],
    # )
    #
    # df = session.create_dataframe(pd)
    # results = df.to_pandas()
    # assert_frame_equal(results, pd, check_dtype=False)


def test_write_pandas_temp_table_and_irregular_column_names(session):
    pd = PandasDF(
        [
            (1, 4.5, "t1"),
            (2, 7.5, "t2"),
            (3, 10.5, "t3"),
        ],
        columns=["id".upper(), "foot size".upper(), "shoe model".upper()],
        # TODO: connector's write_pandas doesn't support double quote in column name. It should support column name "foot\"size".
    )
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        session.write_pandas(
            pd, table_name, auto_create_table=True, create_temp_table=True
        )
        table_info = session.sql(f"show tables like '{table_name}'").collect()
        assert table_info[0]["kind"] == "TEMPORARY"
    finally:
        Utils.drop_table(session, table_name)


def test_write_pandas_with_timestamps(session):
    datetime_with_tz = datetime(
        1997, 6, 3, 14, 21, 32, 00, tzinfo=timezone(timedelta(hours=+10))
    )
    datetime_with_ntz = datetime(1997, 6, 3, 14, 21, 32, 00)
    pd = PandasDF(
        [
            [datetime_with_tz, datetime_with_ntz],
        ],
        columns=["tm_tz", "tm_ntz"],
    )
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        session.write_pandas(
            pd, table_name, auto_create_table=True, create_temp_table=True
        )
        data = session.sql(f'select * from "{table_name}"').collect()
        assert data[0]["tm_tz"] is not None
        assert data[0]["tm_ntz"] is not None
        # TODO: Schema detection on the server-side has bugs dealing with timestamp_ntz and timestamp_tz.
        #  After the bugs are fixed, change the assertion to `data[0]["tm_tz"] == datetime_with_tz`
        #  and `data[0]["tm_ntz"] == datetime_with_ntz`,
        #  JIRA https://snowflakecomputing.atlassian.net/browse/SNOW-524865
        #  JIRA https://snowflakecomputing.atlassian.net/browse/SNOW-359205
        #  JIRA https://snowflakecomputing.atlassian.net/browse/SNOW-507644
    finally:
        Utils.drop_table(session, table_name)


def test_auto_create_table_similar_column_names(session):
    """Tests whether similar names cause issues when auto-creating a table as expected."""
    table_name = "numbas"
    df_data = [(10, 11), (20, 21)]

    df = PandasDF(df_data, columns=["number", "Number"])
    select_sql = f'SELECT * FROM "{table_name}"'
    drop_sql = f'DROP TABLE IF EXISTS "{table_name}"'
    try:
        session.write_pandas(
            df, table_name, quote_identifiers=True, auto_create_table=True
        )

        # Check table's contents
        data = session.sql(select_sql).collect()
        for row in data:
            # The auto create table functionality does not auto-create an incrementing ID
            assert (
                row["number"],
                row["Number"],
            ) in df_data
    finally:
        session.sql(drop_sql).collect()


@pytest.mark.parametrize("auto_create_table", [True, False])
def test_special_name_quoting(
    session,
    auto_create_table: bool,
):
    """Tests whether special column names get quoted as expected."""
    table_name = "users"
    df_data = [("Mark", 10), ("Luke", 20)]

    df = PandasDF(df_data, columns=["00name", "bAl ance"])
    create_sql = (
        f'CREATE OR REPLACE TABLE "{table_name}"'
        '("00name" STRING, "bAl ance" INT, "id" INT AUTOINCREMENT)'
    )
    select_sql = f'SELECT * FROM "{table_name}"'
    drop_sql = f'DROP TABLE IF EXISTS "{table_name}"'
    if not auto_create_table:
        session.sql(create_sql).collect()
    try:
        session.write_pandas(
            df,
            table_name,
            quote_identifiers=True,
            auto_create_table=auto_create_table,
        )
        # Check table's contents
        data = session.sql(select_sql).collect()
        for row in data:
            # The auto create table functionality does not auto-create an incrementing ID
            if not auto_create_table:
                assert row["id"] in (1, 2)
            assert (
                row["00name"],
                row["bAl ance"],
            ) in df_data
    finally:
        session.sql(drop_sql).collect()
