#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import logging
from datetime import datetime, timedelta, timezone

import pytest
from pandas import DataFrame as PandasDF
from pandas.testing import assert_frame_equal

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark._internal.utils import TempObjectType, warning_dict
from snowflake.snowpark.exceptions import SnowparkPandasException
from snowflake.snowpark.types import (
    BooleanType,
    DecimalType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from tests.utils import Utils

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


@pytest.mark.parametrize("quote_identifiers", [True, False])
@pytest.mark.parametrize("auto_create_table", [True, False])
@pytest.mark.parametrize("overwrite", [True, False])
def test_write_pandas_with_overwrite(
    session,
    quote_identifiers: bool,
    auto_create_table: bool,
    overwrite: bool,
):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        pd1 = PandasDF(
            [
                (1, 4.5, "Nike"),
                (2, 7.5, "Adidas"),
                (3, 10.5, "Puma"),
            ],
            columns=["id".upper(), "foot_size".upper(), "shoe_make".upper()],
        )

        pd2 = PandasDF(
            [(1, 8.0, "Dia Dora")],
            columns=["id".upper(), "foot_size".upper(), "shoe_make".upper()],
        )

        pd3 = PandasDF(
            [(1, "dash", 1000, 32)],
            columns=["id".upper(), "name".upper(), "points".upper(), "age".upper()],
        )

        # Create initial table and insert 3 rows
        df1 = session.write_pandas(
            pd1, table_name, quote_identifiers=quote_identifiers, auto_create_table=True
        )
        results = df1.to_pandas()
        assert_frame_equal(results, pd1, check_dtype=False)

        # Insert 1 row
        df2 = session.write_pandas(
            pd2,
            table_name,
            quote_identifiers=quote_identifiers,
            overwrite=overwrite,
            auto_create_table=auto_create_table,
        )
        results = df2.to_pandas()
        if overwrite:
            # Results should match pd2
            assert_frame_equal(results, pd2, check_dtype=False)
        else:
            # Results count should match pd1 + pd2
            assert results.shape[0] == 4

        # Insert 1 row with new schema
        if auto_create_table:
            if overwrite:
                # In this case, the table is first dropped and since there's a new schema, the results should now match pd3
                df3 = session.write_pandas(
                    pd3,
                    table_name,
                    quote_identifiers=quote_identifiers,
                    overwrite=overwrite,
                    auto_create_table=auto_create_table,
                )
                results = df3.to_pandas()
                assert_frame_equal(results, pd3, check_dtype=False)
            else:
                # In this case, the table is truncated but since there's a new schema, it should fail
                with pytest.raises(ProgrammingError) as ex_info:
                    session.write_pandas(
                        pd3,
                        table_name,
                        quote_identifiers=quote_identifiers,
                        overwrite=overwrite,
                        auto_create_table=auto_create_table,
                    )
                assert "invalid identifier 'NAME'" in str(ex_info)

        with pytest.raises(SnowparkPandasException) as ex_info:
            session.write_pandas(pd1, "tmp_table")
        assert (
            'Cannot write pandas DataFrame to table "tmp_table" because it does not exist. '
            "Create table before trying to write a pandas DataFrame" in str(ex_info)
        )
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
    table_info = session.sql("show tables like 'tmp_table_basic'").collect()
    assert table_info[0]["kind"] == "TABLE"
    results = df.to_pandas()
    assert_frame_equal(results, pd, check_dtype=False)

    # Try to auto create a table that already exists (should NOT throw an error)
    # and upload data again. We use distinct to compare results since we are
    # uploading data twice
    df = session.write_pandas(pd, "tmp_table_basic", auto_create_table=True)
    results = df.distinct().to_pandas()
    assert_frame_equal(results, pd, check_dtype=False)

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


@pytest.mark.parametrize("table_type", ["", "temp", "temporary", "transient"])
def test_write_pandas_with_table_type(session, table_type: str):
    pd = PandasDF(
        [
            (1, 4.5, "t1"),
            (2, 7.5, "t2"),
            (3, 10.5, "t3"),
        ],
        columns=["id".upper(), "foot_size".upper(), "shoe_model".upper()],
    )

    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        df = session.write_pandas(
            pd,
            table_name,
            table_type=table_type,
            auto_create_table=True,
        )
        results = df.to_pandas()
        assert_frame_equal(results, pd, check_dtype=False)
        Utils.assert_table_type(session, table_name, table_type)
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.parametrize("table_type", ["", "temp", "temporary", "transient"])
def test_write_temp_table_no_breaking_change(session, table_type, caplog):
    pd = PandasDF(
        [
            (1, 4.5, "t1"),
            (2, 7.5, "t2"),
            (3, 10.5, "t3"),
        ],
        columns=["id".upper(), "foot_size".upper(), "shoe_model".upper()],
    )

    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        with caplog.at_level(logging.WARNING):
            df = session.write_pandas(
                pd,
                table_name,
                create_temp_table=True,
                auto_create_table=True,
                table_type=table_type,
            )
        assert "create_temp_table is deprecated" in caplog.text
        results = df.to_pandas()
        assert_frame_equal(results, pd, check_dtype=False)
        Utils.assert_table_type(session, table_name, "temp")
    finally:
        Utils.drop_table(session, table_name)
        # clear the warning dict otherwise it will affect the future tests
        warning_dict.clear()


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

    # TODO(SNOW-677098): Uncomment this test
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


def test_create_dataframe_from_pandas_with_schema(session):
    # TODO(SNOW-677098): Add timestamp test
    # TODO(SNOW-677100): Add complex type test
    pd = PandasDF(
        [
            (1, 4.5, "t1", True, 200),
            (2, 7.5, "t2", False, 250),
            (3, 10.5, "t3", True, 1000),
        ],
        columns=[
            "id".upper(),
            "foot_size".upper(),
            "shoe_model".upper(),
            "received".upper(),
            "ship_distance".upper(),
        ],
    )

    schema = StructType(
        [
            StructField("ID", IntegerType()),
            StructField("FOOT_SIZE", FloatType()),
            StructField("SHOE_MODEL", StringType()),
            StructField("RECEIVED", BooleanType()),
            StructField("SHIP_DISTANCE", DecimalType()),
        ]
    )
    df = session.create_dataframe(pd, schema)
    results = df.to_pandas()
    assert_frame_equal(results, pd, check_dtype=False)


def test_create_dataframe_from_pandas_with_schema_negative(session):
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

    with pytest.raises(ValueError, match="Expects StructType for schema"):
        session.create_dataframe(pd, ["ID", "FOOT_SIZE", "SHOE_MODEL", "RECEIVED"])

    # Missing columns
    schema = StructType(
        [
            StructField("ID", IntegerType()),
            StructField("FOOT_SIZE", IntegerType()),
            StructField("SHOE_MODEL", IntegerType()),
        ]
    )
    with pytest.raises(ProgrammingError, match="invalid identifier"):
        session.create_dataframe(pd, schema)

    # Mismatching names
    schema = StructType(
        [
            StructField("ID", IntegerType()),
            StructField("FOOT_SIZE", FloatType()),
            StructField("SHOE_MODEL", StringType()),
            StructField("WRONG_NAME", BooleanType()),
        ]
    )
    with pytest.raises(ProgrammingError, match="invalid identifier"):
        session.create_dataframe(pd, schema)

    # Mismatching types
    schema = StructType(
        [
            StructField("ID", IntegerType()),
            StructField("FOOT_SIZE", IntegerType()),
            StructField("SHOE_MODEL", IntegerType()),
            StructField("RECEIVED", IntegerType()),
        ]
    )
    with pytest.raises(ProgrammingError, match="Failed to cast"):
        session.create_dataframe(pd, schema)


@pytest.mark.parametrize("table_type", ["", "temp", "temporary", "transient"])
def test_write_pandas_temp_table_and_irregular_column_names(session, table_type):
    pd = PandasDF(
        [
            (1, 4.5, "t1"),
            (2, 7.5, "t2"),
            (3, 10.5, "t3"),
        ],
        columns=["id".upper(), "foot size".upper(), "shoe model".upper()],
    )
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        session.write_pandas(
            pd, table_name, auto_create_table=True, table_type=table_type
        )
        Utils.assert_table_type(session, table_name, table_type)
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
        session.write_pandas(pd, table_name, auto_create_table=True, table_type="temp")
        data = session.sql(f'select * from "{table_name}"').collect()
        assert data[0]["tm_tz"] is not None
        assert data[0]["tm_ntz"] is not None
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
