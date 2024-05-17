#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import logging
from datetime import datetime, timedelta, timezone

import pytest

from snowflake.snowpark.types import TimestampTimeZone, TimestampType

try:
    from pandas import DataFrame as PandasDF, to_datetime
    from pandas.testing import assert_frame_equal
except ImportError:
    pytest.skip("pandas is not available", allow_module_level=True)


from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import Row
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    is_in_stored_procedure,
    random_name_for_temp_object,
)
from snowflake.snowpark.exceptions import SnowparkPandasException
from snowflake.snowpark.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from tests.utils import Utils


@pytest.fixture(scope="module")
def tmp_table_basic(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df = session.create_dataframe(
        data=[],
        schema=StructType(
            [
                StructField("id", IntegerType()),
                StructField("foot_size", FloatType()),
                StructField("shoe_model", StringType()),
            ]
        ),
    )
    df.write.save_as_table(table_name)
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1421323: session.write_pandas is not supported in Local Testing.",
)
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1421323: session.write_pandas is not supported in Local Testing.",
)
def test_write_pandas(session, tmp_table_basic):
    pd = PandasDF(
        [
            (1, 4.5, "t1"),
            (2, 7.5, "t2"),
            (3, 10.5, "t3"),
        ],
        columns=["id".upper(), "foot_size".upper(), "shoe_model".upper()],
    )

    df = session.write_pandas(pd, tmp_table_basic, overwrite=True)
    results = df.to_pandas()
    assert_frame_equal(results, pd, check_dtype=False)

    # Auto create a new table
    session._run_query(f'drop table if exists "{tmp_table_basic}"')
    df = session.write_pandas(pd, tmp_table_basic, auto_create_table=True)
    table_info = session.sql(f"show tables like '{tmp_table_basic}'").collect()
    assert table_info[0]["kind"] == "TABLE"
    results = df.to_pandas()
    assert_frame_equal(results, pd, check_dtype=False)

    # Try to auto create a table that already exists (should NOT throw an error)
    # and upload data again. We use distinct to compare results since we are
    # uploading data twice
    df = session.write_pandas(pd, tmp_table_basic, auto_create_table=True)
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

    nonexistent_table = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    with pytest.raises(SnowparkPandasException) as ex_info:
        df = session.write_pandas(pd, nonexistent_table, auto_create_table=False)
    assert (
        f'Cannot write pandas DataFrame to table "{nonexistent_table}" because it does not exist. '
        "Create table before trying to write a pandas DataFrame" in str(ex_info)
    )

    # Drop tables that were created for this test
    session._run_query(f'drop table if exists "{tmp_table_basic}"')


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1421323: session.write_pandas is not supported in Local Testing.",
)
def test_write_pandas_with_use_logical_type(
    session, tmp_table_basic, local_testing_mode
):
    try:
        data = {
            "pandas_datetime": ["2021-09-30 12:00:00", "2021-09-30 13:00:00"],
            "date": [to_datetime("2010-1-1"), to_datetime("2011-1-1")],
            "datetime.datetime": [
                datetime(2010, 1, 1),
                datetime(2010, 1, 1),
            ],
        }
        pdf = PandasDF(data)
        pdf["pandas_datetime"] = to_datetime(pdf["pandas_datetime"])
        pdf["date"] = pdf["date"].dt.tz_localize("Asia/Phnom_Penh")

        session.write_pandas(
            pdf,
            table_name=tmp_table_basic,
            overwrite=True,
            use_logical_type=True,
            auto_create_table=True,
        )
        df = session.table(tmp_table_basic)
        assert df.schema[0].name == '"pandas_datetime"'
        assert df.schema[1].name == '"date"'
        assert df.schema[2].name == '"datetime.datetime"'
        assert df.schema[0].datatype == TimestampType(TimestampTimeZone.NTZ)
        assert df.schema[1].datatype == TimestampType(TimestampTimeZone.LTZ)
        assert df.schema[2].datatype == TimestampType(TimestampTimeZone.NTZ)

        # https://snowflakecomputing.atlassian.net/browse/SNOW-989169
        # session.write_pandas(
        #     pdf,
        #     table_name=tmp_table_basic,
        #     overwrite=True,
        #     use_logical_type=False,
        # )
        # df = session.table(tmp_table_basic)
        # assert df.schema[0].datatype == LongType()
        # assert (df.schema[1].datatype == LongType()) or (
        #     df.schema[1].datatype == TimestampType(TimestampTimeZone.NTZ)
        # )
        # assert df.schema[2].datatype == LongType()
    finally:
        if not local_testing_mode:
            Utils.drop_table(session, tmp_table_basic)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1421323: session.write_pandas is not supported in Local Testing.",
)
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1421323: session.write_pandas is not supported in Local Testing.",
)
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


@pytest.mark.localtest
def test_create_dataframe_from_pandas(session, local_testing_mode):
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1421323: session.write_pandas is not supported in Local Testing.",
)
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


@pytest.mark.localtest
def test_write_pandas_with_timestamps(session, local_testing_mode):
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

    if local_testing_mode:
        sp_df = session.create_dataframe(pd)
        data = sp_df.select("*").collect()
        assert data[0]["tm_tz"] == datetime(1997, 6, 3, 4, 21, 32, 00)
        assert data[0]["tm_ntz"] == 865347692000000
    else:
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        try:
            session.write_pandas(
                pd, table_name, auto_create_table=True, table_type="temp"
            )
            data = session.sql(f'select * from "{table_name}"').collect()
            assert data[0]["tm_tz"] is not None
            assert data[0]["tm_ntz"] is not None
        finally:
            Utils.drop_table(session, table_name)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1421323: session.write_pandas is not supported in Local Testing.",
)
def test_auto_create_table_similar_column_names(session, local_testing_mode):
    """Tests whether similar names cause issues when auto-creating a table as expected."""
    table_name = "numbas"
    df_data = [(10, 11), (20, 21)]

    df = PandasDF(df_data, columns=["number", "Number"])
    drop_sql = f'DROP TABLE IF EXISTS "{table_name}"'
    try:
        session.write_pandas(
            df, table_name, quote_identifiers=True, auto_create_table=True
        )

        # Check table's contents
        data = session.table(f'"{table_name}"').collect()
        for row in data:
            # The auto create table functionality does not auto-create an incrementing ID
            assert (
                row["number"],
                row["Number"],
            ) in df_data
    finally:
        if not local_testing_mode:
            session.sql(drop_sql).collect()


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL query feature AUTOINCREMENT not supported",
    run=False,
)
@pytest.mark.parametrize("auto_create_table", [True, False])
def test_special_name_quoting(
    session,
    auto_create_table: bool,
):
    """Tests whether special column names get quoted as expected."""
    table_name = random_name_for_temp_object(TempObjectType.TABLE)
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1421323: session.write_pandas is not supported in Local Testing.",
)
def test_write_to_different_schema(session, local_testing_mode):
    pd_df = PandasDF(
        [
            (1, 4.5, "Nike"),
            (2, 7.5, "Adidas"),
            (3, 10.5, "Puma"),
        ],
        columns=["id".upper(), "foot_size".upper(), "shoe_make".upper()],
    )
    original_schema_name = session.get_current_schema()
    test_schema_name = Utils.random_temp_schema()

    try:
        if not local_testing_mode:
            Utils.create_schema(session, test_schema_name)
        # For owner's rights stored proc test, current schema does not change after creating a new schema
        if not is_in_stored_procedure():
            session.use_schema(original_schema_name)
        assert session.get_current_schema() == original_schema_name
        table_name = random_name_for_temp_object(TempObjectType.TABLE)
        session.write_pandas(
            pd_df,
            table_name,
            quote_identifiers=False,
            schema=test_schema_name,
            auto_create_table=True,
        )
        Utils.check_answer(
            session.table(f"{test_schema_name}.{table_name}").sort("id"),
            [Row(1, 4.5, "Nike"), Row(2, 7.5, "Adidas"), Row(3, 10.5, "Puma")],
        )
    finally:
        if not local_testing_mode:
            Utils.drop_schema(session, test_schema_name)
