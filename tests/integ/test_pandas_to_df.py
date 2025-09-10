#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import decimal
import json
import logging
import math
from datetime import date, datetime, time, timedelta, timezone
from unittest import mock

import pytest

try:
    import pandas as pd
    from pandas import DataFrame as PandasDF, to_datetime
    from pandas.testing import assert_frame_equal
except ImportError:
    pytest.skip("pandas is not available", allow_module_level=True)


from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import Row, Table
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    is_in_stored_procedure,
    random_name_for_temp_object,
)
from snowflake.snowpark.exceptions import SnowparkPandasException
from snowflake.snowpark.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
)
from tests.utils import IS_IN_STORED_PROC, Utils


def assert_sorted_frame_equal(actual, expected, **kwargs):
    assert_frame_equal(
        actual.sort_values(by=list(actual.columns)).reset_index(drop=True),
        expected.sort_values(by=list(expected.columns)).reset_index(drop=True),
        **kwargs,
    )


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
        assert_sorted_frame_equal(results, pd1, check_dtype=False)

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
            assert_sorted_frame_equal(results, pd2, check_dtype=False)
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
                assert_sorted_frame_equal(results, pd3, check_dtype=False)
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
    assert_sorted_frame_equal(results, pd, check_dtype=False)

    # Auto create a new table
    session._run_query(f'drop table if exists "{tmp_table_basic}"')
    df = session.write_pandas(pd, tmp_table_basic, auto_create_table=True)
    table_info = session.sql(f"show tables like '{tmp_table_basic}'").collect()
    assert table_info[0]["kind"] == "TABLE"
    results = df.to_pandas()
    assert_sorted_frame_equal(results, pd, check_dtype=False)

    # Try to auto create a table that already exists (should NOT throw an error)
    # and upload data again. We use distinct to compare results since we are
    # uploading data twice
    df = session.write_pandas(pd, tmp_table_basic, auto_create_table=True)
    results = df.distinct().to_pandas()
    assert_sorted_frame_equal(results, pd, check_dtype=False, check_like=True)

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
    # assert_sorted_frame_equal(results, pd, check_dtype=False)

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
        assert_sorted_frame_equal(results, pd, check_dtype=False)
        Utils.assert_table_type(session, table_name, table_type)
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1421323: session.write_pandas is not supported in Local Testing.",
)
@pytest.mark.parametrize("use_vectorized_scanner", [False, True])
def test_write_pandas_use_vectorized_scanner(session, use_vectorized_scanner):
    from snowflake.connector.pandas_tools import write_pandas

    original_func = write_pandas

    def wrapper(*args, **kwargs):
        return original_func(*args, **kwargs)

    pd = PandasDF(
        [
            (1, 4.5, "t1"),
            (2, 7.5, "t2"),
            (3, 10.5, "t3"),
        ],
        columns=["id".upper(), "foot_size".upper(), "shoe_model".upper()],
    )
    tb_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    with mock.patch(
        "snowflake.snowpark.session.write_pandas", side_effect=wrapper
    ) as execute:
        session.write_pandas(
            pd,
            tb_name,
            auto_create_table=True,
            use_vectorized_scanner=use_vectorized_scanner,
        )
        _, kwargs = execute.call_args
        assert kwargs["use_vectorized_scanner"] == use_vectorized_scanner


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
        if not IS_IN_STORED_PROC:
            # SNOW-1437979: caplog.text is empty in sp pre-commit env
            assert "create_temp_table is deprecated" in caplog.text
        results = df.to_pandas()
        assert_sorted_frame_equal(results, pd, check_dtype=False)
        Utils.assert_table_type(session, table_name, "temp")
    finally:
        Utils.drop_table(session, table_name)


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
    assert_sorted_frame_equal(results, pd, check_dtype=False)

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
    # assert_sorted_frame_equal(results, pd, check_dtype=False)


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


def test_write_pandas_with_timestamps(session, local_testing_mode):
    # SNOW-1439717 add support for pandas datetime with tzinfo
    datetime_with_tz = datetime(
        1997, 6, 3, 14, 21, 32, 00, tzinfo=timezone(timedelta(hours=+10))
    )
    datetime_with_ntz = datetime(1997, 6, 3, 14, 21, 32, 00)

    if local_testing_mode:
        pd = PandasDF(
            [
                [datetime_with_ntz],
            ],
            columns=["tm_ntz"],
        )
        sp_df = session.create_dataframe(pd)
        data = sp_df.select("*").collect()
        assert data[0]["tm_ntz"] == datetime(1997, 6, 3, 14, 21, 32)
    else:
        pd = PandasDF(
            [
                [datetime_with_tz, datetime_with_ntz],
            ],
            columns=["tm_tz", "tm_ntz"],
        )
        sp_df = session.create_dataframe(pd)
        data = sp_df.select("*").collect()
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        try:
            session.write_pandas(
                pd,
                table_name,
                auto_create_table=True,
                table_type="temp",
                use_logical_type=True,
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


def test_create_from_pandas_basic_pandas_types(session):
    now_time = datetime(
        year=2023, month=10, day=25, hour=13, minute=46, second=12, microsecond=123
    )
    delta_time = timedelta(days=1)
    pandas_df = pd.DataFrame(
        data=[
            ("Name1", 1.2, 1234567890, True, now_time, delta_time),
            ("nAme_2", 20, 1, False, now_time - delta_time, delta_time),
        ],
        columns=[
            "sTr",
            "dOublE",
            "LoNg",
            "booL",
            "timestamp",
            "TIMEDELTA",  # note that in the current snowpark, column name with all upper case is not double quoted
        ],
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert (
        sp_df.schema[0].name == '"sTr"'
        and isinstance(sp_df.schema[0].datatype, StringType)
        and sp_df.schema[0].nullable
    )
    assert (
        sp_df.schema[1].name == '"dOublE"'
        and isinstance(sp_df.schema[1].datatype, DoubleType)
        and sp_df.schema[1].nullable
    )
    assert (
        sp_df.schema[2].name == '"LoNg"'
        and isinstance(sp_df.schema[2].datatype, LongType)
        and sp_df.schema[2].nullable
    )
    assert (
        sp_df.schema[3].name == '"booL"'
        and isinstance(sp_df.schema[3].datatype, BooleanType)
        and sp_df.schema[3].nullable
    )
    assert (
        sp_df.schema[4].name == '"timestamp"'
        and isinstance(sp_df.schema[4].datatype, TimestampType)
        and sp_df.schema[4].nullable
    )
    assert (
        sp_df.schema[5].name == "TIMEDELTA"
        and isinstance(sp_df.schema[5].datatype, LongType)
        and sp_df.schema[5].nullable
    )
    assert isinstance(sp_df, Table)
    # If max string size is not 16mb then it shows up in the schema definition
    max_size = "" if session._conn.max_string_size == 2**24 else "16777216"
    assert (
        str(sp_df.schema)
        == f"""\
StructType([\
StructField('"sTr"', StringType({max_size}), nullable=True), \
StructField('"dOublE"', DoubleType(), nullable=True), \
StructField('"LoNg"', LongType(), nullable=True), \
StructField('"booL"', BooleanType(), nullable=True), \
StructField('"timestamp"', TimestampType(timezone=TimestampTimeZone('ntz')), nullable=True), \
StructField('TIMEDELTA', LongType(), nullable=True)\
])\
"""
    )
    assert sp_df.select('"sTr"').collect() == [Row("Name1"), Row("nAme_2")]
    assert sp_df.select('"dOublE"').collect() == [Row(1.2), Row(20)]
    assert sp_df.select('"LoNg"').collect() == [Row(1234567890), Row(1)]
    assert sp_df.select('"booL"').collect() == [Row(True), Row(False)]
    assert sp_df.select('"timestamp"').collect() == [
        Row(timestamp=datetime(2023, 10, 25, 13, 46, 12, 123)),
        Row(timestamp=datetime(2023, 10, 24, 13, 46, 12, 123)),
    ]
    assert sp_df.select("TIMEDELTA").collect() == [
        Row(86400000000000),
        Row(86400000000000),
    ]

    pandas_df = pd.DataFrame(
        data=[
            float("inf"),
            float("-inf"),
        ],
        columns=["float"],
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert (
        sp_df.schema[0].name == '"float"'
        and isinstance(sp_df.schema[0].datatype, DoubleType)
        and sp_df.schema[0].nullable
    )

    assert sp_df.select('"float"').collect() == [
        Row(float("inf")),
        Row(float("-inf")),
    ]


def test_create_from_pandas_basic_python_types(session):
    date_data = date(year=2023, month=10, day=26)
    time_data = time(hour=12, minute=12, second=12)
    byte_data = b"bytedata"
    dict_data = {"a": 123}
    array_data = [1, 2, 3, 4]
    decimal_data = decimal.Decimal("1.23")
    pandas_df = pd.DataFrame(
        {
            "A": pd.Series([date_data]),
            "B": pd.Series([time_data]),
            "C": pd.Series([byte_data]),
            "D": pd.Series([dict_data]),
            "E": pd.Series([array_data]),
            "F": pd.Series([decimal_data]),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert (
        str(sp_df.schema)
        == """\
StructType([StructField('A', DateType(), nullable=True), StructField('B', TimeType(), nullable=True), StructField('C', BinaryType(), nullable=True), StructField('D', VariantType(), nullable=True), StructField('E', VariantType(), nullable=True), StructField('F', DecimalType(3, 2), nullable=True)])\
"""
    )
    assert sp_df.select("*").collect() == [
        Row(
            date_data,
            time_data,
            bytearray(byte_data),
            json.dumps(dict_data, indent=2),
            json.dumps(array_data, indent=2),
            decimal_data,
        )
    ]


def test_create_from_pandas_datetime_types(session):
    # SNOW-1439717 support pandas with timestamp containing tzinfo
    now_time = datetime(
        year=2023,
        month=10,
        day=25,
        hour=13,
        minute=46,
        second=12,
        microsecond=123,
    )
    delta_time = timedelta(days=1)
    pandas_df = pd.DataFrame(
        {
            "A": pd.Series([now_time]).dt.tz_localize(None),
            "B": pd.Series([delta_time]),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert sp_df.select("A").collect() == [Row(datetime(2023, 10, 25, 13, 46, 12, 123))]
    assert sp_df.select("B").collect() == [Row(86400000000000)]

    pandas_df = pd.DataFrame(
        {
            "A": pd.Series(
                [
                    datetime(
                        1997,
                        6,
                        3,
                        14,
                        21,
                        32,
                        00,
                    )
                ]
            )
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert (
        str(sp_df.schema)
        == "StructType([StructField('A', TimestampType(timezone=TimestampTimeZone('ntz')), nullable=True)])"
    )
    assert sp_df.select("A").collect() == [Row(datetime(1997, 6, 3, 14, 21, 32, 00))]


def test_create_from_pandas_extension_types(session):
    """

    notes:
        pd.SparseDtype is not supported in the live mode due to pyarrow
    """
    pandas_df = pd.DataFrame(
        {
            "A": pd.Series(["a", "b", "c", "a"], dtype=pd.CategoricalDtype()),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert sp_df.select("A").collect() == [Row("a"), Row("b"), Row("c"), Row("a")]

    pandas_df = pd.DataFrame(
        {
            "A": pd.Series([1, 2, 3], dtype=pd.Int8Dtype()),
            "B": pd.Series([1, 2, 3], dtype=pd.Int16Dtype()),
            "C": pd.Series([1, 2, 3], dtype=pd.Int32Dtype()),
            "D": pd.Series([1, 2, 3], dtype=pd.Int64Dtype()),
            "E": pd.Series([1, 2, 3], dtype=pd.UInt8Dtype()),
            "F": pd.Series([1, 2, 3], dtype=pd.UInt16Dtype()),
            "G": pd.Series([1, 2, 3], dtype=pd.UInt32Dtype()),
            "H": pd.Series([1, 2, 3], dtype=pd.UInt64Dtype()),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert (
        sp_df.select("A").collect()
        == sp_df.select("B").collect()
        == sp_df.select("C").collect()
        == sp_df.select("D").collect()
        == sp_df.select("E").collect()
        == sp_df.select("F").collect()
        == sp_df.select("G").collect()
        == sp_df.select("H").collect()
        == [Row(1), Row(2), Row(3)]
    )

    pandas_df = pd.DataFrame(
        {
            "A": pd.Series([1.1, 2.2, 3.3]),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert sp_df.select("A").collect() == [Row(1.1), Row(2.2), Row(3.3)]

    pandas_df = pd.DataFrame(
        {
            "A": pd.Series([1.1, 2.2, 3.3], dtype=pd.Float64Dtype()),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert sp_df.select("A").collect() == [Row(1.1), Row(2.2), Row(3.3)]

    pandas_df = pd.DataFrame(
        {
            "A": pd.Series(["a", "b", "c"], dtype=pd.StringDtype()),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert sp_df.select("A").collect() == [Row("a"), Row("b"), Row("c")]

    pandas_df = pd.DataFrame(
        {
            "A": pd.Series([True, False, True], dtype=pd.BooleanDtype()),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert sp_df.select("A").collect() == [Row(True), Row(False), Row(True)]

    pandas_df = pd.DataFrame(
        {
            "A": pd.Series(
                [pd.Period("2022-01", freq="M")], dtype=pd.PeriodDtype(freq="M")
            ),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert sp_df.select("A").collect() == [Row(624)]

    pandas_df = pd.DataFrame(
        {
            "A": pd.Series([pd.Interval(left=0, right=5)], dtype=pd.IntervalDtype()),
            "B": pd.Series(
                [
                    pd.Interval(
                        pd.Timestamp("2017-01-01 00:00:00"),
                        pd.Timestamp("2018-01-01 00:00:00"),
                    )
                ],
                dtype=pd.IntervalDtype(),
            ),
        }
    )
    # TODO: SNOW-1338196 on parse_json
    sp_df = session.create_dataframe(data=pandas_df)
    ret = sp_df.select("*").collect()
    assert (
        str(sp_df.schema)
        == """\
StructType([StructField('A', VariantType(), nullable=True), StructField('B', VariantType(), nullable=True)])\
"""
    )

    # when executed in stored proc, the timestamp output format in different env is inconsistent
    # e.g. in regression env timestamp outputs Sun, 01 Jan 2017 00:00:00 Z, while in public deployment it is
    # 2017-01-01 00:00:00.000
    # we should eliminate the gap: SNOW-1253700
    assert ret == [
        Row(
            '{\n  "left": 0,\n  "right": 5\n}',
            '{\n  "left": "2017-01-01 00:00:00.000",\n  "right": "2018-01-01 00:00:00.000"\n}',
        )
    ] or (
        IS_IN_STORED_PROC
        and ret
        == [
            Row(
                '{\n  "left": 0,\n  "right": 5\n}',
                '{\n  "left": "Sun, 01 Jan 2017 00:00:00 Z",\n  "right": "Mon, 01 Jan 2018 00:00:00 Z"\n}',
            )
        ]
    )


def test_na_and_null_data(session):
    pandas_df = pd.DataFrame(
        data={
            "A": pd.Series([1, None, 2, math.nan]),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert sp_df.select("A").collect() == [Row(1.0), Row(None), Row(2.0), Row(None)]

    pandas_df = pd.DataFrame(
        data={
            "A": pd.Series(["abc", None, "a", ""]),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert sp_df.select("A").collect() == [Row("abc"), Row(None), Row("a"), Row("")]


def test_datetime_nat_nan(session):

    df = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [None, "2020-01-13", "2020-02-01", "2020-02-23", "2020-03-05"]
            ),
            "num": [None, 1.0, 2.0, 3.0, 4.0],
        }
    )

    expected_schema = StructType(
        [
            StructField('"date"', TimestampType(TimestampTimeZone.NTZ), nullable=True),
            StructField('"num"', DoubleType(), nullable=True),
        ]
    )
    sf_df = session.create_dataframe(data=df)
    assert sf_df.schema == expected_schema
