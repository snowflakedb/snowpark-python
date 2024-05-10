#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pytest

from snowflake.snowpark import Row
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    is_in_stored_procedure,
    random_name_for_temp_object,
)
from snowflake.snowpark.exceptions import SnowparkClientException, SnowparkSQLException
from snowflake.snowpark.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from tests.integ.modin.utils import assert_frame_equal
from tests.utils import Utils


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
        pd1 = pd.DataFrame(
            [
                (1, 4.5, "Nike"),
                (2, 7.5, "Adidas"),
                (3, 10.5, "Puma"),
            ],
            columns=["id".upper(), "foot_size".upper(), "shoe_make".upper()],
        )

        pd2 = pd.DataFrame(
            [(1, 8.0, "Dia Dora")],
            columns=["id".upper(), "foot_size".upper(), "shoe_make".upper()],
        )

        pd3 = pd.DataFrame(
            [(1, "dash", 1000, 32)],
            columns=["id".upper(), "name".upper(), "points".upper(), "age".upper()],
        )

        # Create initial table and insert 3 rows
        table1 = session.write_pandas(
            pd1, table_name, quote_identifiers=quote_identifiers, auto_create_table=True
        )

        assert_frame_equal(pd1, table1.to_pandas(), check_dtype=False)

        # Insert 1 row
        table2 = session.write_pandas(
            pd2,
            table_name,
            quote_identifiers=quote_identifiers,
            overwrite=overwrite,
            auto_create_table=auto_create_table,
        )
        results = table2.to_pandas()
        if overwrite:
            # Results should match pd2
            assert_frame_equal(results, pd2, check_dtype=False)
        else:
            # Results count should match pd1 + pd2
            assert results.shape[0] == 4

        if overwrite:
            # In this case, the table is first dropped and since there's a new schema, the results should now match pd3
            table3 = session.write_pandas(
                pd3,
                table_name,
                quote_identifiers=quote_identifiers,
                overwrite=overwrite,
                auto_create_table=auto_create_table,
            )
            results = table3.to_pandas()
            assert_frame_equal(results, pd3, check_dtype=False)
        else:
            # In this case, the table is truncated but since there's a new schema, it should fail
            with pytest.raises(SnowparkSQLException) as ex_info:
                session.write_pandas(
                    pd3,
                    table_name,
                    quote_identifiers=quote_identifiers,
                    overwrite=overwrite,
                    auto_create_table=auto_create_table,
                )
            assert "Insert value list does not match column list" in str(ex_info)

        with pytest.raises(SnowparkClientException) as ex_info:
            session.write_pandas(pd1, "tmp_table")
        assert (
            'Cannot write Snowpark pandas DataFrame to table "tmp_table" because it does not exist. '
            "Use auto_create_table = True to create table before writing a Snowpark pandas DataFrame"
            in str(ex_info)
        )
    finally:
        Utils.drop_table(session, table_name)
        Utils.drop_table(session, "tmp_table")


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


def test_write_pandas(session, tmp_table_basic):
    df = pd.DataFrame(
        [
            (1, 4.5, "t1"),
            (2, 7.5, "t2"),
            (3, 10.5, "t3"),
        ],
        columns=["id".upper(), "foot_size".upper(), "shoe_model".upper()],
    )

    table = session.write_pandas(df, tmp_table_basic, overwrite=True)
    results = table.to_pandas()
    assert_frame_equal(results, df, check_dtype=False)

    # Auto create a new table
    session._run_query(f'drop table if exists "{tmp_table_basic}"')
    table = session.write_pandas(df, tmp_table_basic, auto_create_table=True)
    table_info = session.sql(f"show tables like '{tmp_table_basic}'").collect()
    assert table_info[0]["kind"] == "TABLE"
    results = table.to_pandas()
    assert_frame_equal(results, df, check_dtype=False)

    nonexistent_table = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    with pytest.raises(SnowparkClientException) as ex_info:
        session.write_pandas(df, nonexistent_table, auto_create_table=False)
        assert (
            f'Cannot write Snowpark pandas DataFrame to table "{nonexistent_table}" because it does not exist. '
            "Use auto_create_table = True to create table before writing a Snowpark pandas DataFrame"
            in str(ex_info)
        )

    # Drop tables that were created for this test
    session._run_query(f'drop table if exists "{tmp_table_basic}"')


@pytest.mark.parametrize("table_type", ["", "temp", "temporary", "transient"])
def test_write_pandas_with_table_type(session, table_type: str):
    df = pd.DataFrame(
        [
            (1, 4.5, "t1"),
            (2, 7.5, "t2"),
            (3, 10.5, "t3"),
        ],
        columns=["id".upper(), "foot_size".upper(), "shoe_model".upper()],
    )

    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        table = session.write_pandas(
            df,
            table_name,
            table_type=table_type,
            auto_create_table=True,
        )
        results = table.to_pandas()
        assert_frame_equal(results, df, check_dtype=False)
        Utils.assert_table_type(session, table_name, table_type)
    finally:
        Utils.drop_table(session, table_name)


def test_write_to_different_schema(session, local_testing_mode):
    pd_df = pd.DataFrame(
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
