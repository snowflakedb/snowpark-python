#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
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
from tests.integ.utils.sql_counter import SqlCounter
from tests.utils import Utils


@pytest.mark.parametrize("auto_create_table", [True, False])
@pytest.mark.parametrize("overwrite", [True, False])
@pytest.mark.parametrize("use_logical_type", [True, False])
def test_write_pandas_with_overwrite(
    session,
    auto_create_table: bool,
    overwrite: bool,
    use_logical_type: bool,
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

        with SqlCounter(query_count=5):
            # Create initial table and insert 3 rows
            table1 = session.write_pandas(
                pd1,
                table_name,
                auto_create_table=True,
                use_logical_type=use_logical_type,
            )

            assert_frame_equal(
                pd1, table1.to_pandas(), check_dtype=False, check_index_type=False
            )

        with SqlCounter(query_count=3 if auto_create_table else 4):
            # Insert 1 row
            table2 = session.write_pandas(
                pd2,
                table_name,
                overwrite=overwrite,
                auto_create_table=auto_create_table,
            )
            results = table2.to_pandas()
            if overwrite:
                # Results should match pd2
                assert_frame_equal(
                    results, pd2, check_dtype=False, check_index_type=False
                )
            else:
                # Results count should match pd1 + pd2
                assert results.shape[0] == 4

        if overwrite:
            with SqlCounter(query_count=3 if auto_create_table else 4):
                # In this case, the table is first dropped and since there's a new schema, the results should now match pd3
                table3 = session.write_pandas(
                    pd3,
                    table_name,
                    overwrite=overwrite,
                    auto_create_table=auto_create_table,
                    use_logical_type=use_logical_type,
                )
                results = table3.to_pandas()
                assert_frame_equal(
                    results, pd3, check_dtype=False, check_index_type=False
                )
        else:
            with SqlCounter(query_count=1 if auto_create_table else 2):
                # In this case, the table is truncated but since there's a new schema, it should fail
                with pytest.raises(SnowparkSQLException) as ex_info:
                    session.write_pandas(
                        pd3,
                        table_name,
                        overwrite=overwrite,
                        auto_create_table=auto_create_table,
                        use_logical_type=use_logical_type,
                    )
                assert "Insert value list does not match column list" in str(ex_info)

        with SqlCounter(query_count=1):
            with pytest.raises(SnowparkClientException) as ex_info:
                session.write_pandas(pd1, "tmp_table")
            assert (
                'Cannot write Snowpark pandas DataFrame or Series to table "tmp_table" because it does not exist. '
                "Use auto_create_table = True to create table before writing a Snowpark pandas DataFrame or Series"
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
    try:
        df = pd.DataFrame(
            [
                (1, 4.5, "t1"),
                (2, 7.5, "t2"),
                (3, 10.5, "t3"),
            ],
            columns=["id".upper(), "foot_size".upper(), "shoe_model".upper()],
        )

        with SqlCounter(query_count=4):
            table = session.write_pandas(df, tmp_table_basic, overwrite=True)
            results = table.to_pandas()
            assert_frame_equal(results, df, check_dtype=False)
    finally:
        session._run_query(f'drop table if exists "{tmp_table_basic}"')

    try:
        with SqlCounter(query_count=6):
            table = session.write_pandas(df, tmp_table_basic, auto_create_table=True)
            table_info = session.sql(f"show tables like '{tmp_table_basic}'").collect()
            assert table_info[0]["kind"] == "TABLE"
            results = table.to_pandas()
            assert_frame_equal(results, df, check_dtype=False)

        nonexistent_table = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        with SqlCounter(query_count=1):
            with pytest.raises(SnowparkClientException) as ex_info:
                session.write_pandas(df, nonexistent_table, auto_create_table=False)
                assert (
                    f'Cannot write Snowpark pandas DataFrame or Series to table "{nonexistent_table}" because it does not exist. '
                    "Use auto_create_table = True to create table before writing a Snowpark pandas DataFrame or Series "
                    in str(ex_info)
                )
    finally:
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
        with SqlCounter(query_count=6):
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


def test_write_to_different_schema(session):
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
    quoted_test_schema_name = '"' + test_schema_name + '"'
    try:
        Utils.create_schema(session, quoted_test_schema_name)
        # For owner's rights stored proc test, current schema does not change after creating a new schema
        if not is_in_stored_procedure():
            session.use_schema(original_schema_name)
        assert session.get_current_schema() == original_schema_name
        table_name = random_name_for_temp_object(TempObjectType.TABLE)
        with SqlCounter(query_count=4):
            table = session.write_pandas(
                pd_df,
                table_name,
                schema=test_schema_name,
                auto_create_table=True,
            )
            Utils.check_answer(
                table.sort("id"),
                [Row(1, 4.5, "Nike"), Row(2, 7.5, "Adidas"), Row(3, 10.5, "Puma")],
            )
    finally:
        Utils.drop_schema(session, quoted_test_schema_name)


def test_write_series(session):
    s = pd.Series([1, 2, 3], name="s")
    try:
        table_name = random_name_for_temp_object(TempObjectType.TABLE)
        with SqlCounter(query_count=5):
            table = session.write_pandas(
                s,
                table_name,
                auto_create_table=True,
            )
            assert_frame_equal(s.to_frame(), table.to_pandas(), check_dtype=False)
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.parametrize("quote_identifiers", [True, False])
@pytest.mark.parametrize("is_modin_dataframe", [True, False])
def test_write_pandas_with_quote_identifiers(
    session,
    is_modin_dataframe: bool,
    quote_identifiers: bool,
):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        pd1 = pd.DataFrame(
            [
                (1, 4.5, "Nike"),
                (2, 7.5, "Adidas"),
                (3, 10.5, "Puma"),
            ],
            columns=["id", "foot_size", "shoe_make"],
        )

        if not is_modin_dataframe:
            pd1 = pd1.to_pandas()

        if is_modin_dataframe and not quote_identifiers:
            with SqlCounter(query_count=0):
                with pytest.raises(NotImplementedError):
                    session.write_pandas(
                        pd1,
                        table_name,
                        quote_identifiers=quote_identifiers,
                        auto_create_table=True,
                    )
        else:
            with SqlCounter(query_count=3 if is_modin_dataframe else 0):
                # Create initial table and insert 3 rows
                table1 = session.write_pandas(
                    pd1,
                    table_name,
                    quote_identifiers=quote_identifiers,
                    auto_create_table=True,
                )

                def is_quoted(name: str) -> bool:
                    return name[0] == '"' and name[-1] == '"'

                if quote_identifiers:
                    assert is_quoted(table1.table_name)
                    for col in table1.columns:
                        assert is_quoted(col)
                elif is_modin_dataframe:
                    assert not is_quoted(table1.table_name)
                    for col in table1.columns:
                        assert not is_quoted(col)

    finally:
        Utils.drop_table(session, table_name)
