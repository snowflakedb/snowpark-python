#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import Utils


@pytest.mark.parametrize(
    "relaxed_ordering",
    [
        pytest.param(
            True,
            marks=pytest.mark.skip(
                "Queries with CALL statements raise a SQL compilation "
                "error when relaxed_ordering=True"
            ),
        ),
        False,
    ],
)
def test_read_snowflake_call_sproc(session, relaxed_ordering):
    expected_query_count = 7 if not relaxed_ordering else 5
    with SqlCounter(query_count=expected_query_count, sproc_count=1):
        session.sql(
            """
        CREATE OR REPLACE PROCEDURE filter_by_role(tableName VARCHAR, role VARCHAR)
        RETURNS TABLE(id NUMBER, name VARCHAR, role VARCHAR)
        LANGUAGE PYTHON
        RUNTIME_VERSION = '3.8'
        PACKAGES = ('snowflake-snowpark-python')
        HANDLER = 'filter_by_role'
        AS $$from snowflake.snowpark.functions import col
def filter_by_role(session, table_name, role):
    df = session.table(table_name)
    return df.filter(col('role') == role)
                $$"""
        ).collect()
        try:
            table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
            session.sql(
                f"""CREATE OR REPLACE TEMPORARY TABLE {table_name}(id NUMBER, name VARCHAR, role VARCHAR) AS SELECT * FROM VALUES(1, 'Alice', 'op'), (2, 'Bob', 'dev')"""
            ).collect()
            df = pd.read_snowflake(
                f"CALL filter_by_role('{table_name}', 'op')",
                relaxed_ordering=relaxed_ordering,
            )
            native_df = native_pd.DataFrame(
                [[1, "Alice", "op"]], columns=["ID", "NAME", "ROLE"]
            )
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(df, native_df)
        finally:
            session.sql("DROP PROCEDURE filter_by_role(VARCHAR, VARCHAR)").collect()


@sql_count_checker(query_count=3)
def test_read_snowflake_call_sproc_relaxed_ordering_neg(session):
    session.sql(
        """
        CREATE OR REPLACE PROCEDURE filter_by_role(tableName VARCHAR, role VARCHAR)
        RETURNS TABLE(id NUMBER, name VARCHAR, role VARCHAR)
        LANGUAGE PYTHON
        RUNTIME_VERSION = '3.8'
        PACKAGES = ('snowflake-snowpark-python')
        HANDLER = 'filter_by_role'
        AS $$from snowflake.snowpark.functions import col
def filter_by_role(session, table_name, role):
    df = session.table(table_name)
    return df.filter(col('role') == role)
                $$"""
    ).collect()
    try:
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        session.sql(
            f"""CREATE OR REPLACE TEMPORARY TABLE {table_name}(id NUMBER, name VARCHAR, role VARCHAR) AS SELECT * FROM VALUES(1, 'Alice', 'op'), (2, 'Bob', 'dev')"""
        ).collect()
        with pytest.raises(
            SnowparkSQLException,
            match="unexpected 'CALL'",
        ):
            pd.read_snowflake(
                f"CALL filter_by_role('{table_name}', 'op')",
                relaxed_ordering=True,
            ).head()
    finally:
        session.sql("DROP PROCEDURE filter_by_role(VARCHAR, VARCHAR)").collect()


@pytest.mark.parametrize("relaxed_ordering", [True, False])
def test_read_snowflake_system_function(session, relaxed_ordering):
    expected_query_count = 4 if not relaxed_ordering else 2
    with SqlCounter(query_count=expected_query_count):
        df = pd.read_snowflake(
            "SELECT SYSTEM$TYPEOF(TRUE)",
            relaxed_ordering=relaxed_ordering,
        )
        native_df = session.sql("SELECT SYSTEM$TYPEOF(TRUE)").to_pandas()
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(df, native_df)
