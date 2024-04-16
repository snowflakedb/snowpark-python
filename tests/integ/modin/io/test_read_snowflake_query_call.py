#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import pandas as native_pd

import snowflake.snowpark.modin.pandas as pd
from snowflake.snowpark._internal.utils import TempObjectType
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
)
from tests.utils import Utils


@sql_count_checker(query_count=7, sproc_count=1)
def test_read_snowflake_call_sproc(session):
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
        df = pd.read_snowflake(f"CALL filter_by_role('{table_name}', 'op')")
        native_df = native_pd.DataFrame(
            [[1, "Alice", "op"]], columns=["ID", "NAME", "ROLE"]
        )
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(df, native_df)
    finally:
        session.sql("DROP PROCEDURE filter_by_role(VARCHAR, VARCHAR)").collect()


@sql_count_checker(query_count=5, sproc_count=2)
def test_read_snowflake_call_system_function(session):
    df = pd.read_snowflake("CALL SYSTEM$TYPEOF(TRUE)")
    native_df = native_pd.DataFrame(session.sql("CALL SYSTEM$TYPEOF(TRUE)").collect())
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(df, native_df)
