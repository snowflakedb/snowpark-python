#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.modin.plugin.utils.exceptions import SnowparkPandasException
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import Utils


@sql_count_checker(query_count=5)
def test_read_snowflake_iceberg_relaxed_ordering(session):
    # create iceberg table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.sql(
        f"""
    CREATE OR REPLACE ICEBERG TABLE {table_name} (
        "NESTED_DATA" OBJECT(
            camelCase STRING,
            snake_case STRING,
            PascalCase STRING,
            nested_map MAP(
                STRING,
                OBJECT(
                    inner_camelCase STRING,
                    inner_snake_case STRING,
                    inner_PascalCase STRING
                )
            )
        )
    ) EXTERNAL_VOLUME = 'python_connector_iceberg_exvol' CATALOG = 'SNOWFLAKE' BASE_LOCATION = 'python_connector_merge_gate';
    """
    ).collect()

    # create dataframe directly from iceberg table
    with SqlCounter(query_count=1):
        # Only one query is used when relaxed_ordering is enabled
        df = pd.read_snowflake(table_name, relaxed_ordering=True)

    # convert to pandas dataframe
    pdf = df.to_pandas()

    # compare two dataframes
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(df, pdf)

    # create dataframe from a query referencing an iceberg table
    SQL_QUERY = f"""SELECT count(*) FROM {table_name}"""
    with SqlCounter(query_count=1):
        # Only one query is used when relaxed_ordering is enabled
        df = pd.read_snowflake(SQL_QUERY, relaxed_ordering=True)

    # convert to pandas dataframe
    pdf = df.to_pandas()

    # compare two dataframes
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(df, pdf)


@sql_count_checker(query_count=1)
def test_read_snowflake_iceberg_no_relaxed_ordering_raises(session):
    # create iceberg table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.sql(
        f"""
    CREATE OR REPLACE ICEBERG TABLE {table_name} (
        "NESTED_DATA" OBJECT(
            camelCase STRING,
            snake_case STRING,
            PascalCase STRING,
            nested_map MAP(
                STRING,
                OBJECT(
                    inner_camelCase STRING,
                    inner_snake_case STRING,
                    inner_PascalCase STRING
                )
            )
        )
    ) EXTERNAL_VOLUME = 'python_connector_iceberg_exvol' CATALOG = 'SNOWFLAKE' BASE_LOCATION = 'python_connector_merge_gate';
    """
    ).collect()

    # reading an iceberg table directly fails when relaxed_ordering is disabled
    with pytest.raises(
        SnowparkPandasException,
        match="Clone Iceberg table should use CREATE ICEBERG TABLE CLONE command",
    ):
        pd.read_snowflake(table_name, relaxed_ordering=False)

    # reading an iceberg table indirectly via a query fails when relaxed_ordering is disabled
    SQL_QUERY = f"""SELECT count(*) FROM {table_name}"""
    with pytest.raises(
        SnowparkPandasException,
        match="Clone Iceberg table should use CREATE ICEBERG TABLE CLONE command",
    ):
        pd.read_snowflake(SQL_QUERY, relaxed_ordering=False)
