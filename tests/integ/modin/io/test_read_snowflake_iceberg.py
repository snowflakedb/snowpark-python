#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark._internal.utils import TempObjectType
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
)
from tests.integ.utils.sql_counter import SqlCounter
from tests.utils import Utils


@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_read_snowflake_iceberg(session, enforce_ordering):
    if "azure" in session.connection.host.split("."):
        pytest.skip("This test doesn't work for Azure.")
    if "gcp" in session.connection.host.split("."):
        pytest.skip("This test doesn't work for GCP.")

    with SqlCounter(query_count=2):
        # create iceberg table
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        session.sql(
            "CREATE EXTERNAL VOLUME if not exists python_connector_iceberg_exvol"
        ).collect()
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

    expected_query_counts = [3, 1] if not enforce_ordering else [5, 3]

    with SqlCounter(query_count=expected_query_counts[0]):
        # create dataframe directly from iceberg table
        with SqlCounter(query_count=expected_query_counts[1]):
            # no eager query is used when enforce_ordering is disabled
            # two eager queries are used when enforce_ordering is enabled
            df = pd.read_snowflake(table_name, enforce_ordering=enforce_ordering)

        # convert to pandas dataframe
        pdf = df.to_pandas()

        # compare two dataframes
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(df, pdf)

    expected_query_counts = [3, 1] if not enforce_ordering else [6, 4]

    with SqlCounter(query_count=expected_query_counts[0]):
        # create dataframe from a query referencing an iceberg table
        SQL_QUERY = f"""SELECT count(*) FROM {table_name}"""
        with SqlCounter(query_count=expected_query_counts[1]):
            # no eager query is used when enforce_ordering is disabled
            # two eager queries are used when enforce_ordering is enabled
            df = pd.read_snowflake(SQL_QUERY, enforce_ordering=enforce_ordering)

        # convert to pandas dataframe
        pdf = df.to_pandas()

        # compare two dataframes
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(df, pdf)
