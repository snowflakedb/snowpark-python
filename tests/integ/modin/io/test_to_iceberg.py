#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark._internal.utils import TempObjectType
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
)
from tests.integ.utils.sql_counter import sql_count_checker
from tests.utils import Utils, iceberg_supported


@pytest.fixture(scope="function")
def native_pandas_df_basic():
    native_df = native_pd.DataFrame(
        {
            "ID": [1, 2, 3],
            "FOOT_SIZE": [32.0, 27.0, 40.0],
            "SHOE_MODEL": ["medium", "small", "large"],
        }
    )
    native_df = native_df.set_index("ID")
    return native_df


@pytest.fixture(scope="function")
def native_pandas_ser_basic():
    native_ser = native_pd.Series([4, 7, 4, 2], name="A")
    return native_ser


class TestDataFrame:
    @sql_count_checker(query_count=6)
    def test_to_iceberg(self, session, native_pandas_df_basic, local_testing_mode):
        if not iceberg_supported(session, local_testing_mode):
            pytest.skip("Test requires iceberg support.")

        snow_dataframe = pd.DataFrame(native_pandas_df_basic)

        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        session.sql(
            "CREATE EXTERNAL VOLUME if not exists python_connector_iceberg_exvol"
        ).collect()
        iceberg_config = {
            "external_volume": "python_connector_iceberg_exvol",
            "catalog": "SNOWFLAKE",
            "base_location": "python_connector_merge_gate",
            "storage_serialization_policy": "OPTIMIZED",
        }
        try:
            pd.to_iceberg(
                obj=snow_dataframe,
                table_name=table_name,
                mode="overwrite",
                iceberg_config=iceberg_config,
            )
            snow_dataframe = pd.read_snowflake(table_name)
            snow_dataframe = snow_dataframe.set_index("ID")
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                snow_dataframe, native_pandas_df_basic
            )
        finally:
            Utils.drop_table(session, table_name)

    @sql_count_checker(query_count=0)
    def test_to_iceberg_config_required(self, native_pandas_df_basic):
        snow_dataframe = pd.DataFrame(native_pandas_df_basic)

        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        with pytest.raises(
            TypeError,
            match="missing 1 required keyword-only argument: 'iceberg_config'",
        ):
            pd.to_iceberg(obj=snow_dataframe, table_name=table_name, mode="overwrite")


class TestSeries:
    @sql_count_checker(query_count=6)
    def test_to_iceberg(self, session, native_pandas_ser_basic, local_testing_mode):
        if not iceberg_supported(session, local_testing_mode):
            pytest.skip("Test requires iceberg support.")

        snow_series = pd.Series(native_pandas_ser_basic)

        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        session.sql(
            "CREATE EXTERNAL VOLUME if not exists python_connector_iceberg_exvol"
        ).collect()
        iceberg_config = {
            "external_volume": "python_connector_iceberg_exvol",
            "catalog": "SNOWFLAKE",
            "base_location": "python_connector_merge_gate",
            "storage_serialization_policy": "OPTIMIZED",
        }
        try:
            pd.to_iceberg(
                obj=snow_series,
                table_name=table_name,
                mode="overwrite",
                iceberg_config=iceberg_config,
            )
            snow_dataframe = pd.read_snowflake(table_name)
            snow_series = snow_dataframe["A"]
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                snow_series, native_pandas_ser_basic
            )
        finally:
            Utils.drop_table(session, table_name)

    @sql_count_checker(query_count=0)
    def test_to_iceberg_config_required(self, native_pandas_ser_basic):
        snow_series = pd.Series(native_pandas_ser_basic)

        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        with pytest.raises(
            TypeError,
            match="missing 1 required keyword-only argument: 'iceberg_config'",
        ):
            pd.to_iceberg(obj=snow_series, table_name=table_name, mode="overwrite")
