#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from unittest import mock

import pytest

import snowflake.snowpark.session
from snowflake.snowpark import (
    DataFrame,
    DataFrameNaFunctions,
    DataFrameReader,
    DataFrameStatFunctions,
)
from snowflake.snowpark._internal.analyzer.analyzer import Analyzer
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlanBuilder
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark.dataframe import _get_unaliased


def test_get_unaliased():
    # Basic single-aliased column
    aliased = "l_gdyf_A"
    unaliased = "A"
    values = _get_unaliased(aliased)
    assert len(values) == 1
    assert values[0] == unaliased

    # Double-aliased column
    aliased = "l_gdyf_l_yuif_A"
    unaliased = "l_yuif_A"
    unaliased2 = "A"
    values = _get_unaliased(aliased)
    assert len(values) == 2
    assert values[0] == unaliased
    assert values[1] == unaliased2

    # Column that isn't aliased
    aliased = "l_hfdjishafud_A"
    unaliased = "l_hfdjishafud_A"
    values = _get_unaliased(aliased)
    assert len(values) == 0


def test_dataframe_method_alias():
    assert DataFrame.minus == DataFrame.subtract == DataFrame.except_
    assert DataFrame.where == DataFrame.filter

    # assert aliases for doc generation
    assert (
        DataFrame.approxQuantile
        == DataFrame.approx_quantile
        == DataFrameStatFunctions.approx_quantile
    )
    assert DataFrame.corr == DataFrameStatFunctions.corr
    assert DataFrame.cov == DataFrameStatFunctions.cov
    assert DataFrame.crosstab == DataFrameStatFunctions.crosstab
    assert DataFrame.sampleBy == DataFrame.sample_by == DataFrameStatFunctions.sample_by

    assert DataFrame.dropna == DataFrameNaFunctions.drop
    assert DataFrame.fillna == DataFrameNaFunctions.fill
    assert DataFrame.replace == DataFrameNaFunctions.replace

    # assert aliases for user code migration
    assert DataFrame.createOrReplaceTempView == DataFrame.create_or_replace_temp_view
    assert DataFrame.createOrReplaceView == DataFrame.create_or_replace_view
    assert DataFrame.crossJoin == DataFrame.cross_join
    assert DataFrame.dropDuplicates == DataFrame.drop_duplicates
    assert DataFrame.groupBy == DataFrame.group_by
    assert DataFrame.toDF == DataFrame.to_df
    assert DataFrame.toPandas == DataFrame.to_pandas
    assert DataFrame.unionAll == DataFrame.union_all
    assert DataFrame.unionAllByName == DataFrame.union_all_by_name
    assert DataFrame.unionByName == DataFrame.union_by_name
    assert DataFrame.withColumn == DataFrame.with_column
    assert DataFrame.withColumnRenamed == DataFrame.with_column_renamed

    # assert DataFrame.groupByGroupingSets == DataFrame. group_by_grouping_sets
    # assert DataFrame.joinTableFunction == DataFrame.join_table_function
    # assert DataFrame.naturalJoin == DataFrame.natural_join
    # assert DataFrame.withColumns == DataFrame.with_columns

    # assert aliases because snowpark scala has rename
    assert DataFrame.rename == DataFrame.with_column_renamed

    # Aliases of DataFrameStatFunctions
    assert DataFrameStatFunctions.sampleBy == DataFrameStatFunctions.sample_by
    assert (
        DataFrameStatFunctions.approxQuantile == DataFrameStatFunctions.approx_quantile
    )


@pytest.mark.parametrize(
    "format_type",
    [
        "json",
        "avro",
        "parquet",
        "orc",
    ],
)
def test_copy_into_format_name_syntax(format_type, sql_simplifier_enabled):
    fake_session = mock.create_autospec(snowflake.snowpark.session.Session)
    fake_session.sql_simplifier_enabled = sql_simplifier_enabled
    fake_session._conn = mock.create_autospec(ServerConnection)
    fake_session._plan_builder = SnowflakePlanBuilder(fake_session)
    fake_session._analyzer = Analyzer(fake_session)
    df = getattr(
        DataFrameReader(fake_session).option("format_name", "TEST_FMT"), format_type
    )("@stage/file")
    assert any("FILE_FORMAT  => 'TEST_FMT'" in q for q in df.queries["queries"])
