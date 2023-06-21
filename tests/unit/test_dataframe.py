#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
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
from snowflake.snowpark.exceptions import SnowparkCreateDynamicTableException


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
    assert DataFrame.order_by == DataFrame.sort
    assert DataFrame.orderBy == DataFrame.order_by

    # assert DataFrame.groupByGroupingSets == DataFrame. group_by_grouping_sets
    # assert DataFrame.joinTableFunction == DataFrame.join_table_function
    # assert DataFrame.naturalJoin == DataFrame.natural_join
    # assert DataFrame.withColumns == DataFrame.with_columns

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


def test_select_bad_input():
    fake_session = mock.create_autospec(snowflake.snowpark.session.Session)
    fake_session._analyzer = mock.MagicMock()
    df = DataFrame(fake_session)
    with pytest.raises(TypeError) as exc_info:
        df.select(123)
    assert (
        "The input of select() must be Column, column name, TableFunctionCall, or a list of them"
        in str(exc_info)
    )


def test_join_bad_input():
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    df1 = session.create_dataframe([[1, 1, "1"], [2, 2, "3"]]).to_df(
        ["int", "int2", "str"]
    )
    df2 = session.create_dataframe([[1, 1, "1"], [2, 2, "3"]]).to_df(
        ["int", "int2", "str"]
    )

    with pytest.raises(
        TypeError,
        match="All list elements for 'on' or 'using_columns' must be string type.",
    ):
        df1.join(df2, [df1["int"] == df2["int"]])

    with pytest.raises(TypeError) as exc_info:
        df1.join(df2, using_columns=123, join_type="inner")
    assert "Invalid input type for join column:" in str(exc_info)

    with pytest.raises(TypeError) as exc_info:
        df1.join("bad_input", join_type="inner")
    assert "Invalid type for join. Must be Dataframe" in str(exc_info)


def test_with_column_renamed_bad_input():
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    df1 = session.create_dataframe([[1, 1, "1"], [2, 2, "3"]]).to_df(["a", "b", "str"])
    with pytest.raises(TypeError) as exc_info:
        df1.with_column_renamed(123, "int4")
    assert "exisitng' must be a column name or Column object." in str(exc_info)


def test_create_or_replace_view_bad_input():
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    df1 = session.create_dataframe([[1, 1, "1"], [2, 2, "3"]]).to_df(["a", "b", "str"])
    with pytest.raises(TypeError) as exc_info:
        df1.create_or_replace_view(123)
    assert (
        "The input of create_or_replace_view() can only a str or list of strs."
        in str(exc_info)
    )


def test_create_or_replace_dynamic_table_bad_input():
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    df1 = session.create_dataframe([[1, 1, "1"], [2, 2, "3"]]).to_df(["a", "b", "str"])
    with pytest.raises(TypeError) as exc_info:
        df1.create_or_replace_dynamic_table(123, warehouse="warehouse", lag="1 minute")
    assert (
        "The name input of create_or_replace_dynamic_table() can only be a str or list of strs."
        in str(exc_info)
    )
    with pytest.raises(TypeError) as exc_info:
        df1.create_or_replace_dynamic_table(
            ["schema", "dt"], warehouse=123, lag="1 minute"
        )
    assert (
        "The warehouse input of create_or_replace_dynamic_table() can only be a str."
        in str(exc_info)
    )
    with pytest.raises(TypeError) as exc_info:
        df1.create_or_replace_dynamic_table("dt", warehouse="warehouse", lag=123)
    assert (
        "The lag input of create_or_replace_dynamic_table() can only be a str."
        in str(exc_info)
    )

    dml_df = session.sql("SHOW TABLES")
    with pytest.raises(SnowparkCreateDynamicTableException) as exc_info:
        dml_df.create_or_replace_dynamic_table(
            "dt", warehouse="warehouse", lag="100 minute"
        )
    assert "Creating dynamic tables from SELECT queries supported only." in str(
        exc_info
    )


def test_create_or_replace_temp_view_bad_input():
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    df1 = session.create_dataframe([[1, 1, "1"], [2, 2, "3"]]).to_df(["a", "b", "str"])
    with pytest.raises(TypeError) as exc_info:
        df1.create_or_replace_temp_view(123)
    assert (
        "The input of create_or_replace_temp_view() can only a str or list of strs."
        in str(exc_info)
    )


@pytest.mark.parametrize(
    "join_type",
    ["inner", "leftouter", "rightouter", "fullouter", "leftsemi", "leftanti", "cross"],
)
def test_same_joins_should_generate_same_queries(join_type):
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    session._conn._telemetry_client = mock.MagicMock()
    df1 = session.create_dataframe([[1, 1, "1"], [2, 2, "3"]]).to_df(
        ["a1", "b1", "str1"]
    )
    df2 = session.create_dataframe([[2, 2, "2"], [3, 3, "4"]]).to_df(
        ["a2", "b2", "str2"]
    )

    assert df1.join(df2, how=join_type).queries == df1.join(df2, how=join_type).queries


def test_statement_params():
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    session._conn._telemetry_client = mock.MagicMock()
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    statement_params = {"param1": "val", "param2": 0, "param3": True}
    df._statement_params = statement_params
    df.collect()
    _, kwargs = mock_connection.execute.call_args
    assert "_statement_params" in kwargs
    assert all(
        param in kwargs["_statement_params"].items()
        for param in statement_params.items()
    )
