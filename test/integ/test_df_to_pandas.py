#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from test.utils import Utils

import pytest
from pandas import DataFrame as PandasDF, Series as PandasSeries

from snowflake.snowpark.exceptions import SnowparkFetchDataException
from snowflake.snowpark.functions import col


def test_to_pandas_new_df_from_range(session):
    # Single column
    python_df = session.range(3, 8)
    pandas_df = python_df.toPandas()

    assert type(pandas_df) == PandasDF
    assert "ID" in pandas_df
    assert len(pandas_df.columns) == 1
    assert type(pandas_df["ID"]) == PandasSeries
    assert all(pandas_df["ID"][i] == i + 3 for i in range(5))

    # Two columns
    python_df = session.range(3, 8).select([col("id"), col("id").alias("other")])
    pandas_df = python_df.toPandas()

    assert type(pandas_df) == PandasDF
    assert "ID" in pandas_df
    assert "OTHER" in pandas_df
    assert len(pandas_df.columns) == 2
    assert type(pandas_df["ID"]) == PandasSeries
    assert all(pandas_df["ID"][i] == i + 3 for i in range(5))
    assert type(pandas_df["OTHER"]) == PandasSeries
    assert all(pandas_df["OTHER"][i] == i + 3 for i in range(5))


def test_to_pandas_non_select(session):
    # `with ... select ...` is also a SELECT statement
    session.sql("select 1").toPandas()
    session.sql("with mytable as (select 1) select * from mytable").toPandas()

    # non SELECT statements will fail
    def check_fetch_data_exception(query: str) -> None:
        with pytest.raises(SnowparkFetchDataException) as ex_info:
            session.sql(query).toPandas()
        assert "the input query can only be a SELECT statement" in str(ex_info)

    temp_table_name = Utils.random_name()
    check_fetch_data_exception("show tables")
    check_fetch_data_exception(f"create temporary table {temp_table_name}(a int)")
    check_fetch_data_exception(f"drop table if exists {temp_table_name}")

    # toPandas should work for the large dataframe
    # batch insertion will run "create" and "insert" first
    df = session.createDataFrame([1] * 2000)
    assert len(df._DataFrame__plan.queries) > 1
    assert df._DataFrame__plan.queries[0].sql.strip().startswith("CREATE")
    assert df._DataFrame__plan.queries[1].sql.strip().startswith("INSERT")
    assert df._DataFrame__plan.queries[2].sql.strip().startswith("SELECT")
    df.toPandas()
