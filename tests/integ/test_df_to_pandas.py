#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from typing import Iterator

import pandas as pd
import pytest
from pandas import DataFrame as PandasDF, Series as PandasSeries
from pandas.util.testing import assert_frame_equal

from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import SnowparkFetchDataException
from snowflake.snowpark.functions import col
from tests.utils import Utils


def test_to_pandas_new_df_from_range(session):
    # Single column
    snowpark_df = session.range(3, 8)
    pandas_df = snowpark_df.to_pandas()

    assert isinstance(pandas_df, PandasDF)
    assert "ID" in pandas_df
    assert len(pandas_df.columns) == 1
    assert isinstance(pandas_df["ID"], PandasSeries)
    assert all(pandas_df["ID"][i] == i + 3 for i in range(5))

    # Two columns
    snowpark_df = session.range(3, 8).select([col("id"), col("id").alias("other")])
    pandas_df = snowpark_df.to_pandas()

    assert isinstance(pandas_df, PandasDF)
    assert "ID" in pandas_df
    assert "OTHER" in pandas_df
    assert len(pandas_df.columns) == 2
    assert isinstance(pandas_df["ID"], PandasSeries)
    assert all(pandas_df["ID"][i] == i + 3 for i in range(5))
    assert isinstance(pandas_df["OTHER"], PandasSeries)
    assert all(pandas_df["OTHER"][i] == i + 3 for i in range(5))


def test_to_pandas_non_select(session):
    # `with ... select ...` is also a SELECT statement
    isinstance(session.sql("select 1").to_pandas(), PandasDF)
    isinstance(
        session.sql("with mytable as (select 1) select * from mytable").to_pandas(),
        PandasDF,
    )

    # non SELECT statements will fail
    def check_fetch_data_exception(query: str) -> None:
        with pytest.raises(SnowparkFetchDataException) as ex_info:
            session.sql(query).to_pandas()
        assert "the input query can only be a SELECT statement" in str(ex_info.value)

    temp_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    check_fetch_data_exception("show tables")
    check_fetch_data_exception(f"create temporary table {temp_table_name}(a int)")
    check_fetch_data_exception(f"drop table if exists {temp_table_name}")

    # to_pandas should work for the large dataframe
    # batch insertion will run "create" and "insert" first
    df = session.create_dataframe([1] * 2000)
    assert len(df._plan.queries) > 1
    assert df._plan.queries[0].sql.strip().startswith("CREATE")
    assert df._plan.queries[1].sql.strip().startswith("INSERT")
    assert df._plan.queries[2].sql.strip().startswith("SELECT")
    isinstance(df.toPandas(), PandasDF)


def test_to_pandas_batches(session):
    df = session.range(100000).cache_result()
    iterator = df.to_pandas_batches()
    assert isinstance(iterator, Iterator)

    entire_pandas_df = df.to_pandas()
    pandas_df_list = list(df.to_pandas_batches())
    assert len(pandas_df_list) > 1
    assert_frame_equal(pd.concat(pandas_df_list), entire_pandas_df)
