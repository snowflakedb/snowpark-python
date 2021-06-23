#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from pandas import DataFrame as PandasDF
from pandas import Series as PandasSeries

from src.snowflake.snowpark.functions import col


def test_to_pandas_new_df_from_range(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:

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
