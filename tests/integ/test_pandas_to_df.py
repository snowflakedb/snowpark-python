#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from datetime import datetime

import pytest
from pandas import DataFrame as PandasDF
from pandas.testing import assert_frame_equal

from snowflake.snowpark.exceptions import SnowparkPandasException
from tests.utils import Utils


@pytest.fixture(scope="module")
def tmp_table(session):
    table_name = Utils.random_name()
    Utils.create_table(
        session, table_name, "id integer, foot_size float, shoe_model varchar"
    )
    try:
        yield table_name
    finally:
        Utils.drop_table(session, table_name)


def test_write_pandas(session, tmp_table):
    pd = PandasDF(
        [
            (1, 4.5, "t1"),
            (2, 7.5, "t2"),
            (3, 10.5, "t3"),
        ],
        columns=["id".upper(), "foot_size".upper(), "shoe_model".upper()],
    )

    df = session.write_pandas(pd, tmp_table)
    results = df.toPandas()
    assert_frame_equal(results, pd, check_dtype=False)

    with pytest.raises(SnowparkPandasException) as ex_info:
        df = session.write_pandas(pd, "tmp_table")
    assert (
        'Cannot write pandas DataFrame to table "tmp_table" because it does not exist. '
        "Create table before trying to write a pandas DataFrame" in str(ex_info)
    )


def test_create_dataframe_from_pandas(session, tmp_table):
    pd = PandasDF(
        [
            (1, 4.5, "t1"),
            (2, 7.5, "t2"),
            (3, 10.5, "t3"),
        ],
        columns=["id".upper(), "foot_size".upper(), "shoe_model".upper()],
    )

    df = session.createDataFrame(pd)
    results = df.toPandas()
    assert_frame_equal(results, pd, check_dtype=False)

    with pytest.raises(SnowparkPandasException) as ex_info:
        df = session.write_pandas(pd, "tmp_table")
    assert (
        'Cannot write pandas DataFrame to table "tmp_table" because it does not exist. '
        "Create table before trying to write a pandas DataFrame" in str(ex_info)
    )
