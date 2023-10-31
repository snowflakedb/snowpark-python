#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import datetime

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from snowflake.snowpark import Session
from snowflake.snowpark.mock.connection import MockServerConnection

session = Session(MockServerConnection())


@pytest.mark.localtest
def test_df_to_pandas_df():
    df = session.create_dataframe(
        [
            [
                1,
                1234567890,
                True,
                1.23,
                "abc",
                b"abc",
                datetime.datetime(
                    year=2023, month=10, day=30, hour=12, minute=12, second=12
                ),
            ]
        ],
        schema=[
            "aaa",
            "BBB",
            "cCc",
            "DdD",
            "e e",
            "ff ",
            " gg",
        ],
    )

    to_compare_df = pd.DataFrame(
        {
            "AAA": pd.Series(
                [1], dtype=np.int8
            ),  # int8 is the snowpark behavior, by default pandas use int64
            "BBB": pd.Series(
                [1234567890], dtype=np.int32
            ),  # int32 is the snowpark behavior, by default pandas use int64
            "CCC": pd.Series([True]),
            "DDD": pd.Series([1.23]),
            "e e": pd.Series(["abc"]),
            "ff ": pd.Series([b"abc"]),
            " gg": pd.Series(
                [
                    datetime.datetime(
                        year=2023, month=10, day=30, hour=12, minute=12, second=12
                    )
                ]
            ),
        }
    )

    # assert_frame_equal also checks dtype
    assert_frame_equal(df.to_pandas(), to_compare_df)
    assert_frame_equal(list(df.to_pandas_batches())[0], to_compare_df)
