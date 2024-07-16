#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result


@sql_count_checker(query_count=1)
def test_series_ffill():
    native_s = native_pd.Series([1, np.nan, 2, 3])
    snow_s = pd.Series(native_s)
    eval_snowpark_pandas_result(
        snow_s,
        native_s,
        lambda s: s.ffill(),
    )


@sql_count_checker(query_count=1)
def test_series_pad():
    native_s = native_pd.Series([1, np.nan, 2, 3])
    snow_s = pd.Series(native_s)
    eval_snowpark_pandas_result(
        snow_s,
        native_s,
        lambda s: s.pad(),
    )
