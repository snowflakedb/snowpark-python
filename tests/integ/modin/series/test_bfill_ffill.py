#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize("func", ["backfill", "bfill", "ffill", "pad"])
@sql_count_checker(query_count=1)
def test_series_ffill(func):
    native_s = native_pd.Series([np.nan, 1, np.nan, 2, 3, np.nan])
    snow_s = pd.Series(native_s)
    eval_snowpark_pandas_result(
        snow_s,
        native_s,
        lambda s: getattr(s, func)(),
    )
