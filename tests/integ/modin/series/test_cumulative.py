#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize(
    "input_data",
    [
        [0, 1, 2, 3],
        [0.1, 0.2, 0.1, 0],
        [None, 0, None, 0],
        [],
    ],
)
@pytest.mark.parametrize("func_name", ["cumsum", "cummin", "cummax"])
@pytest.mark.parametrize("skipna", [True, False])
@sql_count_checker(query_count=1)
def test_series_cumfunc(input_data, func_name, skipna):
    series = pd.Series(input_data)
    native_series = native_pd.Series(input_data)

    eval_snowpark_pandas_result(
        series,
        native_series,
        lambda s: getattr(s, func_name)(skipna=skipna),
    )
