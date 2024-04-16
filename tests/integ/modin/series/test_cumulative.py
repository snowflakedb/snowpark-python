#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import pandas as native_pd
import pytest

import snowflake.snowpark.modin.pandas as pd
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result


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
