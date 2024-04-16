#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#


import pandas as native_pd
import pytest

import snowflake.snowpark.modin.pandas as pd
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result


@pytest.mark.parametrize(
    "series_input",
    [
        {"A": [[1]]},
        None,
    ],
    ids=[
        "list entry",
        "empty column",
    ],
)
@sql_count_checker(query_count=1)
def test_series_ndim(series_input):
    eval_snowpark_pandas_result(
        pd.Series(series_input),
        native_pd.Series(series_input),
        lambda df: df.empty,
        comparator=lambda x, y: x == y,
    )
