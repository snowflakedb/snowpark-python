#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#


import pandas as native_pd
import pytest

import snowflake.snowpark.modin.pandas as pd
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result


@pytest.mark.parametrize(
    "dataframe_input",
    [
        {"A": [[1], [2], [3]], "B": [[4], [5], [6]]},
        {"A": []},
    ],
    ids=[
        "list entry",
        "empty column",
    ],
)
@sql_count_checker(query_count=0)
def test_dataframe_ndim(dataframe_input):
    eval_snowpark_pandas_result(
        pd.DataFrame(dataframe_input),
        native_pd.DataFrame(dataframe_input),
        lambda df: df.ndim,
        comparator=lambda x, y: x == y,
    )
