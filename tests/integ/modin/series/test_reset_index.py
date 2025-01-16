#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=2)
@pytest.mark.parametrize(
    "series", [native_pd.Series([2, 3, 5]), native_pd.Series([2, 3, 5], name="s")]
)
def test_reset_index_drop_true(series):
    eval_snowpark_pandas_result(
        pd.Series(series), series, lambda s: s.reset_index(drop=True)
    )

    eval_snowpark_pandas_result(
        pd.Series(series), series, lambda s: s.reset_index(drop=True, name="new")
    )
