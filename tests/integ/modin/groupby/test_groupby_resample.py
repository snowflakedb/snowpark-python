#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter


def test_groupby_resample():
    idx = pd.date_range("1/1/2000", periods=8, freq="min")
    pandas_df = native_pd.DataFrame(
        data=8 * [range(3)], index=idx, columns=["a", "b", "c"]
    )
    pandas_df.iloc[2, 0] = 5
    pandas_df.iloc[5, 0] = 5
    pandas_df.iloc[7, 0] = 5
    pandas_df = pandas_df.reset_index().drop(index=[3, 4, 5]).set_index("index")
    snow_df = pd.DataFrame(pandas_df)
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            pandas_df,
            lambda df: df.groupby("a").resample("3min", include_groups=False).sum(),
            test_attrs=False,
            check_freq=False,
            check_index_type=False,
        )
    # snow_df.groupby("a").resample("3min", include_groups=False).sum()
