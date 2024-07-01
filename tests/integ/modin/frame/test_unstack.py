#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result


@sql_count_checker(query_count=1)
def test_unstack_input_no_multiindex():
    index = native_pd.MultiIndex.from_tuples(
        [("one", "a"), ("one", "b"), ("two", "a"), ("two", "b")]
    )
    # Note we call unstack below to create a dataframe without a multiindex before
    # calling unstack again
    native_df = native_pd.Series(np.arange(1.0, 5.0), index=index).unstack(level=0)
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.unstack())


@sql_count_checker(query_count=1)
def test_unstack_multiindex():
    index = pd.MultiIndex.from_product([[2, 1], ["a", "b"]])
    native_df = native_pd.DataFrame(np.random.randn(4), index=index, columns=["A"])
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.unstack())


@sql_count_checker(query_count=0)
def test_unstack_sort_notimplemented():
    index = pd.MultiIndex.from_product([[2, 1], ["a", "b"]])
    native_df = native_pd.DataFrame(np.random.randn(4), index=index, columns=["A"])
    snow_df = pd.DataFrame(native_df)

    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas DataFrame/Series.unstack does not yet support the `sort` parameter",
    ):
        snow_df.unstack(sort=False)
