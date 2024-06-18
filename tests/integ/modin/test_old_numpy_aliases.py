#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker


@sql_count_checker(query_count=0)
def test_old_numpy_aliases():
    # Test to ensure that the np.float_ and np.NaN aliases (removed in numpy 2.x)
    # remain valid for use in Snowpark pandas, which is still on numpy 1.26.
    s = pd.Series([0.1, np.NaN])
    assert s.dtype == np.float_
    assert s.dtype == np.float64
    assert s.isnull()[1]
