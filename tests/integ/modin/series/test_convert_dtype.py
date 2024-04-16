#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import pytest

import snowflake.snowpark.modin.pandas as pd
from tests.integ.modin.sql_counter import sql_count_checker


@sql_count_checker(query_count=0)
def test_NotImplementedError():
    s = pd.Series([0, 1, 2])
    with pytest.raises(NotImplementedError):
        s.convert_dtypes()
