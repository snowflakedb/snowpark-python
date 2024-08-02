#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker


@sql_count_checker(query_count=2)
def test_datetime_index_construction():
    # create from native pandas datetime index.
    index = native_pd.DatetimeIndex(["2021-01-01", "2021-01-02", "2021-01-03"])
    snow_index = pd.Index(index)
    assert isinstance(snow_index, pd.DatetimeIndex)

    # create from query compiler with timestamp type.
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=index)
    snow_index = df.index
    assert isinstance(snow_index, pd.DatetimeIndex)

    # create from snowpark pandas datetime index.
    snow_index = pd.Index(pd.DatetimeIndex([123]))
    assert isinstance(snow_index, pd.DatetimeIndex)


@sql_count_checker(query_count=0)
def test_datetime_index_construction_negative():
    # Try to create datatime index query compiler with int type.
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    msg = "DatetimeIndex can only be created from a query compiler with TimestampType"
    with pytest.raises(ValueError, match=msg):
        pd.DatetimeIndex(df._query_compiler)
