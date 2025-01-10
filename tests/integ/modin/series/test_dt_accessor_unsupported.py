#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker


# TODO (SNOW-863790): This test file comes from pandas/tests/series/accessors/test_dt_accessor.py.
#              Pull all tests from this file to enhance the coverage for series.dt methods
class TestSeriesDatetimeValues:
    @pytest.mark.parametrize("freq", ["D", "s", "ms"])
    @sql_count_checker(query_count=0)
    def test_dt_namespace_accessor_datetime64(self, freq):
        # GH#7207, GH#11128
        # test .dt namespace accessor

        # datetimeindex
        dti = native_pd.date_range("20130101", periods=5, freq=freq)
        ser = pd.Series(dti, name="xxx")

        msg = "Snowpark pandas doesn't yet support the property 'Series.dt.freq'"
        with pytest.raises(NotImplementedError, match=msg):
            ser.dt.freq
