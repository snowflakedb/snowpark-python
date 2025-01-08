#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd

from tests.integ.modin.utils import assert_series_equal
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=5, join_count=2)
def test_pop():
    native_ser = native_pd.Series([1, 2, 3])
    snow_ser = pd.Series(native_ser)
    assert isinstance(snow_ser, pd.Series)

    native_popped_val = native_ser.pop(0)
    snow_popped_val = snow_ser.pop(0)

    assert snow_popped_val == native_popped_val
    assert_series_equal(snow_ser, native_ser)
