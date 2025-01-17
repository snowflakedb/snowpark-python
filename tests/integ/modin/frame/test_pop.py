#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import numpy as np
import modin.pandas as pd
import pandas as native_pd
import pytest

from tests.integ.modin.utils import assert_frame_equal, assert_series_equal
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=2)
def test_pop():
    native_df = native_pd.DataFrame(
        [
            ("falcon", "bird", 389.0),
            ("parrot", "bird", 24.0),
            ("lion", "mammal", 80.5),
            ("monkey", "mammal", np.nan),
        ],
        columns=("name", "class", "max_speed"),
    )
    snow_df = pd.DataFrame(native_df)

    native_popped_ser = native_df.pop("class")
    snow_popped_ser = snow_df.pop("class")

    assert_series_equal(snow_popped_ser, native_popped_ser)
    assert_frame_equal(snow_df, native_df)


@sql_count_checker(query_count=0)
def test_pop_not_found():
    native_df = native_pd.DataFrame(
        [
            ("falcon", "bird", 389.0),
            ("parrot", "bird", 24.0),
            ("lion", "mammal", 80.5),
            ("monkey", "mammal", np.nan),
        ],
        columns=("name", "class", "max_speed"),
    )
    snow_df = pd.DataFrame(native_df)

    with pytest.raises(KeyError):
        snow_df.pop("not_found")
