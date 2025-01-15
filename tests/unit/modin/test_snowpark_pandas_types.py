#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import re
from dataclasses import FrozenInstanceError

import numpy as np
import pandas as native_pd
import pytest

from snowflake.snowpark.modin.plugin._internal.snowpark_pandas_types import (
    SnowparkPandasType,
    TimedeltaType,
)
from snowflake.snowpark.types import LongType


def test_timedelta_type_is_immutable():
    """
    Test that Timedelta instances are immutable.

    We neeed SnowparkPandasType subclasses to be immutable so that we can store
    them in InternalFrame.
    """
    with pytest.raises(
        FrozenInstanceError, match=re.escape("cannot assign to field 'x'")
    ):
        TimedeltaType().x = 3


@pytest.mark.parametrize(
    "pandas_obj, snowpark_pandas_type",
    [
        [native_pd.Timedelta("1 day"), TimedeltaType],
        [np.timedelta64(100, "ns"), TimedeltaType],
        [np.timedelta64(100, "s"), TimedeltaType],
        [
            native_pd.Series(native_pd.Timedelta("1 day")).dtype,
            TimedeltaType,
        ],  # Note dtype is an object not a type
        [datetime.timedelta(days=2), TimedeltaType],
        [123, None],
        ["string", None],
        [native_pd.Interval(left=2, right=5, closed="both"), None],
    ],
)
def test_get_snowpark_pandas_type_for_pandas_type(pandas_obj, snowpark_pandas_type):
    pandas_type = pandas_obj if isinstance(pandas_obj, np.dtype) else type(pandas_obj)
    res = SnowparkPandasType.get_snowpark_pandas_type_for_pandas_type(pandas_type)
    if snowpark_pandas_type:
        assert isinstance(res, snowpark_pandas_type)
    else:
        assert res is None


@pytest.mark.parametrize(
    "timedelta, snowpark_pandas_value",
    [
        [native_pd.Timedelta("1 day"), 24 * 3600 * 10**9],
        [np.timedelta64(100, "ns"), 100],
        [np.timedelta64(100, "s"), 100 * 10**9],
        [
            native_pd.Series(native_pd.Timedelta("10000 day"))[0],
            10000 * 24 * 3600 * 10**9,
        ],
    ],
)
def test_TimedeltaType_from_pandas(timedelta, snowpark_pandas_value):
    assert TimedeltaType.from_pandas(timedelta) == snowpark_pandas_value


def test_equals():
    assert TimedeltaType() == TimedeltaType()
    assert TimedeltaType() != LongType()
    assert LongType() != TimedeltaType()
