#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "native_index",
    [
        native_pd.Index([1, None, 4, 3, 4], name="A A A"),
        native_pd.Index([4, None, 1, 3, 4, 1]),
        native_pd.Index([4, None, 1, 3, 4, 1], name="some name"),
        native_pd.Index([1, 10, 4, 3, 4]),
        pytest.param(
            native_pd.Index(
                [
                    pd.Timedelta(1),
                    pd.Timedelta(10),
                    pd.Timedelta(4),
                    pd.Timedelta(3),
                    pd.Timedelta(4),
                ]
            ),
            id="timedelta",
        ),
    ],
)
@pytest.mark.parametrize("func", ["argmax", "argmin"])
@pytest.mark.parametrize("skipna", [True, False])
def test_argmax_argmin_series(native_index, func, skipna):
    snow_index = pd.Index(native_index)

    native_output = native_index.__getattribute__(func)(skipna=skipna)
    snow_output = snow_index.__getattribute__(func)(skipna=skipna)
    assert snow_output == native_output
