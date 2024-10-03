#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import pandas as native_pd
import pytest

from tests.integ.modin.frame.test_items import assert_items_results_equal
from tests.integ.modin.utils import create_test_series, eval_snowpark_pandas_result


@pytest.mark.parametrize(
    "series",
    [
        native_pd.Series(
            ["bear", "bear", "marsupial"],
            index=["panda", "polar", "koala"],
        ),
        native_pd.Series(index=["a"]),
        native_pd.Series(data=["a"]),
        native_pd.Series(native_pd.timedelta_range(10, periods=10)),
    ],
)
def test_items(series):
    eval_snowpark_pandas_result(
        *create_test_series(series),
        lambda series: series.items(),
        comparator=assert_items_results_equal,
    )
