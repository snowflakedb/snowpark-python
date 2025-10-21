#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    create_test_series,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker
from tests.integ.modin.frame.test_interpolate import (
    INTERPOLATE_TEST_DATA,
    INTERPOLATE_DATETIME_DATA,
    LINEAR_FAIL_REASON,
)

pytestmark = pytest.mark.filterwarnings("ignore::FutureWarning")

# More robust cases are covered in the corresponding file in tests/integ/modin/frame/test_interpolate.py


@pytest.mark.parametrize(
    "test_data",
    [
        param(value, id=key)
        for key, value in {**INTERPOLATE_TEST_DATA, **INTERPOLATE_DATETIME_DATA}.items()
    ],
)
@pytest.mark.parametrize(
    "method",
    [
        param("linear", marks=pytest.mark.skip(reason=LINEAR_FAIL_REASON)),
        "pad",
        "bfill",
    ],
)
@sql_count_checker(query_count=1)
def test_series_interpolate(method, test_data):
    eval_snowpark_pandas_result(
        *create_test_series(test_data), lambda s: s.interpolate(method)
    )
