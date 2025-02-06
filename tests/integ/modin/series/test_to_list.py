#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import logging

import numpy as np
import pytest
from numpy.testing import assert_array_equal

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from tests.integ.modin.utils import create_test_series, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


def to_list_comparator(snow, native):
    assert isinstance(
        native, list
    ), f"expected native pandas result to be list, instead got {type(native)}"
    assert isinstance(
        snow, list
    ), f"expected Snowpark pandas result to be list, instead got {type(snow)}"
    # use np testing comparator to handle NaN values
    assert_array_equal(snow, native)


@pytest.mark.parametrize(
    "data",
    [
        [1, 2, 3],
        ["a", "b", "c"],
        [1.1, np.nan],
        [1.1, 2.2],
        [True, False],
        [True, False, None],
        [bytes("snow", "utf-8"), bytes("flake", "utf-8")],
        [bytes("snow", "utf-8"), bytes("flake", "utf-8"), None],
        [datetime.date(2023, 1, 1), datetime.date(2023, 9, 15)],
        [datetime.date(2023, 1, 1), datetime.date(2023, 9, 15), None],
        ["a", 1, "11"],  # mixed types
    ],
)
@sql_count_checker(query_count=1)
def test_to_list(data, caplog):
    caplog.clear()
    WarningMessage.printed_warnings.clear()
    with caplog.at_level(logging.WARNING):
        eval_snowpark_pandas_result(
            *create_test_series(data),
            lambda s: s.to_list(),
            comparator=to_list_comparator,
        )
        assert "The current operation leads to materialization" in caplog.text
