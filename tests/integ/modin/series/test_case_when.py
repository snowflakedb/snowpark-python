#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import List

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas.api.types import is_scalar

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_series_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


def _verify_case_when(series: native_pd.Series, caselist: List[tuple]) -> None:
    native_res = series.case_when(caselist)
    caselist = [
        (
            pd.Series(cond) if isinstance(cond, native_pd.Series) else cond,
            pd.Series(repl) if isinstance(repl, native_pd.Series) else repl,
        )
        for cond, repl in caselist
    ]
    snow_res = pd.Series(series).case_when(caselist)
    assert_series_equal(snow_res, native_res)


@pytest.mark.parametrize(
    "repl", [native_pd.Series([11, 12, 13, 14]), [11, 12, 13, 14], 99]
)
@pytest.mark.parametrize(
    "cond", [native_pd.Series([True, False, True, False]), [True, False, True, False]]
)
def test_case_when(cond, repl):
    with SqlCounter(query_count=1, join_count=1 if is_scalar(repl) else 2):
        series = native_pd.Series([1, 2, 3, 4])
        _verify_case_when(series, [(cond, repl)])


@sql_count_checker(query_count=1, join_count=1)
def test_case_when_misaligned_index():
    series = native_pd.Series([1, 2, 3, 4, 5, 6])
    cond = native_pd.Series([True, False, True, False, True], index=[0, 1, 2, 6, 7])
    _verify_case_when(series, [(cond, 99)])


@sql_count_checker(query_count=1, join_count=2)
def test_case_when_mulitple_cases():
    series = native_pd.Series([1, 2, 3, 4, 5, 6])
    cond1 = native_pd.Series([True, False, True, False, True])
    cond2 = native_pd.Series([False, True, False, False, True])
    caselist = [(cond1, 98), (cond2, 99)]
    _verify_case_when(series, caselist)


@pytest.mark.parametrize("caselist", [[], [()], [(97, 98, 99)]])
@sql_count_checker(query_count=0)
def test_case_when_invalid_caselist(caselist):
    series = native_pd.Series([1, 2, 3, 4, 5, 6])
    if not caselist:
        error_msg = (
            "provide at least one boolean condition, with a corresponding replacement"
        )
    else:
        error_msg = f"Argument 0 must have length 2; a condition and replacement; instead got length {len(caselist[0])}."

    eval_snowpark_pandas_result(
        pd.Series(series),
        series,
        lambda s: s.case_when(caselist),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match=error_msg,
    )


@sql_count_checker(query_count=0)
def test_case_when_invalid_condition_type():
    series = native_pd.Series([1, 2, 3, 4, 5, 6])
    error_msg = (
        "condition must be a Series or 1-D array-like object; instead got <class 'str'>"
    )
    # Native pandas raises  ValueError('Failed to apply condition0 and replacement0.')
    # Snowpark pandas raise more helpful error message.
    with pytest.raises(TypeError, match=error_msg):
        pd.Series(series).case_when([("xyz", 99)])


@sql_count_checker(query_count=0)
def test_case_when_invalid_replacement_type():
    series = native_pd.Series([1, 2, 3, 4, 5, 6])
    error_msg = "replacement must be a Series, 1-D array-like object or scalar; instead got <class 'numpy.ndarray'>"
    # Native pandas raises  ValueError('Failed to apply condition0 and replacement0.')
    # Snowpark pandas raise more helpful error message.
    with pytest.raises(TypeError, match=error_msg):
        pd.Series(series).case_when([(pd.Series([True, False]), np.array(2))])


@sql_count_checker(query_count=0)
def test_case_when_callable_condition_not_implemented_error():
    series = native_pd.Series([1, 2, 3, 4, 5, 6])
    error_msg = "Snowpark pandas method Series.case_when doesn't yet support callable as condition"
    with pytest.raises(NotImplementedError, match=error_msg):
        pd.Series(series).case_when([(lambda x: x > 3, 99)])


@sql_count_checker(query_count=0)
def test_case_when_callable_replacement_not_implemented_error():
    series = native_pd.Series([1, 2, 3, 4, 5, 6])
    error_msg = "Snowpark pandas method Series.case_when doesn't yet support callable as replacement"
    with pytest.raises(NotImplementedError, match=error_msg):
        pd.Series(series).case_when([(pd.Series([True, False]), lambda x: x > 3)])
