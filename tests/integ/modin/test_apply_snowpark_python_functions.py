#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from typing import Union, Any

import math

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas._typing import Scalar
from pandas.core.dtypes.common import is_numeric_dtype

from tests.integ.modin.utils import assert_frame_equal, assert_series_equal
from tests.integ.utils.sql_counter import sql_count_checker
from snowflake.snowpark.functions import trunc, sin, _log10, log, desc, asc


def trunc_with_scale(e: Scalar, scale: Union[Any, int, float] = 0) -> float:
    """
    Util function for validating snowflake.snowpark.functions.trunc in apply
    with pandas input. Rounds the input expression down to the nearest (or equal) integer closer to zero,
    or to the nearest equal or smaller value with the specified number of
    places after the decimal point.

    Args
    ----
    e: The input expression to truncate
    scale: The number of places after the decimal point which to truncate

    Returns
    -------
        truncated input
    """
    assert is_numeric_dtype(type(e))
    return math.trunc(e * 10**scale) / (10**scale)


@sql_count_checker(query_count=4)
def test_apply_sin():

    native_s = native_pd.Series([0.00, -1.23, 10, math.pi, math.pi / 2])
    s = pd.Series(native_s)

    assert_series_equal(s.apply(sin), native_s.apply(math.sin))
    assert_series_equal(s.map(sin), native_s.map(math.sin))
    assert_frame_equal(
        s.to_frame().applymap(sin), native_s.to_frame().applymap(math.sin)
    )
    assert_frame_equal(
        s.to_frame().apply(sin),
        native_s.to_frame().apply(np.sin),  # Note math.sin does not work with df.apply
    )


@sql_count_checker(query_count=4)
def test_apply_log10():

    native_s = native_pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    s = pd.Series(native_s)

    assert_series_equal(s.apply(_log10), native_s.apply(np.log10))
    assert_series_equal(s.map(_log10), native_s.map(np.log10))
    assert_frame_equal(
        s.to_frame().applymap(_log10), native_s.to_frame().applymap(np.log10)
    )
    assert_frame_equal(
        s.to_frame().apply(_log10),
        native_s.to_frame().apply(
            np.log10
        ),  # Note math.sin does not work with df.apply
    )


@sql_count_checker(query_count=4)
def test_apply_trunc_default_scale(session):

    native_s = native_pd.Series([7, 20.033, 4.09, 7.0, None])
    s = pd.Series(native_s)

    assert_series_equal(s.apply(trunc), native_s.apply(np.trunc))
    assert_series_equal(s.map(trunc), native_s.map(np.trunc))
    assert_frame_equal(
        s.to_frame().applymap(trunc), native_s.to_frame().applymap(np.trunc)
    )
    assert_frame_equal(s.to_frame().apply(trunc), native_s.to_frame().apply(np.trunc))


@pytest.mark.parametrize("scale", [-1, 0, 1, 2])
@sql_count_checker(query_count=3)
def test_apply_trunc_scale(session, scale):

    native_s = native_pd.Series(
        [
            -1.0,
            -0.9,
            -0.5234,
            -0.268,
            0.0,
            0.23,
            0.582,
            0.9,
            1.1,
            3.14159,
            31.4159,
            39,
            -31.4159,
            -39,
        ]
    )
    s = pd.Series(native_s)

    assert_series_equal(
        s.apply(trunc, scale=scale), native_s.apply(trunc_with_scale, scale=scale)
    )
    assert_frame_equal(
        s.to_frame().applymap(trunc, scale=scale),
        native_s.to_frame().applymap(trunc_with_scale, scale=scale),
    )
    assert_frame_equal(
        s.to_frame().apply(trunc, scale=scale),
        native_s.to_frame().apply(
            lambda series: series.apply(trunc_with_scale, scale=scale)
        ),
    )


@sql_count_checker(query_count=4)
def test_apply_log_base(session):

    native_s = native_pd.Series([7, 20.033, 4.09, 7.0, 8.33])
    s = pd.Series(native_s)

    assert_series_equal(s.apply(log, base=10), native_s.apply(np.log10))
    assert_series_equal(s.map(log, base=10), native_s.map(np.log10))
    assert_frame_equal(
        s.to_frame().applymap(log, base=10), native_s.to_frame().applymap(np.log10)
    )
    assert_frame_equal(
        s.to_frame().apply(log, base=10), native_s.to_frame().apply(np.log10)
    )


@sql_count_checker(query_count=4)
def test_apply_pass_base_as_column(session):

    native_s = native_pd.Series([7, 20.033, 4.09, 7.0, 8.33])
    s = pd.Series(native_s)

    assert_series_equal(
        s.apply(log, x=2), native_pd.Series(np.emath.logn(n=native_s, x=2))
    )
    assert_series_equal(
        s.map(log, x=2), native_pd.Series(np.emath.logn(n=native_s, x=2))
    )
    assert_frame_equal(
        s.to_frame().applymap(log, x=2),
        native_pd.DataFrame(np.emath.logn(n=native_s, x=2)),
    )
    assert_frame_equal(
        s.to_frame().apply(log, x=2),
        native_pd.DataFrame(np.emath.logn(n=native_s, x=2)),
    )


@sql_count_checker(query_count=0)
def test_apply_snowpark_python_function_not_implemented():

    with pytest.raises(NotImplementedError):
        pd.Series([1, 2, 3]).apply(desc)
    with pytest.raises(NotImplementedError):
        pd.Series([1, 2, 3]).to_frame().apply(asc, na_action="ignore")
    with pytest.raises(NotImplementedError):
        pd.Series([1, 2, 3]).to_frame().applymap(asc, args=[1, 2])
    with pytest.raises(NotImplementedError):
        pd.DataFrame({"a": [1, 2, 3]}).apply(desc)
    with pytest.raises(NotImplementedError):
        pd.DataFrame({"a": [1, 2, 3]}).apply(asc, raw=True)
    with pytest.raises(NotImplementedError):
        pd.DataFrame({"a": [1, 2, 3]}).apply(asc, axis=1)
    with pytest.raises(NotImplementedError):
        pd.DataFrame({"a": [1, 2, 3]}).apply(asc, args=(1, 2))
