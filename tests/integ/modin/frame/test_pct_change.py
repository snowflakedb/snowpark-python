#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import contextlib

import modin.pandas as pd
import pandas as native_pd
import pytest
from pandas._libs.lib import no_default

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result

# From pandas doc example
PCT_CHANGE_NATIVE_FRAME = native_pd.DataFrame(
    {
        "FR": [4.0405, 4.0963, 4.3149],
        "GR": [1.7246, 1.7482, 1.8519],
        "IT": [804.74, 810.01, 860.13],
    },
    index=["1980-01-01", "1980-02-01", "1980-03-01"],
)

# Fake, doctored data with nulls -- do not use this if you actually want to know the prices of the
# French franc, Deutsche Mark, or Italian lira between January/April 1980
PCT_CHANGE_NATIVE_FRAME_WITH_NULLS = native_pd.DataFrame(
    {
        "FR": [4.0405, 4.0963, None, 4.3149],
        "GR": [1.7246, None, None, 1.7482],
        "IT": [804.74, 810.01, 860.13, None],
    },
    index=["1980-01-01", "1980-02-01", "1980-03-01", "1980-04-01"],
)


@pytest.mark.parametrize(
    "native_data",
    [PCT_CHANGE_NATIVE_FRAME, PCT_CHANGE_NATIVE_FRAME_WITH_NULLS],
    ids=["nonulls", "withnulls"],
)
@pytest.mark.parametrize(
    "periods",
    [-1, 0, 1, 2],
)
@pytest.mark.parametrize(
    "fill_method", [None, no_default, "ffill", "pad", "backfill", "bfill"]
)
@pytest.mark.parametrize("axis", ["rows", "columns"])
@sql_count_checker(query_count=1)
def test_pct_change_simple(native_data, periods, fill_method, axis):
    # Check deprecation warnings for non-None fill_method
    if fill_method is no_default:
        cm = pytest.warns(
            FutureWarning, match="default fill_method='pad' .* deprecated"
        )
    elif fill_method is not None:
        cm = pytest.warns(
            FutureWarning, match="'fill_method' keyword being not None .* deprecated"
        )
    else:
        cm = contextlib.nullcontext()
    with cm:
        eval_snowpark_pandas_result(
            pd.DataFrame(native_data),
            native_data,
            lambda df: df.pct_change(
                periods=periods, fill_method=fill_method, axis=axis
            ),
        )


@pytest.mark.parametrize("params", [{"limit": 2}, {"freq": "ME"}])
@sql_count_checker(query_count=0)
def test_pct_change_unsupported_args(params):
    with pytest.raises(NotImplementedError):
        pd.DataFrame(PCT_CHANGE_NATIVE_FRAME).pct_change(**params)


@sql_count_checker(query_count=0)
def test_pct_change_bad_periods():
    # native pandas does not preemptively type check the periods argument
    with pytest.raises(
        TypeError, match="'>' not supported between instances of 'str' and 'int'"
    ):
        PCT_CHANGE_NATIVE_FRAME.pct_change(periods="2", fill_method=None)
    with pytest.raises(
        TypeError, match="periods must be an int. got <class 'str'> instead"
    ):
        pd.DataFrame(PCT_CHANGE_NATIVE_FRAME).pct_change(periods="2", fill_method=None)
