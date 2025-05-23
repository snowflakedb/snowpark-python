#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import contextlib

import modin.pandas as pd
import pandas as native_pd
import pytest
from pandas._libs.lib import no_default

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker

# From pandas doc example
PCT_CHANGE_NATIVE_SERIES = native_pd.Series(
    [4.0405, 4.0963, 4.3149], index=["a0", "b1", "c2"]
)

# Fake, doctored data with nulls -- do not use this if you actually want to know the prices of the
# French franc, Deutsche Mark, or Italian lira between January/April 1980
PCT_CHANGE_NATIVE_SERIES_WITH_NULLS = native_pd.Series(
    [None, None, 4.0405, 4.0963, None, 4.3149, None],
)


@pytest.mark.parametrize(
    "native_data",
    [PCT_CHANGE_NATIVE_SERIES, PCT_CHANGE_NATIVE_SERIES_WITH_NULLS],
    ids=["nonulls", "withnulls"],
)
@pytest.mark.parametrize(
    "periods",
    [-1, 0, 1, 2],
)
@pytest.mark.parametrize(
    "fill_method", [None, no_default, "ffill", "pad", "backfill", "bfill"]
)
@sql_count_checker(query_count=1)
def test_pct_change_simple(native_data, periods, fill_method):
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
            pd.Series(native_data),
            native_data,
            lambda ser: ser.pct_change(periods=periods, fill_method=fill_method),
        )


@pytest.mark.parametrize(
    "params",
    [
        {"limit": 2},
        {"freq": "ME"},
    ],
)
@sql_count_checker(query_count=0)
def test_pct_change_unsupported_args(params):
    with pytest.raises(NotImplementedError):
        pd.Series(PCT_CHANGE_NATIVE_SERIES).pct_change(**params)


@sql_count_checker(query_count=0)
def test_pct_change_bad_periods():
    # native pandas does not preemptively type check the periods argument
    with pytest.raises(
        TypeError, match="'>' not supported between instances of 'str' and 'int'"
    ):
        PCT_CHANGE_NATIVE_SERIES.pct_change(periods="2", fill_method=None)
    with pytest.raises(
        TypeError, match="periods must be an int. got <class 'str'> instead"
    ):
        pd.Series(PCT_CHANGE_NATIVE_SERIES).pct_change(periods="2", fill_method=None)
