#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import math

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

from tests.integ.modin.utils import assert_frame_equal, assert_series_equal
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=4)
def test_apply_sin():
    from snowflake.snowpark.functions import sin

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
    from snowflake.snowpark.functions import _log10

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
    from snowflake.snowpark.functions import trunc

    native_s = native_pd.Series([7, 20.033, 4.09, 7.0, None])
    s = pd.Series(native_s)

    assert_series_equal(s.apply(trunc), native_s.apply(np.trunc))
    assert_series_equal(s.map(trunc), native_s.map(np.trunc))
    assert_frame_equal(
        s.to_frame().applymap(trunc), native_s.to_frame().applymap(np.trunc)
    )
    assert_frame_equal(s.to_frame().apply(trunc), native_s.to_frame().apply(np.trunc))


@sql_count_checker(query_count=4)
def test_apply_log_base(session):
    from snowflake.snowpark.functions import log

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


@sql_count_checker(query_count=0)
def test_apply_snowpark_python_function_not_implemented():
    from snowflake.snowpark.functions import desc, asc

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
