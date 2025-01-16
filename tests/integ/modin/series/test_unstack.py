#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize("level", [-1, 0])
@pytest.mark.parametrize(
    "index_names",
    [
        [None, None],
        ["hello", "world"],
        ["hello", None],
        [None, "world"],
    ],
)
@sql_count_checker(query_count=1)
def test_unstack_multiindex(level, index_names):
    index = native_pd.MultiIndex.from_tuples(
        tuples=[("one", "a"), ("one", "b"), ("two", "a"), ("two", "b")],
        names=index_names,
    )
    native_ser = native_pd.Series(np.arange(1.0, 5.0), index=index)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.unstack(level=level),
        test_attrs=False,  # native pandas does not propagate attrs here but Snowpark pandas does
    )


@sql_count_checker(query_count=0)
def test_unstack_sort_notimplemented():
    index = native_pd.MultiIndex.from_tuples(
        [("one", "a"), ("one", "b"), ("two", "a"), ("two", "b")]
    )
    native_ser = native_pd.Series(np.arange(1.0, 5.0), index=index)
    snow_ser = pd.Series(native_ser)

    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas DataFrame/Series.unstack does not yet support the `sort` parameter",
    ):
        snow_ser.unstack(sort=False)


@sql_count_checker(query_count=0)
def test_unstack_non_integer_level_notimplemented():
    index = native_pd.MultiIndex.from_tuples(
        [("one", "a"), ("one", "b"), ("two", "a"), ("two", "b")]
    )
    native_ser = native_pd.Series(np.arange(1.0, 5.0), index=index)
    snow_ser = pd.Series(native_ser)

    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas DataFrame/Series.unstack does not yet support a non-integer `level` parameter",
    ):
        snow_ser.unstack(level=[0, 1])
