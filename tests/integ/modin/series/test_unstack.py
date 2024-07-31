#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result


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
