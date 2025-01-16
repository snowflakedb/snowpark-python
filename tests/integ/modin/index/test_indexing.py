#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from modin.pandas.utils import is_scalar

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_index_equal
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize(
    "index",
    [
        native_pd.Index([1, 2, 3, 4, 5, 6, 7]),
    ],
)
@pytest.mark.parametrize(
    "key",
    [
        1,
        slice(1, None, None),
        slice(None, None, -2),
        [0, 2, 4],
        [True, False, False, True, False, False, True],
        ...,
    ],
)
def test_index_indexing(index, key):
    if isinstance(key, slice) or key is ... or is_scalar(key):
        join_count = 0  # because slice key uses filter not join
    elif isinstance(key, list) and isinstance(key[0], bool):
        join_count = 1  # because need to join key
    else:
        join_count = 2  # because need to join key and squeeze
    with SqlCounter(query_count=1, join_count=join_count):
        if isinstance(key, (slice, list)) or key is ...:
            assert_index_equal(pd.Index(index)[key], index[key])
        else:
            assert pd.Index(index)[key] == index[key]


@pytest.mark.parametrize(
    "index",
    [
        native_pd.Index([1, 2, 3, 4, 5, 6, 7]),
    ],
)
@pytest.mark.parametrize(
    "key",
    [
        np.array([1, 3, 5]),
        native_pd.Index([0, 1]),
        native_pd.Series([0, 1]),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_index_indexing_other_list_like_key(index, key):
    if isinstance(key, native_pd.Index):
        key1 = pd.Index(key)
    elif isinstance(key, native_pd.Series):
        key1 = pd.Series(key)
    else:
        key1 = key
    assert_index_equal(pd.Index(index)[key1], index[key])


@pytest.mark.parametrize(
    "index",
    [
        native_pd.Index([1, 2, 3, 4, 5, 6, 7]),
    ],
)
@pytest.mark.parametrize(
    "key",
    ["1", ["1"]],
)
@sql_count_checker(query_count=0)
def test_index_indexing_negative(index, key):
    ie = (
        "only integers, slices (`:`), ellipsis (`...`), numpy.newaxis (`None`) and integer or boolean arrays are valid "
        "indices"
    )
    with pytest.raises(IndexError, match=re.escape(ie)):
        index[key]
    with pytest.raises(IndexError, match=re.escape(ie)):
        pd.Index(index)[key]
