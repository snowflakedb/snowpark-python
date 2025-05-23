#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import inspect

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_index_equal, assert_series_equal
from tests.integ.utils.sql_counter import SqlCounter


@pytest.mark.parametrize(
    "name, expected_query_count, expected_join_count",
    [
        ("a", 2, 2),
        ("index", 1, 0),
        ("mean", 0, 0),
    ],
)
def test_getattr(name, expected_query_count, expected_join_count):
    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        native = native_pd.Series([1, 2, 2], index=["a", "b", "c"])
        snow = pd.Series(native)
        snow_res = getattr(snow, name)
        native_res = getattr(native, name)

        if isinstance(snow_res, pd.Series):
            assert_series_equal(snow_res, native_res, check_dtype=False)
        elif isinstance(snow_res, pd.Index):
            assert_index_equal(snow_res, native_res)
        elif inspect.ismethod(snow_res):
            # e.g., mean will return bound method similar to pandas
            assert inspect.ismethod(native_res)
            assert type(snow_res) == type(native_res)
        else:
            assert snow_res == native_res


@pytest.mark.parametrize(
    "name, expected_query_count",
    [
        # columns is whitelisted
        ("columns", 0),
        ("unknown", 1),
        ("____id_pack__", 0),
        ("__name__", 0),
        ("_cache", 0),
    ],  # _ATTRS_NO_LOOKUP
)
def test_getattr_negative(name, expected_query_count):
    with SqlCounter(query_count=expected_query_count):
        native = native_pd.Series([1, 2, 2], index=["a", "b", "c"])
        with pytest.raises(AttributeError, match="has no attribute"):
            getattr(native, name)

        snow = pd.Series(native)
        with pytest.raises(AttributeError):
            getattr(snow, name)
