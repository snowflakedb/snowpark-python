#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import inspect

import modin.pandas as pd
import pandas
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_index_equal,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize(
    "name, expected_query_count",
    [("a", 1), ("columns", 0), ("index", 1), ("mean", 0)],
)
def test_getattr(name, expected_query_count):
    with SqlCounter(query_count=expected_query_count):
        native_df = native_pd.DataFrame(
            {"a": [1, 2, 2], "b": [3, 4, 5], ("c", "d"): [0, 0, 1]}
        )
        snow = pd.DataFrame(native_df)
        snow_res = getattr(snow, name)
        native_res = getattr(native_df, name)
        if isinstance(snow_res, (pd.Series, pd.DataFrame)):
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                snow_res, native_res
            )
        elif isinstance(snow_res, (pd.Index, pandas.Index)):
            assert_index_equal(snow_res, native_res, exact=False)
        else:
            # e.g., mean will return bound method similar to pandas
            assert inspect.ismethod(snow_res)
            assert inspect.ismethod(native_res)
            assert type(snow_res) == type(native_res)


@pytest.mark.parametrize(
    "name", ["unknown", "____id_pack__", "__name__", "_cache"]  # _ATTRS_NO_LOOKUP
)
@sql_count_checker(query_count=0)
def test_getattr_negative(name):
    native_df = native_pd.DataFrame(
        {"a": [1, 2, 2], "b": [3, 4, 5], ("c", "d"): [0, 0, 1]}
    )
    with pytest.raises(AttributeError, match="has no attribute"):
        getattr(native_df, name)

    snow = pd.DataFrame(native_df)
    with pytest.raises(AttributeError):
        getattr(snow, name)


@sql_count_checker(query_count=3)
def test_attribute_access():
    # any label as string that forms a valid Python identifier according to
    # https://docs.python.org/3/reference/lexical_analysis.html#identifiers
    # and doesn't clash with any defined function is allowed in attribute access
    # test here for case sensitivity and python identifier
    data = {
        "A": [1, 2, 3, 4],
        "a": [1, 3, None, 2],
        "some_randomidentifier12345": [None] * 4,
    }

    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)

    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.A)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.a)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.some_randomidentifier12345
    )


@sql_count_checker(query_count=0)
def test_attribute_access_negative():
    snow_df = pd.DataFrame({"A": [1, 2, 3]})

    # check non-contained column
    with pytest.raises(AttributeError):
        snow_df.X
    # check case-sensitivity
    with pytest.raises(AttributeError):
        snow_df.a
