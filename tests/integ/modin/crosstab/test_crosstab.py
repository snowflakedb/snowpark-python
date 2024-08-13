#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import assert_snowpark_pandas_equal_to_pandas


@sql_count_checker(query_count=4, join_count=1)
def test_basic_crosstab():
    a = np.array(
        ["foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar", "foo", "foo", "foo"],
        dtype=object,
    )
    b = np.array(
        ["one", "one", "one", "two", "one", "one", "one", "two", "two", "two", "one"],
        dtype=object,
    )
    c = np.array(
        [
            "dull",
            "dull",
            "shiny",
            "dull",
            "dull",
            "shiny",
            "shiny",
            "dull",
            "shiny",
            "shiny",
            "shiny",
        ],
        dtype=object,
    )
    native_df = native_pd.crosstab(a, [b, c], rownames=["a"], colnames=["b", "c"])
    snow_df = pd.crosstab(a, [b, c], rownames=["a"], colnames=["b", "c"])
    assert_snowpark_pandas_equal_to_pandas(snow_df, native_df)


@sql_count_checker(query_count=4, join_count=4, union_count=3)
def test_basic_crosstab_margins():
    a = np.array(
        ["foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar", "foo", "foo", "foo"],
        dtype=object,
    )
    b = np.array(
        ["one", "one", "one", "two", "one", "one", "one", "two", "two", "two", "one"],
        dtype=object,
    )
    c = np.array(
        [
            "dull",
            "dull",
            "shiny",
            "dull",
            "dull",
            "shiny",
            "shiny",
            "dull",
            "shiny",
            "shiny",
            "shiny",
        ],
        dtype=object,
    )
    native_df = native_pd.crosstab(
        a,
        [b, c],
        rownames=["a"],
        colnames=["b", "c"],
        margins=True,
        margins_name="MARGINS_NAME",
    )
    snow_df = pd.crosstab(
        a,
        [b, c],
        rownames=["a"],
        colnames=["b", "c"],
        margins=True,
        margins_name="MARGINS_NAME",
    )
    assert_snowpark_pandas_equal_to_pandas(snow_df, native_df)


@sql_count_checker(query_count=5, join_count=1)
@pytest.mark.parametrize("normalize", [0, 1, True, "all", "index", "columns"])
def test_basic_crosstab_normalize(normalize):
    a = np.array(
        ["foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar", "foo", "foo", "foo"],
        dtype=object,
    )
    b = np.array(
        ["one", "one", "one", "two", "one", "one", "one", "two", "two", "two", "one"],
        dtype=object,
    )
    c = np.array(
        [
            "dull",
            "dull",
            "shiny",
            "dull",
            "dull",
            "shiny",
            "shiny",
            "dull",
            "shiny",
            "shiny",
            "shiny",
        ],
        dtype=object,
    )
    native_df = native_pd.crosstab(
        a, [b, c], rownames=["a"], colnames=["b", "c"], normalize=normalize
    )
    snow_df = pd.crosstab(
        a, [b, c], rownames=["a"], colnames=["b", "c"], normalize=normalize
    )
    assert_snowpark_pandas_equal_to_pandas(snow_df, native_df)
