#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import numpy as np
import pandas as native_pd
import pytest
from pandas import _testing as tm

import snowflake.snowpark.modin.pandas as pd
from tests.integ.conftest import running_on_public_ci
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import assert_snowpark_pandas_equal_to_pandas


# TODO (SNOW-767685): This whole suite is skipped in ci run because those are tests for unsupported
#   APIs, which is time consuming. we will set up a daily jenkins job to run those daily.
@pytest.fixture(scope="module", autouse=True)
def skip(pytestconfig):
    if running_on_public_ci():
        pytest.skip(
            "Disable series str tests for public ci",
            allow_module_level=True,
        )


# TODO (SNOW-863786): import whole pandas/tests/strings/test_get_dummies.py
@sql_count_checker(query_count=16, fallback_count=2, sproc_count=2)
def test_get_dummies():
    s = pd.Series(["a|b", "a|c", np.nan], dtype=object)
    result = s.str.get_dummies("|")
    expected = native_pd.DataFrame(
        [[1, 1, 0], [1, 0, 1], [0, 0, 0]], columns=list("abc")
    )
    assert_snowpark_pandas_equal_to_pandas(result, expected, check_dtype=False)

    s = pd.Series(["a;b", "a", 7], dtype=object)
    result = s.str.get_dummies(";")
    expected = native_pd.DataFrame(
        [[0, 1, 1], [0, 1, 0], [1, 0, 0]], columns=list("7ab")
    )
    assert_snowpark_pandas_equal_to_pandas(result, expected, check_dtype=False)


@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_get_dummies_with_name_dummy():
    # GH 12180
    # Dummies named 'name' should work as expected
    s = pd.Series(["a", "b,name", "b"], dtype=object)
    result = s.str.get_dummies(",")
    expected = native_pd.DataFrame(
        [[1, 0, 0], [0, 1, 1], [0, 1, 0]], columns=["a", "b", "name"]
    )
    assert_snowpark_pandas_equal_to_pandas(result, expected, check_dtype=False)


@sql_count_checker(query_count=0)
def test_get_dummies_index():
    # GH9980, GH8028
    # This is a complete native pandas execution since Snowpark pandas
    # Index is just an alias of native pandas
    idx = pd.Index(["a|b", "a|c", "b|c"])
    result = idx.str.get_dummies("|")

    expected = pd.MultiIndex.from_tuples(
        [(1, 1, 0), (1, 0, 1), (0, 1, 1)], names=("a", "b", "c")
    )
    tm.assert_index_equal(result, expected)
