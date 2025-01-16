#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import assert_snowpark_pandas_equal_to_pandas
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import running_on_public_ci


# TODO (SNOW-767685): This whole suite is skipped in ci run because those are tests for unsupported
#   APIs, which is time consuming. we will set up a daily jenkins job to run those daily.
@pytest.fixture(scope="module", autouse=True)
def skip(pytestconfig):
    if running_on_public_ci():
        pytest.skip(
            "Disable series str tests for public ci",
            allow_module_level=True,
        )


# TODO (SNOW-863786): import whole pandas/tests/strings/test_extract.py
@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@sql_count_checker(query_count=4)
def test_extract_expand_kwarg_wrong_type_raises():
    values = pd.Series(["fooBAD__barBAD", np.nan, "foo"], dtype=object)
    with pytest.raises(SnowparkSQLException):
        values.str.extract(".*(BAD[_]+).*(BAD)", expand=None)


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@sql_count_checker(query_count=24, fallback_count=3, sproc_count=3)
def test_extract_expand_kwarg():
    s = pd.Series(["fooBAD__barBAD", np.nan, "foo"], dtype=object)
    expected = native_pd.DataFrame(["BAD__", np.nan, np.nan], dtype=object)

    result = s.str.extract(".*(BAD[_]+).*")
    assert_snowpark_pandas_equal_to_pandas(result, expected)

    result = s.str.extract(".*(BAD[_]+).*", expand=True)
    assert_snowpark_pandas_equal_to_pandas(result, expected)

    expected = native_pd.DataFrame(
        [["BAD__", "BAD"], [np.nan, np.nan], [np.nan, np.nan]], dtype=object
    )
    result = s.str.extract(".*(BAD[_]+).*(BAD)", expand=False)
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
def test_extract_expand_capture_groups():
    s = pd.Series(["A1", "B2", "C3"], dtype=object)
    # one group, no matches
    with SqlCounter(query_count=8, sproc_count=1):
        result = s.str.extract("(_)", expand=False)
        expected = native_pd.Series([np.nan, np.nan, np.nan], dtype=object)
        assert_snowpark_pandas_equal_to_pandas(result, expected, check_dtype=False)

    # two groups, no matches
    with SqlCounter(query_count=8, sproc_count=1):
        result = s.str.extract("(_)(_)", expand=False)
        expected = native_pd.DataFrame(
            [[np.nan, np.nan], [np.nan, np.nan], [np.nan, np.nan]], dtype=object
        )
        assert_snowpark_pandas_equal_to_pandas(result, expected, check_dtype=False)

    # one group, some matches
    with SqlCounter(query_count=8, sproc_count=1):
        result = s.str.extract("([AB])[123]", expand=False)
        expected = native_pd.Series(["A", "B", np.nan], dtype=object)
        assert_snowpark_pandas_equal_to_pandas(result, expected, check_dtype=False)

    # two groups, some matches
    with SqlCounter(query_count=8, sproc_count=1):
        result = s.str.extract("([AB])([123])", expand=False)
        expected = native_pd.DataFrame(
            [["A", "1"], ["B", "2"], [np.nan, np.nan]], dtype=object
        )
        assert_snowpark_pandas_equal_to_pandas(result, expected, check_dtype=False)

    # one named group
    with SqlCounter(query_count=8, sproc_count=1):
        result = s.str.extract("(?P<letter>[AB])", expand=False)
        expected = native_pd.Series(["A", "B", np.nan], name="letter", dtype=object)
        assert_snowpark_pandas_equal_to_pandas(result, expected, check_dtype=False)

    # two named groups
    with SqlCounter(query_count=8, sproc_count=1):
        result = s.str.extract("(?P<letter>[AB])(?P<number>[123])", expand=False)
        expected = native_pd.DataFrame(
            [["A", "1"], ["B", "2"], [np.nan, np.nan]],
            columns=["letter", "number"],
            dtype=object,
        )
        assert_snowpark_pandas_equal_to_pandas(result, expected, check_dtype=False)

    # mix named and unnamed groups
    with SqlCounter(query_count=8, sproc_count=1):
        result = s.str.extract("([AB])(?P<number>[123])", expand=False)
        expected = native_pd.DataFrame(
            [["A", "1"], ["B", "2"], [np.nan, np.nan]],
            columns=[0, "number"],
            dtype=object,
        )
        assert_snowpark_pandas_equal_to_pandas(result, expected, check_dtype=False)

    # one normal group, one non-capturing group
    with SqlCounter(query_count=8, sproc_count=1):
        result = s.str.extract("([AB])(?:[123])", expand=False)
        expected = native_pd.Series(["A", "B", np.nan], dtype=object)
        assert_snowpark_pandas_equal_to_pandas(result, expected, check_dtype=False)

    # two normal groups, one non-capturing group
    with SqlCounter(query_count=8, sproc_count=1):
        s = pd.Series(["A11", "B22", "C33"], dtype=object)
        result = s.str.extract("([AB])([123])(?:[123])", expand=False)
        expected = native_pd.DataFrame(
            [["A", "1"], ["B", "2"], [np.nan, np.nan]], dtype=object
        )
        assert_snowpark_pandas_equal_to_pandas(result, expected, check_dtype=False)

    # one optional group followed by one normal group
    with SqlCounter(query_count=8, sproc_count=1):
        s = pd.Series(["A1", "B2", "3"], dtype=object)
        result = s.str.extract("(?P<letter>[AB])?(?P<number>[123])", expand=False)
        expected = native_pd.DataFrame(
            [["A", "1"], ["B", "2"], [np.nan, "3"]],
            columns=["letter", "number"],
            dtype=object,
        )
        assert_snowpark_pandas_equal_to_pandas(result, expected, check_dtype=False)

    # one normal group followed by one optional group
    with SqlCounter(query_count=8, sproc_count=1):
        s = pd.Series(["A1", "B2", "C"], dtype=object)
        result = s.str.extract("(?P<letter>[ABC])(?P<number>[123])?", expand=False)
        expected = native_pd.DataFrame(
            [["A", "1"], ["B", "2"], ["C", np.nan]],
            columns=["letter", "number"],
            dtype=object,
        )
        assert_snowpark_pandas_equal_to_pandas(result, expected, check_dtype=False)
