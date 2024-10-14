#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import assert_snowpark_pandas_equal_to_pandas
from tests.integ.utils.sql_counter import SqlCounter
from tests.utils import running_on_public_ci


@pytest.fixture(scope="module", autouse=True)
def skip(pytestconfig):
    if running_on_public_ci():
        pytest.skip(
            "Disable series str tests for public ci",
            allow_module_level=True,
        )


@pytest.fixture(scope="function")
def snow_series():
    return pd.Series(["a", "a", "b", "b", "c", np.nan])


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
# TODO (SNOW-863786): import whole pandas/tests/strings/test_cat.py
@pytest.mark.parametrize(
    "sep, na_rep, expected, query_count, sproc_count",
    [
        (None, None, "aabbc", 8, 1),
        (None, "-", "aabbc-", 8, 1),
        ("_", "NA", "a_a_b_b_c_NA", 8, 1),
    ],
)
def test_str_cat_single_array(
    snow_series, sep, na_rep, expected, query_count, sproc_count
):
    # "str.cat" Series/Index to ndarray/list
    with SqlCounter(query_count=query_count, sproc_count=sproc_count):
        result = snow_series.str.cat(sep=sep, na_rep=na_rep)
        assert result == expected


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
def test_str_cat_series_with_array(snow_series):
    t = np.array(["a", np.nan, "b", "d", "foo", np.nan], dtype=object)
    expected = native_pd.Series(["aa", "a-", "bb", "bd", "cfoo", "--"])

    # Series/Index with array
    with SqlCounter(query_count=8, sproc_count=1):
        result = snow_series.str.cat(t, na_rep="-")
        assert_snowpark_pandas_equal_to_pandas(result, expected)
    # Series/Index with list
    with SqlCounter(query_count=8, sproc_count=1):
        result = snow_series.str.cat(list(t), na_rep="-")
        assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
def test_str_cat_incorrect_lengths(snow_series):
    # errors for incorrect lengths
    # rgx = r"If `others` contains arrays or lists \(or other list-likes.*"
    z = pd.Series(["1", "2", "3"])

    with SqlCounter(query_count=5):
        with pytest.raises(SnowparkSQLException):
            # This call is expected to raise SnowparkSQLException after
            # calling the stored procedure, which should produce 5 queries
            # as expected
            snow_series.str.cat(z.values)

    with SqlCounter(query_count=6):
        with pytest.raises(SnowparkSQLException):
            # This call is expected to raise SnowparkSQLException after
            # calling the stored procedure, which should produce 5 queries.
            # However, in this case an additional row COUNT() query is
            # executed before the stored procedure fallback to get the number
            # of rows in z.
            snow_series.str.cat(list(z))
