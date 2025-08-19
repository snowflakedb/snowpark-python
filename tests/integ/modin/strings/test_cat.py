#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import running_on_public_ci

# NOTE: Many tests in this file previously fell back to native pandas in stored procedures. These
# have since been updated to expect NotImplementedError, and the original "expected" result has
# been left as a comment to make properly implementing these methods easier.


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


# TODO (SNOW-863786): import whole pandas/tests/strings/test_cat.py
@pytest.mark.parametrize(
    "sep, na_rep, expected",
    [
        (None, None, "aabbc"),
        (None, "-", "aabbc-"),
        ("_", "NA", "a_a_b_b_c_NA"),
    ],
)
@sql_count_checker(query_count=0)
def test_str_cat_single_array(snow_series, sep, na_rep, expected):
    # "str.cat" Series/Index to ndarray/list
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.cat",
    ):
        snow_series.str.cat(sep=sep, na_rep=na_rep)


@sql_count_checker(query_count=0)
def test_str_cat_series_with_array(snow_series):
    t = np.array(["a", np.nan, "b", "d", "foo", np.nan], dtype=object)
    # expected = native_pd.Series(["aa", "a-", "bb", "bd", "cfoo", "--"])

    # Series/Index with array
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.cat",
    ):
        snow_series.str.cat(t, na_rep="-")
    # Series/Index with list
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.cat",
    ):
        snow_series.str.cat(list(t), na_rep="-")


def test_str_cat_incorrect_lengths(snow_series):
    # errors for incorrect lengths
    # rgx = r"If `others` contains arrays or lists \(or other list-likes.*"
    z = pd.Series(["1", "2", "3"])

    with SqlCounter(query_count=1):
        with pytest.raises(
            NotImplementedError,
            match="Snowpark pandas does not yet support the method Series.str.cat",
        ):
            snow_series.str.cat(z.values)

    with SqlCounter(query_count=1):
        with pytest.raises(
            NotImplementedError,
            match="Snowpark pandas does not yet support the method Series.str.cat",
        ):
            # in this case additional row COUNT() query is
            # executed before the error to get the number of rows in z.
            snow_series.str.cat(list(z))
