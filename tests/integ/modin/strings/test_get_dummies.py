#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pytest
from pandas import _testing as tm

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker

# NOTE: Many tests in this file previously fell back to native pandas in stored procedures. These
# have since been updated to expect NotImplementedError, and the original "expected" result has
# been left as a comment to make properly implementing these methods easier.


# TODO (SNOW-863786): import whole pandas/tests/strings/test_get_dummies.py
@sql_count_checker(query_count=0)
def test_get_dummies_unimplemented():
    s = pd.Series(["a|b", "a|c", np.nan], dtype=object)
    # expected = native_pd.DataFrame(
    #     [[1, 1, 0], [1, 0, 1], [0, 0, 0]], columns=list("abc")
    # )
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.get_dummies",
    ):
        s.str.get_dummies("|")

    s = pd.Series(["a;b", "a", 7], dtype=object)
    # expected = native_pd.DataFrame(
    #     [[0, 1, 1], [0, 1, 0], [1, 0, 0]], columns=list("7ab")
    # )
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.get_dummies",
    ):
        s.str.get_dummies(";")


@sql_count_checker(query_count=0)
def test_get_dummies_with_name_dummy_unimplemented():
    # GH 12180
    # Dummies named 'name' should work as expected
    s = pd.Series(["a", "b,name", "b"], dtype=object)
    # expected = native_pd.DataFrame(
    #     [[1, 0, 0], [0, 1, 1], [0, 1, 0]], columns=["a", "b", "name"]
    # )
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.get_dummies",
    ):
        s.str.get_dummies(",")


@sql_count_checker(query_count=1)
def test_get_dummies_index():
    # GH9980, GH8028
    idx = pd.Index(["a|b", "a|c", "b|c"])
    result = idx.str.get_dummies("|")

    expected = pd.MultiIndex.from_tuples(
        [(1, 1, 0), (1, 0, 1), (0, 1, 1)], names=("a", "b", "c")
    )
    tm.assert_index_equal(result, expected)
