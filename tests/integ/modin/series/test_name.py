#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize(
    "sample, expected_query_count",
    [
        (native_pd.Series([1, 2, 3], name="abc"), 1),
        (native_pd.Index([1, 2, 3], name="abc"), 1),
        (native_pd.Index([], name="abc"), 1),
        (
            native_pd.Index(
                [("a", "b"), ("a", "c")], tupleize_cols=False, name="('a', 'b')"
            ),
            1,
        ),  # index with tuple values
    ],
)
def test_create_series_from_object_with_name(sample, expected_query_count):
    with SqlCounter(query_count=expected_query_count):
        # name in sample will be kept as series name
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            pd.Series(sample), native_pd.Series(sample)
        )


@sql_count_checker(query_count=0)
def test_series_with_tuple_name():
    names = [("a", 1), ("a", "b", "c"), "flat"]
    s = pd.Series(name=names[0])
    # The internal representation of the Series stores the name as a column label.
    # When it is a tuple, this label is a MultiIndex object, and this test ensures that
    # the Series's name remains a tuple.
    assert s.name == names[0]
    assert isinstance(s.name, tuple)
    # Setting the name to a tuple of a different level or a non-tuple should not error.
    s.name = names[1]
    assert s.name == names[1]
    assert isinstance(s.name, tuple)
    s.name = names[2]
    assert s.name == names[2]
    assert isinstance(s.name, str)


@pytest.mark.parametrize(
    "multiindex",
    [
        native_pd.Index([("a", "b"), ("a", "c")], name=("a", "b")),
    ],
)
@sql_count_checker(query_count=0)
def test_create_series_from_mi_negative(multiindex):
    # name in sample will be kept as series name
    with pytest.raises(
        NotImplementedError,
        match="initializing a Series from a MultiIndex is not supported",
    ):
        native_pd.Series(multiindex)
    with pytest.raises(
        NotImplementedError,
        match="initializing a Series from a MultiIndex is not supported",
    ):
        pd.Series(multiindex)
