#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_index_equal,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
)
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.fixture(params=[True, False])
def ignore_index(request):
    return request.param


@pytest.mark.parametrize("n", [None, 0, 1, 8, 10])
@sql_count_checker(query_count=4)
@pytest.mark.parametrize(
    "data",
    [
        param(range(100, 110), id="int_range"),
        param(
            native_pd.timedelta_range(100, 110, freq="ns", closed="left"),
            id="timedelta_range",
        ),
    ],
)
def test_series_sample_n(data, n, ignore_index):
    s = pd.Series(data).sample(n=n, ignore_index=ignore_index)
    assert len(s) == (n if n is not None else 1)
    assert_index_equal(s.index, s.index)


@pytest.mark.parametrize("n", [None, 0, 1, 8, 10, 20])
@sql_count_checker(query_count=4, join_count=1)
def test_series_sample_n_replace(n, ignore_index):
    s = pd.Series(range(100, 110)).sample(n=n, replace=True, ignore_index=ignore_index)
    assert len(s) == (n if n is not None else 1)
    assert_index_equal(s.index, s.index)


@pytest.mark.parametrize("frac", [None, 0, 0.1, 0.5, 0.8, 1])
@sql_count_checker(query_count=4)
def test_series_sample_frac(frac, ignore_index):
    s = pd.Series(range(100, 110)).sample(frac=frac, ignore_index=ignore_index)
    assert s.index.is_unique
    assert_index_equal(s.index, s.index)


@pytest.mark.parametrize("frac", [None, 0, 0.1, 0.5, 0.8, 1, 1.1, 1.5, 1.8, 2])
@sql_count_checker(query_count=3, join_count=1)
def test_series_sample_frac_reply(frac, ignore_index):
    s = pd.Series(range(100, 110)).sample(
        frac=frac, replace=True, ignore_index=ignore_index
    )
    assert_index_equal(s.index, s.index)


@pytest.mark.parametrize(
    "ops",
    [
        lambda s: s.sample(weights="weights"),
        lambda s: s.sample(random_state=1),
    ],
)
@sql_count_checker(query_count=0)
def test_series_sample_not_implemented(ops):
    with pytest.raises(NotImplementedError):
        ops(pd.Series(range(10)))


@pytest.mark.parametrize(
    "ops",
    [
        lambda s: s.sample(n=1, frac=1),
        lambda s: s.sample(axis=1),
        lambda s: s.sample(axis="columns"),
        lambda s: s.sample(n=-1),
        lambda s: s.sample(n=0.1),
        lambda s: s.sample(frac=-1),
    ],
)
@sql_count_checker(query_count=0)
def test_series_sample_negative_value_error(ops):
    with pytest.raises(ValueError):
        ops(pd.Series(range(10)))


@pytest.mark.parametrize(
    "ops",
    [
        lambda s: s.sample(n=100),
    ],
)
@sql_count_checker(query_count=2)
def test_series_sample_over_size_n(ops):
    with pytest.raises(
        ValueError,
        match="Cannot take a larger sample than population when 'replace=False'",
    ):
        native_pd.Series(range(10)).sample(n=100)

    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        pd.Series(range(10)).sample(n=100),
        native_pd.Series(range(10)),
    )
