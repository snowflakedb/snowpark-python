#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re
import modin.pandas as pd
import pandas as native_pd
import pytest
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_index_equal,
    assert_series_equal,
    create_test_series,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


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


def test_series_sample_n_random_state():
    s = pd.Series(list(range(10)))
    sample_random_state_42_1 = s.sample(n=1, random_state=42)
    with SqlCounter(query_count=1):
        sample_random_state_42_1_pandas = sample_random_state_42_1.to_pandas()
    # replace=False, so we should select a unique set of rows.
    assert sample_random_state_42_1_pandas.index.is_unique

    # Two different samples with the same random state should be equal.
    sample_random_state_42_2 = s.sample(n=1, random_state=42)
    sample_random_state_42_2_pandsas = sample_random_state_42_2.to_pandas()
    assert_series_equal(
        sample_random_state_42_1_pandas, sample_random_state_42_2_pandsas
    )

    # A sample with a different random state should give a different sample with high probability.
    sample_random_state_43 = s.sample(n=1, random_state=43)
    assert not sample_random_state_43.to_pandas().equals(
        sample_random_state_42_1_pandas
    )


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


def test_series_sample_frac_random_state():
    s = pd.Series(list(range(30)))
    sample_random_state_42_1 = s.sample(frac=0.5, random_state=42)
    with SqlCounter(query_count=1):
        sample_random_state_42_1_pandas = sample_random_state_42_1.to_pandas()

    # replace=False, so we should select a unique set of rows.
    assert sample_random_state_42_1_pandas.index.is_unique

    # Two different samples with the same random state should be equal.
    sample_random_state_42_2 = s.sample(frac=0.5, random_state=42)
    sample_random_state_42_2_pandas = sample_random_state_42_2.to_pandas()
    assert_series_equal(
        sample_random_state_42_1_pandas, sample_random_state_42_2_pandas
    )

    # With high probability, a sample with a different random state should give
    # a different sample.
    sample_random_state_43 = s.sample(frac=0.5, random_state=43)
    assert not sample_random_state_43.to_pandas().equals(
        sample_random_state_42_1_pandas
    )


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


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("random_state", [None, 0])
def test_series_n_larger_than_length_replace_False(random_state):
    eval_snowpark_pandas_result(
        *create_test_series(["a"]),
        lambda s: s.sample(n=2, replace=False, random_state=random_state),
        expect_exception=True,
        assert_exception_equal=False,
        expect_exception_type=ValueError,
        expect_exception_match=re.escape(
            "Cannot take a larger sample than population when 'replace=False'"
        )
    )
