#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest
from pandas._testing import assert_index_equal

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
)


@pytest.fixture(params=[True, False])
def ignore_index(request):
    return request.param


@pytest.mark.parametrize("n", [None, 0, 1, 8, 10])
@sql_count_checker(query_count=4)
def test_series_sample_n(n, ignore_index):
    s = pd.Series(range(100, 110)).sample(n=n, ignore_index=ignore_index)
    assert len(s) == (n if n is not None else 1)
    # TODO: SNOW-1372242: Remove instances of to_pandas when lazy index is implemented
    assert_index_equal(s.index.to_pandas(), s.index.to_pandas())


@pytest.mark.parametrize("frac", [None, 0, 0.1, 0.5, 0.8, 1])
@sql_count_checker(query_count=3)
def test_series_sample_frac(frac, ignore_index):
    s = pd.Series(range(100, 110)).sample(frac=frac, ignore_index=ignore_index)
    index = s.index.to_pandas()
    assert index.is_unique
    assert_index_equal(index, s.index.to_pandas())


@pytest.mark.parametrize(
    "ops",
    [
        lambda s: s.sample(weights="weights"),
        lambda s: s.sample(replace=True),
        lambda s: s.sample(random_state=1),
        lambda s: s.sample(frac=2),
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
