#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pytest
from pandas._testing import assert_index_equal

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker


@pytest.fixture(params=[True, False])
def ignore_index(request):
    return request.param


@pytest.mark.precommit
@sql_count_checker(query_count=1)
def test_df_sample_cols():
    data = np.random.randint(100, size=(20, 20))

    snow_df = pd.DataFrame(data)

    sampled_df = snow_df.sample(5, axis=1)
    assert sampled_df.shape == (20, 5)


@pytest.mark.parametrize("n", [0, 1, 10, 20])
@sql_count_checker(query_count=4)
def test_df_sample_rows_n(n, ignore_index):
    sample_df = pd.DataFrame(np.random.randint(100, size=(20, 20))).sample(
        n=n, ignore_index=ignore_index
    )
    assert len(sample_df) == n
    assert_index_equal(sample_df.index, sample_df.index)


@pytest.mark.parametrize("frac", [0, 0.1, 0.9, 1])
@sql_count_checker(query_count=4)
def test_df_sample_rows_frac(frac, ignore_index):
    sample_df = pd.DataFrame(np.random.randint(100, size=(20, 20))).sample(
        frac=frac, ignore_index=ignore_index
    )
    assert sample_df.index.is_unique
    assert_index_equal(sample_df.index, sample_df.index)


@pytest.mark.parametrize(
    "ops",
    [
        lambda df: df.sample(weights="abc", axis=1),
    ],
)
@sql_count_checker(query_count=0)
def test_df_sample_negative_value_error(ops):
    with pytest.raises(ValueError):
        ops(pd.DataFrame(np.random.randint(100, size=(20, 20))))
