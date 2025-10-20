#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re
import modin.pandas as pd
import numpy as np
import pytest
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_index_equal,
    assert_frame_equal,
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.fixture(params=[True, False])
def ignore_index(request):
    return request.param


@pytest.mark.modin_sp_precommit
@sql_count_checker(query_count=0)
def test_df_sample_cols():
    data = np.random.randint(100, size=(20, 20))

    snow_df = pd.DataFrame(data)

    sampled_df = snow_df.sample(5, axis=1)
    assert sampled_df.shape == (20, 5)


@sql_count_checker(query_count=0)
def test_df_sample_cols_random_state():
    df = pd.DataFrame([list(range(100))])
    n = 10
    sampled_with_random_state_1_1 = df.sample(axis=1, n=n, random_state=1)
    sampled_with_random_state_1_2 = df.sample(axis=1, n=n, random_state=1)
    sampled_with_random_state_2 = df.sample(axis=1, n=n, random_state=2)

    # all the samples should have n columns
    assert len(sampled_with_random_state_1_1.columns) == n
    assert len(sampled_with_random_state_1_2.columns) == n
    assert len(sampled_with_random_state_2.columns) == n

    # the two samples with the same random state should choose the same columns
    assert sampled_with_random_state_1_1.columns.equals(
        sampled_with_random_state_1_2.columns
    )
    # two samples with different random states should have different columns with very
    # high probability.
    assert not sampled_with_random_state_1_1.equals(sampled_with_random_state_2.columns)


@pytest.mark.parametrize("n", [0, 1, 10, 20])
@pytest.mark.parametrize(
    "data",
    [
        param(np.random.randint(100, size=(20, 20)), id="ints"),
        param(
            np.random.randint(100, size=(20, 20)).astype("timedelta64[ns]"),
            id="timedelta",
        ),
    ],
)
@sql_count_checker(query_count=4)
def test_df_sample_rows_n(data, n, ignore_index):
    sample_df = pd.DataFrame(data).sample(n=n, ignore_index=ignore_index)
    assert len(sample_df) == n
    assert_index_equal(sample_df.index, sample_df.index)


def test_df_sample_rows_n_random_state():
    input_df = pd.DataFrame(list(range(30)))
    sample_df_random_state_42_1 = input_df.sample(n=15, random_state=42)
    with SqlCounter(query_count=1):
        sample_df_random_state_42_1_pandas = sample_df_random_state_42_1.to_pandas()
    # replace=False, so we should select a unique set of rows.
    assert sample_df_random_state_42_1_pandas.index.is_unique

    # Two different samples with the same random state should be equal.
    sample_df_random_state_42_2 = input_df.sample(n=15, random_state=42)
    sample_df_random_state_42_2_pandas = sample_df_random_state_42_2.to_pandas()
    assert_frame_equal(
        sample_df_random_state_42_1_pandas, sample_df_random_state_42_2_pandas
    )

    # A sample with a different random state should give a different sample with high probability.
    sample_df_random_state_43 = input_df.sample(n=15, random_state=43)
    assert not sample_df_random_state_43.to_pandas().equals(
        sample_df_random_state_42_1_pandas
    )


@pytest.mark.parametrize(
    "random_state", [-1_000_000, 1_000_000, np.int32(0), np.uint64(1)]
)
def test_df_sample_rows_n_random_state_values(random_state):
    df = pd.DataFrame(list(range(30)))
    with SqlCounter(query_count=1):
        assert df.sample(n=30, random_state=random_state).index.is_unique


@pytest.mark.parametrize("n", [0, 1, 10, 20, 30])
@sql_count_checker(query_count=4, join_count=1)
def test_df_sample_rows_n_replace(n, ignore_index):
    sample_df = pd.DataFrame(np.random.randint(100, size=(20, 20))).sample(
        n=n, replace=True, ignore_index=ignore_index
    )
    assert len(sample_df) == n
    assert_index_equal(sample_df.index, sample_df.index)


@pytest.mark.parametrize("n", [0, 1, 10, 20, 30, 10_000])
def test_df_sample_rows_n_replace_random_state(n):
    input_df = pd.DataFrame(list(range(30)))
    sample_df_random_state_42_1 = input_df.sample(n=n, random_state=42, replace=True)
    with SqlCounter(query_count=1, join_count=1):
        sample_df_random_state_42_1_pandas = sample_df_random_state_42_1.to_pandas()

    # Two different samples with the same random state should be equal.
    sample_df_random_state_42_2 = input_df.sample(n=n, random_state=42, replace=True)
    sample_df_random_state_42_2_pandas = sample_df_random_state_42_2.to_pandas()
    assert_frame_equal(
        sample_df_random_state_42_1_pandas, sample_df_random_state_42_2_pandas
    )


@pytest.mark.parametrize("frac", [0, 0.1, 0.9, 1])
@sql_count_checker(query_count=4)
def test_df_sample_rows_frac(frac, ignore_index):
    sample_df = pd.DataFrame(np.random.randint(100, size=(20, 20))).sample(
        frac=frac, ignore_index=ignore_index
    )
    assert sample_df.index.is_unique
    assert_index_equal(sample_df.index, sample_df.index)


def test_df_sample_rows_frac_random_state():
    input_df = pd.DataFrame(list(range(30)))
    sample_df_random_state_42_1 = input_df.sample(frac=0.5, random_state=42)
    with SqlCounter(query_count=1):
        sample_df_random_state_42_1_pandas = sample_df_random_state_42_1.to_pandas()

    # replace=False, so we should select a unique set of rows.
    assert sample_df_random_state_42_1_pandas.index.is_unique

    # Two different samples with the same random state should be equal.
    sample_df_random_state_42_2 = input_df.sample(frac=0.5, random_state=42)
    sample_df_random_state_42_2_pandas = sample_df_random_state_42_2.to_pandas()
    assert_frame_equal(
        sample_df_random_state_42_1_pandas, sample_df_random_state_42_2_pandas
    )

    # With high probability, a sample with a different random state should give
    # a different sample.
    sample_df_random_state_43 = input_df.sample(frac=0.5, random_state=43)
    assert not sample_df_random_state_43.to_pandas().equals(
        sample_df_random_state_42_1_pandas
    )


@pytest.mark.parametrize("frac", [0, 0.1, 0.9, 1, 1.1, 1.9, 2])
@sql_count_checker(query_count=3, join_count=1)
def test_df_sample_rows_frac_replace(frac, ignore_index):
    sample_df = pd.DataFrame(np.random.randint(100, size=(20, 20))).sample(
        frac=frac, replace=True, ignore_index=ignore_index
    )
    assert_index_equal(sample_df.index, sample_df.index)


@pytest.mark.parametrize("frac", [0, 0.1, 0.9, 1, 1.1, 1.9, 2, 10])
def test_df_sample_rows_frac_replace_random_state(frac):
    input_df = pd.DataFrame(list(range(30)))
    sample_df_random_state_42_1 = input_df.sample(
        frac=frac, random_state=42, replace=True
    )
    with SqlCounter(query_count=1, join_count=1):
        sample_df_random_state_42_1_pandas = sample_df_random_state_42_1.to_pandas()

    # Two different samples with the same random state should be equal.
    sample_df_random_state_42_2 = input_df.sample(
        frac=frac, random_state=42, replace=True
    )
    sample_df_random_state_42_2_pandas = sample_df_random_state_42_2.to_pandas()
    assert_frame_equal(
        sample_df_random_state_42_1_pandas, sample_df_random_state_42_2_pandas
    )


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


@pytest.mark.parametrize("random_state", [None, 0])
@sql_count_checker(query_count=0)
def test_df_sample_negative_frac_larger_than_1_no_replace(random_state):
    snow_df, native_df = create_test_dfs(list(range(30)))
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.sample(frac=1.01, random_state=random_state),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match=re.escape(
            "Replace has to be set to `True` when upsampling the population `frac` > 1."
        ),
    )


@pytest.mark.parametrize("random_state", [None, 0])
@sql_count_checker(query_count=0)
def test_df_sample_negative_n_larger_than_size_no_replace(random_state):
    snow_df, native_df = create_test_dfs(list(range(30)))
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.sample(n=31, random_state=random_state),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match=re.escape(
            "Cannot take a larger sample than population when 'replace=False'"
        ),
    )


@sql_count_checker(query_count=0)
def test_df_sample_negative_valid_non_integer_random_state():
    with pytest.raises(
        NotImplementedError,
        match=re.escape("non-integer `random_state` is not supported."),
    ):
        pd.DataFrame([1]).sample(n=1, random_state=np.random.RandomState(seed=1))


@pytest.mark.parametrize("random_state", [4.0, "Wisconsin"])
@sql_count_checker(query_count=0)
def test_df_sample_negative_invalid_non_integer_random_state(random_state):
    snow_df, native_df = create_test_dfs(list(range(30)))
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.sample(n=1, random_state=random_state),
        expect_exception=True,
        assert_exception_equal=False,
        expect_exception_type=ValueError,
        expect_exception_match=re.escape("random_state must be an integer or None"),
    )
