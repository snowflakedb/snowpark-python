#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

TEST_DATA = [
    {
        "A": [1, 2, 2, 3, 3, 3],
        "B": [2, 3, 3, 1, 1, 1],
    },
    {
        "A": ["1", "2", "2", "3", "3", "3"],
        "B": ["2", "3", "3", "1", "1", "1"],
    },
]


TEST_NULL_DATA = [
    {
        "A": [1, 2, 2, 3, 3, 3, None, np.nan, 4],
        "B": [2, 3, 3, 1, 1, 1, pd.NA, 4, np.nan],
    },
    {
        "A": ["1", "2", "2", "3", "3", "3", None],
        "B": ["2", "3", "3", "1", "1", "1", pd.NA],
    },
]


@pytest.mark.parametrize("test_data", TEST_DATA)
@pytest.mark.parametrize("on_index", [True, False])
@pytest.mark.parametrize(
    "subset",
    [None, "A", "B", ["A"], ["B"], ["A", "B"], ["A", "A", "B"], ["B", "B", "A"]],
)
@pytest.mark.parametrize("dtype", [int, "timedelta64[ns]"])
@sql_count_checker(query_count=1)
def test_value_counts_subset(test_data, on_index, subset, dtype):
    snow_df, native_df = create_test_dfs(test_data, dtype=dtype)
    if on_index:
        snow_df = snow_df.set_index("A")
        native_df = native_df.set_index("A")
    snow_result = snow_df.value_counts(subset=subset)
    native_result = native_df.value_counts(subset=subset)
    if native_result.index.nlevels == 1:
        # In pandas, even if only counting on one column will result in MultiIndex
        # e.g., instead of returning Index([3, 2, 1], dtype='int64', name='A'),
        # pandas returns MultiIndex([(3,), (2,), (1,)], names=['A'])
        # modin has the same issue but it's not resolved yet
        # https://github.com/modin-project/modin/issues/3411
        native_result.index = native_result.index.get_level_values(0)

    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_result, native_result
    )


@pytest.mark.parametrize("test_data", TEST_DATA)
@pytest.mark.parametrize("subset", [["A", "C"], []])
def test_value_counts_subset_negative(test_data, subset):
    snow_df = pd.DataFrame(test_data)
    native_df = native_pd.DataFrame(test_data)

    with SqlCounter(query_count=0):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda x: x.value_counts(subset=subset),
            expect_exception=True,
        )


@pytest.mark.parametrize("test_data", TEST_DATA)
@sql_count_checker(query_count=1)
def test_value_counts_duplicate(test_data):
    native_df = native_pd.DataFrame(test_data)
    native_df["C"] = 1
    native_df.columns = ["A", "B", "B"]
    snow_df = pd.DataFrame(native_df)

    # getting unique column works
    snow_result = snow_df.value_counts(subset=["A"])
    native_result = native_df.value_counts(subset=["A"])
    native_result.index = native_result.index.get_level_values(0)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_result, native_result
    )

    # negative cases
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda x: x.value_counts(),
        expect_exception=True,
        assert_exception_equal=False,
        expect_exception_type=ValueError,
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda x: x.value_counts(subset=["B"]),
        expect_exception=True,
        assert_exception_equal=False,
        expect_exception_type=ValueError,
    )


@pytest.mark.parametrize("test_data", TEST_DATA)
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("ascending", [True, False])
@sql_count_checker(query_count=1)
def test_value_counts_sort_ascending(test_data, sort, ascending):
    snow_df = pd.DataFrame(test_data)
    native_df = native_pd.DataFrame(test_data)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda x: x.value_counts(sort=sort, ascending=ascending),
    )


@pytest.mark.parametrize("test_data", TEST_DATA)
@pytest.mark.parametrize("has_name", [True, False])
@sql_count_checker(query_count=1)
def test_value_counts_normalize(test_data, has_name):
    snow_df = pd.DataFrame(test_data).value_counts(normalize=True)
    native_df = native_pd.DataFrame(test_data).value_counts(normalize=True)
    # snowpark pandas will return a series with decimal type
    assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64(snow_df, native_df)


@pytest.mark.parametrize("test_data", TEST_NULL_DATA)
@pytest.mark.parametrize("dropna", [True, False])
@sql_count_checker(query_count=1)
def test_value_counts_dropna(test_data, dropna):
    if test_data == TEST_NULL_DATA[0] and not dropna:
        pytest.xfail(
            reason="SNOW-1201658"
            # At a glance, the difference between the Snowpark pandas and native pandas output
            # for dropna=False with this data is just that two rows in the index are swapped, as
            # native pandas puts the row (nan, nan) before (nan, 4.0). Normally, this can be circumvented
            # by passing `check_like=True`, but after pandas 2.1.4, the internal call to reindex_like
            # this triggers is mysteriously casting the Snowpark pandas result to float64 and dropping
            # some values. We could not quickly produce a minimal example of this bug, and manually
            # reconstructing the results of the values_count() calls and calling reindex_like on them
            # did not produce the same error. The only difference observed from setting breakpoints in
            # the test is that the MultiIndex generated by Snowpark pandas has a smaller `levels` list than
            # that produced by native pandas:
            #
            # (Pdb) snow_to_native.index.levels
            # FrozenList([[1.0, 2.0, 3.0, 4.0], [1.0, 2.0, 3.0, 4.0]])
            # (Pdb) expected_pandas.index.levels
            # FrozenList([[1.0, 2.0, 3.0, 4.0, nan], [1.0, 2.0, 3.0, nan, 4.0]])
            #
            # TODO: Further investigation is needed to determine why this behavior occurs, and why it changes
            # the reindex_like call as well.
        )
    snow_df = pd.DataFrame(test_data)
    native_df = native_pd.DataFrame(test_data)
    # if NULL value is not dropped, the index will contain NULL
    # Snowpark pandas returns string type but pandas returns mixed type
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda x: x.value_counts(dropna=dropna),
        check_index_type=dropna,
    )


@sql_count_checker(query_count=0)
def test_non_existing_labels():
    # when subset contains non-existing labels, it is unimplemented
    # because of function `get_frame_with_groupby_columns_as_index`
    snow_df = pd.DataFrame({"A": [1, 2, 3]})
    native_df = native_pd.DataFrame({"A": [1, 2, 3]})
    with pytest.raises(NotImplementedError):
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda x: x.value_counts(subset=["A", "B", "C"])
        )
