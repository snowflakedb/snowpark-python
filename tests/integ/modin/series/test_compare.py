#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import create_test_series, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker

# (+1 query, +0 join) materialize first series's index for comparison if multi-index
# (+1 query, +0 join) materialize second series's index for comparison if multi-index
# (+1 query, +1 join) row count query for joining the two series and checking
#                     whether all rows match.
# (+1 query, +1 join) materialize query that joins the two series and checks
#                     whether all rows match.
# (+1 query, +1 join) convert final comparison result with join to pandas
QUERY_COUNT = 5
JOIN_COUNT = 3


@pytest.fixture
def base_series() -> native_pd.Series:
    return native_pd.Series(
        [
            None,
            1,
            2,
            3,
        ],
        index=pd.MultiIndex.from_tuples(
            [
                ("row1", 1),
                # add a duplicate index value to check that we're joining on
                # position instead of on index values.
                ("row1", 1),
                ("row3", 3),
                ("row4", 4),
            ],
            names=["row_level1", "row_level2"],
        ),
        name="base_series",
    )


class TestDefaultParameters:
    @sql_count_checker(
        # copying the original series's index to the final resulting dataframe
        # adds 1 extra query to materialize the index.
        query_count=QUERY_COUNT + 1,
        join_count=JOIN_COUNT,
    )
    def test_no_diff(self, base_series):
        other_series = base_series.copy()
        eval_snowpark_pandas_result(
            *create_test_series(base_series),
            lambda series: series.compare(
                pd.Series(other_series)
                if isinstance(series, pd.Series)
                else other_series
            ),
        )

    @pytest.mark.parametrize("position, new_value", [(0, 1), (1, None), (2, -1)])
    @sql_count_checker(query_count=QUERY_COUNT, join_count=JOIN_COUNT)
    def test_single_value_diff(self, base_series, position, new_value):
        # check that we are changing a value, so the test case is meaningful.
        assert not (
            pd.isna(base_series.iloc[position])
            and pd.isna(new_value)
            or base_series.iloc[position] == new_value
        ), f"base_series already has a value equivalent to {new_value} at position {position}"
        other_series = base_series.copy()
        other_series.iloc[position] = new_value
        eval_snowpark_pandas_result(
            *create_test_series(base_series),
            lambda series: series.compare(
                pd.Series(other_series)
                if isinstance(series, pd.Series)
                else other_series
            ),
        )

    @sql_count_checker(query_count=4, join_count=4)
    def test_default_index_and_name(self, base_series):
        position = 1
        new_value = 2
        # check that we are changing a value, so the test case is meaningful.
        assert not (
            (pd.isna(base_series.iloc[position]) and pd.isna(new_value))
            or base_series.iloc[position] == new_value
        ), f"base_series already has a value equivalent to {new_value} at position {position}"
        base_series.reset_index(inplace=True, drop=True)
        base_series.name = None
        other_series = base_series.copy()
        other_series.iloc[position] = new_value
        eval_snowpark_pandas_result(
            *create_test_series(base_series),
            lambda series: series.compare(
                pd.Series(other_series)
                if isinstance(series, pd.Series)
                else other_series
            ),
        )

    @sql_count_checker(query_count=QUERY_COUNT, join_count=JOIN_COUNT)
    def test_all_values_different(self, base_series):
        other_series = base_series + 1
        eval_snowpark_pandas_result(
            *create_test_series(base_series),
            lambda series: series.compare(
                pd.Series(other_series)
                if isinstance(series, pd.Series)
                else other_series
            ),
        )

    @sql_count_checker(query_count=4, join_count=4)
    def test_different_names(self):
        series = native_pd.Series([1], name="a")
        other_series = native_pd.Series([2], name="b")
        eval_snowpark_pandas_result(
            *create_test_series(series),
            lambda series: series.compare(
                pd.Series(other_series)
                if isinstance(series, pd.Series)
                else other_series
            ),
        )

    @sql_count_checker(
        # Execute a query with join to compare lazy indices.
        query_count=1,
        join_count=1,
    )
    def test_different_index(self):
        series = native_pd.Series([1], index=["a"])
        other_series = native_pd.Series([2], index=["b"])
        eval_snowpark_pandas_result(
            *create_test_series(series),
            lambda series: series.compare(
                pd.Series(other_series)
                if isinstance(series, pd.Series)
                else other_series
            ),
            expect_exception=True,
            expect_exception_type=ValueError,
            # pandas has a slightly different message for dataframes and
            # series, whereas Snowpark pandas uses "Can only compare
            # identically-labeled objects" for both dataframes and series.
            # TODO(https://github.com/modin-project/modin/issues/5699): Check
            # that the exception matches pandas exactly.
            expect_exception_match="Can only compare identically-labeled objects",
            assert_exception_equal=False,
        )


@pytest.mark.xfail(strict=True, raises=NotImplementedError)
@pytest.mark.parametrize("align_axis", [0, "index"])
def test_align_axis(base_series, align_axis):
    position = 1
    new_value = 2
    # check that we are changing a value, so the test case is meaningful.
    assert not (
        pd.isna(base_series.iloc[position])
        and pd.isna(new_value)
        or base_series.iloc[position] == new_value
    ), f"base_series already has a value equivalent to {new_value} at position {position}"
    other_series = base_series.copy()
    other_series.iloc[position] = new_value
    eval_snowpark_pandas_result(
        *create_test_series(base_series),
        lambda series: series.compare(
            pd.Series(other_series) if isinstance(series, pd.Series) else other_series,
            align_axis=align_axis,
        ),
    )


@pytest.mark.xfail(strict=True, raises=NotImplementedError)
def test_keep_shape(base_series):
    position = 1
    new_value = 2
    # check that we are changing a value, so the test case is meaningful.
    assert not (
        pd.isna(base_series.iloc[position])
        and pd.isna(new_value)
        or base_series.iloc[position] == new_value
    ), f"base_series already has a value equivalent to {new_value} at position {position}"
    other_series = base_series.copy()
    other_series.iloc[position] = new_value
    eval_snowpark_pandas_result(
        *create_test_series(base_series),
        lambda series: series.compare(
            pd.Series(other_series) if isinstance(series, pd.Series) else other_series,
            keep_shape=True,
        ),
    )


@pytest.mark.xfail(strict=True, raises=NotImplementedError)
def test_keep_equal(base_series):
    position = 1
    new_value = 2
    # check that we are changing a value, so the test case is meaningful.
    assert not (
        pd.isna(base_series.iloc[position])
        and pd.isna(new_value)
        or base_series.iloc[position] == new_value
    ), f"base_series already has a value equivalent to {new_value} at position {position}"
    other_series = base_series.copy()
    other_series.iloc[position] = new_value
    eval_snowpark_pandas_result(
        *create_test_series(base_series),
        lambda series: series.compare(
            pd.Series(other_series) if isinstance(series, pd.Series) else other_series,
            keep_equal=True,
        ),
    )


@pytest.mark.xfail(strict=True, raises=NotImplementedError)
def test_result_names(base_series):
    position = 1
    new_value = 2
    # check that we are changing a value, so the test case is meaningful.
    assert not (
        pd.isna(base_series.iloc[position])
        and pd.isna(new_value)
        or base_series.iloc[position] == new_value
    ), f"base_series already has a value equivalent to {new_value} at position {position}"
    other_series = base_series.copy()
    other_series.iloc[position] = new_value
    eval_snowpark_pandas_result(
        *create_test_series(base_series),
        lambda series: series.compare(
            pd.Series(other_series) if isinstance(series, pd.Series) else other_series,
            result_names=("left", "right"),
        ),
    )
