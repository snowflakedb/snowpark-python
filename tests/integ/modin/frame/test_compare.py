#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker

# (+1 query, +0 join) materialize first frame's index for comparison
# (+1 query, +0 join) materialize second frame's index for comparison
# (+1 query, +1 join) row count query for joining the two frames and checking
#                     for columns where all rows match.
# (+1 query, +1 join) materialize query that joins the two frames and checks
#                     for columns where all rows match.
# (+1 query, +1 join) convert final comparison result with join to pandas
QUERY_COUNT_MULTI_LEVEL_INDEX = 5
JOIN_COUNT_MULTI_LEVEL_INDEX = 3


# (+1 query, +1 join) execute a query to compare two frames' lazy indexes.
# (+1 query, +1 join) row count query for joining the two frames and checking
#                     for columns where all rows match.
# (+1 query, +1 join) materialize query that joins the two frames and checks
#                     for columns where all rows match.
# (+1 query, +1 join) convert final comparison result with join to pandas
QUERY_COUNT_SINGLE_LEVEL_INDEX = 4
JOIN_COUNT_SINGLE_LEVEL_INDEX = 4


@pytest.fixture
def base_df() -> native_pd.DataFrame:
    return native_pd.DataFrame(
        [
            [None, None, 3.1, pd.Timestamp("2024-01-01"), [130], pd.Timedelta(1)],
            ["a", 1, 4.2, pd.Timestamp("2024-02-01"), [131], pd.Timedelta(11)],
            ["b", 2, 5.3, pd.Timestamp("2024-03-01"), [132], pd.Timedelta(21)],
            [None, 3, 6.4, pd.Timestamp("2024-04-01"), [133], pd.Timedelta(13)],
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
            names=("row_level1", "row_level2"),
        ),
        columns=pd.MultiIndex.from_tuples(
            [
                ("group_1", "string_col"),
                ("group_1", "int_col"),
                ("group_2", "float_col"),
                ("group_2", "timestamp_col"),
                ("group_2", "list_col"),
                ("group_2", "timedelta_col"),
            ],
            names=["column_level1", "column_level2"],
        ),
    )


class TestDefaultParameters:
    @sql_count_checker(
        query_count=QUERY_COUNT_MULTI_LEVEL_INDEX,
        join_count=JOIN_COUNT_MULTI_LEVEL_INDEX,
    )
    def test_no_diff(self, base_df):
        other_df = base_df.copy()
        eval_snowpark_pandas_result(
            *create_test_dfs(base_df),
            lambda df: df.compare(
                pd.DataFrame(other_df) if isinstance(df, pd.DataFrame) else other_df
            ),
            # In snowpark pandas, the column index of the empty resulting frame
            # has the correct values and names, but the incorrect inferred_type
            # for some of its levels. Ignore that bug for now.
            # TODO(SNOW-1510921): fix the bug.
            check_index_type=False,
            check_column_type=False,
        )

    @sql_count_checker(
        # no joins because we can skip the joins when comparing df to df.copy()
        query_count=QUERY_COUNT_SINGLE_LEVEL_INDEX,
        join_count=0,
    )
    def test_no_diff_timedelta(self):
        eval_snowpark_pandas_result(
            *create_test_dfs([pd.Timedelta(1)]),
            lambda df: df.compare(df.copy()),
            check_index_type=False,
            check_column_type=False,
            test_attrs=False,  # native pandas propagates here while we do not
        )

    @sql_count_checker(
        query_count=QUERY_COUNT_SINGLE_LEVEL_INDEX,
        join_count=JOIN_COUNT_SINGLE_LEVEL_INDEX,
    )
    def test_one_diff_timedelta(self):
        base_snow_df, base_pandas_df = create_test_dfs(
            [[pd.Timedelta(1), pd.Timedelta(2)]]
        )
        other_snow_df, other_pandas_df = create_test_dfs(
            [[pd.Timedelta(1), pd.Timedelta(3)]]
        )
        eval_snowpark_pandas_result(
            (base_snow_df, other_snow_df),
            (base_pandas_df, other_pandas_df),
            lambda t: t[0].compare(t[1]),
            check_index_type=False,
            check_column_type=False,
        )

    @sql_count_checker(
        query_count=QUERY_COUNT_SINGLE_LEVEL_INDEX,
        join_count=JOIN_COUNT_SINGLE_LEVEL_INDEX,
    )
    def test_timedelta_compared_with_int(self):
        base_snow_df, base_pandas_df = create_test_dfs([[pd.Timedelta(1), 2]])
        other_snow_df, other_pandas_df = create_test_dfs(
            [[pd.Timedelta(1), pd.Timedelta(2)]]
        )
        eval_snowpark_pandas_result(
            (base_snow_df, other_snow_df),
            (base_pandas_df, other_pandas_df),
            lambda t: t[0].compare(t[1]),
            check_index_type=False,
            check_column_type=False,
        )

    @pytest.mark.parametrize(
        "position, new_value",
        [
            ((0, 0), "c"),
            ((1, 1), 11),
            ((2, 2), 10.8),
            ((3, 3), pd.Timestamp("2024-05-01")),
            ((3, 4), [201]),
        ],
    )
    @sql_count_checker(
        query_count=QUERY_COUNT_MULTI_LEVEL_INDEX,
        join_count=JOIN_COUNT_MULTI_LEVEL_INDEX,
    )
    def test_single_value_diff(self, base_df, position, new_value):
        # check that we are changing a value, so the test case is meaningful.
        assert not (
            (native_pd.isna(base_df.iloc[position]) and native_pd.isna(new_value))
            or base_df.iloc[position] == new_value
        ), f"base_df already has a value equivalent to {new_value} at position {position}"
        other_df = base_df.copy()
        other_df.iloc[position] = new_value
        eval_snowpark_pandas_result(
            *create_test_dfs(base_df),
            lambda df: df.compare(
                pd.DataFrame(other_df) if isinstance(df, pd.DataFrame) else other_df
            ),
        )

    @sql_count_checker(query_count=4, join_count=4)
    def test_default_index_on_both_axes(self, base_df):
        position = (0, 0)
        new_value = "c"
        # check that we are changing a value, so the test case is meaningful.
        assert not (
            (native_pd.isna(base_df.iloc[position]) and native_pd.isna(new_value))
            or base_df.iloc[position] == new_value
        ), f"base_df already has a value equivalent to {new_value} at position {position}"
        base_df.reset_index(inplace=True)
        base_df.columns = list(range(len(base_df.columns)))
        other_df = base_df.copy()
        other_df.iloc[position] = new_value
        eval_snowpark_pandas_result(
            *create_test_dfs(base_df),
            lambda df: df.compare(
                pd.DataFrame(other_df) if isinstance(df, pd.DataFrame) else other_df
            ),
        )

    @sql_count_checker(
        query_count=QUERY_COUNT_MULTI_LEVEL_INDEX,
        join_count=JOIN_COUNT_MULTI_LEVEL_INDEX,
    )
    def test_different_value_in_every_column_and_row(self, base_df):
        other_df = base_df.copy()
        other_df.iloc[0, 0] = "c"
        other_df.iloc[1, 1] = 11
        other_df.iloc[2, 2] = 10.8
        other_df.iloc[3, 3] = pd.Timestamp("2024-05-01")
        other_df.iloc[3, 4] = [201]
        eval_snowpark_pandas_result(
            *create_test_dfs(base_df),
            lambda df: df.compare(
                pd.DataFrame(other_df) if isinstance(df, pd.DataFrame) else other_df
            ),
        )

    @sql_count_checker(
        # Execute a query with join to compare lazy indices.
        query_count=1,
        join_count=1,
    )
    def test_different_index(self):
        df = native_pd.DataFrame([1], index=["a"])
        other_df = native_pd.DataFrame([1], index=["b"])
        eval_snowpark_pandas_result(
            *create_test_dfs(df),
            lambda df: df.compare(
                pd.DataFrame(other_df) if isinstance(df, pd.DataFrame) else other_df
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

    @sql_count_checker(
        # columns are different, so we don't have to execut queries to compare
        # the dataframes.
        query_count=0
    )
    def test_different_columns(self):
        df = native_pd.DataFrame([1], columns=["a"])
        other_df = native_pd.DataFrame([1], columns=["b"])
        eval_snowpark_pandas_result(
            *create_test_dfs(df),
            lambda df: df.compare(
                pd.DataFrame(other_df) if isinstance(df, pd.DataFrame) else other_df
            ),
            expect_exception=True,
            # pandas has a slightly different message for dataframes and
            # series, whereas Snowpark pandas uses "Can only compare
            # identically-labeled objects" for both dataframes and series.
            # TODO(https://github.com/modin-project/modin/issues/5699): Check
            # that the exception matches pandas exactly.
            expect_exception_type=ValueError,
            expect_exception_match="Can only compare identically-labeled objects",
            assert_exception_equal=False,
        )


@pytest.mark.xfail(strict=True, raises=NotImplementedError)
@pytest.mark.parametrize("align_axis", [0, "index"])
def test_align_axis(base_df, align_axis):
    position = (0, 0)
    new_value = "c"
    # check that we are changing a value, so the test case is meaningful.
    assert not (
        (native_pd.isna(base_df.iloc[position]) and native_pd.isna(new_value))
        or base_df.iloc[position] == new_value
    ), f"base_df already has a value equivalent to {new_value} at position {position}"
    other_df = base_df.copy()
    other_df.iloc[position] = new_value
    eval_snowpark_pandas_result(
        *create_test_dfs(base_df),
        lambda df: df.compare(
            pd.DataFrame(other_df) if isinstance(df, pd.DataFrame) else other_df,
            align_axis=align_axis,
        ),
    )


@pytest.mark.xfail(strict=True, raises=NotImplementedError)
def test_keep_shape(base_df):
    position = (0, 0)
    new_value = "c"
    # check that we are changing a value, so the test case is meaningful.
    assert not (
        (native_pd.isna(base_df.iloc[position]) and native_pd.isna(new_value))
        or base_df.iloc[position] == new_value
    ), f"base_df already has a value equivalent to {new_value} at position {position}"
    other_df = base_df.copy()
    other_df.iloc[position] = new_value
    eval_snowpark_pandas_result(
        *create_test_dfs(base_df),
        lambda df: df.compare(
            pd.DataFrame(other_df) if isinstance(df, pd.DataFrame) else other_df,
            keep_shape=True,
        ),
    )


@pytest.mark.xfail(strict=True, raises=NotImplementedError)
def test_keep_equal(base_df):
    position = (0, 0)
    new_value = "c"
    # check that we are changing a value, so the test case is meaningful.
    assert not (
        (native_pd.isna(base_df.iloc[position]) and native_pd.isna(new_value))
        or base_df.iloc[position] == new_value
    ), f"base_df already has a value equivalent to {new_value} at position {position}"
    other_df = base_df.copy()
    other_df.iloc[position] = new_value
    eval_snowpark_pandas_result(
        *create_test_dfs(base_df),
        lambda df: df.compare(
            pd.DataFrame(other_df) if isinstance(df, pd.DataFrame) else other_df,
            keep_equal=True,
        ),
    )


@pytest.mark.xfail(strict=True, raises=NotImplementedError)
def test_result_names(base_df):
    position = (0, 0)
    new_value = "c"
    # check that we are changing a value, so the test case is meaningful.
    assert not (
        (native_pd.isna(base_df.iloc[position]) and native_pd.isna(new_value))
        or base_df.iloc[position] == new_value
    ), f"base_df already has a value equivalent to {new_value} at position {position}"
    other_df = base_df.copy()
    other_df.iloc[position] = new_value
    eval_snowpark_pandas_result(
        *create_test_dfs(base_df),
        lambda df: df.compare(
            pd.DataFrame(other_df) if isinstance(df, pd.DataFrame) else other_df,
            result_names=("left", "right"),
        ),
    )
