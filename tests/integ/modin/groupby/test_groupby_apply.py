#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import pathlib
import sys

import cloudpickle
import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas.api.types import is_number
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.modin.plugin.extensions.utils import try_convert_index_to_native
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equal_to_pandas,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    assert_values_equal,
    create_test_dfs,
    create_test_series,
    eval_snowpark_pandas_result as _eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import RUNNING_ON_GH

pytestmark = [pytest.mark.udf]

# Use the workaround shown below for applying functions that are attributes
# of this module.
# https://github.com/cloudpipe/cloudpickle?tab=readme-ov-file#overriding-pickles-serialization-mechanism-for-importable-constructs
cloudpickle.register_pickle_by_value(sys.modules[__name__])


def eval_snowpark_pandas_result(*args, **kwargs):
    # Some calls to the native pandas function propagate attrs while some do not, depending on the values of its arguments
    return _eval_snowpark_pandas_result(*args, test_attrs=False, **kwargs)


@pytest.fixture
def set_sql_simplifier(request):
    """Set pd.session.sql_simplifier_enabled  and restore it after the test."""
    old = pd.session.sql_simplifier_enabled
    pd.session.sql_simplifier_enabled = request.param
    yield
    pd.session.sql_simplifier_enabled = old


def add_one(df: native_pd.DataFrame) -> native_pd.DataFrame:
    return df.applymap(
        lambda v: (
            v + "_1"
            if isinstance(v, str)
            else (v + 1)
            if is_number(v)
            else str(v) + "_1"
        )
    )


def normalize_numeric_columns_by_sum(df: native_pd.DataFrame) -> native_pd.DataFrame:
    numeric = df.select_dtypes("number")
    df_copy = df.copy()
    df_copy.loc[:, numeric.columns] = numeric / numeric.sum()
    return df_copy


def duplicate_df_rowwise(df: native_pd.DataFrame) -> native_pd.DataFrame:
    return native_pd.concat([df, df])


def transform_that_changes_columns(df: native_pd.DataFrame) -> native_pd.DataFrame:
    return native_pd.DataFrame(
        {
            "custom_sum": df["int_col"].cumsum() + df["int_col"].max(),
            "custom_string": (df["string_col_2"].astype("object").cumsum())
            + df["string_col_2"].str.cat(sep="-"),
        }
    )


def get_scalar_from_numeric_series(group: native_pd.Series) -> str:
    """Get a scalar that aggregates series sum and mean and includes the group name."""
    # if name is null or has nulls in it, the string representation of name is
    # wrong due to SNOW-1248872. Work around that by replacing nulls with 'None'.
    # TODO(SNOW-1248872): Remove work around.
    if native_pd.isna(group.name):
        fixed_name = "None"
    elif isinstance(group.name, tuple):
        fixed_name = tuple("None" if native_pd.isna(v) else v for v in group.name)
    else:
        fixed_name = group.name

    return f"{group.sum()}_{group.mean()}_{fixed_name}"


def get_dataframe_from_numeric_series(group: native_pd.Series):
    """Get a dataframe that aggregates series sum and mean and includes the group name."""
    return native_pd.DataFrame(
        {"sum": [group.sum()], "mean": [group.mean()], "name": [group.name]},
        index=native_pd.Index(["row0"], name="row_index"),
    )


def get_series_from_numeric_series(group: native_pd.Series):
    return native_pd.Series(
        {"sum": group.sum(), "mean": group.mean(), "name": group.name},
        name="custom_metrics",
    )


def series_transform_returns_frame(group: native_pd.Series):
    # use a common name so that we don't hit SNOW-1232201
    return group.rename(None).to_frame()


def series_transform_returns_series(group: native_pd.Series):
    return group + 1


@pytest.fixture
def grouping_dfs_with_multiindexes() -> tuple[pd.DataFrame, native_pd.DataFrame]:
    # Repeat k0 a couple of times so that we can get groups of size > 1 when
    # when grouping by k0. Also repeat some values of index level 0, and repeat
    # values of the combination (k0, level_0).
    return create_test_dfs(
        [
            ["k0", 13, "e"],
            ["k1", 14, "d"],
            ["k0", 15, "c"],
            ["k0", 16, "b"],
            [None, 17, "a"],
        ],
        index=pd.MultiIndex.from_tuples(
            [("i1", "i3"), ("i1", "i2"), ("i0", "i0"), ("i1", "i4"), (None, "i0")],
            names=["level_0", "level_1"],
        ),
        columns=pd.MultiIndex.from_tuples(
            [("a", "string_col_1"), ("b", "int_col"), ("b", "string_col_2")],
            names=["c1", "c2"],
        ),
    )


# For almost all test cases, the query count is 4,
# the join count is 1, and the UDTF count is 1:

# 0. Create temporary stage for UDTF (we filter this out when counting queries)
# 0. Get pandas package and version (we filter this out when counting queries)
# 1. list the contents of the temporary stage with a statement like `ls '@"TESTDB_SNOWPANDAS"."PUBLIC".SNOWPARK_TEMP_STAGE_SU8KJXMUZS'`
# 2. Get `name` from the result of the previous `ls` command.
# 2. Put a zip file with the UDTF definition in the temporary stage (we filter this out when counting queries)
# 3. Create the UDTF (increasing UDTF count by 1)
# 3. Apply the UDTF using a join (increasing join count by 1)
# 4. convert result to pandas

QUERY_COUNT = 4
JOIN_COUNT = 1
UDTF_COUNT = 1


@pytest.mark.parametrize(
    "include_groups", [True, False], ids=lambda v: f"include_groups_{v}"
)
class TestFuncReturnsDataFrame:
    @pytest.mark.parametrize(
        "func",
        [
            normalize_numeric_columns_by_sum,
            param(
                lambda df: df.iloc[:, [1, 0]],
                id="different_columns_but_same_index",
            ),
            param(
                lambda df: (
                    native_pd.DataFrame(
                        [
                            list(range(0, len(df.columns))),
                            list(range(1, 1 + len(df.columns))),
                        ],
                        index=native_pd.Index([None, 3], name="new_index"),
                        columns=df.columns,
                    )
                    if df.index[0][0] == "i1"
                    else native_pd.DataFrame(
                        [list(range(2, 2 + len(df.columns)))],
                        index=native_pd.Index([None, 3], name="new_index"),
                        columns=df.columns,
                    )
                ),
                id="same_columns_but_different_index",
            ),
            param(lambda df: df.iloc[[-1, 0], ::-1], id="different_columns_and_index"),
        ],
    )
    @sql_count_checker(
        query_count=QUERY_COUNT,
        udtf_count=UDTF_COUNT,
        join_count=JOIN_COUNT,
    )
    def test_group_by_one_column_and_one_level_with_default_kwargs(
        self, grouping_dfs_with_multiindexes, func, include_groups
    ):
        eval_snowpark_pandas_result(
            *grouping_dfs_with_multiindexes,
            lambda df: df.groupby(
                ["level_0", ("a", "string_col_1")],
            ).apply(func, include_groups=include_groups),
        )

    @sql_count_checker(
        query_count=QUERY_COUNT,
        udtf_count=UDTF_COUNT,
        join_count=JOIN_COUNT,
    )
    def test_df_with_default_index(
        self, grouping_dfs_with_multiindexes, include_groups
    ):
        eval_snowpark_pandas_result(
            *grouping_dfs_with_multiindexes,
            lambda df: df.reset_index(drop=True)
            .groupby(("a", "string_col_1"))
            .apply(normalize_numeric_columns_by_sum, include_groups=include_groups),
        )

    @sql_count_checker(
        query_count=QUERY_COUNT,
        udtf_count=UDTF_COUNT,
        join_count=JOIN_COUNT,
    )
    def test_func_returns_empty_frame(self, include_groups):
        eval_snowpark_pandas_result(
            *create_test_dfs([[1, 2], [3, 4]]),
            lambda df: df.groupby(0).apply(
                lambda df: native_pd.DataFrame(index=[1, 3], columns=[4, 5]),
                include_groups=include_groups,
            ),
        )

    @sql_count_checker(
        query_count=QUERY_COUNT,
        udtf_count=UDTF_COUNT,
        join_count=JOIN_COUNT,
    )
    def test_args_and_kwargs(self, grouping_dfs_with_multiindexes, include_groups):
        def func(df, num1, str1):
            return df.applymap(lambda x: "_".join((str(x), num1, str1)))

        eval_snowpark_pandas_result(
            *grouping_dfs_with_multiindexes,
            lambda df: df.groupby("level_0").apply(
                func, "0.3", str1="str1", include_groups=include_groups
            ),
        )

    @pytest.mark.parametrize(
        "level",
        [
            0,
            ["level_1", "level_0"],
        ],
    )
    @sql_count_checker(
        query_count=QUERY_COUNT,
        udtf_count=UDTF_COUNT,
        join_count=JOIN_COUNT,
    )
    def test_group_by_level_basic(
        self, grouping_dfs_with_multiindexes, level, include_groups
    ):
        eval_snowpark_pandas_result(
            *grouping_dfs_with_multiindexes,
            lambda df: df.groupby(level=level).apply(
                lambda df: df.iloc[::-1, ::-1], include_groups=include_groups
            ),
        )

    @pytest.mark.skipif(RUNNING_ON_GH, reason="Slow test")
    @pytest.mark.parametrize(
        "level",
        [
            [0],
            [1, 0],
            "level_0",
            ["level_0"],
            [0, "level_1"],
        ],
    )
    @sql_count_checker(
        query_count=QUERY_COUNT,
        udtf_count=UDTF_COUNT,
        join_count=JOIN_COUNT,
    )
    def test_group_by_level(
        self, grouping_dfs_with_multiindexes, level, include_groups
    ):
        eval_snowpark_pandas_result(
            *grouping_dfs_with_multiindexes,
            lambda df: df.groupby(level=level).apply(
                lambda df: df.iloc[::-1, ::-1], include_groups=include_groups
            ),
        )

    def test_dropna_false(self, grouping_dfs_with_multiindexes, include_groups):
        snow_df, pandas_df = grouping_dfs_with_multiindexes
        # check that we are going to group by a column that has nulls.
        assert pandas_df[("a", "string_col_1")].isna().sum() > 0

        def operation(df: native_pd.DataFrame) -> native_pd.DataFrame:
            return df.groupby(
                ("a", "string_col_1"),
                dropna=False,
            ).apply(normalize_numeric_columns_by_sum, include_groups=include_groups)

        with SqlCounter(
            # When dropna=False, we can skip the dropna query
            query_count=4,
            udtf_count=UDTF_COUNT,
            join_count=JOIN_COUNT,
        ):
            pandas_result = operation(pandas_df)
            snow_result = operation(snow_df)
            # results are equal if we ignore index
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                snow_result.reset_index(drop=True), pandas_result.reset_index(drop=True)
            )
        # pandas index is not equal to snow index due to https://github.com/pandas-dev/pandas/issues/29111,
        # so hardcode the index for comparison
        assert_values_equal(
            snow_result.index,
            native_pd.MultiIndex.from_tuples(
                [
                    ("k0", "i1", "i3"),
                    ("k0", "i0", "i0"),
                    ("k0", "i1", "i4"),
                    ("k1", "i1", "i2"),
                    (np.nan, np.nan, "i0"),
                ],
                names=[("a", "string_col_1"), "level_0", "level_1"],
            ),
            check_index_type=False,
        )

    @sql_count_checker(
        query_count=QUERY_COUNT,
        join_count=JOIN_COUNT,
        udtf_count=UDTF_COUNT,
    )
    @pytest.mark.parametrize(
        "null_value",
        [
            param(
                None,
                marks=pytest.mark.xfail(
                    strict=True, raises=SnowparkSQLException, reason="SNOW-1233832"
                ),
            ),
            np.nan,
        ],
    )
    def test_group_dataframe_with_column_of_all_nulls_snow_1233832(
        self, null_value, include_groups
    ):
        eval_snowpark_pandas_result(
            *create_test_dfs({"null_col": [null_value], "int_col": [1]}),
            lambda df: df.groupby("int_col").apply(
                lambda x: x, include_groups=include_groups
            ),
        )

    @sql_count_checker(
        query_count=QUERY_COUNT,
        udtf_count=UDTF_COUNT,
        join_count=JOIN_COUNT,
    )
    @pytest.mark.parametrize(
        "by",
        [
            "level_0",
            ("a", "string_col_1"),
        ],
    )
    def test_sort_false(self, grouping_dfs_with_multiindexes, by, include_groups):
        eval_snowpark_pandas_result(
            *grouping_dfs_with_multiindexes,
            lambda df: df.groupby(by, sort=False).apply(
                normalize_numeric_columns_by_sum, include_groups=include_groups
            ),
        )

    @sql_count_checker(
        query_count=QUERY_COUNT,
        udtf_count=UDTF_COUNT,
        join_count=JOIN_COUNT,
    )
    @pytest.mark.parametrize("by", ["level_0", ("a", "string_col_1")])
    @pytest.mark.parametrize(
        "func",
        # normalize_numeric_columns_by_sum is a transform and
        # duplicate_df_rowwise is not. Include both because as_index
        # behavior depends on whether the function is a transform.
        [normalize_numeric_columns_by_sum, duplicate_df_rowwise],
    )
    def test_as_index_false(
        self, grouping_dfs_with_multiindexes, by, func, include_groups
    ):
        eval_snowpark_pandas_result(
            *grouping_dfs_with_multiindexes,
            lambda df: df.groupby(by=by, as_index=False).apply(
                func, include_groups=include_groups
            ),
        )

    @pytest.mark.parametrize(
        "as_index",
        # parametrize by as_index because as_index seems to only have an effect for group_keys=False: https://github.com/pandas-dev/pandas/issues/57656
        [True, False],
        ids=lambda v: f"as_index_{v}",
    )
    @sql_count_checker(
        # when group_keys=False, we have to check whether the function was a
        # transform because we only reindex to the original ordering if
        query_count=QUERY_COUNT,
        udtf_count=UDTF_COUNT,
        join_count=JOIN_COUNT,
    )
    def test_group_keys_false(
        self, grouping_dfs_with_multiindexes, as_index, include_groups
    ):
        eval_snowpark_pandas_result(
            *grouping_dfs_with_multiindexes,
            lambda df: df.groupby(
                by=["level_0", ("a", "string_col_1")],
                as_index=as_index,
                group_keys=False,
            ).apply(normalize_numeric_columns_by_sum, include_groups=include_groups),
        )

    @sql_count_checker(query_count=0)
    @pytest.mark.xfail(strict=True, raises=NotImplementedError)
    def test_axis_one(self, grouping_dfs_with_multiindexes, include_groups):
        eval_snowpark_pandas_result(
            *grouping_dfs_with_multiindexes,
            lambda df: df.groupby(level=0, axis=1).apply(
                normalize_numeric_columns_by_sum, include_groups=include_groups
            ),
        )

    @pytest.mark.parametrize("by", ["index", "string_col_1"], ids=lambda v: f"by_{v}")
    @pytest.mark.parametrize("as_index", [True, False], ids=lambda v: f"as_index_{v}")
    @pytest.mark.parametrize(
        "func",
        [
            transform_that_changes_columns,
            duplicate_df_rowwise,
        ],
    )
    @pytest.mark.parametrize(
        "group_keys", [True, False], ids=lambda v: f"group_keys_{v}"
    )
    @pytest.mark.parametrize(
        "dfs_kwargs",
        [
            param(
                {
                    "data": [
                        ["k0", 13, "e"],
                        ["k1", 14, "d"],
                        ["k0", 15, "c"],
                        ["k0", 16, "b"],
                        [None, 17, "a"],
                    ],
                    "index": native_pd.Index(
                        ["i0", "i1", "i2", "i1", None], name="index"
                    ),
                    "columns": native_pd.Index(
                        ["string_col_1", "int_col", "string_col_2"], name="x"
                    ),
                },
                id="with_non_unique_index",
            ),
            param(
                {
                    "data": [
                        ["k0", 13, "e"],
                        ["k1", 14, "d"],
                        ["k0", 15, "c"],
                        ["k0", 16, "b"],
                    ],
                    "index": native_pd.Index(["i1", None, "i0", "i2"], name="index"),
                    "columns": native_pd.Index(
                        ["string_col_1", "int_col", "string_col_2"], name="x"
                    ),
                },
                id="with_unique_index",
            ),
        ],
    )
    def test_df_with_single_level_labels(
        self, by, as_index, func, group_keys, dfs_kwargs, include_groups
    ):
        mdf, pdf = create_test_dfs(**dfs_kwargs)

        def operation(df: native_pd.DataFrame) -> native_pd.DataFrame:
            return df.groupby(
                by=by,
                group_keys=group_keys,
                as_index=as_index,
            ).apply(func, include_groups=include_groups)

        pandas_result = operation(pdf)
        with SqlCounter(
            query_count=QUERY_COUNT,
            join_count=JOIN_COUNT + 1,
            udtf_count=UDTF_COUNT,
        ):
            snow_result = operation(mdf)
            if (
                not group_keys
                and func is transform_that_changes_columns
                and not pdf.index.is_unique
            ):
                # for transforms of a dataframe with non-unique index when
                # group_keys=False, pandas loses the original row order and sorts
                # the result in comparison order of the original index. This is a bug,
                # with ongoing discussion in https://github.com/pandas-dev/pandas/issues/57656
                # pandas source: https://github.com/pandas-dev/pandas/blob/e14a9bd41d8cd8ac52c5c958b735623fe0eae064/pandas/core/groupby/groupby.py#L1234-L1246
                # Snowpark does the correct thing, which is to preserve input order,
                # so we have to hard-code the expected output.
                if by == "index":
                    # note that as_index=False has no effect when group_keys=False.
                    # The index doesn't include the group keys as its first levels;
                    # it consists only of the index from the func() result.
                    # for details see https://github.com/pandas-dev/pandas/issues/57656
                    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                        snow_result,
                        native_pd.DataFrame(
                            {
                                "custom_sum": [26, 30, 30, 46],
                                "custom_string": ["ee", "dd-b", "cc", "dbd-b"],
                            },
                            index=native_pd.Index(
                                ["i0", "i1", "i2", "i1"], name="index"
                            ),
                        ),
                    )
                else:
                    assert by == "string_col_1"
                    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                        snow_result,
                        native_pd.DataFrame(
                            {
                                "custom_sum": [29, 28, 44, 60],
                                "custom_string": [
                                    "ee-c-b",
                                    "dd",
                                    "ece-c-b",
                                    "ecbe-c-b",
                                ],
                            },
                            index=native_pd.Index(
                                ["i0", "i1", "i2", "i1"], name="index"
                            ),
                        ),
                    )
            else:
                assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                    snow_result, pandas_result
                )

    @pytest.mark.parametrize("set_sql_simplifier", [True, False], indirect=True)
    @sql_count_checker(
        query_count=QUERY_COUNT,
        join_count=JOIN_COUNT,
        udtf_count=UDTF_COUNT,
    )
    def test_apply_transfform_to_subset(
        self, grouping_dfs_with_multiindexes, set_sql_simplifier, include_groups
    ):
        """Test a bug where groupby.apply on a subset of columns was giving a syntax error only if sql simplifier was off."""
        eval_snowpark_pandas_result(
            *grouping_dfs_with_multiindexes,
            lambda df: df.groupby("level_0", group_keys=False)[
                [("b", "int_col"), ("b", "string_col_2")]
            ].apply(normalize_numeric_columns_by_sum, include_groups=include_groups),
        )

    @pytest.mark.parametrize(
        "result",
        [
            native_pd.DataFrame(["a", np.int64(5)]),
            native_pd.DataFrame([["a", np.int64(5)]]),
            param(
                native_pd.DataFrame([[["a", np.int64(5)]]]),
                marks=pytest.mark.xfail(
                    strict=True,
                    raises=SnowparkSQLException,
                    reason="SNOW-1229760: np.int64 is nested inside the "
                    + "single value of the dataframe, so we don't find it or "
                    + "convert it to int.",
                ),
            ),
        ],
    )
    @sql_count_checker(
        query_count=QUERY_COUNT,
        join_count=JOIN_COUNT,
        udtf_count=UDTF_COUNT,
    )
    def test_numpy_ints_in_result(
        self, grouping_dfs_with_multiindexes, result, include_groups
    ):
        eval_snowpark_pandas_result(
            *grouping_dfs_with_multiindexes,
            lambda df: df.groupby(level=0).apply(
                lambda grp: result, include_groups=include_groups
            ),
        )

    @pytest.mark.xfail(
        raises=AssertionError,
        reason="If function that returns different labels for different groups, we"
        + " pick the labels based on dummy input. This should be very rare in"
        + " practice",
    )
    def test_mismatched_data_column_positions(
        self, grouping_dfs_with_multiindexes, include_groups
    ):
        eval_snowpark_pandas_result(
            *grouping_dfs_with_multiindexes,
            lambda df: df.groupby("level_0").apply(
                lambda df: native_pd.DataFrame([0], columns=["a"])
                if df.iloc[0, 1] == 13
                else native_pd.DataFrame([1], columns=["b"]),
                include_groups=include_groups,
            ),
        )

    @sql_count_checker(
        query_count=QUERY_COUNT,
        join_count=JOIN_COUNT,
        udtf_count=UDTF_COUNT,
    )
    def test_mismatched_index_column_positions(
        self, grouping_dfs_with_multiindexes, include_groups
    ):
        eval_snowpark_pandas_result(
            *grouping_dfs_with_multiindexes,
            lambda df: df.groupby("level_0").apply(
                lambda df: native_pd.DataFrame(
                    [0],
                    index=native_pd.Index([0], name="a"),
                )
                if df.iloc[0, 1] == 13
                else native_pd.DataFrame(
                    [0],
                    index=native_pd.Index([0], name="b"),
                ),
                include_groups=include_groups,
            ),
        )

    def test_duplicate_index_groupby_mismatch_with_pandas(self, include_groups):
        # use a frame that has duplicates in its index to reproduce https://github.com/pandas-dev/pandas/issues/57906
        # this bug is fixed in snowpark pandas but not in pandas.
        snow_df, pandas_df = create_test_dfs(
            [
                ["k0", 13, "e"],
                ["k1", 14, "d"],
                ["k0", 15, "c"],
                ["k0", 16, "b"],
                [None, 17, "a"],
            ],
            index=native_pd.Index(["i1", None, "i0", "i2", None], name="index"),
            columns=native_pd.Index(
                ["string_col_1", "int_col", "string_col_2"], name="x"
            ),
        )

        def groupby_apply_without_sort(df):
            return df.groupby("index", sort=False, dropna=False, group_keys=False)[
                "int_col"
            ].apply(lambda v: v, include_groups=include_groups)

        # Assertion fails because index order is different due to pandas issue
        # 57906.
        with pytest.raises(AssertionError) as ex:
            with SqlCounter(
                query_count=QUERY_COUNT,
                udtf_count=UDTF_COUNT,
                join_count=JOIN_COUNT,
            ):
                eval_snowpark_pandas_result(
                    snow_df, pandas_df, groupby_apply_without_sort
                )
        assert "Series.index are different" in str(ex.value)
        # Assertion succeeds when we coerce order to be the same in pandas and
        # Snowpark pandas.
        pandas_result = groupby_apply_without_sort(pandas_df)
        # check that result values are unique so that sorting by values gives
        # a deterministic order.
        assert pandas_result.is_unique
        with SqlCounter(
            query_count=QUERY_COUNT,
            udtf_count=UDTF_COUNT,
            join_count=JOIN_COUNT + 1,
        ):
            assert_snowpark_pandas_equal_to_pandas(
                groupby_apply_without_sort(snow_df).sort_values(),
                pandas_result.sort_values(),
            )


@pytest.mark.parametrize(
    "include_groups", [True, False], ids=lambda v: f"include_groups_{v}"
)
class TestFuncReturnsScalar:
    @pytest.mark.parametrize("sort", [True, False], ids=lambda v: f"sort_{v}")
    @pytest.mark.parametrize("as_index", [True, False], ids=lambda v: f"as_index_{v}")
    @pytest.mark.parametrize(
        "group_keys", [True, False], ids=lambda v: f"group_keys_{v}"
    )
    @pytest.mark.parametrize("dropna", [True, False], ids=lambda v: f"dropna_{v}")
    @sql_count_checker(
        query_count=QUERY_COUNT,
        udtf_count=UDTF_COUNT,
        join_count=JOIN_COUNT,
    )
    def test_volume_from_brazil_per_year(
        self, sort, dropna, group_keys, as_index, include_groups
    ):
        """Test an example that a user provided here: https://snowflake.slack.com/archives/C05RX90ETGU/p1707126781811689"""
        # TODO: group_keys should have no impact when func: df -> scalar
        # (normally it tells whether to include group keys in the index)
        # TODO: as_index=False is dropping the index, whereas it should be including the
        # group keys as columns.
        eval_snowpark_pandas_result(
            *create_test_dfs(
                {
                    "volume": [5, 6, 20, 9, 11, 13, 0.5],
                    "country": [
                        "brazil",
                        "usa",
                        "brazil",
                        "brazil",
                        "usa",
                        "brazil",
                        "usa",
                    ],
                    "year": [2020, 2020, None, 2020, 2019, 2019, None],
                }
            ),
            lambda df: df.groupby(
                "year",
                as_index=as_index,
                sort=sort,
                group_keys=group_keys,
                dropna=dropna,
            ).apply(
                lambda grp: grp[grp.country == "brazil"].volume.sum()
                / grp.volume.sum(),
                include_groups=include_groups,
            ),
        )

    @sql_count_checker(
        query_count=QUERY_COUNT,
        udtf_count=UDTF_COUNT,
        join_count=JOIN_COUNT,
    )
    def test_root_mean_squared_error(self, include_groups):
        """Test an example that a user provided here: https://groups.google.com/a/snowflake.com/g/snowpark-pandas-api-customer-adoption-DL/c/0PDdj9-p5Hs/m/pRJ-I08dBAAJ"""
        eval_snowpark_pandas_result(
            *create_test_dfs(
                {
                    "actual": [1, 3, 5, 100],
                    "expected": [2, -5, 9, 101],
                    "customer_id": ["a", "a", "d", "c"],
                }
            ),
            lambda df: df.groupby("customer_id").apply(
                lambda grp: np.sqrt((grp.actual - grp.expected) ** 2).mean(),
                include_groups=include_groups,
            ),
        )

    @pytest.mark.parametrize(
        "by", [["level_0", ("a", "string_col_1")], "level_0", ("a", "string_col_1")]
    )
    @pytest.mark.parametrize("sort", [True, False], ids=lambda v: f"sort_{v}")
    @pytest.mark.parametrize("as_index", [True, False], ids=lambda v: f"as_index_{v}")
    @sql_count_checker(
        query_count=QUERY_COUNT,
        udtf_count=UDTF_COUNT,
        join_count=JOIN_COUNT,
    )
    def test_multiindex_df(
        self, grouping_dfs_with_multiindexes, by, sort, as_index, include_groups
    ):
        eval_snowpark_pandas_result(
            *grouping_dfs_with_multiindexes,
            lambda df: df.groupby(by, sort=sort, as_index=as_index,).apply(
                lambda df: df.astype(str).astype(object).sum().sum(),
                include_groups=include_groups,
            ),
        )

    @pytest.mark.parametrize(
        "return_value",
        [
            param(None, id="None"),
            param(np.nan, id="nan"),
            param(1, id="int"),
            param(np.int64(1), id="int64"),
            param([1, "a"], id="list_of_int_and_string"),
            param({"1": 4}, id="dict"),
            param([1, 4], id="list_of_ints"),
            param([[1, 4]], id="list_of_list_of_lists"),
            param((1, 4), id="tuple_of_ints"),
            param(
                datetime.date(day=29, month=7, year=1994),
                id="date",
                marks=pytest.mark.xfail(
                    strict=True, raises=AssertionError, reason="SNOW-1217565"
                ),
            ),
        ],
    )
    @sql_count_checker(
        query_count=QUERY_COUNT,
        udtf_count=UDTF_COUNT,
        join_count=JOIN_COUNT,
    )
    def test_non_series_or_dataframe_return_types(
        self, return_value, grouping_dfs_with_multiindexes, include_groups
    ):
        """These return types are scalars in the sense that they are not pandas Series or DataFrames."""
        snow_df, pandas_df = grouping_dfs_with_multiindexes

        def operation(df):
            return df.groupby(level=0).apply(
                lambda df: return_value, include_groups=include_groups
            )

        if return_value is None:
            # this is a pandas bug: https://github.com/pandas-dev/pandas/issues/57775
            assert operation(pandas_df).equals(native_pd.DataFrame())
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                operation(snow_df),
                native_pd.Series(
                    [None, None], index=native_pd.Index(["i0", "i1"], name="level_0")
                ),
            )
        else:
            eval_snowpark_pandas_result(
                snow_df,
                pandas_df,
                operation,
            )

    @sql_count_checker(
        query_count=7,
        udtf_count=UDTF_COUNT,
        join_count=JOIN_COUNT,
    )
    def test_group_apply_return_df_from_lambda(self, include_groups):
        diamonds_path = (
            pathlib.Path(__file__).parent.parent.parent.parent
            / "resources"
            / "diamonds.csv"
        )
        diamonds_pd = native_pd.read_csv(diamonds_path)
        eval_snowpark_pandas_result(
            pd.DataFrame(diamonds_pd),
            diamonds_pd,
            lambda diamonds: diamonds.groupby("cut").apply(
                # Use the stable "mergesort" algorithm to make the result order
                # deterministic (see SNOW-1434962).
                lambda x: x.sort_values(
                    "price", ascending=False, kind="mergesort"
                ).head(5),
                include_groups=include_groups,
            ),
        )

    @pytest.mark.xfail(strict=True, raises=AssertionError, reason="SNOW-1619940")
    def test_return_timedelta(self, include_groups):
        eval_snowpark_pandas_result(
            *create_test_dfs([[1, 2]]),
            lambda df: df.groupby(0).apply(
                lambda df: native_pd.Timedelta(df.sum().sum()),
                include_groups=include_groups,
            ),
        )

    @pytest.mark.xfail(strict=True, raises=NotImplementedError)
    @pytest.mark.parametrize(
        "pandas_df",
        [
            param(
                native_pd.DataFrame([["key0", native_pd.Timedelta(1)]]),
                id="timedelta_column",
            ),
            param(
                native_pd.DataFrame(
                    [["key0", "value1"]],
                    index=native_pd.Index([native_pd.Timedelta(1)]),
                ),
                id="timedelta_index",
            ),
        ],
    )
    def test_timedelta_input(self, pandas_df, include_groups):
        eval_snowpark_pandas_result(
            *create_test_dfs(pandas_df),
            lambda df: df.groupby(0).apply(lambda df: 1, include_groups=include_groups),
        )


@pytest.mark.parametrize(
    "include_groups", [True, False], ids=lambda v: f"include_groups_{v}"
)
class TestFuncReturnsSeries:
    @pytest.mark.parametrize(
        "by,level",
        [
            ("level_0", None),
            (("a", "string_col_1"), None),
            ([("a", "string_col_1"), "level_0"], None),
            (None, 1),
            (None, [1, 0]),
            (None, [0, "level_1"]),
        ],
    )
    @pytest.mark.parametrize("as_index", [True, False], ids=lambda v: f"as_index={v}")
    @pytest.mark.parametrize("sort", [True, False], ids=lambda v: f"sort={v}")
    @pytest.mark.parametrize(
        "group_keys", [True, False], ids=lambda v: f"group_keys={v}"
    )
    @sql_count_checker(
        query_count=QUERY_COUNT,
        udtf_count=UDTF_COUNT,
        join_count=JOIN_COUNT,
    )
    def test_return_series_with_two_columns(
        self,
        grouping_dfs_with_multiindexes,
        by,
        level,
        as_index,
        sort,
        group_keys,
        include_groups,
    ):
        eval_snowpark_pandas_result(
            *grouping_dfs_with_multiindexes,
            lambda df: df.groupby(
                by=by, level=level, as_index=as_index, sort=sort, group_keys=group_keys
            ).apply(
                lambda group: native_pd.Series(
                    {
                        "custom_sum": group[("b", "int_col")].sum()
                        + group[("b", "int_col")].max(),
                        "custom_string": group[("b", "string_col_2")].str.cat(sep="-")
                        + group[("b", "string_col_2")].str.cat(sep="_"),
                    },
                    name="custom_metrics",
                ),
                include_groups=include_groups,
            ),
        )

    @sql_count_checker(
        query_count=QUERY_COUNT,
        udtf_count=UDTF_COUNT,
        join_count=JOIN_COUNT,
    )
    def test_args_and_kwargs(self, grouping_dfs_with_multiindexes, include_groups):
        eval_snowpark_pandas_result(
            *grouping_dfs_with_multiindexes,
            lambda df: df.groupby(level=0).apply(
                lambda group, arg0, kwarg0="a", kwarg1="b": native_pd.Series(
                    {
                        "custom_sum": group[("b", "int_col")].sum() + arg0,
                        "custom_string": group[("a", "string_col_1")].str.cat(sep="-")
                        + group[("b", "string_col_2")].str.cat(sep="_")
                        + kwarg0
                        + kwarg1,
                    },
                    name="custom_metrics",
                ),
                7,
                kwarg1="x",
                include_groups=include_groups,
            ),
        )

    @pytest.mark.parametrize("dropna", [True, False])
    @sql_count_checker(
        # One extra query to convert index to native pandas in dataframe constructor to create test dataframes
        query_count=QUERY_COUNT,
        udtf_count=UDTF_COUNT,
        join_count=JOIN_COUNT + 1,
    )
    @pytest.mark.parametrize("index", [[2.0, np.nan, 2.0, 1.0], [np.nan] * 4])
    def test_dropna(self, dropna, index, include_groups):
        pandas_index = native_pd.Index(index, name="index")
        if dropna and pandas_index.isna().all():
            pytest.xfail(
                reason="We drop all the rows, apply the UDTF, and try to "
                + "pivot the result, but pivoting an empty frame "
                + "causes a SQL error due to SNOW-1233895"
            )
        # use a df without multiindexes so we don't have to work around
        # https://github.com/pandas-dev/pandas/issues/29111
        eval_snowpark_pandas_result(
            *create_test_dfs(
                [
                    ["k0", 13, "e"],
                    ["k1", 14, "d"],
                    ["k0", 15, "c"],
                    ["k0", 16, "b"],
                ],
                index=pandas_index,
                columns=native_pd.Index(
                    ["string_col_1", "int_col", "string_col_2"], name="x"
                ),
            ),
            lambda df: df.groupby("index", dropna=dropna).apply(
                lambda group: native_pd.Series(
                    {
                        "custom_sum": group["int_col"].sum() + group["int_col"].max(),
                        "custom_string": group["string_col_1"].str.cat(sep="-")
                        + group["string_col_2"].str.cat(sep="_"),
                    },
                    name="custom_metrics",
                ),
                include_groups=include_groups,
            ),
        )

    @pytest.mark.xfail(
        raises=AssertionError,
        strict=True,
        reason="Snowpark pandas uses name returned from first group, while pandas "
        + "returns None. This should be very rare in practice",
    )
    @sql_count_checker(
        query_count=QUERY_COUNT,
        udtf_count=UDTF_COUNT,
        join_count=JOIN_COUNT,
    )
    def test_returning_series_with_different_names(
        self, grouping_dfs_with_multiindexes, include_groups
    ):
        eval_snowpark_pandas_result(
            *grouping_dfs_with_multiindexes,
            lambda df: df.groupby(("a", "string_col_1")).apply(
                lambda group: native_pd.Series(
                    {
                        "int_sum": group[("b", "int_col")].sum(),
                        "string_sum": group[("b", "string_col_2")].astype(object).sum(),
                    },
                    name="name_" + str(group.iloc[0, 0]),
                ),
                include_groups=include_groups,
            ),
        )

    @pytest.mark.xfail(
        raises=AssertionError,
        strict=True,
        reason="Snowpark pandas return a DataFrame but native pandas returns a"
        + " Series. This should be very rare in practice.",
    )
    @sql_count_checker(
        query_count=QUERY_COUNT,
        udtf_count=UDTF_COUNT,
        join_count=JOIN_COUNT,
    )
    def test_returning_series_with_conflicting_indexes(
        self, grouping_dfs_with_multiindexes, include_groups
    ):
        eval_snowpark_pandas_result(
            *grouping_dfs_with_multiindexes,
            lambda df: df.groupby(("a", "string_col_1")).apply(
                lambda group: native_pd.Series(
                    {
                        str(group[("b", "int_col")].iloc[0]): group[
                            ("b", "int_col")
                        ].sum(),
                        str(group[("b", "int_col")].iloc[0])
                        + "_2": group[("b", "int_col")].astype(object).sum(),
                    },
                ),
                include_groups=include_groups,
            ),
        )


@pytest.mark.parametrize(
    "func",
    [
        param(get_dataframe_from_numeric_series, id="non_transform_returns_dataframe"),
        param(series_transform_returns_frame, id="transform_returns_dataframe"),
        param(get_series_from_numeric_series, id="non_transform_returns_series"),
        param(series_transform_returns_series, id="transform_returns_series"),
        param(get_scalar_from_numeric_series, id="return_scalar"),
    ],
)
@pytest.mark.parametrize("group_keys", [True, False], ids=lambda v: f"group_keys_{v}")
@pytest.mark.parametrize("sort", [True, False], ids=lambda v: f"sort_{v}")
@pytest.mark.parametrize("dropna", [True, False], ids=lambda v: f"dropna_{v}")
@pytest.mark.parametrize(
    "include_groups", [True, False], ids=lambda v: f"include_groups_{v}"
)
class TestSeriesGroupBy:
    @pytest.mark.parametrize("by", ["string_col_1", ["index", "string_col_1"], "index"])
    def test_dataframe_groupby_getitem(
        self, by, func, dropna, group_keys, sort, include_groups
    ):
        """Test apply() on a SeriesGroupBy that we get by DataFrameGroupBy.__getitem__"""
        if (
            func in (get_dataframe_from_numeric_series, get_series_from_numeric_series)
            and not dropna
            and by == ["index", "string_col_1"]
        ):
            # The resulting dataframe has elements that are tuples of nans like
            # (pd.NA, k1) that we cannot serialize.
            pytest.xfail(reason="SNOW-1229760")
        with SqlCounter(
            query_count=QUERY_COUNT,
            udtf_count=UDTF_COUNT,
            join_count=JOIN_COUNT + 1,
        ):
            eval_snowpark_pandas_result(
                *create_test_dfs(
                    [
                        ["k0", 13, "e"],
                        ["k1", 14, "d"],
                        ["k0", 15, "c"],
                        ["k0", 16, "b"],
                        [None, 17, "a"],
                    ],
                    index=native_pd.Index(["i1", None, "i0", "i2", "i3"], name="index"),
                    columns=native_pd.Index(
                        ["string_col_1", "int_col", "string_col_2"], name="x"
                    ),
                ),
                lambda df: df.groupby(
                    by,
                    dropna=dropna,
                    group_keys=group_keys,
                    sort=sort,
                )["int_col"].apply(func, include_groups=include_groups),
            )

    @pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="SNOW-1238546")
    def test_grouping_series_by_self(
        self, func, dropna, group_keys, sort, include_groups
    ):
        """Test apply() on a SeriesGroupBy that we get by grouping a series by itself."""
        eval_snowpark_pandas_result(
            *create_test_series([0, 1, 2]),
            lambda s: s.groupby(
                s,
                dropna=dropna,
                group_keys=group_keys,
                sort=sort,
            ).apply(func, include_groups=include_groups),
        )

    @pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="SNOW-1238546")
    def test_grouping_series_by_external_by(
        self, func, dropna, group_keys, sort, include_groups
    ):
        """Test apply() on a SeriesGroupBy that we get by grouping a series by its index."""
        # This example is from pandas SeriesGroupBy apply docstring.
        eval_snowpark_pandas_result(
            *create_test_series([0, 1, 2], index=["a", "a", "b"]),
            lambda s: s.groupby(
                try_convert_index_to_native(s.index),
                dropna=dropna,
                group_keys=group_keys,
                sort=sort,
            ).apply(func, include_groups=include_groups),
        )


class SeriesGroupByWithTimedelta:
    @pytest.mark.xfail(strict=True, raises=AssertionError, reason="SNOW-1619940")
    def test_return_timedelta(self):
        eval_snowpark_pandas_result(
            *create_test_dfs([[1, 2]]),
            lambda df: df.groupby(0,)[
                1
            ].apply(lambda series: native_pd.Timedelta(series.sum())),
        )

    @pytest.mark.xfail(strict=True, raises=NotImplementedError)
    @pytest.mark.parametrize(
        "pandas_df",
        [
            param(
                native_pd.DataFrame([["key0", native_pd.Timedelta(1)]]),
                id="timedelta_column",
            ),
            param(
                native_pd.DataFrame(
                    [["key0", "value1"]],
                    index=native_pd.Index([native_pd.Timedelta(1)]),
                ),
                id="timedelta_index",
            ),
        ],
    )
    def test_timedelta_input(
        self,
        pandas_df,
    ):
        eval_snowpark_pandas_result(
            *create_test_dfs(pandas_df),
            lambda df: df.groupby(0)[1].apply(lambda series: 1),
        )


class TestNonCallableFunc:
    @pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="SNOW-1177529")
    def test_non_callable_aggregation(self, grouping_dfs_with_multiindexes):
        eval_snowpark_pandas_result(
            *grouping_dfs_with_multiindexes,
            lambda df: df.groupby(("a", "string_col_1")).apply("min"),
        )


class TestCallableWithMixedReturnTypes:
    """
    Test callables that return a mix of Series, DataFrame, and scalar.

    pandas behavior here is buggy or ill-defined, so assume all these tests
    fail and defer fixing them till a user complains. SNOW-1236959 tracks
    efforts to fix these cases.

    pandas behavior seems to depend on the order of the types of the returned
    objects (e.g. returning a dataframe for group 1 and series for group 2
    has a different effect than returning a series for group 1 and a dataframe
    for group 2). We won't enumerate every possible permutation here.
    """

    @pytest.mark.xfail(
        strict=True,
        raises=SnowparkSQLException,
        reason="Results in mismatch between udtf schema and the actual result."
        + " This should be rare in practice.",
    )
    def test_scalar_then_dataframe(self):
        eval_snowpark_pandas_result(
            *create_test_dfs([["b", 8], ["a", 7]]),
            lambda df: df.groupby(0).apply(
                lambda group: 1
                if group.iloc[0, 0] == "b"
                else native_pd.DataFrame([[2, 4], [5, 6]])
            ),
        )

    @pytest.mark.xfail(
        strict=True,
        raises=AssertionError,
        reason="Results in mismatch between udtf schema and the actual result."
        + " This should be rare in practice.",
    )
    def test_series_then_dataframe(self):
        eval_snowpark_pandas_result(
            *create_test_dfs([["b", 8], ["a", 7]]),
            lambda df: df.groupby(0).apply(
                lambda group: native_pd.Series(["a", "b"])
                if group.iloc[0, 0] == "b"
                else native_pd.DataFrame([1])
            ),
        )

    @pytest.mark.xfail(
        strict=True,
        raises=AttributeError,
        reason="pandas gives AttributeError: 'int' object has no attribute 'index'. See SNOW-1236959",
    )
    def test_scalar_then_series(self):
        eval_snowpark_pandas_result(
            *create_test_dfs([["b", 8], ["a", 7]]),
            lambda df: df.groupby(0).apply(
                lambda group: 1
                if group.iloc[0, 0] == "b"
                else native_pd.Series([2, 3, 4])
            ),
        )

    @pytest.mark.xfail(
        strict=True,
        raises=AttributeError,
        reason="pandas gives AttributeError: 'int' object has no attribute 'index'. See SNOW-1236959",
    )
    def test_scalar_then_series_then_dataframe(self):
        eval_snowpark_pandas_result(
            *create_test_dfs([["b", 8], ["a", 7], ["c", 9]]),
            lambda df: df.groupby(0).apply(
                lambda group: 1
                if group.iloc[0, 0] == "b"
                else native_pd.Series([2, 3, 4])
                if group.iloc[0, 0] == "a"
                else native_pd.DataFrame([[2, 4], [5, 6]])
            ),
        )


@sql_count_checker(
    query_count=QUERY_COUNT,
    join_count=JOIN_COUNT,
    udtf_count=UDTF_COUNT,
)
def test_include_groups_default_value(grouping_dfs_with_multiindexes):
    """
    Test that the default value behavior include_groups matches pandas.

    We don't test the default include_groups value for all test cases
    because that would substantially increase the size of the test suite.
    """
    eval_snowpark_pandas_result(
        *grouping_dfs_with_multiindexes,
        lambda df: df.groupby(("a", "string_col_1")).apply(lambda df: df.count()),
    )
