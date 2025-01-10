#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Union

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas.io.formats.printing import PrettyDict
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_dicts_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


def _pandas_groupby_groups_workaround(
    df: native_pd.DataFrame, **groupby_kwargs: dict
) -> dict:
    """
    Do pandas groupby.groups with fewer bugs.

    This function exists to work around these bugs:

        - https://github.com/pandas-dev/pandas/issues/35202: When grouping by a single column that has nulls with dropna=False, groupby.groups raises ValueError: Categorical categories cannot be null"
        - https://github.com/pandas-dev/pandas/issues/55919: groupby.groups with multiple grouping columns always treats dropna as False.
        - https://github.com/pandas-dev/pandas/issues/56851: groupby.groups and groupby.indices give incorrect result order when dropna=False
        - https://github.com/pandas-dev/pandas/issues/56966: groupby.groups with multiple group keys always treats sort as True.

    Args:
        df: pandas dataframe
        groupby_kwargs: groupby arguments

    Returns:
        PrettyDict mapping group labels to the portion of the index that falls into that group.
    """
    df["_snowpark_test_row_number_column"] = range(len(df))
    # https://github.com/pandas-dev/pandas/issues/56965: groupby.groups and groupby.indices always treat as_index as True.
    groupby_kwargs["as_index"] = True
    return PrettyDict(
        df.groupby(**groupby_kwargs)
        # to get the group's index, select index values at all the row positions that are in this
        # group.
        .apply(
            lambda group_df: df.index[group_df["_snowpark_test_row_number_column"]]
        ).to_dict()
    )


def _pandas_groupby_indices_workaround(df, **groupby_kwargs) -> dict:
    """
    Do pandas groupby.indices with fewer bugs.

    This function exists to work around this bug:

        - https://github.com/pandas-dev/pandas/issues/56851: groupby.groups and groupby.indices give incorrect result order when dropna=False

    Args:
        df: pandas dataframe
        groupby_kwargs: groupby arguments

    Returns:
        PrettyDict mapping group labels to the positions of rows that belong in that group.
    """
    df["_snowpark_test_row_number_column"] = range(len(df))
    # https://github.com/pandas-dev/pandas/issues/56965: groupby.groups and groupby.indices always treat as_index as True.
    groupby_kwargs["as_index"] = True
    return (
        df.groupby(**groupby_kwargs)
        .apply(lambda group_df: group_df["_snowpark_test_row_number_column"].to_numpy())
        .to_dict()
    )


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        param(
            dict(by="col1", dropna=False),
            PrettyDict(
                {
                    0.0: native_pd.Index([3, 5]),
                    1.0: native_pd.Index([1, 2]),
                    np.nan: native_pd.Index([0, 4]),
                }
            ),
            id="pandas_issue_35202",
        ),
        param(
            dict(by=["col1", "col2"], dropna=True),
            PrettyDict(
                {
                    (0.0, 5.0): native_pd.Index([5]),
                    (0.0, 7.0): native_pd.Index([3]),
                    (1.0, 5.0): native_pd.Index([1]),
                }
            ),
            id="pandas_issue_55919",
        ),
        param(
            dict(by=["col1", "col2"], dropna=False),
            PrettyDict(
                {
                    (0.0, 5.0): native_pd.Index([5]),
                    (0.0, 7.0): native_pd.Index([3]),
                    (1.0, 5.0): native_pd.Index([1]),
                    (1.0, np.nan): native_pd.Index([2]),
                    (np.nan, 4.0): native_pd.Index([0, 4]),
                }
            ),
            id="pandas_issue_56851",
        ),
        param(
            dict(by=["col1", "col2"], sort=False),
            PrettyDict(
                {
                    (1.0, 5.0): native_pd.Index([1]),
                    (0.0, 7.0): native_pd.Index([3]),
                    (0.0, 5.0): native_pd.Index([5]),
                }
            ),
            id="pandas_issue_56966",
        ),
    ],
)
@sql_count_checker(query_count=1)
def test_pandas_groupby_groups_workaround(
    basic_snowpark_pandas_df_with_missing_values, kwargs, expected
):
    """Test that this test module's version of pandas groupby.groups fixes the bugs that it's supposed to fix."""
    assert_dicts_equal(
        _pandas_groupby_groups_workaround(
            basic_snowpark_pandas_df_with_missing_values.to_pandas(), **kwargs
        ),
        expected,
    )


@sql_count_checker(query_count=1)
def test_pandas_groupby_indices_workaround_pandas_issue_56851(
    basic_snowpark_pandas_df_with_missing_values,
):
    """Test that this test module's version of pandas groupby.indices fixes the bugs that it's supposed to fix."""
    # pandas incorrectly switches the order of (0.0, 7.0) and (0.0, 5.0)
    assert_dicts_equal(
        _pandas_groupby_indices_workaround(
            basic_snowpark_pandas_df_with_missing_values.to_pandas(),
            by=["col1", "col2"],
            dropna=False,
            sort=False,
        ),
        {
            (np.nan, 4.0): np.array([0, 4]),
            (1.0, 5.0): np.array([1]),
            (1.0, np.nan): np.array([2]),
            (0.0, 7.0): np.array([3]),
            (0.0, 5.0): np.array([5]),
        },
    )


@pytest.mark.parametrize(
    "groupby_property,pandas_workaround",
    [
        param("groups", _pandas_groupby_groups_workaround, id="groups"),
        param("indices", _pandas_groupby_indices_workaround, id="indices"),
    ],
)
@sql_count_checker(
    # The to_pandas() to create the native frame causes one query,
    # and the groupby.indices/groupby.groups causes another.
    query_count=2,
    join_count=0,
)
@pytest.mark.parametrize(
    "index_cols,by,level",
    [
        (None, "col1", None),
        (None, ["col1", "col2"], None),
        ("col3", "col1", None),
        ("col3", ["col1", "col2"], None),
        (["col3", "col4"], "col1", None),
        (["col3", "col4"], ["col1", "col2"], None),
        (None, None, 0),
        ("col3", "col3", None),
        ("col3", None, 0),
        ("col3", None, "col3"),
        (["col3", "col4"], "col3", None),
        (["col3", "col4"], None, (0, 1)),
        (["col3", "col4"], None, (0, "col4")),
    ],
)
@pytest.mark.parametrize(
    "df_name",
    ["basic_snowpark_pandas_df", "basic_snowpark_pandas_df_with_missing_values"],
)
@pytest.mark.parametrize("dropna", [True, False], ids=lambda v: f"dropna_{v}")
@pytest.mark.parametrize(
    "group_series", [True, False], ids=lambda v: f"group_series_{v}"
)
@pytest.mark.parametrize("as_index", [True, False], ids=lambda v: f"as_index_{v}")
@pytest.mark.parametrize("sort", [True, False], ids=lambda v: f"sort_{v}")
def test_groups_and_indices(
    basic_snowpark_pandas_df,
    basic_snowpark_pandas_df_with_missing_values,
    df_name,
    groupby_property,
    index_cols,
    by,
    level,
    dropna,
    group_series,
    as_index,
    sort,
    pandas_workaround,
):
    df = eval(df_name)

    if index_cols is not None:
        df.set_index(index_cols, inplace=True)

    def get_property(df: Union[native_pd.DataFrame, pd.DataFrame]):
        if isinstance(df, native_pd.DataFrame):
            # note that group_series should have no effect on the expected
            # output, so we can ignore it.
            return pandas_workaround(
                df, by=by, level=level, dropna=dropna, as_index=as_index, sort=sort
            )
        assert isinstance(df, pd.DataFrame)
        dataframe_groupby = df.groupby(
            by=by, level=level, dropna=dropna, as_index=as_index, sort=sort
        )
        groupby_to_use = (
            dataframe_groupby["col5"] if group_series else dataframe_groupby
        )
        return getattr(groupby_to_use, groupby_property)

    eval_snowpark_pandas_result(
        df,
        df.to_pandas(),
        get_property,
        comparator=assert_dicts_equal,
    )


@pytest.mark.parametrize("groupby_property", ["groups", "indices"])
@sql_count_checker(query_count=1, join_count=0)
def test_groups_and_indices_column_name_conflicts_with_index_name(groupby_property):
    native_df = native_pd.DataFrame(
        {"col0": 0, "col1": 1}, index=native_pd.Index(["row0"], name="col0")
    )
    eval_snowpark_pandas_result(
        pd.DataFrame(native_df),
        native_df,
        lambda df: getattr(df.groupby("col1"), groupby_property),
        comparator=assert_dicts_equal,
    )


@sql_count_checker(query_count=1, join_count=0)
def test_groups_subindex_type_matches_original_index_type():
    """
    the index type has to match the original index type, even if the subindex
    for this group has objects of a less restrictive type. For example,
    here we do groupby.groups on a dataframe with an index of object dtype.
    The type of the index in groups[1] is 'object' even though that part of
    the original index only contains the integer 3, so pd.Index([3]).dtype
    would be 3.
    """
    native_df = native_pd.DataFrame({"key": [0, 1]}, index=["a", 3])
    eval_snowpark_pandas_result(
        pd.DataFrame(native_df),
        native_df,
        lambda df: df.groupby("key").groups,
        comparator=assert_dicts_equal,
    )
