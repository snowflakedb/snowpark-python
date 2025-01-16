#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

TEST_LABELS = np.array(["A", "B", "C", "D", "E"])
TEST_DATA = [
    [0, 1, 2, 3, pd.Timedelta(4)],
    [0, 0, 0, 0, pd.Timedelta(0)],
    [None, 0, None, 0, pd.Timedelta(0)],
    [None, None, None, None, None],
]

# which original dataframe (constructed from slicing) to test for
TEST_SLICES = [
    (0, slice(None)),
    (slice(None), 0),
    (slice(None), slice(None)),
    (slice(None), -1),
]


@pytest.mark.parametrize("axes_slices", TEST_SLICES)
@pytest.mark.parametrize("dropna", [True, False])
def test_dataframe_nunique(axes_slices, dropna):
    df = pd.DataFrame(
        pd.DataFrame(TEST_DATA, columns=TEST_LABELS).iloc[
            axes_slices[0], axes_slices[1]
        ]
    )
    native_df = native_pd.DataFrame(
        native_pd.DataFrame(TEST_DATA, columns=TEST_LABELS).iloc[
            axes_slices[0], axes_slices[1]
        ]
    )

    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            df,
            native_df,
            lambda df: df.nunique(axis=0, dropna=dropna),
        )


@pytest.mark.parametrize(
    "native_df",
    [
        native_pd.DataFrame([]),
        native_pd.DataFrame([], index=native_pd.Index([1, 2, 3])),
    ],
)
@sql_count_checker(query_count=2)
def test_dataframe_nunique_no_columns(native_df):
    df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        df,
        native_df,
        lambda df: df.nunique(axis=0),
    )


@pytest.mark.parametrize(
    "index",
    [
        pytest.param(None, id="default_index"),
        pytest.param(
            [["bar", "bar", "baz", "foo"], ["one", "two", "one", "two"]], id="2D_index"
        ),
    ],
)
@pytest.mark.parametrize(
    "columns",
    [
        pytest.param(None, id="default_columns"),
        pytest.param(
            [["bar", "bar", "baz", "foo", "foo"], ["one", "two", "one", "two", "one"]],
            id="2D_columns",
        ),
    ],
)
@sql_count_checker(query_count=1)
def test_dataframe_nunique_multiindex(index, columns):
    eval_snowpark_pandas_result(
        *create_test_dfs(TEST_DATA, index=index, columns=columns),
        lambda df: df.nunique(axis=0),
    )


@sql_count_checker(query_count=0)
def test_dataframe_unique_axis_negative():
    df = pd.DataFrame(TEST_DATA, columns=TEST_LABELS)
    with pytest.raises(ValueError, match="No axis named 2 for object type DataFrame"):
        df.nunique(axis=2)


@sql_count_checker(query_count=0)
def test_dataframe_unique_dropna_negative():
    df = pd.DataFrame(TEST_DATA, columns=TEST_LABELS)
    with pytest.raises(ValueError, match="dropna must be of type bool"):
        df.nunique(dropna=42)


@pytest.mark.parametrize("dropna", [True, False])
@sql_count_checker(query_count=0)
def test_dataframe_unique_axis1_not_implemented(dropna):
    df = pd.DataFrame(TEST_DATA, columns=TEST_LABELS)
    msg = "Snowpark pandas nunique API doesn't yet support axis == 1"
    with pytest.raises(NotImplementedError, match=msg):
        df.nunique(axis=1, dropna=dropna)
