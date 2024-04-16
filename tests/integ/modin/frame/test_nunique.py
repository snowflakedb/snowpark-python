#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.pandas as pd
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result

TEST_LABELS = np.array(["A", "B", "C", "D"])
TEST_DATA = [[0, 1, 2, 3], [0, 0, 0, 0], [None, 0, None, 0], [None, None, None, None]]

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
    expected_join_count = 0
    if axes_slices == (0, slice(None)):
        expected_join_count = 4

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

    with SqlCounter(query_count=1, join_count=expected_join_count):
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
            [["bar", "bar", "baz", "foo"], ["one", "two", "one", "two"]],
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
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_dataframe_unique_axis1_fallback(dropna):
    df = pd.DataFrame(TEST_DATA, columns=TEST_LABELS)
    native_df = native_pd.DataFrame(TEST_DATA, columns=TEST_LABELS)

    eval_snowpark_pandas_result(
        df,
        native_df,
        lambda df: df.nunique(axis=1, dropna=dropna),
    )
