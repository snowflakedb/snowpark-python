#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    _GROUPBY_UNSUPPORTED_GROUPING_MESSAGE,
)
from tests.integ.modin.utils import (
    eval_snowpark_pandas_result as _eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker


def eval_snowpark_pandas_result(*args, **kwargs):
    # Native pandas does not propagate attrs for bfill/ffill, while Snowpark pandas does. We cannot easily
    # match this behavior because these use the query compiler method groupby_fillna, and the native
    # pandas GroupBy.fillna method does propagate attrs.
    return _eval_snowpark_pandas_result(*args, test_attrs=False, **kwargs)


TEST_DF_DATA = {
    "A": [None, 99, None, None, None, 98, 98, 98, None, 97],
    "B": [88, None, None, None, 87, 88, 89, None, 86, None],
    "C": [None, None, 1.99, 1.98, 1.97, 1.96, None, None, None, None],
}

TEST_DF_INDEX_1 = native_pd.Index([0, 0, 0, 1, 1, 1, 1, 1, 2, 3], name="I")
TEST_DF_COLUMNS_1 = native_pd.Index(["A", "B", "C"], name="X")

TEST_DF_DATA_2 = [
    [2, None, None, 99],
    [2, 10, None, 98],
    [2, None, 1.1, None],
    [2, 15, None, 97],
    [2, None, 1.1, None],
    [1, None, 2.2, None],
    [1, None, None, 96],
    [1, None, 2.3],
    [1, 20, 3.3, 95],
    [2, None, None, 94],
    [2, 30, None, None],
    [2, None, 300, None],
]

TEST_DF_INDEX_2 = pd.MultiIndex.from_tuples(
    [
        (1, "a"),
        (1, "a"),
        (1, "a"),
        (1, "a"),
        (1, "a"),
        (1, "b"),
        (1, "b"),
        (0, "a"),
        (0, "a"),
        (0, "a"),
        (0, "a"),
        (0, "a"),
    ],
    names=["I", "J"],
)
TEST_DF_COLUMNS_2 = pd.MultiIndex.from_tuples(
    [(5, "A"), (5, "B"), (6, "C"), (6, "D")], names=["X", "Y"]
)

TEST_DF_DATA_3 = (
    [[None, 100 + 10 * i, 200 + 10 * i] for i in range(6)]
    + [[300 + 10 * i, None, 500 + 10 * i] for i in range(6)]
    + [[400 + 10 * i, 600 + 10 * i, None] for i in range(6)]
)

TEST_DF_INDEX_3 = native_pd.Index([50] * len(TEST_DF_DATA_3), name="I")
TEST_DF_COLUMNS_3 = native_pd.Index(["A", "B", "C"], name="X")


@pytest.mark.parametrize("method", ["bfill", "ffill"])
@pytest.mark.parametrize("groupby_list", ["X", "key", ["X", "key"]])
@pytest.mark.parametrize("limit", [None, 1, 3, 5, 10])
@sql_count_checker(query_count=1)
def test_groupby_bfill_ffill_basic(method, groupby_list, limit):
    native_df = native_pd.DataFrame(
        {
            "key": [0, 0, 1, 1, 1],
            "A": [np.nan, 2, np.nan, 3, np.nan],
            "B": [2, 3, np.nan, np.nan, np.nan],
            "C": [np.nan, np.nan, 2, np.nan, np.nan],
        },
        index=native_pd.Index(["A", "B", "C", "D", "E"], name="X"),
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: getattr(df.groupby(groupby_list), method)(limit=limit),
    )


@pytest.mark.parametrize("method", ["bfill", "ffill"])
@pytest.mark.parametrize("by_list", ["I", 0, "A"])
@sql_count_checker(query_count=1)
def test_groupby_bfill_ffill_single_index(method, by_list):
    native_df = native_pd.DataFrame(
        TEST_DF_DATA, index=TEST_DF_INDEX_1, columns=TEST_DF_COLUMNS_1
    )
    snow_df = pd.DataFrame(native_df)

    if isinstance(by_list, int):
        level = by_list
        by_list = None
    else:
        level = None

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: getattr(df.groupby(by_list, level=level), method)(limit=None),
    )


@pytest.mark.parametrize("method", ["bfill", "ffill"])
@pytest.mark.parametrize("by_list", ["I", 0])
@pytest.mark.parametrize("limit", [None, 1, 3, 5, 10])
@sql_count_checker(query_count=1)
def test_groupby_bfill_ffill_with_limit(method, by_list, limit):
    native_df = native_pd.DataFrame(
        TEST_DF_DATA_3, index=TEST_DF_INDEX_3, columns=TEST_DF_COLUMNS_3
    )
    snow_df = pd.DataFrame(native_df)

    if isinstance(by_list, int):
        level = by_list
        by_list = None
    else:
        level = None

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: getattr(df.groupby(by_list, level=level), method)(limit=limit),
    )


@pytest.mark.parametrize("method", ["bfill", "ffill"])
@pytest.mark.parametrize("by", ["I", ["I", "J"]])
@pytest.mark.parametrize("limit", [None, 1, 3, 5, 10])
@sql_count_checker(query_count=1)
def test_groupby_bfill_ffill_multiindex(method, by, limit):
    native_df = native_pd.DataFrame(
        TEST_DF_DATA_2, index=TEST_DF_INDEX_2, columns=TEST_DF_COLUMNS_2
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: getattr(df.groupby(by=by, level=None, axis=0), method)(limit=limit),
    )


@pytest.mark.parametrize("method", ["bfill", "ffill"])
@pytest.mark.parametrize("level", [0, 1, [0, 1]])
@pytest.mark.parametrize("limit", [None, 1, 3, 5, 10])
@sql_count_checker(query_count=1)
def test_groupby_bfill_ffill_multiindex_with_level(method, level, limit):
    native_df = native_pd.DataFrame(
        TEST_DF_DATA_2, index=TEST_DF_INDEX_2, columns=TEST_DF_COLUMNS_2
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: getattr(df.groupby(by=None, level=level, axis=0), method)(
            limit=limit
        ),
    )


@pytest.mark.parametrize("method", ["bfill", "ffill"])
@pytest.mark.parametrize("by_list", [["I", "A"], ["A"], ["A", "B"], 10])
@sql_count_checker(query_count=0)
def test_groupby_bfill_ffill_multiindex_negative(method, by_list):
    native_df = native_pd.DataFrame(
        TEST_DF_DATA_2, index=TEST_DF_INDEX_2, columns=TEST_DF_COLUMNS_2
    )
    snow_df = pd.DataFrame(native_df)

    if isinstance(by_list, int):
        level = by_list
        by_list = None
    else:
        level = None
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: getattr(df.groupby(by_list, level=level, axis=0), method)(
            limit=None
        ),
        expect_exception=True,
        expect_exception_type=IndexError if level is not None else KeyError,
    )


@pytest.mark.parametrize("method", ["bfill", "ffill"])
@sql_count_checker(query_count=0)
def test_groupby_bfill_ffill_unsupported_grouping_negative(method):
    native_df = native_pd.DataFrame(
        TEST_DF_DATA, index=TEST_DF_INDEX_1, columns=TEST_DF_COLUMNS_1
    )
    snow_df = pd.DataFrame(native_df)

    with pytest.raises(
        NotImplementedError,
        match=f"{_GROUPBY_UNSUPPORTED_GROUPING_MESSAGE}",
    ):
        # call to_pandas to trigger the evaluation of the operation
        getattr(snow_df.groupby("I", axis=1), method)(limit=None).to_pandas()


@pytest.mark.parametrize("method", ["bfill", "ffill"])
@pytest.mark.parametrize("level", [0, 1, [0, 1]])
@pytest.mark.parametrize("limit", [None, 1, 3, 5, 10])
@sql_count_checker(query_count=1)
def test_groupby_series_bfill_ffill(method, level, limit):
    native_df = native_pd.DataFrame(
        TEST_DF_DATA_2, index=TEST_DF_INDEX_2, columns=TEST_DF_COLUMNS_2
    )
    native_ser = native_df[(5, "A")]
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: getattr(ser.groupby(by=None, level=level, axis=0), method)(
            limit=limit
        ),
    )
