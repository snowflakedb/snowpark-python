#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    _GROUPBY_UNSUPPORTED_GROUPING_MESSAGE,
)
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

METHOD_OR_VALUES = [
    ("ffill", None),
    ("bfill", None),
    (None, 123),
]

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


@pytest.mark.parametrize("method_or_value", METHOD_OR_VALUES)
@pytest.mark.parametrize("groupby_list", ["X", "key", ["X", "key"]])
@sql_count_checker(query_count=1)
def test_groupby_fillna_basic(groupby_list, method_or_value):
    method, value = method_or_value
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
        lambda df: df.groupby(groupby_list).fillna(method=method, value=value),
    )


@pytest.mark.parametrize("method_or_value", METHOD_OR_VALUES)
@pytest.mark.parametrize("fillna_axis", [0, 1])
@pytest.mark.parametrize("by_list", ["I", 0, "A"])
@sql_count_checker(query_count=1)
def test_groupby_fillna_single_index_ffill_bfill(method_or_value, by_list, fillna_axis):
    method, value = method_or_value
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
        lambda df: df.groupby(by_list, level=level).fillna(
            method=method, value=value, axis=fillna_axis
        ),
    )


@pytest.mark.parametrize("method", ["ffill", "bfill"])
@pytest.mark.parametrize("by_list", ["I", 0])
@pytest.mark.parametrize("limit", [None, 1, 3, 5, 10])
@sql_count_checker(query_count=1)
def test_groupby_fillna_ffill_bfill_with_limit_axis_0(method, by_list, limit):
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
        lambda df: df.groupby(by_list, level=level).fillna(
            method=method, axis=0, limit=limit
        ),
    )


@pytest.mark.parametrize("method", ["ffill", "bfill"])
@pytest.mark.parametrize("by_list", ["X", 0])
@pytest.mark.parametrize("limit", [None, 1, 3, 5, 10])
@sql_count_checker(query_count=1)
def test_groupby_fillna_ffill_bfill_with_limit_axis_1(method, by_list, limit):
    native_df = native_pd.DataFrame(
        TEST_DF_DATA_3, index=TEST_DF_INDEX_3, columns=TEST_DF_COLUMNS_3
    ).T
    snow_df = pd.DataFrame(native_df)

    if isinstance(by_list, int):
        level = by_list
        by_list = None
    else:
        level = None

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(by_list, level=level).fillna(
            method=method, axis=1, limit=limit
        ),
    )


@pytest.mark.parametrize("method_or_value", METHOD_OR_VALUES)
@pytest.mark.parametrize("fillna_axis", [0, 1])
@pytest.mark.parametrize("by", ["I", ["I", "J"]])
@sql_count_checker(query_count=1)
def test_groupby_fillna_multiindex_ffill_bfill(method_or_value, fillna_axis, by):
    method, value = method_or_value
    native_df = native_pd.DataFrame(
        TEST_DF_DATA_2, index=TEST_DF_INDEX_2, columns=TEST_DF_COLUMNS_2
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(by=by, level=None, axis=0).fillna(
            method=method, value=value, axis=fillna_axis
        ),
    )


@pytest.mark.parametrize("method_or_value", METHOD_OR_VALUES)
@pytest.mark.parametrize("fillna_axis", [0, 1])
@pytest.mark.parametrize("level", [0, 1, [0, 1]])
@sql_count_checker(query_count=1)
def test_groupby_fillna_multiindex_ffill_bfill_with_level(
    method_or_value, fillna_axis, level
):
    method, value = method_or_value
    native_df = native_pd.DataFrame(
        TEST_DF_DATA_2, index=TEST_DF_INDEX_2, columns=TEST_DF_COLUMNS_2
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(by=None, level=level, axis=0).fillna(
            method=method, value=value, axis=fillna_axis
        ),
    )


@pytest.mark.parametrize("method_or_value", METHOD_OR_VALUES)
@pytest.mark.parametrize("fillna_axis", [None, 1])
@pytest.mark.parametrize("by_list", [["I", "A"], ["A"], ["A", "B"], 10])
def test_groupby_fillna_multiindex_ffill_bfill_negative(
    method_or_value, fillna_axis, by_list
):
    method, value = method_or_value
    native_df = native_pd.DataFrame(
        TEST_DF_DATA_2, index=TEST_DF_INDEX_2, columns=TEST_DF_COLUMNS_2
    )
    snow_df = pd.DataFrame(native_df)

    if isinstance(by_list, int):
        level = by_list
        by_list = None
    else:
        level = None

    with SqlCounter(query_count=0):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.groupby(by_list, level=level, axis=0).fillna(
                method=method, value=value, axis=fillna_axis
            ),
            expect_exception=True,
            expect_exception_type=IndexError if level is not None else KeyError,
        )


@pytest.mark.parametrize(
    "method_or_value", [("buzz", None), (None, None), ("ffill", 123)]
)
@sql_count_checker(query_count=0)
def test_groupby_fillna_invalid_method_negative(method_or_value):
    method, value = method_or_value
    native_df = native_pd.DataFrame(
        TEST_DF_DATA_2, index=TEST_DF_INDEX_2, columns=TEST_DF_COLUMNS_2
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(["I"], axis=0).fillna(method=method, value=value, axis=0),
        expect_exception=True,
        expect_exception_type=ValueError,
    )


@sql_count_checker(query_count=0)
def test_groupby_fillna_value_not_type_compatible_negative():
    native_df = native_pd.DataFrame(
        TEST_DF_DATA, index=TEST_DF_INDEX_1, columns=TEST_DF_COLUMNS_1
    )
    snow_df = pd.DataFrame(native_df)

    message = "Numeric value 'str' is not recognized"
    # native pandas is able to upcast the column to object type if the type for the fillna
    # value is different compare with the column data type. However, in Snowpark pandas, we stay
    # consistent with the Snowflake type system, and a SnowparkSQLException is raised if the type
    # for the fillna value is not compatible with the column type.
    with pytest.raises(SnowparkSQLException, match=message):
        # call to_pandas to trigger the evaluation of the operation
        snow_df.groupby("I").fillna(value="str").to_pandas()


@sql_count_checker(query_count=0)
def test_groupby_fillna_downcast_not_supported_negative():
    native_df = native_pd.DataFrame(
        TEST_DF_DATA, index=TEST_DF_INDEX_1, columns=TEST_DF_COLUMNS_1
    )
    snow_df = pd.DataFrame(native_df)

    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas fillna API doesn't yet support 'downcast' parameter",
    ):
        # call to_pandas to trigger the evaluation of the operation
        snow_df.groupby("I").fillna(method="ffill", downcast={"A": "str"}).to_pandas()


@sql_count_checker(query_count=0)
def test_groupby_fillna_other_not_supported_negative():
    native_df = native_pd.DataFrame(
        TEST_DF_DATA, index=TEST_DF_INDEX_1, columns=TEST_DF_COLUMNS_1
    )
    snow_df = pd.DataFrame(native_df)

    with pytest.raises(
        NotImplementedError,
        match=f"Snowpark pandas GroupBy.fillna {_GROUPBY_UNSUPPORTED_GROUPING_MESSAGE}",
    ):
        # call to_pandas to trigger the evaluation of the operation
        snow_df.groupby("I", axis=1).fillna(method="ffill").to_pandas()

    with pytest.raises(
        ValueError,
        match="Cannot specify both 'value' and 'method'.",
    ):
        # call to_pandas to trigger the evaluation of the operation
        snow_df.groupby("I").fillna(method="ffill", value=123).to_pandas()

    with pytest.raises(
        ValueError,
        match="Must specify a fill 'value' or 'method'.",
    ):
        # call to_pandas to trigger the evaluation of the operation
        snow_df.groupby("I").fillna(method=None, value=None).to_pandas()

    with pytest.raises(
        ValueError,
        match=r"Invalid fill method. Expecting pad \(ffill\) or backfill \(bfill\). Got bazz",
    ):
        # call to_pandas to trigger the evaluation of the operation
        snow_df.groupby("I").fillna(method="bazz", value=None).to_pandas()


@pytest.mark.parametrize("method_or_value", METHOD_OR_VALUES)
@pytest.mark.parametrize("level", [0, 1, [0, 1]])
@sql_count_checker(query_count=1)
def test_groupby_series_fillna_ffill_bfill(method_or_value, level):
    method, value = method_or_value
    native_df = native_pd.DataFrame(
        TEST_DF_DATA_2, index=TEST_DF_INDEX_2, columns=TEST_DF_COLUMNS_2
    )
    native_ser = native_df[(5, "A")]
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.groupby(by=None, level=level, axis=0).fillna(
            method=method, value=value
        ),
    )
