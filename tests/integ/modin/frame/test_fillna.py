#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.fixture(scope="function")
def test_fillna_df():
    return native_pd.DataFrame(
        [
            [np.nan, 2, np.nan, 0],
            [3, 4, np.nan, 1],
            [np.nan, np.nan, np.nan, np.nan],
            [np.nan, 3, np.nan, 4],
        ],
        columns=list("ABCD"),
    )


@pytest.fixture(scope="function")
def test_fillna_df_limit():
    return native_pd.DataFrame(
        [
            [1, 2, np.nan, 4],
            [np.nan, np.nan, 7, np.nan],
            [np.nan, 10, np.nan, 12],
            [np.nan, np.nan, 15, 16],
        ],
        columns=list("ABCD"),
    )


@pytest.fixture(scope="function")
def test_fillna_df_none_index():
    # test case to make sure fillna only fill missing values in data columns not index columns
    return native_pd.DataFrame(
        [
            [np.nan, 2, np.nan, 0],
            [3, 4, np.nan, 1],
            [np.nan, np.nan, np.nan, np.nan],
            [np.nan, 3, np.nan, 4],
        ],
        columns=list("ABCD"),
        index=[1, np.nan, 2, np.nan],
    )


@pytest.fixture(scope="function")
def test_fillna_df_2():
    return native_pd.DataFrame(
        [
            [3, 2, np.nan, 0],
            [3, 8, np.nan, 1],
            [np.nan, 4, np.nan, np.nan],
            [5, 7, np.nan, 4],
        ],
        columns=list("ABCD"),
    )


@pytest.fixture(scope="function")
def test_fillna_df_none_index_2():
    return native_pd.DataFrame(
        [
            [3, 2, np.nan, 0],
            [3, 8, np.nan, 1],
            [np.nan, 4, np.nan, np.nan],
            [5, 7, np.nan, 4],
        ],
        columns=list("ABCD"),
        index=[1, np.nan, 2, np.nan],
    )


@pytest.fixture(scope="function")
def test_fillna_df_dup():
    # test case for df with duplicated column names
    return native_pd.DataFrame(
        [
            [np.nan, 2, np.nan, 0],
            [3, 4, np.nan, 1],
            [np.nan, np.nan, np.nan, np.nan],
            [np.nan, 3, np.nan, 4],
        ],
        columns=list("ABBB"),
    )


@sql_count_checker(query_count=0)
def test_fillna_for_both_value_and_method_None_negative():
    native_df = native_pd.DataFrame()
    snow_df = pd.DataFrame()

    # Check error when `value` and `method` are both `None`.
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.fillna(),
        expect_exception=True,
        assert_exception_equal=True,
        expect_exception_match="Must specify a fill 'value' or 'method'.",
        expect_exception_type=ValueError,
    )

    # Check error when `value` and `method` are both *not* `None`.
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.fillna(value=1, method="ffill"),
        expect_exception=True,
        assert_exception_equal=True,
        expect_exception_type=ValueError,
        expect_exception_match="Cannot specify both 'value' and 'method'.",
    )


@sql_count_checker(query_count=0)
def test_fillna_invalid_method_negative():
    native_df = native_pd.DataFrame()
    snow_df = pd.DataFrame()

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.fillna(method="invalid_method"),
        expect_exception=True,
        assert_exception_equal=True,
        expect_exception_type=ValueError,
        expect_exception_match=r"Invalid fill method. Expecting pad \(ffill\) or backfill \(bfill\)\. Got invalid_method",
    )


@sql_count_checker(query_count=1)
def test_value_scalar(test_fillna_df):
    eval_snowpark_pandas_result(
        pd.DataFrame(test_fillna_df),
        test_fillna_df,
        lambda df: df.fillna(1),
    )


@sql_count_checker(query_count=2)
def test_timedelta_value_scalar(test_fillna_df):
    timedelta_df = test_fillna_df.astype("timedelta64[ns]")
    eval_snowpark_pandas_result(
        pd.DataFrame(timedelta_df),
        timedelta_df,
        lambda df: df.fillna(pd.Timedelta(1)),  # dtype keeps to be timedelta64[ns]
    )

    # Snowpark pandas dtype will be changed to int in this case
    eval_snowpark_pandas_result(
        pd.DataFrame(timedelta_df),
        test_fillna_df,
        lambda df: df.fillna(1),
    )


@sql_count_checker(query_count=1)
def test_value_scalar_none_index(test_fillna_df_none_index):
    # note: none in index should not be filled
    eval_snowpark_pandas_result(
        pd.DataFrame(test_fillna_df_none_index),
        test_fillna_df_none_index,
        lambda df: df.fillna(1),
    )


@sql_count_checker(query_count=1)
def test_value_scalar_axis_1(test_fillna_df):
    # axis=1 has no effect when fillna with scalar value.
    eval_snowpark_pandas_result(
        pd.DataFrame(test_fillna_df),
        test_fillna_df,
        lambda df: df.fillna(1, axis=1),
    )


@sql_count_checker(query_count=0)
def test_value_scalar_diff_type(test_fillna_df):
    snow_df = pd.DataFrame(test_fillna_df)
    # native pandas is able to upcast the column to object type if the type for the fillna
    # value is different compare with the column data type. However, in Snowpark pandas, we stay
    # consistent with the Snowflake type system, and a SnowparkSQLException is raised if the type
    # for the fillna value is not compatible with the column type.
    message = "Numeric value 'str' is not recognized"
    with pytest.raises(SnowparkSQLException, match=message):
        # call to_pandas to trigger the evaluation of the operation
        snow_df.fillna("str").to_pandas()


@sql_count_checker(query_count=1)
def test_value_dict(test_fillna_df):
    eval_snowpark_pandas_result(
        pd.DataFrame(test_fillna_df),
        test_fillna_df,
        lambda df: df.fillna({"A": 1, "B": 3.0}),
    )


@sql_count_checker(query_count=1)
def test_value_dict_dup(test_fillna_df_dup):
    # test df with duplicate column names
    eval_snowpark_pandas_result(
        pd.DataFrame(test_fillna_df_dup),
        test_fillna_df_dup,
        lambda df: df.fillna(
            {"A": 1, "B": 2.0, "B": 5}  # noqa: F601
        ),  # note pandas use the last one if it's duplicated when value is a dict
    )


@sql_count_checker(query_count=1)
def test_dup_index_data_column_with_value_dict(test_fillna_df_none_index):
    # set the index column name to be 'A', whose name is duplicate with one of the data
    # columns
    test_fillna_df_none_index.index.names = ["A"]

    eval_snowpark_pandas_result(
        pd.DataFrame(test_fillna_df_none_index),
        test_fillna_df_none_index,
        lambda df: df.fillna({"A": 1, "B": 2.0}),
    )


@pytest.mark.parametrize(
    "value_dict",
    [
        {("A", "B"): 1, "A": 2, ("A",): 3, ("A", "B", "C"): 4},
        {"A": 2, ("A",): 3},
        {("A",): 3, "A": 2},
        {("A",): 3},
    ],
)
@sql_count_checker(query_count=1)
def test_value_dict_dup_multiindex(value_dict):
    eval_snowpark_pandas_result(
        pd.DataFrame(
            [1, 2, 3, None], columns=pd.MultiIndex.from_tuples([("A", "B", "C")])
        ),
        native_pd.DataFrame(
            [1, 2, 3, None], columns=pd.MultiIndex.from_tuples([("A", "B", "C")])
        ),
        lambda df: df.fillna(value_dict),
    )


@sql_count_checker(query_count=2)
def test_value_series(test_fillna_df):
    eval_snowpark_pandas_result(
        pd.DataFrame(test_fillna_df),
        test_fillna_df,
        lambda df: df.fillna(pd.Series({"A": 1, "B": 3.0}))
        if isinstance(df, pd.DataFrame)
        else df.fillna(native_pd.Series({"A": 1, "B": 3.0})),
    )


@sql_count_checker(query_count=2)
def test_value_series_dup(test_fillna_df):
    # note that pandas use the first value when value is duplicated in a series
    series_dup = native_pd.Series([0, 1, 2, 3], index=["A", "B", "B", "C"])
    eval_snowpark_pandas_result(
        pd.DataFrame(test_fillna_df),
        test_fillna_df,
        lambda df: df.fillna(pd.Series(series_dup))
        if isinstance(df, pd.DataFrame)
        else df.fillna(series_dup),
    )


@sql_count_checker(query_count=1)
def test_value_scalar_inplace(test_fillna_df):
    eval_snowpark_pandas_result(
        pd.DataFrame(test_fillna_df),
        test_fillna_df,
        lambda df: df.fillna(1, inplace=True),
        inplace=True,
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("axis", [0, 1])
@pytest.mark.parametrize("limit", [1, 2, 3, 100])
@pytest.mark.parametrize("method", ["ffill", "bfill"])
def test_fillna_limit(test_fillna_df_limit, method, limit, axis):
    native_df = test_fillna_df_limit
    if axis == 1:
        native_df = native_df.T
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.fillna(method=method, limit=limit, axis=axis)
    )


@sql_count_checker(query_count=0)
def test_argument_negative(test_fillna_df):
    snow_df = pd.DataFrame(test_fillna_df)
    # value = None and method = None
    eval_snowpark_pandas_result(
        snow_df,
        test_fillna_df,
        lambda df: df.fillna(None),
        expect_exception=True,
        expect_exception_match="Must specify a fill 'value' or 'method'.",
        expect_exception_type=ValueError,
    )
    eval_snowpark_pandas_result(
        snow_df,
        test_fillna_df,
        lambda df: df.fillna(),
        expect_exception=True,
        expect_exception_match="Must specify a fill 'value' or 'method'.",
        expect_exception_type=ValueError,
    )
    eval_snowpark_pandas_result(
        snow_df,
        test_fillna_df,
        lambda df: df.fillna(None, method=None),
        expect_exception=True,
        expect_exception_match="Must specify a fill 'value' or 'method'.",
        expect_exception_type=ValueError,
    )
    eval_snowpark_pandas_result(
        snow_df,
        test_fillna_df,
        lambda df: df.fillna(1, method="1"),
        expect_exception=True,
        expect_exception_match="Cannot specify both 'value' and 'method'.",
        expect_exception_type=ValueError,
    )
    eval_snowpark_pandas_result(
        snow_df,
        test_fillna_df,
        lambda df: df.fillna(method="1"),
        expect_exception=True,
        expect_exception_match=re.escape(
            "Invalid fill method. Expecting pad (ffill) or backfill (bfill). Got 1"
        ),
        expect_exception_type=ValueError,
    )
    eval_snowpark_pandas_result(
        snow_df,
        test_fillna_df,
        lambda df: df.fillna(1, limit="1"),
        expect_exception=True,
        expect_exception_match=re.escape("Limit must be an integer"),
        expect_exception_type=ValueError,
    )
    eval_snowpark_pandas_result(
        snow_df,
        test_fillna_df,
        lambda df: df.fillna(1, limit=0),
        expect_exception=True,
        expect_exception_match=re.escape("Limit must be greater than 0"),
        expect_exception_type=ValueError,
    )
    eval_snowpark_pandas_result(
        snow_df,
        test_fillna_df,
        lambda df: df.fillna((1, 2)),
        expect_exception=True,
        expect_exception_match=re.escape('"value" parameter must be a scalar or dict'),
        expect_exception_type=TypeError,
    )
    # note axis=1 with a mapping value is not implemented in pandas
    eval_snowpark_pandas_result(
        snow_df,
        test_fillna_df,
        lambda df: df.fillna({"A": 1}, axis=1),
        expect_exception=True,
        expect_exception_match=re.escape(
            "Currently only can fill with dict/Series column by column"
        ),
        expect_exception_type=NotImplementedError,
    )


@pytest.fixture(scope="function")
def test_fillna_multiindex():
    tuples = [(1, "red", 3), (1, "blue", 3), ("blue", "red", 3), (1, "blue", 4)]
    return pd.MultiIndex.from_tuples(tuples, names=("l1", "l2", "l3"))


@pytest.fixture(scope="function")
def test_fillna_multiindex_df(test_fillna_multiindex):
    return native_pd.DataFrame(
        [
            [np.nan, 2, np.nan, 0],
            [3, 4, np.nan, 1],
            [np.nan, np.nan, np.nan, np.nan],
            [np.nan, 3, np.nan, 4],
        ],
        columns=test_fillna_multiindex,
    )


@sql_count_checker(query_count=1)
def test_multiindex_df_values_dict(test_fillna_multiindex_df):
    values = {(1, "red"): 9, (2, "blue"): 30}
    eval_snowpark_pandas_result(
        pd.DataFrame(test_fillna_multiindex_df),
        test_fillna_multiindex_df,
        lambda df: df.fillna(values),
    )


@sql_count_checker(query_count=1)
def test_multiindex_df_values_dict_various_levels(test_fillna_multiindex_df):
    values = {(1, "red"): 9, 2: 12, (2, "blue"): 30}
    eval_snowpark_pandas_result(
        pd.DataFrame(test_fillna_multiindex_df),
        test_fillna_multiindex_df,
        lambda df: df.fillna(values),
    )


@sql_count_checker(query_count=2)
def test_multiindex_df_values_series(test_fillna_multiindex_df, test_fillna_multiindex):
    values = pd.Series([10, 1, 2, 3], index=test_fillna_multiindex)
    native_values = native_pd.Series([10, 1, 2, 3], index=test_fillna_multiindex)
    eval_snowpark_pandas_result(
        pd.DataFrame(test_fillna_multiindex_df),
        test_fillna_multiindex_df,
        lambda df: df.fillna(values)
        if isinstance(df, pd.DataFrame)
        else df.fillna(native_values),
    )


@sql_count_checker(query_count=2)
def test_multiindex_df_values_dict_level_diff(test_fillna_multiindex_df):
    # key is 'blue' and pandas behavior is to match the top level in the multiindex
    values = {"blue": 30}
    eval_snowpark_pandas_result(
        pd.DataFrame(test_fillna_multiindex_df),
        test_fillna_multiindex_df,
        lambda df: df.fillna(values),
    )
    # similarly, key is (1,'blue') and pandas behavior is to match the prefix levels in the multiindex
    values = {(1, "blue"): 30}
    eval_snowpark_pandas_result(
        pd.DataFrame(test_fillna_multiindex_df),
        test_fillna_multiindex_df,
        lambda df: df.fillna(values),
    )


@sql_count_checker(query_count=1, join_count=1)
def test_values_df(test_fillna_df, test_fillna_df_2):
    eval_snowpark_pandas_result(
        pd.DataFrame(test_fillna_df),
        test_fillna_df,
        lambda df: df.fillna(pd.DataFrame(test_fillna_df_2))
        if isinstance(df, pd.DataFrame)
        else df.fillna(test_fillna_df_2),
    )


@sql_count_checker(query_count=1, join_count=1)
def test_values_df_none_index(test_fillna_df_none_index, test_fillna_df_none_index_2):
    eval_snowpark_pandas_result(
        pd.DataFrame(test_fillna_df_none_index),
        test_fillna_df_none_index,
        lambda df: df.fillna(pd.DataFrame(test_fillna_df_none_index_2))
        if isinstance(df, pd.DataFrame)
        else df.fillna(test_fillna_df_none_index_2),
    )


@pytest.mark.parametrize("method", ["ffill", "pad", "bfill", "backfill"])
@pytest.mark.parametrize("axis", [0, 1])
class TestFillNAMethod:
    @sql_count_checker(query_count=1)
    def test_df_fillna(self, test_fillna_df, method, axis):
        eval_snowpark_pandas_result(
            pd.DataFrame(test_fillna_df),
            test_fillna_df,
            lambda df: df.fillna(method=method, axis=axis),
        )

    @sql_count_checker(query_count=1)
    def test_df_fillna_none_index(self, test_fillna_df_none_index, method, axis):
        eval_snowpark_pandas_result(
            pd.DataFrame(test_fillna_df_none_index),
            test_fillna_df_none_index,
            lambda df: df.fillna(method=method, axis=axis),
        )

    @sql_count_checker(query_count=1)
    def test_df_fillna_all_nan_values(self, method, axis):
        native_df = native_pd.DataFrame(
            [
                [np.nan] * 4,
                [np.nan] * 4,
                [np.nan] * 4,
                [np.nan] * 4,
            ]
        )
        snow_df = pd.DataFrame(native_df)
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.fillna(method=method, axis=axis)
        )

    @sql_count_checker(query_count=1)
    def test_df_fillna_multiple_fill_values(self, method, axis):
        # This test checks that if we have say the following df:
        #      A    B    C
        # 0  1.0  NaN  3.0
        # 1  NaN  5.0  NaN
        # 2  7.0  8.0  9.0
        # 3  NaN  NaN  NaN
        # using ffill, the NaN value in row 1 column A would be
        # filled by 1, while the NaN value in row 3 column A
        #  would be filled by 7 instead.
        native_df = native_pd.DataFrame(
            [[1, np.nan, 3], [np.nan, 5, np.nan], [7, 8, 9], [np.nan, np.nan, np.nan]],
            columns=["A", "B", "C"],
        )
        if axis == 1:
            native_df = native_df.T
        snow_df = pd.DataFrame(native_df)

        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.fillna(method=method, axis=axis)
        )


@pytest.mark.parametrize("method", ["ffill", "pad", "bfill", "backfill"])
@sql_count_checker(query_count=4)
def test_df_fillna_method_tall_df(method):
    first_row = [native_pd.DataFrame([[1, 2, 3, 4]], columns=["A", "B", "C", "D"])]
    first_middle_rows = [
        native_pd.DataFrame([[5, np.nan, np.nan, 8]], columns=["A", "B", "C", "D"])
    ] * 2500
    second_middle_rows = [
        native_pd.DataFrame([[np.nan, 10, np.nan, 12]], columns=["A", "B", "C", "D"])
    ] * 2500
    third_middle_rows = [
        native_pd.DataFrame(
            [[np.nan, np.nan, np.nan, np.nan]], columns=["A", "B", "C", "D"]
        )
    ] * 2500
    all_rows = (
        first_row
        + first_middle_rows
        + second_middle_rows
        + third_middle_rows
        + first_row
    )
    native_df = native_pd.concat(all_rows)
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.fillna(method=method),
    )


@pytest.mark.parametrize("method", ["ffill", "pad", "bfill", "backfill"])
@sql_count_checker(query_count=1)
def test_df_fillna_method_reindexed_df_reversed_columns(method):
    native_df = native_pd.DataFrame(
        [
            [1, np.nan, 3, np.nan, 5],
            [6, np.nan, 8, np.nan, 10],
            [11, np.nan, 13, np.nan, 15],
            [16, np.nan, 18, np.nan, 20],
        ],
        columns=["A", "B", "C", "D", "E"],
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.iloc[:, ::-1].fillna(method=method, axis=1)
    )


@pytest.mark.parametrize("method", ["ffill", "pad", "bfill", "backfill"])
@sql_count_checker(query_count=1)
def test_df_fillna_method_reindexed_df_reordered_columns(method):
    native_df = native_pd.DataFrame(
        [
            [1, np.nan, 3, np.nan, 5],
            [6, np.nan, 8, np.nan, 10],
            [11, np.nan, 13, np.nan, 15],
            [16, np.nan, 18, np.nan, 20],
        ],
        columns=["A", "B", "C", "D", "E"],
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df[["B", "E", "A", "C", "D"]].fillna(method=method, axis=1),
    )


@sql_count_checker(query_count=0)
@pytest.mark.xfail(
    reason="TODO SNOW-1489309 investigate why it starts to work now on qa"
)
def test_df_fillna_method_with_type_coercion_errors_for_variant_column_negative():
    # Thanks to Snowflake's type coercions, we don't match pandas
    # behavior when we are using fillna that involves filling values
    # in differently typed columns.
    native_df = native_pd.DataFrame(
        [
            [True, None, 3, np.nan, False],
            [None, False, np.nan, 4, None],
            [7, None, 5, None, True],
            [None, None, np.nan, np.nan, None],
        ],
        columns=["A", "B", "C", "D", "E"],
    )
    snow_df = pd.DataFrame(native_df)

    # This fails in Snowpark pandas, but not in vanilla pandas, because Snowpark pandas
    # attempts to fill the value 7 from the variant column "A" into the boolean column
    # "B", which fails due to casting errors.
    with pytest.raises(
        SnowparkSQLException, match="Failed to cast variant value 7 to BOOLEAN"
    ):
        snow_df.fillna(method="ffill", axis=1).to_pandas()


def test_df_fillna_method_with_type_coercion_casts_all_as_bool_negative():
    # Thanks to Snowflake's type coercions, we don't match pandas
    # behavior when we are using fillna that involves filling values
    # in differently typed columns.
    native_df = native_pd.DataFrame(
        [
            [True, None, 3, np.nan, False],
            [None, False, np.nan, 4, None],
            [7, None, 5, None, True],
            [None, None, np.nan, np.nan, None],
        ],
        columns=["A", "B", "C", "D", "E"],
    )
    snow_df = pd.DataFrame(native_df)

    # In this next case, pandas will keep the original types of columns
    # that have mixed values, but Snowpark pandas will convert the int
    # columns to boolean.
    def fillna_helper(df):
        if isinstance(df, native_pd.DataFrame):
            new_df = df.iloc[:, ::-1].fillna(method="ffill", axis=1)
            # Can't use `astype(bool)` here since that converts missing values
            # (None values) to `False`, but we need to keep the `None` values.
            new_df["B"] = new_df["B"].apply(lambda x: bool(x) if x is not None else x)
            new_df["C"] = new_df["C"].apply(lambda x: bool(x) if x is not None else x)
            new_df["D"] = new_df["D"].apply(lambda x: bool(x) if x is not None else x)
            return new_df
        return df.iloc[:, ::-1].fillna(method="ffill", axis=1)

    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            fillna_helper,
        )

    # This checks that the same values (positionally) are filled in
    # in the previous test.
    def check_which_values_filled(df):
        return df.iloc[:, ::-1].fillna(method="ffill", axis=1).isna()

    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            check_which_values_filled,
        )
