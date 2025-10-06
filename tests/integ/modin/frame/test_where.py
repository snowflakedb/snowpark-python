#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re
from typing import Any, Optional, Union

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.fixture
def test_data():
    return {
        "A": [1, 2, 3, 4, 5],
        "B": [2, 3, 4, 5, 6],
        "C": [3, 4, 5, 6, 7],
        "D": [4, 5, 6, 7, 8],
        "E": [5, 6, 7, 8, 9],
    }


@pytest.fixture
def test_cond():
    return {
        "A": [True, False, True, False, True],
        "B": [False, True, False, True, False],
        "C": [True, False, True, False, True],
        "D": [False, True, False, True, False],
        "E": [True, False, True, False, True],
    }


@pytest.fixture
def test_others():
    return {
        "A": [200, 201, 202, 203, 204],
        "B": [300, 301, 302, 303, 304],
        "C": [400, 401, 402, 403, 404],
        "D": [500, 501, 502, 503, 504],
        "E": [600, 601, 602, 603, 604],
    }


def make_test_dataframe(df_data, preprocess_df=None):
    if isinstance(df_data, tuple):
        native_df = native_pd.DataFrame(df_data[0], columns=df_data[1])
        snow_df = pd.DataFrame(df_data[0], columns=df_data[1])
    else:
        native_df = native_pd.DataFrame(df_data)
        snow_df = pd.DataFrame(df_data)

    if preprocess_df:
        native_df = preprocess_df(native_df)
        snow_df = preprocess_df(snow_df)

    return native_df, snow_df


def dataframe_with_test_args(df, test_args):
    if test_args:
        if test_args[0]:
            df.set_index(test_args[0], inplace=True)
        if test_args[1]:
            df.columns = test_args[1]
    return df


def make_native_dataframe_or_scalar(test_data, test_args):
    if isinstance(test_data, tuple):
        df = native_pd.DataFrame(test_data[0], columns=test_data[1])
    elif isinstance(test_data, dict):
        df = native_pd.DataFrame(test_data)
    else:
        # It is not a dataframe, but rather a scalar value or callable
        return test_data

    return dataframe_with_test_args(df, test_args)


def make_snow_dataframe(test_data, test_args):
    if isinstance(test_data, tuple):
        df = pd.DataFrame(test_data[0], columns=test_data[1])
    elif isinstance(test_data, dict):
        df = pd.DataFrame(test_data)
    else:
        # It is not a dataframe, but rather a scalar value
        return test_data

    return dataframe_with_test_args(df, test_args)


def where_test_helper(
    df_data_list: list[Union[list, dict, np.array, Any]],
    df_data_args: Optional[list[tuple[Any]]] = None,
    coerce_to_float64: bool = True,
    expect_exception: bool = False,
    expect_exception_type: Optional[type[Exception]] = None,
    expect_exception_match: Optional[str] = None,
    assert_exception_equal: bool = True,
    extra_where_args: Optional[dict[Any, Any]] = None,
):
    """
    Helper for validating pivot_table tests, specifically this ensures the output is normalized to float64
    with acceptable precision so can compare the results if coerce_to_float64 is True.

    df_data_list: The raw data to be put in df as list of data/columns, dictionary of data values (col:series) or np.array
    df_data_args: Arguments to pass to `make_{native|snow}_dataframe{_or_scalar}
    coerce_to_float64: Coerce the results to float64 result since irrational numbers can result in object type
    expect_exception: Whether the call *should* raise an exception
    expect_exception_type: if not None, assert the exception type is expected
    expect_exception_match: if not None, assert the exception match the expected regex
    assert_exception_equal: bool. Whether to assert the exception from Snowpark pandas eqauls to pandas
    extra_where_args: Additional arguments to pass to where as a dictionary.
    """
    extra_where_args = extra_where_args or {}
    df_data_args = df_data_args or [None] * len(df_data_list)
    native_dfs = [
        make_native_dataframe_or_scalar(data, data_args)
        for data, data_args in zip(df_data_list, df_data_args)
    ]
    snow_dfs = [
        make_snow_dataframe(data, data_args)
        for data, data_args in zip(df_data_list, df_data_args)
    ]

    eval_snowpark_pandas_result(
        snow_dfs[0],
        native_dfs[0],
        lambda df: df.where(native_dfs[1], native_dfs[2], **extra_where_args)
        if isinstance(df, native_pd.DataFrame)
        else df.where(snow_dfs[1], snow_dfs[2], **extra_where_args),
        comparator=assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64
        if coerce_to_float64
        else assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
        expect_exception=expect_exception,
        expect_exception_type=expect_exception_type,
        expect_exception_match=expect_exception_match,
        assert_exception_equal=assert_exception_equal,
    )


@pytest.mark.parametrize(
    "test_args",
    [
        # The test arguments specify different index & column combinations
        # ([(data index, data columns)], ([cond index], [cond columns]), ([other index], [other columns]))
        (
            (["A", "B"], ["C", "D", "E"]),
            (["A", "B"], ["C", "D", "E"]),
            (["A", "B"], ["C", "D", "E"]),
        ),
        (
            (["A", "B"], ["C", "D", "E"]),
            (["A", "B"], ["E", "D", "C"]),
            (["A", "B"], ["C", "E", "D"]),
        ),
        (
            (["A", "B"], ["C", "D", "E"]),
            (["A", "B"], ["X", "Y", "C"]),
            (["A", "B"], ["X", "Y", "C"]),
        ),
        (
            (["A", "B"], ["C", "D", "E"]),
            (["A", "B"], ["X", "Y", "Z"]),
            (["A", "B"], ["C", "D", "E"]),
        ),
        (
            (["A", "B"], ["C", "D", "E"]),
            (["A", "B"], ["C", "D", "E"]),
            (["A", "B"], ["X", "Y", "Z"]),
        ),
        (
            (["A", "B", "C", "D"], ["E"]),
            (["A", "B", "C", "D"], ["E"]),
            (["A", "B", "C", "D"], ["E"]),
        ),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_dataframe_where_with_cond_and_others_unmatching_data_column_incompatible_index_type(
    test_data, test_cond, test_others, test_args
):
    where_test_helper([test_data, test_cond, test_others], test_args)


@sql_count_checker(query_count=1, join_count=2)
def test_dataframe_where_with_cond_and_others_unmatching_data_column_compatible_index_type(
    test_data, test_cond, test_others
):
    where_test_helper([test_data, test_cond, test_others], (None, None, None))


@pytest.mark.parametrize(
    "test_args",
    [
        # The test arguments specify different index & column combinations
        # ([(data index, data columns)], ([cond index], [cond columns]), ([other index], [other columns]))
        (None, None, None),
        (
            (["A", "B"], ["C", "D", "E"]),
            (["A", "B"], ["C", "D", "E"]),
            (["A", "B"], ["C", "D", "E"]),
        ),
        (
            (["A", "B"], ["C", "D", "E"]),
            (["A", "B"], ["E", "D", "C"]),
            (["A", "B"], ["C", "E", "D"]),
        ),
        (
            (["A", "B"], ["C", "D", "E"]),
            (["A", "B"], ["X", "Y", "C"]),
            (["A", "B"], ["X", "Y", "C"]),
        ),
        (
            (["A", "B"], ["C", "D", "E"]),
            (["A", "B"], ["X", "Y", "Z"]),
            (["A", "B"], ["C", "D", "E"]),
        ),
        (
            (["A", "B"], ["C", "D", "E"]),
            (["A", "B"], ["C", "D", "E"]),
            (["A", "B"], ["X", "Y", "Z"]),
        ),
        (
            (["A", "B", "C", "D"], ["E"]),
            (["A", "B", "C", "D"], ["E"]),
            (["A", "B", "C", "D"], ["E"]),
        ),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_dataframe_where_with_cond_and_others_matching_index(
    test_data, test_cond, test_others, test_args
):
    data = test_data.copy()
    cond = test_cond.copy()
    others = test_others.copy()

    # Ensure they have matching index values or won't get any results
    if test_args and test_args[0]:
        for col in test_args[0][0]:
            cond[col] = data[col]
            others[col] = data[col]

    where_test_helper([data, cond, others], test_args)


@pytest.mark.parametrize(
    "test_args",
    [
        # The test arguments specify different index & column combinations
        # ([(data index, data columns)], ([cond index], [cond columns]), ([other index], [other columns]))
        (
            (["A"], ["B", "C", "D", "E"]),
            (["A"], ["B", "C", "D", "E"]),
            (["A"], ["B", "C", "D", "E"]),
        ),
        (
            (["A"], ["B", "C", "D", "E"]),
            (["A"], ["E", "D", "C", "B"]),
            (["A"], ["B", "D", "C", "E"]),
        ),
        (
            (["A"], ["B", "C", "D", "E"]),
            (["A"], ["F", "G", "H", "I"]),
            (["A"], ["J", "K", "L", "M"]),
        ),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_dataframe_where_with_cond_and_others_succeed_in_snowflake(
    test_data, test_cond, test_others, test_args
):
    snow_df = pd.DataFrame(test_data)
    snow_cond_df = pd.DataFrame(test_cond)
    snow_other_df = pd.DataFrame(test_others)

    # Note the corresponding where in pandas fails in what looks like a bug since the mask and data sizes are the
    # same in contrast to the error message.
    #
    # 'ValueError: putmask: mask and data must be the same size

    snow_result_df = snow_df.where(snow_cond_df, snow_other_df)

    native_df = native_pd.DataFrame(test_data)
    native_cond_df = native_pd.DataFrame(test_cond)
    native_other_df = native_pd.DataFrame(test_others)

    expected_result_df = native_df.apply(
        lambda x: native_pd.Series(
            list(
                map(
                    lambda t: t[0] if t[1] else t[2],
                    list(
                        zip(
                            native_df[x.name].to_numpy(),
                            native_cond_df[x.name].to_numpy(),
                            native_other_df[x.name].to_numpy(),
                        )
                    ),
                )
            ),
            name=x.name,
        )
    )

    assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64(
        snow_result_df, expected_result_df
    )


@pytest.mark.parametrize(
    "cond_test",
    [
        lambda x: x >= 6,
        lambda x: x % 2 == 0,
    ],
)
@sql_count_checker(query_count=1, join_count=0)
def test_dataframe_where_with_cond_is_lambda(test_data, cond_test):
    where_test_helper([test_data, cond_test, None], [[["A", "B"], None], None, None])


@pytest.mark.parametrize(
    "cond_test",
    [
        lambda x: True,
        lambda x: False,
    ],
)
@sql_count_checker(query_count=0)
def test_dataframe_where_with_cond_is_lambda_true_and_false(test_data, cond_test):
    where_test_helper(
        [test_data, cond_test, None],
        expect_exception=True,
        expect_exception_type=ValueError,
        assert_exception_equal=True,
        expect_exception_match="Array conditional must be same shape as self",
    )


@pytest.mark.parametrize(
    "other_test, expected_query_count",
    [
        (lambda x: x**2, 1),
        (
            lambda x: x + x,
            1,
        ),
        (lambda x: x, 1),
        (lambda y: y + 10, 1),
    ],
)
def test_dataframe_where_with_other_is_lambda(
    test_data, test_cond, other_test, expected_query_count
):
    cond = test_cond.copy()
    cond["A"] = test_data["A"]
    cond["B"] = test_data["B"]
    with SqlCounter(
        query_count=expected_query_count,
    ):
        where_test_helper(
            [test_data, cond, other_test],
            [[["A", "B"], None], [["A", "B"], None], None],
        )


@pytest.mark.parametrize("test_other_scalar", [None, -1, 99.9])
@sql_count_checker(query_count=1, join_count=1)
def test_dataframe_where_with_cond_and_scalar_others(
    test_data, test_cond, test_other_scalar
):
    where_test_helper([test_data, test_cond, test_other_scalar])


@pytest.mark.parametrize(
    "test_other_scalar",
    [
        123,
        99.987,
        "x",
        True,
        lambda x: "x" if min(x) == 3 else "y",
        dict(zip(list("ABCDE"), [list("ABCDE")] * 5)),
    ],
)
def test_dataframe_where_with_cond_and_scalar_others_with_type_incompatible(
    test_data, test_cond, test_other_scalar
):
    expected_join_count = 2 if isinstance(test_other_scalar, dict) else 1
    with SqlCounter(query_count=1, join_count=expected_join_count):
        where_test_helper([test_data, test_cond, test_other_scalar])


@sql_count_checker(query_count=0)
def test_dataframe_where_cond_non_boolean_negative_test(
    test_data, test_cond, test_others
):
    test_cond2 = test_cond.copy()
    test_cond2["C"] = ["a", "b", "c", "d", "e"]

    where_test_helper(
        [test_data, test_cond2, test_others],
        expect_exception=True,
        expect_exception_match="Boolean array expected for the condition, not object",
        expect_exception_type=ValueError,
    )


@sql_count_checker(query_count=0)
def test_dataframe_where_cond_is_none_negative(test_data):
    where_test_helper(
        [test_data, None, None],
        expect_exception=True,
        expect_exception_match=r"Array conditional must be same shape as self",
        expect_exception_type=ValueError,
        assert_exception_equal=True,
    )


@sql_count_checker(query_count=0)
def test_dataframe_where_not_implemented(test_data, test_cond, test_others):
    index_data = [["A", "B"], ["C", "D", "E"]]
    df_data_list = [test_data, test_cond, test_others]
    df_data_args = [index_data, index_data, index_data]
    snow_dfs = [
        make_snow_dataframe(data, data_args)
        for data, data_args in zip(df_data_list, df_data_args)
    ]
    with pytest.raises(NotImplementedError):
        snow_dfs[0].where(snow_dfs[1], snow_dfs[2], axis=1)


@sql_count_checker(query_count=1, join_count=2)
def test_dataframe_where_cond_is_array(caplog):
    data = [[1, 2], [3, 4]]
    cond = np.array([[True, False], [False, True]])

    snow_df = pd.DataFrame(data=data)
    native_df = native_pd.DataFrame(data=data)

    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.where(cond))


@sql_count_checker(query_count=0)
def test_dataframe_where_cond_is_array_wrong_size_negative():
    data = [[1, 2, 3], [3, 4, 5], [5, 6, 7]]
    cond = np.array([[True, False], [False, True]])

    snow_df = pd.DataFrame(data=data)
    native_df = native_pd.DataFrame(data=data)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.where(cond),
        expect_exception=True,
        expect_exception_type=ValueError,
        assert_exception_equal=True,
    )


@sql_count_checker(query_count=1)
def test_dataframe_where_with_callable_cond():
    data = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    class CallableCond:
        def __call__(self, df):
            return df % 2 == 0

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.where(CallableCond(), -1),
    )


@sql_count_checker(query_count=1)
def test_dataframe_where_with_callable_other():
    data = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    class CallableOther:
        def __call__(self, df):
            return df**2

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.where(df % 2 == 0, CallableOther()),
    )


@sql_count_checker(query_count=1, join_count=2)
def test_dataframe_where_other_is_array():
    data = [[1, 3], [2, 4]]
    other = np.array([[99, -99], [101, -101]])

    snow_df = pd.DataFrame(data=data)
    native_df = native_pd.DataFrame(data=data)

    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.where(lambda x: x >= 3, other)
    )


@sql_count_checker(query_count=0)
def test_dataframe_where_other_is_array_wrong_size_negative():
    data = [[1, 2, 3], [3, 4, 5], [5, 6, 7]]
    other = np.array([[99, -99], [101, -101]])

    snow_df = pd.DataFrame(data=data)
    native_df = native_pd.DataFrame(data=data)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.where(lambda x: x >= 3, other),
        expect_exception=True,
        expect_exception_type=ValueError,
        assert_exception_equal=True,
    )


@sql_count_checker(query_count=0)
def test_dataframe_where_sizes_do_not_match_negative_test(test_data, test_cond):
    snow_df = pd.DataFrame(test_data)
    snow_df.set_index(["A"], inplace=True)

    snow_cond_df = pd.DataFrame(test_cond)
    snow_cond_df.set_index(["B", "C"], inplace=True)

    with pytest.raises(ValueError, match="cannot join with no overlapping index names"):
        snow_df.where(snow_cond_df)


@sql_count_checker(query_count=1, join_count=3)
def test_dataframe_where_with_np_array_cond():
    data = [1, 2, 3]
    cond = np.array([[False, True, False]]).T
    other = [4, 5, 6]

    snow_df = pd.DataFrame(data, columns=["A"])
    snow_other_df = pd.DataFrame(
        other,
        columns=["A"],
    )

    native_df = native_pd.DataFrame(data, columns=["A"])
    native_other_df = native_pd.DataFrame(
        other,
        columns=["A"],
    )

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.where(cond, native_other_df)
        if isinstance(df, native_pd.DataFrame)
        else df.where(cond, snow_other_df),
    )


@sql_count_checker(query_count=1, join_count=3)
def test_dataframe_where_with_np_array_cond_mismatched_labels():
    data = [1, 2, 3]
    cond = np.array([[False, True, False]]).T
    other = [4, 5, 6]

    snow_df = pd.DataFrame(data, columns=["A"])
    snow_other_df = pd.DataFrame(
        other, columns=["B"], index=pd.Index([1, 2, 3], name="A")
    )

    native_df = native_pd.DataFrame(data, columns=["A"])
    native_other_df = native_pd.DataFrame(
        other, columns=["B"], index=native_pd.Index([1, 2, 3], name="A")
    )

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.where(native_pd.DataFrame(cond), native_other_df)
        if isinstance(df, native_pd.DataFrame)
        else df.where(pd.DataFrame(cond), snow_other_df),
    )


@sql_count_checker(query_count=1, join_count=3)
def test_dataframe_where_with_dataframe_cond_single_index_different_names():
    data = [1, 2, 3]
    cond = [False, True, False]
    other = [4, 5, 6]

    snow_df = pd.DataFrame(data, columns=["A"])
    snow_cond_df = pd.DataFrame(cond, columns=["A"])
    snow_other_df = pd.DataFrame(
        other, columns=["B"], index=pd.Index([1, 2, 3], name="A")
    )

    native_df = native_pd.DataFrame(data, columns=["A"])
    native_cond_df = native_pd.DataFrame(cond, columns=["A"])
    native_other_df = native_pd.DataFrame(
        other, columns=["B"], index=native_pd.Index([1, 2, 3], name="A")
    )

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.where(native_cond_df, native_other_df)
        if isinstance(df, native_pd.DataFrame)
        else df.where(snow_cond_df, snow_other_df),
    )


@sql_count_checker(query_count=1, join_count=3)
def test_dataframe_where_with_dataframe_cond_single_index_different_names_2():
    data = [1, 2, 3]
    cond = [False, True, False]
    other = [4, 5, 6]

    snow_df = pd.DataFrame(data, columns=["A"], index=pd.Index([1, 2, 3], name="B"))
    snow_cond_df = pd.DataFrame(cond, columns=["A"])
    snow_other_df = pd.DataFrame(other, columns=["B"])

    native_df = native_pd.DataFrame(
        data, columns=["A"], index=native_pd.Index([1, 2, 3], name="B")
    )
    native_cond_df = native_pd.DataFrame(cond, columns=["A"])
    native_other_df = native_pd.DataFrame(other, columns=["B"])

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.where(native_cond_df, native_other_df)
        if isinstance(df, native_pd.DataFrame)
        else df.where(snow_cond_df, snow_other_df),
    )


@pytest.mark.parametrize(
    "cond_frame",
    [
        native_pd.DataFrame({"A": [False, True, False, True]}),
        native_pd.DataFrame(
            {"A": [False, True, False, True], "B": [False, False, False, True]}
        ),
    ],
)
@pytest.mark.parametrize(
    "other",
    [
        10,
        native_pd.DataFrame({"A": [6, 6, 6, 10]}),
        native_pd.DataFrame({"A": [6, 6, 6, 10], "B": [8, 8, 9, 9]}),
    ],
)
def test_dataframe_where_with_duplicated_index_aligned(cond_frame, other):
    data = [3, 4, 5, 2]
    # index with duplicated value 2
    index = pd.Index([2, 1, 2, 3], name="index")
    native_index = native_pd.Index([2, 1, 2, 3], name="index")
    snow_df = pd.DataFrame({"A": data}, index=index)
    native_df = native_pd.DataFrame({"A": data}, index=native_index)

    native_cond = cond_frame
    native_cond.index = native_index
    snow_cond = pd.DataFrame(native_cond)

    if isinstance(other, native_pd.DataFrame):
        native_other = other
        native_other.index = native_index
        snow_other = pd.DataFrame(native_other)
    else:
        native_other = other
        snow_other = other

    expected_join_count = 1 if isinstance(other, int) else 2
    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.where(native_cond, native_other)
            if isinstance(df, native_pd.DataFrame)
            else df.where(snow_cond, snow_other),
        )


# 3 extra queries to convert index to native pandas when creating the 3 snowpark pandas dataframe
@sql_count_checker(query_count=4, join_count=2)
def test_dataframe_where_with_duplicated_index_unaligned():
    data = [3, 4, 5, 2]
    df_index = pd.Index([2, 1, 2, 3], name="index")
    snow_df = pd.DataFrame({"A": data}, index=df_index)

    index = pd.Index([1, 2, 2, 3], name="index")
    cond_data = [False, True, False, True]
    other_data = [4, 5, 6, 7]
    snow_cond = pd.DataFrame({"A": cond_data}, index=index)
    snow_other = pd.DataFrame({"A": other_data}, index=index)
    snow_res = snow_df.where(snow_cond, snow_other)

    # This behavior is different compare with native pandas. If index value in other and condition contains
    # duplication, and not aligned with the index of the dataframe, Native pandas errors out with
    # ValueError: putmask: mask and data must be the same size
    # Snowpark pandas will suceed with left join behavior because validation of index uniqueness
    # requires eager evaluation.
    expected_pandas = native_pd.DataFrame(
        {"A": [3, 3, 5, 6, 4, 5, 5, 5, 6, 2]},
        index=native_pd.Index([2, 2, 2, 2, 1, 2, 2, 2, 2, 3], name="index"),
    )
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_res, expected_pandas
    )


@pytest.mark.parametrize(
    "cond_column_names, others_column_names, expected_error_msg",
    [
        (
            ["A", "A", "C", "D", "E"],
            None,
            "Multiple columns are mapped to each label in ['A'] in DataFrame condition",
        ),
        (
            None,
            ["A", "C", "C", "D", "E"],
            "Multiple columns are mapped to each label in ['C'] in DataFrame other",
        ),
        (
            ["A", "B", "C", "C", "F"],
            ["A", "C", "C", "D", "E"],
            "Multiple columns are mapped to each label in ['C'] in DataFrame condition",
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_dataframe_where_with_duplicated_columns_negative(
    test_data,
    test_cond,
    test_others,
    cond_column_names,
    others_column_names,
    expected_error_msg,
):
    native_df = native_pd.DataFrame(test_data)
    native_cond = native_pd.DataFrame(test_cond)
    native_other = native_pd.DataFrame(test_others)
    snow_df = pd.DataFrame(native_df)
    snow_cond = pd.DataFrame(native_cond)
    snow_other = pd.DataFrame(native_other)
    # set the column to new names
    if cond_column_names is not None:
        native_cond.columns = cond_column_names
        snow_cond.columns = cond_column_names
    if others_column_names is not None:
        native_other.columns = others_column_names
        snow_other.columns = others_column_names

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.where(native_cond, native_other)
        if isinstance(df, native_pd.DataFrame)
        else df.where(snow_cond, snow_other),
        expect_exception=True,
        assert_exception_equal=False,
        # The error message raised under those cases are different from native pandas.
        # Native pandas raises ValueError with message "cannot reindex on an axis with duplicate labels"
        # for duplication occurs in the condition frame,and raises InvalidIndexError with no message for
        # duplication occurs in other frame.
        # Snowpark pandas gives a clear message to the customer about what is the problem with the code.
        expect_exception_type=ValueError,
        expect_exception_match=re.escape(expected_error_msg),
    )


@sql_count_checker(query_count=4)
def test_where_cond_with_base_df():
    native_df = native_pd.DataFrame([1, 2], columns=["a"], index=[0, 1])
    snow_df = pd.DataFrame(native_df)

    def func1(df):
        df["b"] = df["a"] == 1
        return df

    eval_snowpark_pandas_result(snow_df, native_df, func1, inplace=True)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df["a"].where(df["b"], df["a"] - 100),
    )

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df["a"].where(df["b"] != True, df["a"] - 100),  # noqa: E712
    )

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df["a"].where(df["a"] == 1, df["a"] - 100),
    )


@sql_count_checker(query_count=1)
def test_where_cond_with_base_df_filter_on_key():
    native_df = native_pd.DataFrame({"key": [0, 1], "value": [2, 3]})
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.value.where(df.key == 0)
    )


@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.parametrize(
    "data",
    [[True], [True, False, True], [True, False, True, False]],
    ids=[
        "series_shorter_than_dataframe",
        "series_same_length_as_dataframe",
        "series_longer_than_dataframe",
    ],
)
def test_where_series_cond(data):
    native_df = native_pd.DataFrame(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["col1", "col2", "col3"]
    )
    snow_df = pd.DataFrame(native_df)

    def perform_where(df):
        if isinstance(df, pd.DataFrame):
            return df.where(pd.Series(data, name="SERIES_NAME"))
        else:
            return df.where(native_pd.Series(data, name="SERIES_NAME"))

    eval_snowpark_pandas_result(snow_df, native_df, perform_where)


@pytest.mark.parametrize(
    "cond",
    [1, [1], [[1]]],
    ids=["scalar_cond", "scalar_cond_in_list", "scalar_cond_in_nested_list"],
)
def test_where_with_scalar_cond(cond):
    native_ser = native_pd.DataFrame([[1, 2, 3]])
    snow_ser = pd.DataFrame(native_ser)

    with SqlCounter(query_count=0):
        eval_snowpark_pandas_result(
            snow_ser,
            native_ser,
            lambda ser: ser.where(cond, 1),
            expect_exception=True,
            expect_exception_type=ValueError,
            expect_exception_match="Array conditional must be same shape as self",
            assert_exception_equal=True,
        )


@sql_count_checker(query_count=0)
def test_where_series_other_axis_not_specified():
    native_df = native_pd.DataFrame([[1, 2, 3]])
    snow_df = pd.DataFrame(native_df)

    def perform_where(df):
        if isinstance(df, pd.DataFrame):
            return df.where([[True] * 3], pd.Series([1, 2, 3]))
        else:
            return df.where([[True] * 3], native_pd.Series([1, 2, 3]))

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        perform_where,
        assert_exception_equal=False,
        expect_exception=True,
        expect_exception_match=r"df.where requires an axis parameter \(0 or 1\) when given a Series",
        expect_exception_type=ValueError,
    )


@sql_count_checker(query_count=1, join_count=3)
@pytest.mark.parametrize(
    "data",
    [[10], [10, 11, 12], [10, 11, 12, 13]],
    ids=[
        "series_shorter_than_dataframe",
        "series_same_length_as_dataframe",
        "series_longer_than_dataframe",
    ],
)
@pytest.mark.parametrize(
    "index", [True, False], ids=["matching_index", "unmatched_index"]
)
def test_where_series_other_axis_0(index, data):
    native_df = native_pd.DataFrame(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["col1", "col2", "col3"]
    )
    snow_df = pd.DataFrame(native_df)

    # Can't use string index here for unmatched index, since Snowpark Pandas does not support
    # join with different index types.
    index = [0, 1, 2, 3] if index else [4, 5, 6, 7]
    index = index[: len(data)]

    def perform_where(df):
        if isinstance(df, pd.DataFrame):
            return df.where(
                [[False] * 3, [False] * 3, [False] * 3],
                pd.Series(data, index=index),
                axis=0,
            )
        else:
            return df.where(
                [[False] * 3, [False] * 3, [False] * 3],
                native_pd.Series(data, index=index),
                axis=0,
            )

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        perform_where,
    )


@sql_count_checker(query_count=1, join_count=3, union_count=1)
@pytest.mark.parametrize(
    "data",
    [[10], [10, 11, 12], [10, 11, 12, 13]],
    ids=[
        "series_shorter_than_dataframe",
        "series_same_length_as_dataframe",
        "series_longer_than_dataframe",
    ],
)
@pytest.mark.parametrize(
    "index", [True, False], ids=["matching_index", "unmatched_index"]
)
def test_where_series_other_axis_1(index, data):
    native_df = native_pd.DataFrame(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["col1", "col2", "col3"]
    )
    snow_df = pd.DataFrame(native_df)

    # Can't use int index here for unmatched index, since Snowpark Pandas does not support
    # join with different index types.
    index = [f"col{i}" for i in (range(4) if index else range(4, 9))]
    index = index[: len(data)]

    def perform_where(df):
        if isinstance(df, pd.DataFrame):
            return df.where(
                [[False] * 3, [False] * 3, [False] * 3],
                pd.Series(data, index=index),
                axis=1,
            )
        else:
            return df.where(
                [[False] * 3, [False] * 3, [False] * 3],
                native_pd.Series(data, index=index),
                axis=1,
            )

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        perform_where,
    )


@sql_count_checker(query_count=1, join_count=1)
def test_where_series_cond_after_join():
    snow_df1 = pd.DataFrame({"A": [1, 2]})
    snow_df = snow_df1.join(snow_df1, lsuffix="_l", rsuffix="_r")
    snow_df = snow_df.where(snow_df["A_l"] != snow_df["A_r"])
    native_df1 = native_pd.DataFrame({"A": [1, 2]})
    native_df = native_df1.join(native_df1, lsuffix="_l", rsuffix="_r")
    native_df = native_df.where(native_df["A_l"] != native_df["A_r"])
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, native_df)


@sql_count_checker(query_count=1, join_count=0)
def test_where_with_zero_other_mixed_types_SNOW_1372268():
    data = {"n": ["A", "B", "B", "C", "C", "C"]}
    df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)
    df_result = df.where(df["n"] == "C", 0)
    native_df_result = native_df.where(native_df["n"] == "C", 0)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        df_result, native_df_result.astype("str")
    )


@sql_count_checker(query_count=1, join_count=0)
def test_where_with_zero_other_SNOW_1372268():
    data = {"n": [99, 99, 99, -99, -99, -99]}
    df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)
    df_result = df.where(df["n"] == -99, 0)
    native_df_result = native_df.where(native_df["n"] == -99, 0)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        df_result, native_df_result
    )


@sql_count_checker(query_count=1)
def test_where_timedelta(test_data):
    native_df = native_pd.DataFrame(test_data, dtype="timedelta64[ns]")
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.where(df > pd.Timedelta(1))
    )
