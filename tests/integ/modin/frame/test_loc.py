#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import random
import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from modin.pandas import DataFrame
from pandas._libs.lib import is_bool, is_scalar
from pandas.errors import IndexingError

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.modin.plugin.extensions.utils import try_convert_index_to_native
from tests.integ.modin.utils import (
    assert_frame_equal,
    assert_snowpark_pandas_equal_to_pandas,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
    generate_a_random_permuted_list_exclude_self,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import running_on_public_ci

EMPTY_LIST_LIKE_VALUES = [
    [],
    native_pd.Index([]),
    np.array([]),
    native_pd.Series([]),
]


@pytest.fixture(params=[True, False])
def use_default_index(request):
    return request.param


@pytest.fixture(params=["series", "list", "array", "index"], scope="module")
def key_type(request):
    return request.param


boolean_indexer = [
    [True, True, False, False, False, True, True],
    np.array([True, True, False, False, False, True, True]),
    native_pd.Index([True, True, False, False, False, True, True]),
]
row_inputs = [
    ["a", "a", "c", "c", "b"],
    native_pd.Index(["c", "a", "a", "b", "a"]),
    slice("b", "f"),
    [True, False, True, False, True, True, True],
    "a",
] + boolean_indexer

col_inputs = [
    slice("B", "E", 2),
    slice("F", "B", -3),
    "A",
    ["A", "A", "C", "C", "B"],
    native_pd.Index(["A", "A", "C", "C", "B"]),
    np.array(["C", "A", "B"]),
    ("A", "B"),
]
len_mismatch_boolean_indexer = [
    [True],
    [True] * 8,
    np.array([], dtype=bool),
    native_pd.Index([], dtype=bool),
]
row_negative_inputs = [
    (("A",), ("A",), ("A",)),  # nested tuple
]
snowpark_pandas_row_and_col_inputs = [
    "empty_series",
]
snowpark_pandas_col_inputs = [
    "series[label]_col",
    "series[bool]_col",
    "multi_index_series_col",
] + snowpark_pandas_row_and_col_inputs
list_like_time_col_inputs = [
    ["2023-01-01"],
    ["2023-01-01 03:00:00+03:00"],
]
diff2native_negative_row_inputs = [
    # set: same error type and message as pandas-2.0
    (
        {1, 3},
        TypeError,
        "Passing a set as an indexer is not supported. Use a list instead.",
    ),
    # dict: same error type and message as pandas-2.0
    (
        {"a": 1},
        TypeError,
        "Passing a dict as an indexer is not supported. Use a list instead.",
    ),
    (
        native_pd.Series([2, 4]),
        TypeError,
        "Please convert this to Snowpark pandas objects by calling modin",
    ),
    (
        native_pd.DataFrame(),
        TypeError,
        "Please convert this to Snowpark pandas objects by calling modin",
    ),
]

out_of_bound_col_inputs = [["C", "A", "A", "y", "y"], "x"]

negative_snowpark_pandas_input_keys = [
    "dataframe",
]

snowpark_pandas_int_index_row_inputs = [
    "int_float",
]

ITEM_TYPE_LIST_CONVERSION = [
    ["list", lambda x: x],
    ["array", lambda x: np.array(x)],
    ["tuple", lambda x: tuple(x)],
    ["index", lambda x: pd.Index(x)],
]


@pytest.mark.parametrize(
    "row",
    row_inputs,
)
@pytest.mark.parametrize(
    "col",
    col_inputs,
)
def test_df_loc_get_tuple_key(
    row, col, str_index_snowpark_pandas_df, str_index_native_df
):
    if isinstance(row, native_pd.Index):
        snow_row = pd.Index(row)
    else:
        snow_row = row

    query_count = 1
    if is_scalar(row) or isinstance(row, tuple):
        query_count = 2

    with SqlCounter(
        query_count=query_count,
    ):
        eval_snowpark_pandas_result(
            str_index_snowpark_pandas_df,
            str_index_native_df,
            lambda df: df.loc[snow_row, col]
            if isinstance(df, pd.DataFrame)
            else df.loc[row, col],
        )


@pytest.mark.parametrize(
    "row",
    [
        lambda x: ["a", "c"],
    ],
)
@pytest.mark.parametrize(
    "col",
    [
        lambda x: ["A", "C"],
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_df_loc_get_callable_key(
    row, col, str_index_snowpark_pandas_df, str_index_native_df
):
    eval_snowpark_pandas_result(
        str_index_snowpark_pandas_df,
        str_index_native_df,
        lambda df: df.loc[row, col],
    )


@pytest.mark.parametrize(
    "key",
    col_inputs,
)
def test_df_loc_get_col_non_boolean_key(
    key, str_index_snowpark_pandas_df, str_index_native_df
):
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            str_index_snowpark_pandas_df,
            str_index_native_df,
            lambda df: df.loc[:, key],
        )
    if not is_scalar(key) and not isinstance(key, slice):
        with SqlCounter(query_count=2):
            eval_snowpark_pandas_result(
                str_index_snowpark_pandas_df,
                str_index_native_df,
                lambda df: df.loc[
                    :,
                    pd.Series(key)
                    if isinstance(df, pd.DataFrame)
                    else native_pd.Series(key),
                ],
            )


@pytest.mark.parametrize(
    "key",
    boolean_indexer,
)
@sql_count_checker(query_count=3)
def test_df_loc_get_col_boolean_indexer(
    key, str_index_snowpark_pandas_df, str_index_native_df
):
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            str_index_snowpark_pandas_df,
            str_index_native_df,
            lambda df: df.loc[:, key],
        )

    with SqlCounter(query_count=2):
        eval_snowpark_pandas_result(
            str_index_snowpark_pandas_df,
            str_index_native_df,
            lambda df: df.loc[
                :,
                pd.Series(key, index=str_index_native_df.columns)
                if isinstance(df, pd.DataFrame)
                else native_pd.Series(key, index=str_index_native_df.columns),
            ],
        )


@pytest.mark.parametrize(
    "key",
    list_like_time_col_inputs,
)
@sql_count_checker(query_count=1)
def test_df_loc_get_col_time_df(
    key, time_column_snowpark_pandas_df, time_column_native_df
):
    eval_snowpark_pandas_result(
        time_column_snowpark_pandas_df,
        time_column_native_df,
        lambda df: df.loc[:, key],
    )


@pytest.mark.parametrize(
    "key",
    snowpark_pandas_int_index_row_inputs,
)
@sql_count_checker(query_count=1, join_count=1)
def test_df_loc_get_int_index_row_snowpark_pandas_input(
    key,
    default_index_snowpark_pandas_df,
    default_index_native_df,
    loc_snowpark_pandas_input_map,
):
    snow_result = default_index_snowpark_pandas_df.loc[
        loc_snowpark_pandas_input_map[key][0]
    ]
    native_result = default_index_native_df.loc[loc_snowpark_pandas_input_map[key][1]]
    # Snowpark pandas index type is different from native pandas index type.
    # In snowpark pandas we generate index columns using
    # iff(..., left_index, right_index) expression which changes type from int to float.
    assert_frame_equal(
        snow_result, native_result, check_dtype=False, check_index_type=False
    )


@pytest.mark.parametrize(
    "key",
    snowpark_pandas_col_inputs,
)
def test_df_loc_get_col_snowpark_pandas_input(
    key,
    str_index_snowpark_pandas_df,
    str_index_native_df,
    loc_snowpark_pandas_input_map,
):
    with SqlCounter(query_count=2):
        eval_snowpark_pandas_result(
            str_index_snowpark_pandas_df,
            str_index_native_df,
            lambda df: df.loc[:, loc_snowpark_pandas_input_map[key][0]]
            if isinstance(df, DataFrame)
            else df.loc[:, loc_snowpark_pandas_input_map[key][1]],
        )


@pytest.mark.parametrize(
    "key",
    [
        ([]),
        (([], [])),
        ((slice(None), [])),
        ((slice(None), slice(None))),
    ],
)
def test_df_loc_get_empty_key(
    key,
    empty_snowpark_pandas_df,
    default_index_snowpark_pandas_df,
    default_index_native_df,
):

    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            empty_snowpark_pandas_df,
            native_pd.DataFrame(),
            lambda df: df.loc[key],
            comparator=assert_snowpark_pandas_equal_to_pandas,
            check_column_type=False,
        )
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            lambda df: df.loc[key],
        )


@pytest.mark.parametrize(
    "key",
    row_negative_inputs,
)
@sql_count_checker(query_count=0)
def test_df_loc_get_row_negative_same2native(
    key, str_index_snowpark_pandas_df, str_index_native_df
):
    eval_snowpark_pandas_result(
        str_index_snowpark_pandas_df,
        str_index_native_df,
        lambda df: df.loc[key],
        expect_exception=True,
    )


@pytest.mark.parametrize(
    "key",
    len_mismatch_boolean_indexer,
)
@sql_count_checker(query_count=1)
def test_df_loc_get_col_len_mismatch_boolean_indexer(
    key, str_index_snowpark_pandas_df, str_index_native_df
):
    eval_snowpark_pandas_result(
        str_index_snowpark_pandas_df,
        str_index_native_df,
        lambda df: df.loc[:, key]
        if isinstance(df, pd.DataFrame)
        else df.iloc[:, 0 : len(key)].loc[:, key[: len(df)]],
    )


@pytest.mark.parametrize(
    "key,error_type,error_msg",
    diff2native_negative_row_inputs,
)
@sql_count_checker(query_count=0)
def test_df_loc_get_negative_row_diff2native(
    key, error_type, error_msg, str_index_snowpark_pandas_df
):
    with pytest.raises(
        error_type,
        match=error_msg,
    ):
        _ = str_index_snowpark_pandas_df.loc[key]


@pytest.mark.parametrize(
    "key",
    out_of_bound_col_inputs,
)
def test_df_loc_get_out_of_bound_col(
    key, str_index_native_df, str_index_snowpark_pandas_df
):
    match_str = r".* not in index" if not is_scalar(key) else f"{key}"
    with SqlCounter(query_count=0):
        with pytest.raises(KeyError, match=match_str):
            eval_snowpark_pandas_result(
                str_index_snowpark_pandas_df,
                str_index_native_df,
                lambda df: df.loc[:, key],
            )


@pytest.mark.parametrize(
    "key",
    negative_snowpark_pandas_input_keys,
)
@sql_count_checker(query_count=0)
def test_df_loc_get_negative_snowpark_pandas_input(
    key,
    str_index_snowpark_pandas_df,
    negative_loc_snowpark_pandas_input_map,
    str_index_native_df,
):
    eval_snowpark_pandas_result(
        str_index_snowpark_pandas_df,
        str_index_native_df,
        lambda df: df.loc[negative_loc_snowpark_pandas_input_map[key][0]]
        if isinstance(df, DataFrame)
        else df.loc[negative_loc_snowpark_pandas_input_map[key][1]],
        expect_exception=True,
    )


@pytest.fixture(scope="function")
def mi_table_df():
    tuples = [
        ("mark i", "mark v"),
        ("mark i", "mark vi"),
        ("sidewinder", "mark i"),
        ("sidewinder", "mark ii"),
        ("viper", "mark ii"),
        ("viper", "mark iii"),
    ]
    index = pd.MultiIndex.from_tuples(tuples)
    values = [[12, 2], [0, 4], [10, 20], [1, 4], [7, 1], [16, 36]]
    tuples_columns = [("fizz1", "buzz1"), ("fizz2", "buzz2")]
    columns = pd.MultiIndex.from_tuples(tuples_columns, names=["fizz", "buzz"])

    return native_pd.DataFrame(values, columns=columns, index=index)


@pytest.mark.parametrize(
    "key, native_error",
    [
        # scalar key behavior: prefix match plus drop level
        ["fizz1", None],
        [(("fizz1",)), None],
        [(("fizz1", "buzz1")), None],
        ["buzz1", KeyError],
        # list-like key with non-list-like value behavior: prefix match
        [["fizz1"], None],  # single value
        [["fizz1", "fizz2", "fizz1"], None],
        [
            ["invalid"],
            KeyError,
        ],  # return empty data frame if no match found while native pandas raise error
        # list-like key with list-like value behavior: exact match
        [[("fizz1", "buzz1")], None],
        [[["fizz1", "buzz1"]], None],
        [[("fizz1", "buzz1"), ["fizz1", "buzz1"], ("fizz2", "buzz2")], None],
        [
            [("buzz1",)],
            AssertionError,
        ],  # return empty data frame since no exact match found
    ],
)
def test_mi_df_loc_get_non_boolean_list_col_key(mi_table_df, key, native_error):
    df = pd.DataFrame(mi_table_df)
    if native_error:
        query_count = 0
    else:
        # other list like key
        query_count = 1
    with SqlCounter(query_count=query_count):
        if native_error:
            with pytest.raises(native_error):
                _ = mi_table_df.loc[:, key]
        else:
            eval_snowpark_pandas_result(
                df,
                mi_table_df,
                lambda df: df.loc[:, key],
            )


@pytest.mark.parametrize(
    "key, native_error",
    [
        # scalar key behavior: prefix match plus drop level
        ["mark i", None],
        [(("mark i",)), None],
        [(("mark i", "mark vi"), slice(None)), None],
        ["mark ii", KeyError],
        # list-like key with non-list-like value behavior: prefix match
        [["mark i"], None],  # single value
        [["viper", "mark i", "viper"], None],
        [
            ["invalid"],
            KeyError,
        ],  # return empty data frame if no match found while native pandas raise error
        # list-like key with list-like value behavior: exact match
        [[("mark i", "mark vi")], None],
        [[["mark i", "mark vi"]], None],
        [[("mark i", "mark vi"), ["mark i", "mark vi"], ("viper", "mark ii")], None],
        [
            [("mark i",)],
            AssertionError,
        ],  # return empty data frame since no exact match found
    ],
)
def test_mi_df_loc_get_non_boolean_list_row_key(mi_table_df, key, native_error):
    df = pd.DataFrame(mi_table_df)
    if isinstance(key, tuple) or is_scalar(key):
        # it uses filter so no join count
        query_count, join_count = 1, 0
        if isinstance(key, tuple) and len(key) == 2:
            # multiindex full lookup requires squeeze to run
            query_count += 1
    else:
        # other list like key
        query_count, join_count = 1, 1
    with SqlCounter(query_count=query_count, join_count=join_count):
        if native_error:
            with pytest.raises(native_error):
                _ = mi_table_df.loc[key]
            assert df.loc[key].empty
        else:
            eval_snowpark_pandas_result(
                df,
                mi_table_df,
                lambda df: df.loc[key],
            )


@pytest.mark.parametrize(
    "row",
    [
        # scalar key behavior: prefix match plus drop level
        "mark i",
        (("mark i",)),
        (("mark i", "mark vi")),
        # list-like key with non-list-like value behavior: prefix match
        ["mark i"],
        ["viper", "mark i", "viper"],
        # list-like key with list-like value behavior: exact match
        [("mark i", "mark vi")],
        [["mark i", "mark vi"]],
        [["mark i", "mark vi", "oversize"]],
        [("mark i", "mark vi"), ["mark i", "mark vi"], ("viper", "mark ii")],
        # empty tuple
        (()),
        (("mark i", slice(None))),
        ((slice(None), slice(None))),
        ((slice(None), "mark vi")),
        ((slice("mark i", "sidewinder"), "mark vi")),
        ((slice("a", "z"),)),
    ],
)
@pytest.mark.parametrize(
    "col",
    [
        # scalar key behavior: prefix match plus drop level
        "fizz1",
        (("fizz1",)),
        (("fizz1", "buzz1")),
        # list-like key with non-list-like value behavior: prefix match
        ["fizz1"],  # single value
        ["fizz1", "fizz2", "fizz1"],
        # list-like key with list-like value behavior: exact match
        [("fizz1", "buzz1")],
        [["fizz1", "buzz1"]],
        [["fizz1", "buzz1", "oversize"]],
        [("fizz1", "buzz1"), ["fizz1", "buzz1"], ("fizz2", "buzz2")],
        # empty tuple
        (()),
        ((slice(None), "buzz1")),
        ((slice(None), slice(None))),
        ((slice("fizz0", "fizz1"), slice("buzz1", "buzz2"))),
        (("fizz1", slice(None))),
    ],
)
def test_mi_df_loc_get_non_boolean_list_tuple_key(mi_table_df, row, col):
    df = pd.DataFrame(mi_table_df)
    if isinstance(row, tuple) or is_scalar(row):
        # it uses filter so no join count
        query_count, join_count = 1, 0
        if (
            isinstance(row, tuple)
            and len(row) == 2
            and not any(isinstance(r, slice) for r in row)
        ):
            # multiindex full lookup requires squeeze to run
            query_count += 1
    else:
        # other list like key
        query_count, join_count = 1, 1
    with SqlCounter(query_count=query_count, join_count=join_count):
        if (
            isinstance(row, tuple)
            and len(row) == 2
            and not any(isinstance(r, slice) for r in row)
            and isinstance(col, tuple)
            and len(col) == 2
            and not any(isinstance(c, slice) for c in col)
        ):
            assert df.loc[row, col] == mi_table_df.loc[row, col]
        else:
            eval_snowpark_pandas_result(
                df,
                mi_table_df,
                lambda df: df.loc[row, col],
            )


@sql_count_checker(query_count=2, join_count=2)
def test_mi_df_loc_get_boolean_series_row_key(mi_table_df):
    df = pd.DataFrame(mi_table_df)
    bool_indexer = [False, True, True, False, False, True]

    eval_snowpark_pandas_result(
        df,
        mi_table_df,
        lambda df: df.loc[bool_indexer],
    )

    tuples2 = [
        ("mark i", "mark vi"),
        ("mark i", "mark v"),
        ("sidewinder", "mark ii"),
        ("sidewinder", "mark i"),
        ("viper", "mark iii"),
        ("viper", "mark ii"),
    ]

    eval_snowpark_pandas_result(
        df,
        mi_table_df,
        lambda df: df.loc[
            pd.Series(bool_indexer, index=pd.MultiIndex.from_tuples(tuples2))
        ]
        if isinstance(df, DataFrame)
        else df.loc[
            native_pd.Series(bool_indexer, index=pd.MultiIndex.from_tuples(tuples2))
        ],
    )


@sql_count_checker(query_count=3, join_count=0)
def test_mi_df_loc_get_boolean_series_col_key(mi_table_df):
    df = pd.DataFrame(mi_table_df)
    bool_indexer = [False, True]

    eval_snowpark_pandas_result(
        df,
        mi_table_df,
        lambda df: df.loc[:, bool_indexer],
    )

    tuples2 = [("fizz1", "buzz1"), ("fizz2", "buzz2")]

    eval_snowpark_pandas_result(
        df,
        mi_table_df,
        lambda df: df.loc[
            :, pd.Series(bool_indexer, index=pd.MultiIndex.from_tuples(tuples2))
        ]
        if isinstance(df, DataFrame)
        else df.loc[
            :, native_pd.Series(bool_indexer, index=pd.MultiIndex.from_tuples(tuples2))
        ],
    )


@pytest.mark.parametrize(
    "loc_with_slice",
    [
        lambda df: df.loc["mark i":"sidewinder"],
        lambda df: df.loc[("mark i", "mark v"):],
        lambda df: df.loc[("mark i",):],
        lambda df: df.loc[("mark i", "mark v"):("sidewinder", "mark i")],
        lambda df: df.loc["mark i":("sidewinder", "mark i")],
        lambda df: df.loc["mark i":"sidewinder":2],
        lambda df: df.loc["sidewinder":"mark i":-2],
        lambda df: df.loc["mark v":"mark vi"],
    ],
)
@sql_count_checker(query_count=1)
def test_mi_df_loc_slice_row_key(mi_table_df, loc_with_slice):
    df = pd.DataFrame(mi_table_df)
    eval_snowpark_pandas_result(
        df,
        mi_table_df,
        loc_with_slice,
        check_index_type=False,
    )


@pytest.mark.parametrize(
    "loc_with_slice",
    [
        lambda df: df.loc[:, "fizz1":"fizz3"],
        lambda df: df.loc[:, ("fizz1", "buzz1"):],
        lambda df: df.loc[:, ("fizz1",):],
        lambda df: df.loc[:, ("fizz1", "buzz1"):("fizz2", "buzz2")],
        lambda df: df.loc[:, "fizz1":("fizz2", "buzz2")],
        lambda df: df.loc[:, "fizz1"::2],
        lambda df: df.loc[:, "fizz1"::-2],
    ],
)
@sql_count_checker(query_count=1)
def test_mi_df_loc_slice_col_key(mi_table_df, loc_with_slice):
    df = pd.DataFrame(mi_table_df)
    eval_snowpark_pandas_result(
        df,
        mi_table_df,
        loc_with_slice,
        check_index_type=False,
    )


@pytest.mark.parametrize(
    "row_key",
    [
        native_pd.Series(
            [
                True,
                False,
                True,
            ]
        ),
        native_pd.Series(
            [
                False,
                False,
                False,
            ]
        ),
        native_pd.Series(
            [
                False,
                True,
                False,
                True,
                False,
                True,
            ]
        ),
        native_pd.Series(
            [
                False,
                True,
            ]
        ),
        native_pd.Series(
            [
                0,  # 0 does not exist in item, so the row values will be set to NULL
                1,
                2,
            ]
        ),
        native_pd.Series([]),
        native_pd.Series(
            [
                2,
                1,
                0,
                1,
                2,
            ]
        ),  # duplicates with no order
    ],
)
def test_df_loc_set_series_row_key(row_key):
    df = native_pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["A", "B", "C"])
    item = native_pd.DataFrame(
        [[10, 20, 30], [40, 50, 60], [70, 80, 90]],
        columns=["C", "A", "B"],
        index=[
            3,  # 3 does not exist in the row key, so it will be skipped
            2,
            1,
        ],
    )

    # test case for df.loc[row_key] = item
    def loc_set_helper(df):
        if isinstance(df, native_pd.DataFrame):
            if row_key.dtype == bool and len(row_key) < len(df):
                # pandas raises IndexingError if the length of the boolean series row key is less than the number of
                # rows
                with pytest.raises(IndexingError, match="Unalignable boolean Series"):
                    df.loc[row_key] = item
                _row_key = native_pd.Series(
                    row_key.tolist() + [False] * (len(df) - len(row_key))
                )
            else:
                _row_key = row_key
            df.loc[_row_key] = item
        else:
            df.loc[pd.Series(row_key)] = pd.DataFrame(item)

    eval_snowpark_pandas_result(pd.DataFrame(df), df, loc_set_helper, inplace=True)

    # test case for df.loc[row_key, :] = item
    def loc_set_helper(df):
        if isinstance(df, native_pd.DataFrame):
            if row_key.dtype == bool and len(row_key) < len(df):
                # pandas raise TypeError if row key is boolean series with mismatched length
                with pytest.raises(TypeError, match="unhashable type"):
                    df.loc[row_key, :] = item
                _row_key = native_pd.Series(
                    row_key.tolist() + [False] * (len(df) - len(row_key))
                )
            else:
                _row_key = row_key
            df.loc[_row_key, :] = item
        else:
            df.loc[pd.Series(row_key), :] = pd.DataFrame(item)

    expected_join_count = 3 if not row_key.dtype == bool else 2

    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(pd.DataFrame(df), df, loc_set_helper, inplace=True)


@pytest.mark.parametrize(
    "row_key",
    [
        native_pd.Series(
            [
                True,
                False,
                True,
            ]
        ),
    ],
)
@pytest.mark.parametrize(
    "col_key",
    [
        "A",
        ["A", "B"],
        ["C", "X"],
        "X",
        ["X", "Y"],
        ["X", "Z"],
    ],
)
@pytest.mark.parametrize(
    "item",
    [100, [90, 91]],
)
def test_df_loc_set_boolean_row_indexer(row_key, col_key, item):
    df = native_pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["A", "B", "C"])

    def loc_set_helper(df):
        df.loc[row_key, col_key] = item

    expected_join_count = (
        4 if isinstance(col_key, str) and isinstance(item, list) else 1
    )

    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(pd.DataFrame(df), df, loc_set_helper, inplace=True)


@pytest.mark.parametrize(
    "row_key",
    [
        [
            True,
            False,
            True,
        ],
        [
            False,
            False,
            False,
        ],
        [
            False,
            True,
            False,
            True,
            False,
            True,
        ],
        [
            False,
            True,
        ],
        [
            0,  # 0 does not exist in item, so the row values will be set to NULL
            1,
            2,
        ],
        [],
        [
            2,
            1,
            0,
            1,
            2,
        ],  # duplicates with no order
    ],
)
@pytest.mark.parametrize("key_type", ["list", "array", "index"])
def test_df_loc_set_list_like_row_key(row_key, key_type):
    native_df = native_pd.DataFrame(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["A", "B", "C"]
    )
    item = native_pd.DataFrame(
        [[10, 20, 30], [40, 50, 60], [70, 80, 90]],
        columns=["C", "A", "B"],
        index=[
            3,  # 3 does not exist in the row key, so it will be skipped
            2,
            1,
        ],
    )

    expected_join_count = (
        2 if all(isinstance(i, bool) for i in row_key) and len(row_key) > 0 else 3
    )

    # test case for df.loc[row_key] = item
    def key_converter(key, df):
        # Convert key to the required type.
        _key = key
        if key_type == "index":
            _key = (
                pd.Index(key) if isinstance(df, pd.DataFrame) else native_pd.Index(key)
            )
        elif key_type == "array":
            _key = np.array(key)
        return _key

    def loc_set_helper(df):
        if isinstance(df, native_pd.DataFrame):
            if (0 < len(row_key) != len(df)) and is_bool(row_key[0]):
                # pandas raises IndexError if length of like-like boolean row key is not equal to the number of rows.
                with pytest.raises(IndexError, match="Boolean index has wrong length"):
                    df.loc[key_converter(row_key, df)] = item
                _row_key = key_converter(
                    row_key + [False] * (len(df) - len(row_key)), df
                )[: len(df)]
            else:
                _row_key = key_converter(row_key, df)
            df.loc[_row_key] = item
        else:
            _row_key = key_converter(row_key, df)
            df.loc[_row_key] = pd.DataFrame(item)

    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            pd.DataFrame(native_df), native_df, loc_set_helper, inplace=True
        )

    # test case for df.loc[row_key, :] = item
    def loc_set_helper(df):
        if isinstance(df, native_pd.DataFrame):
            if (0 < len(row_key) != len(df)) and is_bool(row_key[0]):
                # pandas raises IndexError if length of like-like boolean row key is not equal to the number of rows.
                with pytest.raises(IndexError, match="Boolean index has wrong length"):
                    df.loc[key_converter(row_key, df)] = item
                _row_key = key_converter(
                    row_key + [False] * (len(df) - len(row_key)), df
                )[: len(df)]
            else:
                _row_key = key_converter(row_key, df)
            df.loc[_row_key, :] = item
        else:
            _row_key = key_converter(row_key, df)
            df.loc[_row_key, :] = pd.DataFrame(item)

    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            pd.DataFrame(native_df), native_df, loc_set_helper, inplace=True
        )


@sql_count_checker(query_count=2, join_count=6)
def test_df_loc_set_series_and_list_like_row_key_negative(key_type):
    # This test verifies pandas raise ValueError when row key is out-of-bounds but Snowpandas pandas will ignore the
    # out-of-bound index
    df = native_pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["A", "B", "C"])
    item = native_pd.DataFrame(
        [[10, 20, 30], [40, 50, 60], [70, 80, 90]],
        columns=["C", "A", "B"],
        index=[
            3,  # 3 does not exist in the row key, so it will be skipped
            2,
            1,
        ],
    )
    # row key with out-of-bound values
    row_key_with_oob = [
        3,  # 3 does not exist in df
        1,
        2,
    ]
    valid_row_key = [
        1,
        2,
    ]

    def key_converter(key):
        _key = key
        # Convert key to the required type.
        if key_type == "index":
            _key = native_pd.Index(key)
        elif key_type == "array":
            _key = np.array(key)
        elif key_type == "series":
            _key = native_pd.Series(key)
        return _key

    # convert keys to appropriate type
    row_key_with_oob, valid_row_key = key_converter(row_key_with_oob), key_converter(
        valid_row_key
    )

    # test case for df.loc[row_key] = item
    def loc_set_helper(df):
        if isinstance(df, native_pd.DataFrame):
            with pytest.raises(KeyError, match="not in index"):
                df.loc[try_convert_index_to_native(row_key_with_oob)] = item
            df.loc[try_convert_index_to_native(valid_row_key)] = item
        else:
            _row_key_with_oob = (
                pd.Series(row_key_with_oob)
                if key_type == "series"
                else row_key_with_oob
            )
            df.loc[_row_key_with_oob] = pd.DataFrame(item)

    eval_snowpark_pandas_result(pd.DataFrame(df), df, loc_set_helper, inplace=True)

    # test case for df.loc[row_key, :] = item
    def loc_set_helper(df):
        if isinstance(df, native_pd.DataFrame):
            with pytest.raises(KeyError, match="not in index"):
                df.loc[try_convert_index_to_native(row_key_with_oob), :] = item
            df.loc[
                try_convert_index_to_native(valid_row_key),
                :,
            ] = item
        else:
            _row_key_with_oob = (
                pd.Series(row_key_with_oob)
                if key_type == "series"
                else row_key_with_oob
            )
            df.loc[_row_key_with_oob, :] = pd.DataFrame(item)

    eval_snowpark_pandas_result(pd.DataFrame(df), df, loc_set_helper, inplace=True)


LOC_SET_COL_KEYS = [
    slice(None),
    ["A"],
    ["Y"],  # append new column
    slice("A", "B", None),
    native_pd.Index(["A", "C"]),  # more array type
    np.array(["C", "A"]),
    [False, True, True, False],
    np.array([1, 0, 0, 1], dtype=bool),
    native_pd.Series(
        [True, False, False, True], index=["B", "A", "D", "C"], dtype=bool
    ),  # boolean series list
    # duplicates + new columns, deviating behavior: SNOW-1320623 pandas 2.2.1 upgrade for test:
    # test_df_loc_set_general_col_key_type
    ["B", "E", 1, "B", "C", "X", "C", 2, "C"],  #
    native_pd.Series(["B", "E", 1, "B", "C", "X", "C", 2, "C"]),
]


@pytest.mark.parametrize("col_key", LOC_SET_COL_KEYS)
@pytest.mark.parametrize(
    "row_key",
    [
        [True, False, True],
        [
            0,  # 0 does not exist in item, so the row values will be set to NULL
            1,
            2,
        ],
    ],
)
def test_df_loc_set_general_col_key_type(row_key, col_key, key_type):
    df = native_pd.DataFrame(
        [[1, 2, 3, 4], [4, 5, 6, 7], [7, 8, 9, 10]], columns=["D", "B", "C", "A"]
    )
    item = native_pd.DataFrame(
        [[1, 2, 3, 4], [4, 5, 6, 7], [7, 8, 9, 10]],
        columns=["A", "B", "C", "X"],
        index=[
            3,  # 3 does not exist in the row key, so it will be skipped
            2,
            1,
        ],
    )

    def key_converter(df):
        _row_key = row_key
        # Convert key to the required type.
        if key_type == "index":
            _row_key = (
                pd.Index(_row_key)
                if isinstance(df, pd.DataFrame)
                else native_pd.Index(_row_key)
            )
        elif key_type == "array":
            _row_key = np.array(_row_key)
        elif key_type == "series":
            _row_key = (
                pd.Series(_row_key)
                if isinstance(df, pd.DataFrame)
                else native_pd.Series(_row_key)
            )
        return _row_key

    # There is a bug in pandas 2.2.0+ behavior; issue here: https://github.com/pandas-dev/pandas/issues/58317
    # The problem arises with duplicated columns when a column key of the format: [existing column(s), new column,
    # duplicated existing column(s)].
    # Some of the existing columns are modified (when they are not supposed to be) and have empty values.
    # In this test, this happens when the col_key is ["B", "E", 1, "B", "C", "X", "C", 2, "C"] or the Series version.
    # We get around this by first assigning NaN values before the actual loc testing performed.

    def loc_set_helper(df):
        # convert row key to appropriate type
        row_key = key_converter(df)
        if isinstance(df, native_pd.DataFrame):
            # 2 is a column only in the col_keys with deviating behavior
            if isinstance(col_key, (list, native_pd.Series)) and 2 in col_key:
                # Set the new columns to NaN values to prevent assignment of byte values.
                df.loc[:, ["E", 1, "X", 2]] = np.nan
            df.loc[
                row_key,
                try_convert_index_to_native(col_key),
            ] = item
        else:
            key = (
                row_key,
                pd.Series(col_key)
                if isinstance(col_key, native_pd.Series)
                else col_key,
            )
            df.loc[key] = pd.DataFrame(item)

    query_count, join_count = 1, 2
    if not all(isinstance(rk_val, bool) for rk_val in row_key):
        join_count += 1
    if isinstance(col_key, native_pd.Series):
        query_count += 1
    with SqlCounter(query_count=query_count, join_count=join_count):
        eval_snowpark_pandas_result(pd.DataFrame(df), df, loc_set_helper, inplace=True)


@pytest.mark.parametrize("col_key", LOC_SET_COL_KEYS)
def test_df_loc_set_general_col_key_type_with_duplicate_columns(col_key, key_type):
    df = native_pd.DataFrame(
        [[1, 2, 3, 4], [4, 5, 6, 7], [7, 8, 9, 10]],
        columns=["A", "B", "C", "C"],  # contains duplicate labels
    )
    row_key = [
        0,  # 0 does not exist in item, so the row values will be set to NULL
        1,
        2,
    ]
    item = native_pd.DataFrame(
        [[1, 2, 3, 4], [4, 5, 6, 7], [7, 8, 9, 10]],
        columns=["A", "B", "C", "X"],
        index=[
            3,  # 3 does not exist in the row key, so it will be skipped
            2,
            1,
        ],
    )

    def key_converter(df):
        _row_key = row_key
        # Convert key to the required type.
        if key_type == "index":
            _row_key = (
                pd.Index(_row_key)
                if isinstance(df, pd.DataFrame)
                else native_pd.Index(_row_key)
            )
        elif key_type == "array":
            _row_key = np.array(_row_key)
        elif key_type == "series":
            _row_key = (
                pd.Series(_row_key)
                if isinstance(df, pd.DataFrame)
                else native_pd.Series(_row_key)
            )
        return _row_key

    def loc_set_helper(df):
        # convert row key to appropriate type
        row_key = key_converter(df)
        if isinstance(df, native_pd.DataFrame):
            df.loc[
                try_convert_index_to_native(row_key),
                try_convert_index_to_native(col_key),
            ] = item
        else:
            key = (
                row_key,
                pd.Series(col_key)
                if isinstance(col_key, native_pd.Series)
                else col_key,
            )
            df.loc[key] = pd.DataFrame(item)

    # pandas raise error if the main frame columns have duplicates when enlargement may happen.
    query_count, join_count, expect_exception = 0, 0, True

    if (
        isinstance(col_key, slice)
        or (
            (hasattr(col_key, "dtype") and col_key.dtype == bool)
            or isinstance(col_key[0], bool)
        )
        # otherwise, pandas raise ValueError: cannot reindex on an axis with duplicate labels
        or (df.columns.equals(df.columns.union(col_key)))
    ):
        query_count, join_count, expect_exception = 1, 3, False
    if isinstance(col_key, native_pd.Series):
        query_count += 1

    with SqlCounter(
        query_count=query_count,
        join_count=join_count,
    ):
        eval_snowpark_pandas_result(
            pd.DataFrame(df),
            df,
            loc_set_helper,
            inplace=True,
            expect_exception=expect_exception,
            expect_exception_type=ValueError,
            expect_exception_match="cannot reindex on an axis with duplicate labels",
        )


@pytest.mark.parametrize(
    "item",
    [
        native_pd.DataFrame(
            [[91, 92, 93, 94], [94, 95, 96, 97], [97, 98, 99, 100]],
            columns=["A", "B", "C", "X"],
            index=[
                3,  # 3 does not exist in the row key, so it will be skipped
                2,
                1,
            ],
        ),
        native_pd.DataFrame(
            [[91, 92, 93, 94], [94, 95, 96, 97], [97, 98, 99, 100]],
            columns=["A", "B", "C", "X"],
            index=[
                1,  # duplicated index
                2,
                1,
            ],
        ),
    ],
)
def test_df_loc_set_general_key_with_duplicate_rows(item, key_type):
    df = native_pd.DataFrame(
        [[1, 2, 3, 4], [4, 5, 6, 7], [7, 8, 9, 10]],
        columns=["B", "A", "C", "D"],
        index=[1, 1, 2],
    )
    row_key = [
        1,  # 0 does not exist in item, so the row values will be set to NULL
        1,
    ]

    def key_converter(df):
        _row_key = row_key
        # Convert key to the required type.
        if key_type == "index":
            _row_key = (
                pd.Index(_row_key)
                if isinstance(df, pd.DataFrame)
                else native_pd.Index(_row_key)
            )
        elif key_type == "ndarray":
            _row_key = np.array(_row_key)
        elif key_type == "series":
            _row_key = (
                pd.Series(_row_key)
                if isinstance(df, pd.DataFrame)
                else native_pd.Series(_row_key)
            )
        return _row_key

    # test case for df.loc[row_key, :] = item
    def loc_set_helper(df):
        # convert row key to appropriate type
        row_key = key_converter(df)
        if isinstance(df, native_pd.DataFrame):
            df.loc[row_key, :] = item
        else:
            df.loc[row_key, :] = pd.DataFrame(item)

    with SqlCounter(query_count=1, join_count=3):
        if item.index.has_duplicates:
            # pandas fails to update duplicated rows with duplicated item
            with pytest.raises(
                ValueError,
                match=re.escape("cannot reindex on an axis with duplicate labels"),
            ):
                loc_set_helper(df)
            snow = pd.DataFrame(df)
            loc_set_helper(snow)
            # Snowpark pandas won't raise error but the total number of rows will be changed (e.g., from 3 to 5)
            row_key = [1]
            item = item[~item.index.duplicated(keep="last")]
            loc_set_helper(df)
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow, df)
        else:
            eval_snowpark_pandas_result(
                pd.DataFrame(df), df, loc_set_helper, inplace=True
            )


@sql_count_checker(query_count=1, join_count=3)
def test_df_loc_set_duplicate_cols_in_df_and_col_key():
    df = native_pd.DataFrame(
        [[1, 2, 3, 4], [4, 5, 6, 7], [7, 8, 9, 10]], columns=["D", "B", "B", "A"]
    )
    row_key = native_pd.Series(
        [
            0,  # 0 does not exist in item, so the row values will be set to NULL
            1,
            2,
        ]
    )
    col_key = ["B", "B", "A"]

    item = native_pd.DataFrame(
        [[1, 2, 4, 5, 6], [4, 5, 6, 7, 8]],
        columns=["A", "B", "C", "D", "E"],
        index=[
            3,  # 3 does not exist in the row key, so it will be skipped
            2,
        ],
    )

    def loc_set_helper(df):
        if isinstance(df, native_pd.DataFrame):
            df.loc[row_key, col_key] = item
        else:
            df.loc[pd.Series(row_key), col_key] = pd.DataFrame(item)

    eval_snowpark_pandas_result(
        pd.DataFrame(df),
        df,
        loc_set_helper,
        inplace=True,
    )


@sql_count_checker(query_count=0)
def test_df_loc_set_number_of_cols_mismatch_negative():
    df = native_pd.DataFrame(
        [[1, 2, 3, 4], [4, 5, 6, 7], [7, 8, 9, 10]], columns=["D", "B", "B", "A"]
    )
    row_key = native_pd.Series(
        [
            0,  # 0 does not exist in item, so the row values will be set to NULL
            1,
            2,
        ]
    )

    item = native_pd.DataFrame(
        [[1, 2, 4, 5, 6], [4, 5, 6, 7, 8]],
        columns=["A", "B", "C", "D", "E"],
        index=[
            3,  # 3 does not exist in the row key, so it will be skipped
            2,
        ],
    )

    # test case for df.loc[row_key] = item
    def loc_set_helper(df):
        if isinstance(df, native_pd.DataFrame):
            df.loc[row_key] = item
        else:
            df.loc[pd.Series(row_key)] = pd.DataFrame(item)

    eval_snowpark_pandas_result(
        pd.DataFrame(df),
        df,
        loc_set_helper,
        inplace=True,
        expect_exception=True,
        assert_exception_equal=False,
        expect_exception_type=ValueError,
        expect_exception_match="shape mismatch",
    )

    item = item.iloc[:, 0:3]
    eval_snowpark_pandas_result(
        pd.DataFrame(df),
        df,
        loc_set_helper,
        inplace=True,
        expect_exception=True,
        assert_exception_equal=False,
        expect_exception_type=ValueError,
        expect_exception_match="shape mismatch",
    )


@pytest.mark.parametrize(
    "key,value",
    [(5, 0), (-2, 20), ("A", 7), (slice(None), 2), (slice(None, None, 2), 0)],
)
@pytest.mark.parametrize(
    "data,index,columns",
    [
        ([[1, 2, 3], [4, 5, 6], [7, 8, 9]], None, ["A", "B", "C"]),
        ([1, 2, 3], None, ["A"]),
        # To prevent dtype mismatch error, we cast the empty index (default int dtype) to object
        (None, native_pd.Index([], dtype=object), ["A", "B"]),
        (None, ["A", "B"], ["X"]),
    ],
)
def test_df_loc_set_with_non_matching_1d_scalar_key(data, index, columns, key, value):
    native_df = native_pd.DataFrame(data=data, index=index, columns=columns)
    snow_df = pd.DataFrame(native_df)

    def helper(df):
        df.loc[key] = value

    if (
        not isinstance(key, slice)
        and len(native_df.index) > 0
        and not isinstance(key, type(native_df.index[0]))
    ):
        with SqlCounter(query_count=0):
            # We should expect this case will fail because of snowflake type system mismatch.
            with pytest.raises(
                SnowparkSQLException, match="Numeric value 'A' is not recognized"
            ):
                helper(snow_df)
                # Trigger action to run query
                snow_df.to_pandas()
    else:
        expected_query_count = 1
        expected_join_count = 1
        if key == slice(None):
            expected_join_count = 0
        elif isinstance(key, slice) and key.step == 2:
            expected_join_count += 1

        with SqlCounter(
            query_count=expected_query_count, join_count=expected_join_count
        ):
            # Treat index like any other column in a DataFrame when it comes to types,
            # therefore Snowpark pandas returns a Index(dtype="object") for an empty index
            # whereas pandas returns RangeIndex()
            # This is compatible with behavior for empty dataframe in other tests.
            eval_snowpark_pandas_result(
                snow_df, native_df.copy(), helper, inplace=True, check_index_type=False
            )


@pytest.mark.parametrize(
    "key,value",
    [
        (5, 0),
        (-2, 20),
    ],
)
@pytest.mark.parametrize("index", [None, [1, 2, 3]])
@sql_count_checker(query_count=0)
def test_df_loc_set_for_empty_dataframe_negative(index, key, value):
    native_df = native_pd.DataFrame(index=index)
    # assigning a new value via loc[x] does not work when dataframe has no columns.
    snow_df = pd.DataFrame(native_df)

    # Only Snowpark pandas raises ValueError.
    err_msg = "cannot set a frame with no defined columns"
    with pytest.raises(ValueError, match=err_msg):
        snow_df.loc[key] = value
        native_df.loc[key] = value
        assert_frame_equal(snow_df, native_df)


@pytest.mark.parametrize(
    "row_key, col_key, value",
    [
        (5, "A", 0),
        (-2, "B", 20),
        ("A", "C", 7),
        ("X", "D", 7),
    ],
)
@pytest.mark.parametrize(
    "native_df",
    [
        native_pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["A", "B", "C"]),
        # To prevent dtype mismatch error, we cast the empty index (default int dtype) to object
        native_pd.DataFrame(index=native_pd.Index([], dtype=object)),
    ],
)
def test_df_loc_set_row_col_with_non_matching_scalar_key(
    native_df, row_key, col_key, value
):
    snow_df = pd.DataFrame(native_df.copy())

    def helper(df):
        df.loc[row_key, col_key] = value

    if len(native_df.index) > 0 and not isinstance(row_key, type(native_df.index[0])):
        # We should expect this case will fail because of snowflake type system mismatch.
        with SqlCounter(query_count=0):
            with pytest.raises(
                SnowparkSQLException, match="Numeric value '.*' is not recognized"
            ):
                helper(snow_df)
                # Trigger action to run query
                snow_df.to_pandas()
    else:
        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snow_df,
                native_df.copy(),
                helper,
                inplace=True,
            )


@pytest.mark.parametrize(
    "key,value",
    [
        ((slice(None), "D"), 7),
    ],
)
@pytest.mark.parametrize(
    "native_df",
    [
        native_pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["A", "B", "C"]),
    ],
)
@sql_count_checker(query_count=1, join_count=0)
def test_df_loc_set_row_col_with_col_enlargement(native_df, key, value):
    snow_df = pd.DataFrame(native_df)

    def helper(df):
        df.loc[key] = value

    eval_snowpark_pandas_result(snow_df, native_df, helper, inplace=True)


@sql_count_checker(query_count=0)
def test_df_loc_set_with_multi_index_not_implemented():
    df = pd.DataFrame(
        [1, 2, 3, 4],
        columns=["A"],
        index=pd.MultiIndex.from_tuples([(1, 1), (1, 2), (2, 1), (1, 1)]),
    )

    # multi-index not yet supported, track it here TODO SNOW-962260
    with pytest.raises(
        NotImplementedError, match="loc set for multiindex is not yet implemented"
    ):
        df.loc[(1, 1)] = 42


@pytest.mark.parametrize(
    "key",
    [
        "c1",
        "c2",
        ["c2", "c1", "c2"],
    ],
)
@sql_count_checker(query_count=1)
def test_df_loc_get_col_str_key(key):
    df = native_pd.DataFrame({"c1": [1, 2, 3], "c2": [4, 5, 6]})
    snow_df = pd.DataFrame(df)
    eval_snowpark_pandas_result(snow_df, df, lambda df: df.loc[:, key])


@pytest.mark.parametrize(
    "key",
    [
        [],
        [True] * 7,
        [False] * 7,
        [random.choice([True, False]) for _ in range(7)],
        # length mismatch
        [random.choice([True, False]) for _ in range(random.randint(1, 7))],
        [random.choice([True, False]) for _ in range(random.randint(8, 20))],
    ],
)
def test_df_loc_get_key_bool(
    key, key_type, default_index_snowpark_pandas_df, default_index_native_df
):
    def loc_helper(df):
        if isinstance(df, native_pd.DataFrame):
            # If native pandas DataFrame, truncate the df and key.
            _df = df[: len(key)]
            _key = key[: len(_df)]
        else:
            _key, _df = key, df

        # Convert key to the required type.
        if key_type == "index":
            _key = (
                pd.Index(_key, dtype=bool)
                if isinstance(df, pd.DataFrame)
                else native_pd.Index(_key, dtype=bool)
            )
        elif key_type == "ndarray":
            _key = np.array(_key, dtype=bool)
        elif key_type == "series":
            _key = (
                pd.Series(_key)
                if isinstance(_df, pd.DataFrame)
                else native_pd.Series(_key)
            )

        return _df.loc[_key]

    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            loc_helper,
        )


@sql_count_checker(query_count=2, join_count=0)
def test_df_loc_get_key_bool_self_series():
    native_df = native_pd.DataFrame(
        {
            "A": [2, 4, 5, 6, 1, 0],
            "B": [True, False, True, False, False, True],
            "C": ["true", "test", "apple", "bee", "jack", "mail"],
        },
        index=native_pd.Index(["a", "b", "c", 1, 3, "e"]),
    )
    snow_df = pd.DataFrame(native_df)

    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.loc[df["B"]]
            if isinstance(df, pd.DataFrame)
            else df.loc[df["B"]],
        )

    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.loc[(df["A"] > 4) | (df["A"] < 2)]
            if isinstance(df, pd.DataFrame)
            else df.loc[(df["A"] > 4) | (df["A"] < 2)],
        )


@pytest.mark.parametrize(
    "key",
    [
        [True] * 5,
        [False] * 5,
        [random.choice([True, False]) for _ in range(5)],
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_df_loc_get_key_bool_series_with_aligned_indices(key, use_default_index):
    # aligned indices means both row_pos and index are exactly match
    if use_default_index:
        index = None
    else:
        # index can have null values and duplicates
        index = native_pd.Index(["a", "a", None, "b", "b"], name="index")
    native_df = native_pd.DataFrame(
        {"c1": [1, 2, 3, 4, 5], "c2": ["x", "y", "z", "d", "e"]}, index=index
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.loc[pd.Series(key, index=index, dtype="bool")]
        if isinstance(df, pd.DataFrame)
        else df.loc[native_pd.Series(key, index=index, dtype="bool")],
    )


@pytest.mark.parametrize(
    "key",
    [
        [True] * 5,
        [False] * 5,
        [random.choice([True, False]) for _ in range(5)],
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_df_loc_get_key_bool_series_with_unaligned_and_distinct_indices(
    key, use_default_index
):
    # unaligned and distinct indices: e.g., [1,2,3,4,5] vs [5,4,3,2,1]
    if use_default_index:
        index = None
        key_index = np.random.permutation(range(5))
    else:
        index_value = ["a", "b", "c", "d", "e"]
        index = native_pd.Index(index_value, name="index")
        key_index = generate_a_random_permuted_list_exclude_self(index_value)
    native_df = native_pd.DataFrame(
        {"c1": [1, 2, 3, 4, 5], "c2": ["x", "y", "z", "d", "e"]}, index=index
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.loc[pd.Series(key, index=key_index, dtype="bool")]
        if isinstance(df, pd.DataFrame)
        else df.loc[native_pd.Series(key, index=key_index, dtype="bool")],
    )


@sql_count_checker(query_count=1, join_count=2)
def test_df_loc_get_key_bool_series_with_unaligned_and_duplicate_indices():
    key = [True] * 5
    # index can have null values and duplicates
    index_value = ["a", "a", None, "b", "b"]
    index = native_pd.Index(index_value, name="index")
    native_df = native_pd.DataFrame(
        {"c1": [1, 2, 3, 4, 5], "c2": ["x", "y", "z", "d", "e"]}, index=index
    )
    permuted_index_value = generate_a_random_permuted_list_exclude_self(index_value)
    native_key_index = native_pd.Index(permuted_index_value, dtype="string")
    snow_key_index = pd.Index(native_key_index, dtype="string")
    snow_df = pd.DataFrame(native_df)
    series_key = pd.Series(key, index=snow_key_index, dtype="bool", name="key")
    native_series_key = native_pd.Series(
        key, index=native_key_index, dtype="bool", name="key"
    )

    # Note:
    # pandas: always raise IndexingError when indices with duplicates are not aligned
    # Snowpark pandas: perform outer join on index and no error will be raised
    with pytest.raises(IndexingError):
        native_df.loc[native_series_key]

    assert_frame_equal(
        snow_df.loc[series_key],
        native_pd.DataFrame(
            {
                "c1": [1, 1, 2, 2, 3, 4, 4, 5, 5],
                "c2": ["x", "x", "y", "y", "z", "d", "d", "e", "e"],
            },
            index=native_pd.Index(
                ["a", "a", "a", "a", None, "b", "b", "b", "b"], name="index"
            ),
        ),
        check_dtype=False,
    )


@pytest.mark.parametrize(
    "key",
    [
        [],
        [
            random.choice([True, False]) for _ in range(random.randint(0, 4))
        ],  # shorter length
        [
            random.choice([True, False]) for _ in range(random.randint(6, 10))
        ],  # larger length
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_df_loc_get_key_bool_series_with_mismatch_index_len(key, use_default_index):
    if use_default_index:
        index = None
        key_index = np.random.permutation(len(key))
    else:
        index = ["a", "b", "c", "d", "e", "a1", "b1", "c1", "d1", "e1"]
        key_index = native_pd.Index(
            np.random.permutation(index[: len(key)]), dtype="string"
        )
        index = np.random.permutation(index[:5])
    native_df = native_pd.DataFrame(
        {"c1": [1, 2, 3, 4, 5], "c2": ["x", "y", "z", "d", "e"]}, index=index
    )
    snow_df = pd.DataFrame(native_df)
    native_series_key = native_pd.Series(key, index=key_index, dtype="bool")

    series_key = pd.Series(key, index=key_index, dtype="bool")
    if len(key) < 5:
        # Native pandas raises error if any index from native_df is not in the key; when no missing index exists, native
        # pandas will perform as expected even though the key includes out-of-bound index
        with pytest.raises(IndexingError):
            native_df.loc[native_series_key]
        # Snowpark pandas does not raise error but select the index existing in the key. So the behavior is same as
        # the native one if the missing ones are dropped from native_df
        native_df = native_df.drop(
            index=[i for i in native_df.index if i not in native_series_key.index]
        )
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.loc[series_key]
        if isinstance(df, pd.DataFrame)
        else df.loc[native_series_key],
    )


@pytest.mark.parametrize(
    "key",
    [
        [],
        [random.choice([True, False]) for _ in range(random.randint(0, 100))],
        [random.choice([True, False]) for _ in range(random.randint(1000, 2000))],
    ],
)
@pytest.mark.skipif(running_on_public_ci(), reason="slow test")
def test_df_loc_get_key_bool_series_with_1k_shape(key, native_df_1k_1k):
    def loc_helper(df):
        # Note:
        # if key length does not match with df, Snowpark will only select the row position the key contains; while
        # pandas will raise error, so we first truncate the df for pandas and then compare the result
        return (
            df.loc[pd.Series(key, dtype="bool")]
            if isinstance(df, pd.DataFrame)
            else df.iloc[: len(key)].loc[native_pd.Series(key, dtype="bool")]
        )

    # 4 queries includes 3 queries to prepare the temp table for df, including create,
    # insert, drop the temp table (3) and one select query.
    # 7 queries add extra 3 queries to prepare the temp table for key.
    query_count = 7 if len(key) >= 300 else 4
    _test_df_loc_with_1k_shape(native_df_1k_1k, loc_helper, query_count)


def _test_df_loc_with_1k_shape(
    native_df_1k_1k, loc_helper, query_count, high_count_reason=None
):
    df_1k_1k = pd.DataFrame(native_df_1k_1k)
    high_count_expected = high_count_reason is not None

    # test df with default index
    with SqlCounter(
        query_count=query_count,
        join_count=1,
        high_count_expected=high_count_expected,
        high_count_reason=high_count_reason,
    ):
        eval_snowpark_pandas_result(
            df_1k_1k,
            native_df_1k_1k,
            loc_helper,
        )

    # test df with non-default index
    native_df_1k_1k_non_default_index = native_df_1k_1k.reset_index().set_index("index")
    df_1k_1k_non_default_index = pd.DataFrame(native_df_1k_1k_non_default_index)
    with SqlCounter(
        query_count=query_count,
        join_count=1,
        high_count_expected=high_count_expected,
        high_count_reason=high_count_reason,
    ):
        eval_snowpark_pandas_result(
            df_1k_1k_non_default_index,
            native_df_1k_1k_non_default_index,
            loc_helper,
        )

    # test df 1 col with default index
    native_df_1k_1 = native_df_1k_1k[["c0"]]
    df_1k_1 = pd.DataFrame(native_df_1k_1)
    with SqlCounter(
        query_count=query_count,
        join_count=1,
        high_count_expected=high_count_expected,
        high_count_reason=high_count_reason,
    ):
        eval_snowpark_pandas_result(
            df_1k_1,
            native_df_1k_1,
            loc_helper,
        )

    native_df_1k_1_non_default_index = (
        native_df_1k_1k[["c0", "c1"]].reset_index().set_index("index")
    )
    df_1k_1_non_default_index = pd.DataFrame(native_df_1k_1_non_default_index)

    # test df 1 col with non-default index
    with SqlCounter(
        query_count=query_count,
        join_count=1,
        high_count_expected=high_count_expected,
        high_count_reason=high_count_reason,
    ):
        eval_snowpark_pandas_result(
            df_1k_1_non_default_index,
            native_df_1k_1_non_default_index,
            loc_helper,
        )


def test_df_loc_get_key_scalar(
    default_index_snowpark_pandas_df, default_index_native_df
):
    key = random.choice(range(0, len(default_index_native_df)))
    # squeeze and to_pandas triggers additional queries
    with SqlCounter(query_count=2, join_count=2):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            lambda df: df.loc[key],
        )


@pytest.mark.parametrize(
    "native_series_key",
    [
        native_pd.Series([]),
        native_pd.Series([0]),
        native_pd.Series([3, 2, 1]),
        native_pd.Series([3, 2, 1]),
        native_pd.Series([3, 2, 1], index=[300, 244, 234]),  # index is ignored
        native_pd.Series([2, 3, 1, 3, 2, 1]),
        native_pd.Series(
            [random.choice(range(0, 5)) for _ in range(random.randint(0, 20))]
        ),
    ],
)
def test_df_loc_get_key_non_boolean(
    native_series_key,
    key_type,
    default_index_snowpark_pandas_df,
    default_index_native_df,
):
    def loc_key_type_convert(key, is_snow_type, index_name=None):
        if key_type == "series":
            return pd.Series(key) if is_snow_type else native_pd.Series(key)
        elif key_type == "list":
            return key.to_list()
        elif key_type == "array":
            return key.to_numpy()
        elif key_type == "index":
            # native pandas has a bug to overwrite loc result's index name to the key's index name
            # so for testing, we overwrite the index name to be the same as the index name in the main frame
            return (
                pd.Index(key.to_list(), name=index_name)
                if is_snow_type
                else native_pd.Index(key.to_list(), name=index_name)
            )

    # default index
    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            lambda df: df.loc[
                loc_key_type_convert(native_series_key, isinstance(df, pd.DataFrame))
            ],
        )

    # non default index
    non_default_index_native_df = default_index_native_df.reset_index().set_index(
        "index"
    )
    non_default_index_snowpark_pandas_df = pd.DataFrame(non_default_index_native_df)
    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            non_default_index_snowpark_pandas_df,
            non_default_index_native_df,
            lambda df: df.loc[
                loc_key_type_convert(
                    native_series_key, isinstance(df, pd.DataFrame), index_name="index"
                )
            ],
        )

    # non default index with duplicates and null
    dup_native_df = native_pd.concat(
        [
            non_default_index_native_df,
            non_default_index_native_df,
            non_default_index_native_df.set_index([[None] * 7]),
        ]
    )
    dup_snowpandas_df = pd.DataFrame(dup_native_df)
    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            dup_snowpandas_df,
            dup_native_df,
            lambda df: df.loc[
                loc_key_type_convert(native_series_key, isinstance(df, pd.DataFrame))
            ],
        )

    # use key with null values
    native_series_key = native_pd.concat([native_series_key.astype("Int64"), None])
    non_default_index_native_df = (
        default_index_native_df.reset_index()
        .astype({"index": "Int64"})
        .set_index("index")
    )

    dup_native_df = native_pd.concat(
        [
            non_default_index_native_df,
            non_default_index_native_df,
            non_default_index_native_df.set_index([[None] * 7]),
        ]
    )
    dup_snowpandas_df = pd.DataFrame(dup_native_df)
    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            dup_snowpandas_df,
            dup_native_df,
            lambda df: df.loc[
                loc_key_type_convert(native_series_key, isinstance(df, pd.DataFrame))
            ],
        )


@pytest.mark.parametrize(
    "key",
    [
        [],
        # key with short size (i.e., generated by inline sql)
        [random.randint(-1500, 1500) for _ in range(random.randint(0, 100))],
        # key with large size (i.e., generated by temp table)
        [random.randint(-1500, 1500) for _ in range(random.randint(1000, 1500))],
    ],
)
@pytest.mark.skipif(running_on_public_ci(), reason="slow test")
def test_df_loc_get_key_non_boolean_series_with_1k_shape(key, native_df_1k_1k):
    def loc_helper(df):
        # similarly, remove out-of-bound values, so we can avoid error and compare
        return (
            df.loc[pd.Series(key)]
            if isinstance(df, pd.DataFrame)
            else df.loc[[k for k in key if k in range(1000)]]
        )

    # 4 queries includes 3 queries to prepare the temp table for df, including create,
    # insert, drop the temp table (3) and one select query.
    # 7 queries add extra 3 queries to prepare the temp table for key.
    query_count = 7 if len(key) >= 300 else 4
    _test_df_loc_with_1k_shape(native_df_1k_1k, loc_helper, query_count)


@pytest.mark.parametrize(
    "start",
    [None, -1, 1, 4, 10],
)
@pytest.mark.parametrize(
    "stop",
    [None, -1, 1, 4, 10],
)
@pytest.mark.parametrize(
    "step",
    [None, 1, -1, 2, -2, 9, -9],
)
@pytest.mark.parametrize("monotonic_decreasing", [False, True])
@sql_count_checker(query_count=1)
def test_df_loc_get_key_slice(
    start,
    stop,
    step,
    monotonic_decreasing,
    default_index_snowpark_pandas_df,
    default_index_native_df,
):
    if monotonic_decreasing:
        native_df = default_index_native_df[::-1]
        snow_df = pd.DataFrame(native_df)
    else:
        native_df = default_index_native_df
        snow_df = default_index_snowpark_pandas_df

    # test both slice and range
    if start is not None and stop is not None and step is not None:
        key = range(start, stop, step)
    else:
        key = slice(start, stop, step)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        # pandas does not allow out-of-bounds in range like key, so we always use slice for native pandas for testing
        lambda df: df.loc[key]
        if isinstance(df, pd.DataFrame)
        else df.loc[slice(start, stop, step)],
    )


@pytest.mark.parametrize(
    "start",
    [
        None,
        1,
        4,
        6,
    ],  # no out-of-bound keys because pandas will raise KeyError when the index is unordered
)
@pytest.mark.parametrize(
    "stop",
    [None, 1, 4, 6],
)
@pytest.mark.parametrize(
    "step",
    [None, 1, -1, 2, -2, 9, -9],
)
@sql_count_checker(query_count=1)
def test_df_loc_get_key_slice_with_unordered_index(
    start,
    stop,
    step,
    default_index_native_df,
):
    unordered_index = [0, None, 6, 1, 4, 5, None]
    native_df = default_index_native_df
    native_df.index = unordered_index
    snow_df = pd.DataFrame(native_df)

    # test both slice and range
    if start is not None and stop is not None and step is not None:
        key = range(start, stop, step)
    else:
        key = slice(start, stop, step)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.loc[key]
        if isinstance(df, pd.DataFrame)
        else df.loc[slice(start, stop, step)],
    )


@pytest.mark.parametrize(
    "start",
    [
        None,
        1,
        4,
        6,
    ],  # no out-of-bound keys because pandas will raise KeyError when the index is unordered
)
@pytest.mark.parametrize(
    "stop",
    [None, 1, 4, 6],
)
@pytest.mark.parametrize(
    "step",
    [-1, -2, -9],
)
@pytest.mark.parametrize(
    "na_position",
    ["last", "first"],
)
@sql_count_checker(query_count=1)
def test_df_loc_get_reversed_key_slice_with_unordered_nullable_index(
    start,
    stop,
    step,
    default_index_native_df,
    na_position,
):
    unordered_index = [0, None, 6, 1, 4, 5, None]
    native_df = default_index_native_df.assign(has_null=[0, 2, None, None, 3, None, -1])
    native_df.index = unordered_index

    snow_df = pd.DataFrame(native_df)
    native_df = native_df.sort_values(by="has_null", na_position=na_position)
    snow_df = snow_df.sort_values(by="has_null", na_position=na_position)

    # test both slice and range
    if start is not None and stop is not None and step is not None:
        key = range(start, stop, step)
    else:
        key = slice(start, stop, step)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.loc[key]
        if isinstance(df, pd.DataFrame)
        else df.loc[slice(start, stop, step)],
    )


@pytest.mark.parametrize("key", [slice("b", "d"), slice("a", "f")])
@sql_count_checker(query_count=1)
def test_df_loc_get_with_duplicate_index_get_key_slice(key):
    native_df = native_pd.DataFrame(
        {"c": [0, 1, 2, 3, 4]}, index=["b", "b", "d", "d", "e"]
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.loc[key],
    )


@pytest.mark.parametrize(
    "key, expected_index",
    [
        # When start and stop can be found, the left bound will be the one with minimal row position and the right bound
        # will be the one with maximum row position
        [slice("b", "d"), ["b", "d", None, "d"]],
        # When start or stop cannot be found, find any values in between
        [slice("a", "f"), ["b", "d", None, "d", "b", "e"]],
        [slice("f", "a"), []],
        [slice("d", "b"), ["d", None, "d", "b"]],
    ],
)
# one extra query to convert to list at end
@sql_count_checker(query_count=2)
def test_df_loc_get_with_non_monotonic_index_get_key_slice(key, expected_index):
    native_df = native_pd.DataFrame(
        {"c": [0, 1, 2, 3, 4, 5]}, index=["b", "d", None, "d", "b", "e"]
    )
    snow_df = pd.DataFrame(native_df)

    # native df cannot get the bound for non-unique labels
    with pytest.raises(KeyError):
        native_df.loc[key]

    assert list(snow_df.loc[key].index) == expected_index


@pytest.mark.parametrize(
    "key, expected_error_type, expected_exception_match",
    [
        [slice(None, None, 0), ValueError, "slice step cannot be zero"],
        [slice(None, None, 1.1), TypeError, "slice step must be integer"],
    ],
)
@sql_count_checker(query_count=0)
def test_df_loc_get_key_slice_negative(
    key,
    expected_error_type,
    expected_exception_match,
    default_index_snowpark_pandas_df,
    default_index_native_df,
):
    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_df,
        default_index_native_df,
        lambda df: df.loc[key],
        expect_exception=True,
        expect_exception_type=expected_error_type,
        expect_exception_match=expected_exception_match,
        assert_exception_equal=False,
    )


@pytest.mark.parametrize(
    "df",
    [
        native_pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["A", "B", "C"]),
        native_pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            columns=["A", "B", "C"],
            index=["x", "y", "z"],
        ),
        native_pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            columns=["A", "B", "C"],
            index=["d", "d", "d"],
        ),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_df_loc_self_df_set_aligned_row_key(df):
    item = native_pd.DataFrame(
        [[10, 20, 30], [40, 50, 60], [70, 80, 90]],
        columns=["C", "A", "B"],
        index=df.index,
    )

    def loc_set_helper(df):
        df.loc[df["A"] > 1] = (
            item if isinstance(df, native_pd.DataFrame) else pd.DataFrame(item)
        )

    if df.index.has_duplicates:
        # pandas raises error for duplicates while Snowpark pandas can perform correctly
        with pytest.raises(
            ValueError, match="cannot reindex on an axis with duplicate labels"
        ):
            loc_set_helper(df)
        snow = pd.DataFrame(df)
        loc_set_helper(snow)
        assert_frame_equal(
            snow,
            native_pd.DataFrame(
                [[1, 2, 3], [40, 50, 60], [70, 80, 90]],
                columns=["A", "B", "C"],
                index=["d", "d", "d"],
            ),
            check_dtype=False,
        )
    else:
        eval_snowpark_pandas_result(pd.DataFrame(df), df, loc_set_helper, inplace=True)


@pytest.mark.parametrize(
    "row_key, col_key, item_values",
    [
        # Test single row (existing) and combinations of existing/new columns
        ("a", None, 99),
        ("a", "C", 99),
        ("c", "T", 97),
        ("d", ["V", "T"], 96),
        ("a", ["B", "T"], 95),
        pytest.param(
            "a",
            ["B", "B", "T", "T"],
            95,
            marks=pytest.mark.xfail(
                strict=True,
                reason="SNOW-1057861: Investigate locset behavior with missing index value",
            ),
        ),
        # Test single row (new / not existing) and combinations of existing/new columns
        ("b", ["A", "D"], 98),
        ("x", None, 94),
        ("x", "D", 94),
        ("y", ["B", "C"], 93),
        ("z", "T", 92),
        pytest.param(
            "w",
            ["V", "T"],
            91,
            marks=pytest.mark.xfail(
                strict=True,
                reason="SNOW-1321196: pandas 2.2.1 migration test failure",
            ),
        ),
        pytest.param(
            "u",
            ["C", "T"],
            90,
            marks=pytest.mark.xfail(
                strict=True,
                reason="SNOW-1321196: pandas 2.2.1 migration test failure",
            ),
        ),
        pytest.param(
            "v",
            ["B", "B", "T", "T"],
            95,
            marks=pytest.mark.xfail(
                strict=True,
                reason="SNOW-1321196: pandas 2.2.1 migration test failure",
            ),
        ),
        # Test list like item
        ("a", None, [99]),
        ("a", None, [99, 98, 97, 96]),
        ("y", ["B", "C"], [0]),
        ("y", ["B", "C"], [0, 1]),
        pytest.param(
            "u",
            ["X", "T"],
            [90, 91],
            marks=pytest.mark.xfail(
                strict=True,
                reason="SNOW-1321196: pandas 2.2.1 migration test failure",
            ),
        ),
    ],
)
@pytest.mark.parametrize(
    "data_index",
    [
        # Test with unique index values
        ["a", "b", "c", "d"],
        # Test with duplicate index values
        ["a", "a", "c", "d"],
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_df_loc_set_scalar_row_key_enlargement(
    row_key, col_key, item_values, data_index
):
    """
    Some tests above are marked as xfail since the new pandas behavior from versions 2.2.0+ seems like a bug in
    pandas; issue here: https://github.com/pandas-dev/pandas/issues/58316

    The problem arises when loc set with a scalar item is performed with new rows and columns. The "new" column values
    contain byte values b'' instead of NaN.
    """
    data = {
        "A": [5, 8, 11, 14],
        "B": [6, 9, 12, 15],
        "C": [7, 10, 13, 16],
        "D": [8, 11, 14, 17],
    }

    snow_df = pd.DataFrame(data, index=data_index)
    native_df = native_pd.DataFrame(data, index=data_index)

    def set_loc_helper(df):
        if col_key is None:
            df.loc[row_key] = item_values
        else:
            df.loc[row_key, col_key] = item_values

    eval_snowpark_pandas_result(snow_df, native_df, set_loc_helper, inplace=True)


@pytest.mark.parametrize(
    "row_key, col_key, item_values",
    [
        # Test single row (existing) and combinations of existing/new columns
        (
            "a",
            ["B", "B", "T", "T"],
            95,
        ),
        # Test single row (new / not existing) and combinations of existing/new columns
        (
            "w",
            ["V", "T"],
            91,
        ),
        (
            "u",
            ["C", "T"],
            90,
        ),
        (
            "v",
            ["B", "B", "T", "T"],
            95,
        ),
        # Test list like item
        (
            "u",
            ["X", "T"],
            [90, 91],
        ),
    ],
)
@pytest.mark.parametrize(
    "data_index",
    [
        # Test with unique index values
        ["a", "b", "c", "d"],
        # Test with duplicate index values
        ["a", "a", "c", "d"],
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_df_loc_set_scalar_row_key_enlargement_deviates_from_native_pandas(
    row_key, col_key, item_values, data_index
):
    """
    This test is to check whether the xfail'd tests above work as expected in Snowpark pandas.
    See pandas issue: https://github.com/pandas-dev/pandas/issues/58316
    """
    data = {
        "A": [5, 8, 11, 14],
        "B": [6, 9, 12, 15],
        "C": [7, 10, 13, 16],
        "D": [8, 11, 14, 17],
    }

    snow_df = pd.DataFrame(data, index=data_index)
    native_df = native_pd.DataFrame(data, index=data_index)

    def set_loc_helper(df):
        if isinstance(df, native_pd.DataFrame):
            # Explicitly set the values in the new column to NaN to prevent byte data output.
            new_col_key = [col for col in ["V", "X", "T"] if col in col_key]
            df.loc[:, new_col_key] = np.nan
        df.loc[row_key, col_key] = item_values

    eval_snowpark_pandas_result(snow_df, native_df, set_loc_helper, inplace=True)


@pytest.mark.parametrize(
    "col_key, item_value, expect_pandas_fail, expect_snowpark_fail",
    [
        (None, native_pd.DataFrame([99]), True, True),
        ("a", native_pd.DataFrame([99]), True, True),
        # Note that pandas succeeds here only when col_key is a new column but seems to always set to Nan values, so
        # we'll just fail this case also.
        ("w", native_pd.DataFrame([99]), False, True),
        ("a", native_pd.Series([1]), True, True),
        # Note that pandas fails when col_key is a new column and item is a list or tuple, Snowpark pandas works for all
        # these cases
        ("a", [1], True, False),
        ("a", (1,), True, False),
        # Snowpark pandas does not support set cell with list like item
        ("w", [1], False, True),
        ("w", (1,), False, True),
        ("a", np.array([1]), False, True),
        ("a", native_pd.Index([1]), False, True),
    ],
)
def test_df_loc_set_scalar_with_item_negative(
    col_key, item_value, expect_pandas_fail, expect_snowpark_fail
):
    native_df = native_pd.DataFrame(
        {"a": [1, 2, 3], "b": [4, 5, 6]}, index=["x", "y", "z"]
    )
    snow_df = pd.DataFrame(native_df)

    row_key = "x"

    def perform_loc_set(df):
        item_ = item_value
        if isinstance(df, pd.DataFrame):
            if isinstance(item_value, native_pd.DataFrame):
                item_ = pd.DataFrame(item_value)
            elif isinstance(item_value, native_pd.Series):
                item_ = pd.Series(item_value)
        else:
            item_ = try_convert_index_to_native(item_)
        if col_key is None:
            df.loc[row_key] = item_
        else:
            df.loc[row_key, col_key] = item_

    if not expect_pandas_fail and not expect_snowpark_fail:
        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                perform_loc_set,
                inplace=True,
            )
    else:
        with SqlCounter(query_count=0):
            if expect_pandas_fail and expect_snowpark_fail:
                eval_snowpark_pandas_result(
                    snow_df,
                    native_df,
                    perform_loc_set,
                    expect_exception=True,
                    assert_exception_equal=False,
                    inplace=True,
                )


def test_empty_df_loc_set_scalar():
    # Check `loc` with row scalar on empty DataFrame.
    native_df = native_pd.DataFrame()
    snow_df = pd.DataFrame(native_df)
    with pytest.raises(ValueError, match="cannot set a frame with no defined columns"):
        native_df.loc[0] = 1

    with SqlCounter(query_count=1):
        snow_df.loc[0] = 1
        assert_snowpark_pandas_equal_to_pandas(
            snow_df,
            native_pd.DataFrame(index=[0]),
            check_column_type=False,
        )

    # Check `loc` with column scalar on empty DataFrame.
    native_df = native_pd.DataFrame()
    snow_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=1):
        with pytest.raises(
            ValueError, match="cannot set a frame with no defined index and a scalar"
        ):
            native_df.loc[:, 0] = 1
        snow_df.loc[:, 0] = 1
        assert snow_df.empty

    def row_loc(df):
        df.loc[0] = 1

    def col_loc(df):
        df.loc[:, "A"] = 1

    native_df = native_pd.DataFrame(index=[0, 1, 2])
    snow_df = pd.DataFrame(native_df)
    # Check `loc` with row scalar on empty DataFrame with non-empty index.
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df, native_df, row_loc, inplace=True, check_column_type=False
        )

    native_df = native_pd.DataFrame(index=[0, 1, 2])
    snow_df = pd.DataFrame(native_df)
    # Check `loc` with column scalar on empty DataFrame with non-empty index.
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df, native_df, col_loc, inplace=True, check_column_type=False
        )

    native_df = native_pd.DataFrame(columns=["A", "B", "C"])
    snow_df = pd.DataFrame(native_df)
    # Check `loc` with row scalar on empty DataFrame with non-empty columns.
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            row_loc,
            inplace=True,
        )

    native_df = native_pd.DataFrame(columns=["A", "B", "C"])
    snow_df = pd.DataFrame(native_df)
    # Check `loc` with column scalar on empty DataFrame with non-empty columns.
    with SqlCounter(query_count=1):
        col_loc(snow_df)
        assert_snowpark_pandas_equal_to_pandas(
            snow_df,
            native_pd.DataFrame(columns=["A", "B", "C"]),
            check_dtype=False,
            check_index_type=False,
        )

    native_df = native_pd.DataFrame(index=[0, 1, 2], columns=["A", "B", "C"])
    snow_df = pd.DataFrame(native_df)
    # Check `loc` with row scalar on empty DataFrame with non-empty index and columns.
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            row_loc,
            inplace=True,
        )

    native_df = native_pd.DataFrame(index=[0, 1, 2], columns=["A", "B", "C"])
    snow_df = pd.DataFrame(native_df)
    # Check `loc` with column scalar on empty DataFrame with non-empty index and columns.
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            col_loc,
            inplace=True,
        )

    # Test enlargening of empty DataFrame
    snow_df = pd.DataFrame()
    with SqlCounter(query_count=1):
        snow_df.loc[0] = 0
        snow_df.loc[:, 0] = 0
        assert_snowpark_pandas_equal_to_pandas(
            snow_df,
            native_pd.DataFrame([[0]]),
            check_dtype=False,
        )


@pytest.mark.parametrize(
    "native_item",
    [
        [1, 2, 3],
        np.array([1, 2, 3]),
        native_pd.Series([1, 2, 3]),
        native_pd.Series([1, 2, 3], index=["a", "b", "c"]),
        native_pd.Series(["abc", 4, 9.0]),
        native_pd.Series([8, None, None, 1], native_pd.Index(["a", None, None, "d"])),
    ],
)
def test_empty_df_loc_set_series_and_list(native_item):
    # To prevent dtype mismatch error in Snowpark pandas, we cast the empty index (default int dtype) to object
    snow_df = pd.DataFrame(index=pd.Index([], dtype=object))
    native_df = native_pd.DataFrame()
    snow_item = (
        pd.Series(native_item)
        if isinstance(native_item, native_pd.Series)
        else native_item
    )

    expected_join_count = 2 if isinstance(native_item, native_pd.Series) else 3

    def setitem_op(df):
        item = native_item if isinstance(df, native_pd.DataFrame) else snow_item
        df.loc[:, "A"] = item

    with SqlCounter(query_count=1, join_count=expected_join_count):
        if isinstance(native_item, native_pd.Series):
            eval_snowpark_pandas_result(snow_df, native_df, setitem_op, inplace=True)
        else:
            # When item is a list Snowpark pandas behavior is different from native pandas.
            # In Snowpark pandas output df will have null index values. To match native
            # pandas reset index values.
            setitem_op(snow_df)
            expected_df = native_pd.Series(
                native_item, name="A", index=[None] * len(native_item)
            ).to_frame()
            assert_frame_equal(
                snow_df, expected_df, check_index_type=False, check_dtype=False
            )


@pytest.mark.parametrize(
    "start",
    [None, -1, 1, 4, 10],
)
@pytest.mark.parametrize(
    "stop",
    [None, -1, 1, 4, 10],
)
@pytest.mark.parametrize(
    "step",
    [None, 1, -1, 2, -2, 9, -9],
)
@pytest.mark.parametrize("monotonic_decreasing", [False, True])
def test_df_loc_set_key_slice(
    start,
    stop,
    step,
    monotonic_decreasing,
):
    data = {
        "A": [1, 2, 3, 4, 6, 7],
        "B": [5, 6, 7, 8, 9, 10],
        "C": [9, 10, 11, 12, 13, 14],
        "D": [13, 14, 15, 16, 17, 18],
    }

    native_df = native_pd.DataFrame(data)
    if monotonic_decreasing:
        native_df = native_df[::-1]

    native_item_df = -native_df

    snow_df = pd.DataFrame(native_df)
    snow_item_df = pd.DataFrame(native_item_df)

    key = slice(start, stop, step)

    def set_loc_helper(df):
        if isinstance(df, pd.DataFrame):
            df.loc[key] = snow_item_df
        else:
            df.loc[key] = native_item_df

    expected_join_count = 1 if key == slice(None, None, None) else 3
    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(snow_df, native_df, set_loc_helper, inplace=True)


@pytest.mark.parametrize(
    "val_index",
    [
        ["v"],
        ["x"],
    ],
)
@pytest.mark.parametrize(
    "val_columns",
    [
        ["A"],
        ["Z"],
    ],
)
@pytest.mark.parametrize(
    "key",
    [
        ["A"],  # matching_item_columns_by_label = True
        "A",  # matching_item_columns_by_label = False
    ],
)
def test_df_loc_set_item_df_single_value(key, val_index, val_columns):
    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "x", "z", "w"],
        columns=["A", "B", "C", "D"],
        dtype=float,
    )

    val = native_pd.DataFrame([100], columns=val_columns, index=val_index)

    def setitem(df):
        if isinstance(df, pd.DataFrame):
            df.loc[:, key] = pd.DataFrame(val)
        else:
            # There is a bug in pandas when assigning a DataFrame item when the key is a scalar.
            # In the case of this test, that is when `key == "A"`.
            # To make sure Snowpark pandas works as expected, the column key is hard coded to ["A"], and the result
            # for `df.loc[:, "A"] = val` is evaluated.
            # SNOW-1057861, pandas issue: https://github.com/pandas-dev/pandas/issues/58482
            if key == "A" and val_index == ["x"] and val_columns == ["Z"]:
                df.iloc[[0, 1], 0] = 100
                df.iloc[[2, 3], 0] = np.nan
            else:
                df.loc[:, ["A"]] = val

    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            pd.DataFrame(native_df), native_df, setitem, inplace=True
        )


@pytest.mark.parametrize(
    "col_key",
    [
        "A",
        ["A", "B"],
        ["B", "A", "C"],
        "X",
        pytest.param(
            ["A", "X", "A"],
            marks=pytest.mark.xfail(
                strict=True,
                reason="SNOW-1321196: pandas 2.2.1 migration",
            ),
        ),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_df_loc_set_with_scalar_item(col_key):
    item = 100
    data = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    native_columns = ["A", "B", "C"]
    native_index = ["x", "y", "z"]
    row_key = ["x", "z"]

    native_df = native_pd.DataFrame(data, columns=native_columns, index=native_index)
    snow_df = pd.DataFrame(native_df)

    def loc_set_helper(df):
        df.loc[row_key, col_key] = item

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        loc_set_helper,
        inplace=True,
    )


@pytest.mark.parametrize(
    "col_key",
    [
        "A",
        "X",
        ["B"],
        ["Y"],
        native_pd.Index(["A"]),
        None,  # Should enlarge dataframe and create new column named `None`.
    ],
)
@pytest.mark.parametrize(
    "item",
    [
        [999],
        [99, 98],
        [99, 98, 97, 96],
    ],
)
@pytest.mark.parametrize("item_type_name,item_to_type", ITEM_TYPE_LIST_CONVERSION)
def test_df_loc_set_with_column_wise_list_like_item(
    col_key, item, item_type_name, item_to_type
):
    data = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    native_columns = ["A", "B", "C"]
    native_index = ["x", "y", "z"]
    row_key = ["x", "z"]

    native_df = native_pd.DataFrame(data, columns=native_columns, index=native_index)
    snow_df = pd.DataFrame(native_df)
    native_item = item

    def loc_set_helper(df):
        if isinstance(df, pd.DataFrame):
            df.loc[row_key, col_key] = item_to_type(item)
        else:
            # Native pandas does not allow enlargement with col_key ["B"] but allows enlargement with "B". Need to
            # convert ["B"] to "B" for comparison.
            df.loc[
                row_key, (col_key[0] if convert_list_to_string else col_key)
            ] = try_convert_index_to_native(item_to_type(native_item))

    convert_list_to_string = False
    if (
        not is_scalar(col_key)
        # when len(item) == len(row_key AND/OR col_key), native pandas has enough item values to assign
        and len(item) != len(col_key)
        and len(item) != len(row_key)
    ):
        # when col_key and item are lists of unequal length, pandas raises error if
        # 1. the length of item and col_key do not match when col_key length > 1
        # 2. the length of item and row_key do not match when col_key length = 1
        # Snowpark pandas works with such input - given a list of items, they are assigned to row-col combinations
        # based on the row_key and col_key order. For a given df,
        # >>> snow_df
        #    A  B  C
        # x  1  2  3
        # y  4  5  6
        # z  7  8  9
        # >>> snow_df.loc[["x", "z"], ["B"]] = [99, 98, 97, 96]
        # >>> snow_df
        #    A   B  C
        # x  1  99  3
        # y  4   5  6
        # z  7  98  9
        # The column keys ["B"] and "B" produce same behavior in Snowpark pandas.
        # Convert the native pandas row/col key to one that behaves like Snowpark pandas does (ignore extra values).
        native_item = item
        if len(col_key) == 1:
            # Truncate the item to be as long as the row key or insert the last value in item to make it as long at the
            # row key - Snowpark pandas fills in empty spots in item (if len(item) < len(row_key)) with the last
            # element in item.
            native_item = (
                item[: len(row_key)]
                if len(row_key) <= len(item)
                else item + [item[-1]] * (len(row_key) - len(item))
            )
        elif len(col_key) > 1:
            # Truncate or elongate item based on col key.
            native_item = (
                item[: len(col_key)]
                if len(col_key) <= len(item)
                else item + [item[-1]] * (len(col_key) - len(item))
            )
        if col_key[0] in native_columns and len(item) > len(native_columns):
            # Native pandas treats col_key "B" and ["B"] differently, need to convert ["B"] to "B" for comparison.
            # Here, B is present in the df. ["Y"] works. This applies only for columns len(item) > len(df col).
            convert_list_to_string = True
    elif (
        # When a col_key list len(col_key) == 0 which is a label present in df.columns, it can only be assigned an item
        # of length 1.
        # TODO: SNOW-1008469 write this test to support list-like col_key longer than 1 and document behavior.
        not is_scalar(col_key)
        and col_key[0] in native_columns
        and len(item) != 1
    ):
        # pandas raises error for this case when "A" is an existing column, e.g., df[["x","z"], ["A"]] = [99,98]. If we
        # change "A" to "X" a new label, then it will work. Snowpark pandas will keep handling it as row wise case.
        with pytest.raises(ValueError):
            loc_set_helper(native_df)
        # change ["A"] to "A" will make both work
        col_key = col_key[0]
    elif (
        is_scalar(col_key)
        and not (col_key in native_columns and len(item) == 1)
        and not (
            item_type_name in ["array", "index"] and len(item) == 1
        )  # e.g., col_key = 'X', item = [999], item_type_name = 'array' works
        and len(item) != len(row_key)
    ):
        # CASE: scalar col_key raises ValueError in native pandas if there is a mismatch in row and item length.
        # Native pandas only supports using a new scalar label/col_key (one that is not in the df column list,
        # e.g., "X", 2, "abc") if the number of items being assigned is equal to the length of the row key. If an item
        # whose length is not equal to the row key length, a ValueError is raised.
        #
        # Snowpark pandas does not check length of item and will pass regardless of whether the row_key and item are
        # of the same length. Snowpark pandas uses the item data available and fills extra spots with NaN
        # if len(row_key) > len(item). If len(row_key) < len(item), the extra item values are skipped.
        # >>> snow_df
        #    A  B  C
        # x  1  2  3
        # y  4  5  6
        # z  7  8  9
        # >>> snow_df.loc[["x", "z"], "B"] = [99, 98, 97, 96]
        # >>> snow_df
        #    A   B  C
        # x  1  99  3
        # y  4   5  6
        # z  7  98  9
        # The column keys ["B"] and "B" produce same behavior in Snowpark pandas.
        # Verify that native pandas raises ValueError. There are two types of error messages raised based on item type.
        if item_type_name in ["list", "tuple"] and not (
            col_key in native_columns and len(item) > len(row_key)
        ):
            err_msg = "Must have equal len keys and value when setting with an iterable"
        else:  # array, index
            # E.g., for col_key = 'X', item = [99, 98, 97, 96], full error message:
            # Error could be one of two messages depending on the input:
            #   'shape mismatch: value array of shape (4,) could not be broadcast to indexing result of shape (2,)'
            # or
            #   'setting an array element with a sequence.'
            # We just look for "array" to make things simple since they are both ValueErrors
            err_msg = " array "
        with pytest.raises(ValueError, match=err_msg):
            loc_set_helper(native_df)

        # Truncate the item to be as long as the row key or fill in empty spots with the last element to make item as
        # long as the row key.
        native_item = item[: len(row_key)]
        if len(native_item) < len(row_key):
            native_item = native_item + [native_item[-1]] * (
                len(row_key) - len(native_item)
            )

    expected_join_count = 3 if len(item) > 1 else 2
    # 4 extra queries for index, 1 for converting to native pandas in loc_set_helper, 2 for iter and 1 for tolist
    with SqlCounter(
        query_count=5 if item_type_name == "index" else 1,
        join_count=expected_join_count,
    ):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            loc_set_helper,
            inplace=True,
        )


@pytest.mark.parametrize(
    "col_key",
    [
        ["A", "B"],
        ["B", "A", "C"],
        ["A", "X"],
        ["X", "A"],
        ["X", "Y"],
        ["B", "X", "A", "C"],
    ],
)
@pytest.mark.parametrize(
    "item",
    [
        [999],
        [99, 98],
        [99, 98, 97, 96],
    ],
)
@pytest.mark.parametrize("item_type_name,item_to_type", ITEM_TYPE_LIST_CONVERSION)
def test_df_loc_set_with_row_wise_list_like_item(
    col_key, item, item_type_name, item_to_type
):
    data = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    native_columns = ["A", "B", "C"]
    native_index = ["x", "y", "z"]
    row_key = ["x", "z"]

    native_df = native_pd.DataFrame(data, columns=native_columns, index=native_index)
    snow_df = pd.DataFrame(native_df)
    native_item = item

    def loc_set_helper(df):
        if isinstance(df, pd.DataFrame):
            new_item = item_to_type(item)
            df.loc[row_key, col_key] = new_item
        else:
            new_item = item_to_type(native_item)
            df.loc[row_key, col_key] = try_convert_index_to_native(new_item)

    # Native pandas has different error messages depending on whether a column not present in the df is used in the
    # column key.
    if "X" in col_key and len(item) != len(col_key):
        err_msg = "Must have equal len keys and value when setting with an iterable"
        if len(item) > 1:
            # When col_key is list and item's length > 1 or new label exists, both native pandas and Snowpark pandas
            # raises error if the length of item and col_key do not match when col_key length > 1
            # 4 extra queries for index, 1 for converting to native pandas in loc_set_helper, 2 for iter and 1 for tolist
            with SqlCounter(
                query_count=4 if item_type_name == "index" else 0, join_count=0
            ):
                eval_snowpark_pandas_result(
                    snow_df,
                    native_df,
                    loc_set_helper,
                    inplace=True,
                    expect_exception=True,
                    expect_exception_match=err_msg,
                )
        else:
            # Only native pandas raises an error if a column not present in df is used when item and column key lengths
            # don't match.
            with pytest.raises(ValueError, match=err_msg):
                native_df.loc[row_key, col_key] = try_convert_index_to_native(
                    item_to_type(item)
                )
            # Change item so that native pandas result matches expected Snowpark pandas result.
            native_item = (
                item[: len(col_key)]
                if len(col_key) <= len(item)
                else item + ([item[-1]] * (len(col_key) - len(item)))
            )
            # 4 extra queries for index, 1 for converting to native pandas in loc_set_helper, 2 for iter and 1 for tolist
            with SqlCounter(
                query_count=5 if item_type_name == "index" else 1, join_count=2
            ):
                eval_snowpark_pandas_result(
                    snow_df, native_df, loc_set_helper, inplace=True
                )

    elif len(item) > 1 and len(item) != len(col_key):
        # When col_key is list and item's length > 1 or new label exists, both native pandas and Snowpark pandas raises
        # error if the length of item and col_key do not match when col_key length > 1
        # Could be one of two error messages:
        # ValueError: setting an array element with a sequence.
        # ValueError: shape mismatch: value array of shape (4,) could not be broadcast to indexing result of shape (2,3)
        native_err_msg = re.escape("array")
        with pytest.raises(ValueError, match=native_err_msg):
            native_df.loc[row_key, col_key] = try_convert_index_to_native(
                item_to_type(item)
            )
        # 3 extra queries for index, 2 for iter and 1 for tolist
        with SqlCounter(
            query_count=3 if item_type_name == "index" else 0, join_count=0
        ):
            snowpark_err_msg = (
                "Must have equal len keys and value when setting with an iterable"
            )
            with pytest.raises(ValueError, match=snowpark_err_msg):
                snow_df.loc[row_key, col_key] = item_to_type(item)
                assert_frame_equal(snow_df, native_df)  # to trigger computation

    else:
        # Both Snowpark pandas and Native pandas should have same non-error behavior.
        # 4 extra queries for index, 1 for converting to native pandas in loc_set_helper, 2 for iter and 1 for tolist
        with SqlCounter(
            query_count=5 if item_type_name == "index" else 1, join_count=2
        ):
            eval_snowpark_pandas_result(
                snow_df, native_df, loc_set_helper, inplace=True
            )


def test_df_loc_set_columns_with_boolean_series_optimized():
    data = {"a": [1, 2, 3], "b": [6, 5, 4], "c": [7, 8, 8]}
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)

    def helper(df):
        df.loc[df["a"] != 1, "b"] = 10

    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(snow_df, native_df, helper, inplace=True)

    def helper(df):
        df.loc[df["a"] != 1, "b"] = df["c"]

    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(snow_df, native_df, helper, inplace=True)


@sql_count_checker(query_count=1, join_count=1)
def test_df_loc_set_boolean_series_with_non_default_index_key_and_scalar_item():
    native_df = native_pd.DataFrame([1, 2, 3], columns=["A"])
    snow_df = pd.DataFrame(native_df)

    # Series key with non-default index
    row_key = native_pd.Series([False, True, True, False], index=[0, 1, 6, 2])

    native_df.loc[row_key, "A"] = 99
    snow_df.loc[pd.Series(row_key), "A"] = 99
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, native_df)


@pytest.mark.parametrize(
    "index, columns, item",
    [
        [[1, 1], "x", ["abc", "xyz"]],  # existing column
        [[1, 1], "y", ["abc", "xyz"]],  # new column
        [
            [1, 1],
            "x",
            native_pd.Series(["abc", "xyz"], index=[1, 1]),
        ],  # series, existing column
        [
            [1, 1],
            "y",
            native_pd.Series(["abc", "xyz"], index=[1, 1]),
        ],  # series, new column
        [
            [1, 1],
            ["x"],
            native_pd.DataFrame({"x": ["abc", "xyz"]}, index=[1, 1]),
        ],  # df, existing column
        [
            [1, 1],
            ["y"],
            native_pd.DataFrame({"x": ["abc", "xyz"]}, index=[1, 1]),
        ],  # df, new column
    ],
)
@pytest.mark.parametrize(
    "self_index_type, self_index_val",
    [
        ["unique", list(range(4))],
        ["duplicate", [1, 1, 2, 3]],
    ],
)
@sql_count_checker(query_count=1, join_count=3)
def test_df_loc_set_duplicate_index(
    self_index_type, self_index_val, index, columns, item
):
    data = {"x": ["a", "b", "c", "d"]}
    snow_df = pd.DataFrame(data, index=self_index_val)
    native_df = native_pd.DataFrame(data, index=self_index_val)

    def helper(df):
        if isinstance(df, pd.DataFrame):
            if isinstance(item, native_pd.DataFrame):
                item_ = pd.DataFrame(item)
            elif isinstance(item, native_pd.Series):
                item_ = pd.Series(item)
            else:
                item_ = item
            df.loc[index, columns] = item_
        else:
            df.loc[index, columns] = item
            if isinstance(item, list) and (index, columns, item) == (
                [1, 1],
                "y",
                ["abc", "xyz"],
            ):
                # Due to a pandas bug introduced in 2.1.2, missing string values are now the
                # string value "nan" rather than numeric np.nan.
                # https://github.com/pandas-dev/pandas/issues/56379
                assert "nan" in df["y"].unique()
                df.replace({"nan": None}, inplace=True)

    if self_index_type == "unique":
        eval_snowpark_pandas_result(snow_df, native_df, helper, inplace=True)
    else:
        # pandas only works where item is dataframe and the column is new; otherwise, it raises errors.
        if isinstance(item, native_pd.DataFrame) and columns == ["y"]:
            eval_snowpark_pandas_result(snow_df, native_df, helper, inplace=True)
        else:
            with pytest.raises(ValueError):
                helper(native_df)
            # Snowpark pandas always work as expected
            helper(snow_df)
            native_df.loc[1, columns] = "xyz"
            if isinstance(item, list) and (index, columns, item) == (
                [1, 1],
                "y",
                ["abc", "xyz"],
            ):
                # Due to a pandas bug introduced in 2.1.2, missing string values are now the
                # string value "nan" rather than numeric np.nan.
                # https://github.com/pandas-dev/pandas/issues/56379
                assert "nan" in native_df["y"].unique()
                native_df.replace({"nan": None}, inplace=True)
            assert_snowpark_pandas_equal_to_pandas(snow_df, native_df)


@pytest.mark.parametrize(
    "index, columns, item",
    [
        [slice(None), ["x"], native_pd.DataFrame({"x": ["abc", "xyz"]}, index=[1, 1])],
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_df_loc_set_duplicate_index_negative(index, columns, item):
    data = {"x": ["a", "b", "c", "d"]}
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)

    def helper(df):
        if isinstance(df, pd.DataFrame):
            if isinstance(item, native_pd.DataFrame):
                item_ = pd.DataFrame(item)
            elif isinstance(item, native_pd.Series):
                item_ = pd.Series(item)
            else:
                item_ = item
            df.loc[index, columns] = item_
        else:
            df.loc[index, columns] = item

    # pandas raise error when duplicate index in rhs item
    with pytest.raises(ValueError):
        helper(native_df)
    # Snowpark pandas perform a left join behavior which leads to more rows
    helper(snow_df)
    assert snow_df.index.to_list() == [0, 1, 1, 2, 3]


@pytest.mark.parametrize(
    "indexer",
    [
        (slice(None), ["A", "B"]),
        (slice(None), ["B", "A"]),
        (["z", "w"], slice(None)),
        (["w", "z"], slice(None)),
        (["z", "w"], ["A", "B"]),
        (["w", "z"], ["B", "A"]),
        (slice(None), slice(None)),
        (slice(None), native_pd.Series(["A", "B"])),
        (slice(None), native_pd.Series(["B", "A"], index=["A", "B"])),
    ],
)
@pytest.mark.parametrize("item_type", ["numpy_array", "native_list"])
def test_df_loc_set_item_2d_array(indexer, item_type):
    from math import prod

    query_count = 1
    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "x", "z", "w"],
        columns=["A", "B", "C", "D"],
    )

    snow_indexers = []
    for i in indexer:
        if isinstance(i, native_pd.Series):
            query_count += 1
            snow_indexers.append(pd.Series(i))
        else:
            snow_indexers.append(i)
    snow_indexers = tuple(snow_indexers)

    snow_df = pd.DataFrame(native_df)

    # Rather than re-shaping a NumPy array to ensure we get the correct types, we can just
    # use pandas' loc behavior to get the right shape.
    item = np.arange(prod(native_df.loc[indexer].shape)).reshape(
        native_df.loc[indexer].shape
    )

    if item_type == "native_list":
        item = [list(i) for i in item]

    def loc_set_helper(df):
        if isinstance(df, native_pd.DataFrame):
            df.loc[indexer] = item
        else:
            df.loc[snow_indexers] = item

    expected_join_count = 3
    if isinstance(indexer[0], slice):
        expected_join_count = 1

    with SqlCounter(query_count=query_count, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            loc_set_helper,
            inplace=True,
        )


@sql_count_checker(query_count=0)
def test_df_loc_set_scalar_indexer_2d_array_negative():
    # pandas error: ValueError: setting an array element with a sequence.
    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "x", "z", "w"],
        columns=["A", "B", "C", "D"],
    )

    def loc_set_helper(df):
        df.loc["z", "A"] = [[1]]

    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        loc_set_helper,
        expect_exception=True,
        expect_exception_type=ValueError,
        inplace=True,
        assert_exception_equal=False,
        expect_exception_match="Scalar indexer incompatible with list item",
    )

    def loc_set_helper(df):
        df.loc["z", "A"] = [[1, 2, 3, 4]]

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        loc_set_helper,
        expect_exception=True,
        expect_exception_type=ValueError,
        inplace=True,
        assert_exception_equal=False,
        expect_exception_match="Scalar indexer incompatible with list item",
    )


@sql_count_checker(query_count=0)
def test_df_loc_set_item_2d_array_scalar_row_loc_negative():
    # Test when scalar row loc is duplicated.
    snow_df = pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "x", "z", "w"],
        columns=["A", "B", "C", "D"],
    )
    val = np.array([[1, 2, 3, 4], [5, 6, 7, 8]])
    # This succeeds in pandas, but we fail.
    with pytest.raises(
        ValueError,
        match="Scalar indexer incompatible with ndarray item",
    ):
        snow_df.loc["x", :] = val

    # Test when single scalar row loc
    # This fails in pandas as well, but with a different error.
    with pytest.raises(
        ValueError,
        match="Scalar indexer incompatible with ndarray item",
    ):
        snow_df.loc["w", :] = val


def test_df_loc_set_item_2d_array_row_length_no_match():
    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "y", "z", "w"],
        columns=["A", "B", "C", "D"],
    )

    snow_df = pd.DataFrame(native_df)
    val = np.array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]])

    # When there are too many rows
    with pytest.raises(
        ValueError,
        match=r"setting an array element with a sequence.",
    ):
        native_df.loc[["x", "y"], :] = val

    def loc_helper(df):
        if isinstance(df, native_pd.DataFrame):
            df.loc[["z", "y"], :] = val[:-1]
        else:
            df.loc[["z", "y"], :] = val

    with SqlCounter(query_count=1, join_count=3):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            loc_helper,
            inplace=True,
        )

    # When there is exactly one row (pandas will broadcast, we ffill).
    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "y", "z", "w"],
        columns=["A", "B", "C", "D"],
    )

    snow_df = pd.DataFrame(native_df)

    def loc_helper(df):
        df.loc[["x", "y"], :] = val[:-2]

    with SqlCounter(query_count=1, join_count=3):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            loc_helper,
            inplace=True,
        )

    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "y", "z", "w"],
        columns=["A", "B", "C", "D"],
    )

    snow_df = pd.DataFrame(native_df)

    def loc_helper(df):
        if isinstance(df, native_pd.DataFrame):
            df.loc[["x", "y", "z", "w"], :] = [
                list(val[0]),
                list(val[1]),
                list(val[1]),
                list(val[1]),
            ]
        else:
            snow_df.loc[["x", "y", "z", "w"], :] = val[:2]

    with SqlCounter(query_count=1, join_count=3):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            loc_helper,
            inplace=True,
        )


def test_df_loc_set_item_2d_array_col_length_no_match():
    # pandas error message:
    # ValueError: shape mismatch: value array of shape <VALUE_SHAPE> could not
    # be broadcast to indexing result of shape <INDEXED_SELF_SHAPE>
    # Snowpark pandas error message:
    # ValueError: shape mismatch: the number of columns <NUM_VALUE_COLS> from the item
    # does not match with the number of columns <NUM_INDEXED_SELF_COLS> to set
    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "y", "z", "w"],
        columns=["A", "B", "C", "D"],
    )

    snow_df = pd.DataFrame(native_df)
    val = np.array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]])

    # When there are too few cols
    def loc_helper(df):
        df.loc[["x", "y"], :] = val[:, :-1]

    with SqlCounter(query_count=0):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            loc_helper,
            inplace=True,
            expect_exception=True,
            expect_exception_type=ValueError,
            assert_exception_equal=False,
            expect_exception_match="shape mismatch: the number of columns 3 from the item does not match with the number of columns 4 to set",
            # Our error message is slightly different from pandas.
        )

    # When there are too many cols
    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "y", "z", "w"],
        columns=["A", "B", "C", "D"],
    )

    snow_df = pd.DataFrame(native_df)

    def loc_helper(df):
        df.loc[["x", "y"], :] = np.hstack((val, val[:, :1]))

    with SqlCounter(query_count=0):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            loc_helper,
            inplace=True,
            assert_exception_equal=False,
            expect_exception=True,
            expect_exception_type=ValueError,
            expect_exception_match="shape mismatch: the number of columns 5 from the item does not match with the number of columns 4 to set",
            # Our error message is slightly different from pandas.
        )


@sql_count_checker(query_count=1, join_count=1)
def test_df_loc_set_2d_array_with_explicit_na_values():
    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "y", "z", "w"],
        columns=["A", "B", "C", "D"],
    )

    snow_df = pd.DataFrame(native_df)
    val = np.array(
        [
            [1, np.nan, 3, 4],
            [5, np.nan, 7, 8],
            [9, None, np.nan, 12],
            [np.nan, None, np.nan, 15],
        ]
    )

    def loc_helper(df):
        df.loc[:, :] = val

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        loc_helper,
        inplace=True,
    )


@sql_count_checker(query_count=1, join_count=1)
def test_df_loc_set_2d_array_with_ffill_na_values_negative():
    # Ideally, we want NA values to be propagated if they are the last
    # value present, but we currently do not support this.
    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "y", "z", "w"],
        columns=["A", "B", "C", "D"],
    )

    snow_df = pd.DataFrame(native_df)
    val = np.array([[1, 2, 3, 4], [5, np.nan, 7, 8]])
    ffilled_vals = np.array(
        [[1, 2, 3, 4], [5, np.nan, 7, 8], [5, 2, 7, 8], [5, 2, 7, 8]]
    )

    def loc_helper(df):
        if isinstance(df, native_pd.DataFrame):
            # This is what it would be if our ffilling would correctly propagate NA values.
            # df.loc[:, :] = [list(val[0]), list(val[1]), list(val[1]), list(val[1])]
            # Instead, our ffill value picks the most recent *non-NA* value
            df.loc[:, :] = ffilled_vals
        else:
            df.loc[:, :] = val

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        loc_helper,
        inplace=True,
    )


@pytest.mark.parametrize("key", EMPTY_LIST_LIKE_VALUES)
@pytest.mark.parametrize("item", EMPTY_LIST_LIKE_VALUES)
@sql_count_checker(query_count=0)
def test_df_loc_set_with_empty_key_and_empty_item_negative(
    key,
    item,
    default_index_snowpark_pandas_df,
    default_index_native_df,
):
    # df.loc[empty list-like/series key] = empty list-like/series item
    # ----------------------------------------------------------------
    # Both native pandas and Snowpark pandas fail because the key and value are of different lengths.
    # Both raise ValueErrors with different error messages.
    # Snowpark pandas Exception: "The length of the value/item to set is empty"
    # Native pandas Exception: "Must have equal len keys and value when setting with an iterable"

    def loc_set_helper(df):
        if isinstance(key, native_pd.Series) and isinstance(df, pd.DataFrame):
            df.loc[pd.Series(key)] = item
        else:
            df.loc[try_convert_index_to_native(key)] = try_convert_index_to_native(item)

    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_df,
        default_index_native_df,
        loc_set_helper,
        expect_exception=True,
        assert_exception_equal=False,
        inplace=True,
    )


@pytest.mark.parametrize("key", EMPTY_LIST_LIKE_VALUES)
def test_series_loc_set_with_empty_key_and_scalar_item(
    key,
    default_index_snowpark_pandas_df,
    default_index_native_df,
    simple_snowpark_pandas_df,
    simple_native_pandas_df,
):
    # df.loc[empty list-like/series key] = scalar item
    # ------------------------------------------------
    # In native pandas, there is no change to the df. Snowpark pandas mirrors this behavior in most cases.

    item = 32  # any scalar

    def loc_set_helper(df):
        if isinstance(key, native_pd.Series) and isinstance(df, pd.DataFrame):
            df.loc[pd.Series(key)] = item
        else:
            df.loc[try_convert_index_to_native(key)] = item

    # CASE 1: type of Snowflake column matches item type:
    # The df should not change.
    with SqlCounter(query_count=1, join_count=2):
        eval_snowpark_pandas_result(
            simple_snowpark_pandas_df,
            simple_native_pandas_df,
            loc_set_helper,
            inplace=True,
        )

    # CASE 2: Snowflake SQL Exception: type of Snowflake column does not match item type:
    # In the case of Snowpark pandas, two columns in the df used (`default_index_snowpark_pandas_df`) are columns of
    # `ARRAY` type in Snowflake. The item being set to this column is an int, there is a type mismatch. Therefore,
    # Snowflake raises the error:
    # SnowparkSQLException: SQL compilation error: Can not convert parameter '32' of type [NUMBER(2,0)] into expected
    # type [ARRAY]

    def loc_set_helper(df):
        if isinstance(key, native_pd.Series) and isinstance(df, pd.DataFrame):
            df.loc[pd.Series(key)] = item
        else:
            df.loc[key] = item

    # Using pytest raises since the eval method cannot verify exceptions if they differ from native pandas.
    err_msg = "Can not convert parameter"
    with SqlCounter(query_count=0, join_count=0):
        with pytest.raises(SnowparkSQLException, match=err_msg):
            eval_snowpark_pandas_result(
                default_index_snowpark_pandas_df,
                default_index_native_df,
                loc_set_helper,
                inplace=True,
            )


@pytest.mark.parametrize("key", EMPTY_LIST_LIKE_VALUES)
def test_df_loc_set_with_empty_key_and_list_like_item(
    key,
    simple_snowpark_pandas_df,
    simple_native_pandas_df,
):
    # df.loc[empty list-like/series key] = list-like item
    # ---------------------------------------------------
    # Snowpark pandas and Native pandas have the same behavior -- nothing in the df changes.

    item = pd.Index([random.randint(0, 4) for _ in range(4)])

    def loc_set_helper(df):
        _key = key
        if isinstance(df, pd.DataFrame):
            _key = pd.Series(key) if isinstance(key, native_pd.Series) else key
            df.loc[_key] = item
        else:
            _key = try_convert_index_to_native(_key)
            df.loc[_key] = try_convert_index_to_native(item)

    # 4 extra queries, 1 for converting to native pandas in loc_set_helper, 2 for iter and 1 for tolist
    with SqlCounter(query_count=5, join_count=2):
        eval_snowpark_pandas_result(
            simple_snowpark_pandas_df,
            simple_native_pandas_df,
            loc_set_helper,
            inplace=True,
        )


@pytest.mark.parametrize("key", EMPTY_LIST_LIKE_VALUES)
def test_df_loc_set_with_empty_key_and_series_item_negative(
    key,
    simple_snowpark_pandas_df,
    simple_native_pandas_df,
):
    # df.loc[empty list-like/series key] = series item
    # ------------------------------------------------
    # In native pandas, the df does not change.
    # In Snowpark pandas, ValueError: "shape mismatch: the number of columns 1 from the item does not match with the
    # number of columns 4 to set"

    item = native_pd.Series([random.randint(0, 4) for _ in range(4)])

    def loc_set_helper(df):
        _key, _item = key, item
        if isinstance(df, pd.DataFrame):
            _key = pd.Series(key) if isinstance(key, native_pd.Series) else key
            _item = pd.Series(item) if isinstance(item, native_pd.Series) else item
        else:
            _key = try_convert_index_to_native(key)
            _item = try_convert_index_to_native(item)
        df.loc[_key] = _item

    with SqlCounter(query_count=0, join_count=0):
        err_msg = "setting an array element with a sequence."
        with pytest.raises(ValueError, match=err_msg):
            eval_snowpark_pandas_result(
                simple_snowpark_pandas_df,
                simple_native_pandas_df,
                loc_set_helper,
                inplace=True,
            )


@pytest.mark.parametrize("key", [True, False, 0, 1])
@pytest.mark.parametrize(
    "index",
    [
        [0, 1, True, False, "x"],
        [0, 1, True, "x"],
        [1, True, False, "x"],
        [1, True, "x"],
        [0, 1, False, "x"],
        [0, False, "x"],
        [2, "x"],
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_df_setitem_boolean_key(key, index):
    item = 99
    num_columns = 3
    data = {str(chr(ord("a") + i)): list(range(len(index))) for i in range(num_columns)}
    native_df = native_pd.DataFrame(data, index=index)
    snow_df = pd.DataFrame(native_df)

    snow_df.loc[key] = item

    # In pandas, setitem for 0/False and 1/True will potentially set multiple values or fail to set at all if neither
    # keys already exist in the index.  In Snowpark pandas we treat 0 & False, and 1 & True as distinct values, so
    # they are independently settable, whether they exist or do not already exist in the series index.
    try:
        key_index = [str(v) for v in native_df.index].index(str(key))
        native_df.iloc[key_index] = item
    except ValueError:
        native_df = native_pd.concat(
            [
                native_df,
                native_pd.DataFrame(
                    [item for i in range(len(native_df.columns))],
                    index=native_df.columns.tolist(),
                    columns=[key],
                ).T,
            ]
        )

    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, native_df)


@sql_count_checker(query_count=4)
@pytest.mark.parametrize(
    "ops",
    [
        lambda df: df.loc["2013"],
        lambda df: df["2013-1-15":"2013-1-15 12:30:00"],
    ],
)
def test_df_partial_string_indexing(ops):
    native_df = native_pd.DataFrame(
        np.random.randn(100000, 1),
        columns=["A"],
        index=native_pd.date_range("20130101", periods=100000, freq="min"),
    )

    snowpark_df = pd.DataFrame(native_df)

    # need to set check_freq=False since Snowpark pandas index's freq is always null
    eval_snowpark_pandas_result(snowpark_df, native_df, ops, check_freq=False)


@sql_count_checker(query_count=1)
def test_df_partial_string_indexing_with_timezone():
    native_df = native_pd.DataFrame(
        [0], index=native_pd.DatetimeIndex(["2019-01-01"], tz="America/Los_Angeles")
    )

    snowpark_df = pd.DataFrame(native_df)

    # need to set check_freq=False since Snowpark pandas index's freq is always null
    eval_snowpark_pandas_result(
        snowpark_df,
        native_df,
        lambda df: df["2019-01-01 12:00:00+04:00":"2019-01-01 13:00:00+04:00"],
        check_freq=False,
    )


@sql_count_checker(query_count=1)
def test_df_single_value_with_slice_key():
    native_df = native_pd.DataFrame([0], index=[0])

    snowpark_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(snowpark_df, native_df, lambda df: df.loc[0:1])


@sql_count_checker(query_count=1, join_count=1)
def test_df_loc_set_none():
    native_df = native_pd.DataFrame({"a": [1, 2, 3]})

    def loc_set_helper(df):
        df.loc[None, "a"] = 100

    # pandas raises KeyError where loc with None on a dataframe, but works well on a series, see
    # test_series_loc_set_none()
    with pytest.raises(KeyError, match="None"):
        loc_set_helper(native_df)

    # Snowpark pandas will do the row enlargement correctly
    df = pd.DataFrame(native_df)
    loc_set_helper(df)

    assert_frame_equal(
        df,
        native_pd.DataFrame({"a": [1, 2, 3, 100]}, index=[0, 1, 2, None]),
        check_dtype=False,
    )


@sql_count_checker(query_count=1, join_count=3)
def test_df_loc_set_with_index_and_column_labels():
    """
    Create a DataFrame using 3 Series objects and perform loc set with a scalar.
    2 joins are performed since the concat() operation is used to concat the three Series
    into one DataFrame object, 1 join from loc set operation.
    """

    def loc_set_helper(df):
        df.loc["a", "three"] = 1.0

    series1 = native_pd.Series(np.random.randn(3), index=["a", "b", "c"])
    series2 = native_pd.Series(np.random.randn(4), index=["a", "b", "c", "d"])
    series3 = native_pd.Series(np.random.randn(3), index=["b", "c", "d"])

    native_df = native_pd.DataFrame(
        {
            "one": series1,
            "two": series2,
            "three": series3,
        }
    )
    snow_df = pd.DataFrame(
        {
            "one": pd.Series(series1),
            "two": pd.Series(series2),
            "three": pd.Series(series3),
        }
    )
    eval_snowpark_pandas_result(snow_df, native_df, loc_set_helper, inplace=True)


@sql_count_checker(query_count=0)
def test_raise_set_cell_with_list_like_value_error():
    s = pd.Series([[1, 2], [3, 4]])
    with pytest.raises(NotImplementedError):
        s.loc[0] = [0, 0]
    with pytest.raises(NotImplementedError):
        s.to_frame().loc[0, 0] = [0, 0]


@pytest.mark.parametrize(
    "key, query_count, join_count",
    [
        pytest.param(
            "1 day",
            2,
            4,
            marks=pytest.mark.xfail(
                reason="SNOW-1652608 result series name incorrectly set"
            ),
        ),  # 1 join from df creation, 1 join from squeeze, 2 joins from to_pandas during eval
        pytest.param(
            native_pd.to_timedelta("1 day"),
            2,
            4,
            marks=pytest.mark.xfail(
                reason="SNOW-1652608 result series name incorrectly set"
            ),
        ),  # 1 join fron df creation, 1 join from squeeze, 2 joins from to_pandas during eval
        (["1 day", "3 days"], 1, 1),
        ([True, False, False], 1, 1),
        (slice(None, "4 days"), 1, 0),
        (slice(None, "4 days", 2), 1, 0),
        (slice("1 day", "2 days"), 1, 0),
        (slice("1 day 1 hour", "2 days 2 hours", -1), 1, 0),
    ],
)
def test_df_loc_get_with_timedelta(key, query_count, join_count):
    data = {
        "A": [1, 2, 3],
        "B": [4, 5, 6],
        "C": [7, 8, 9],
    }
    idx = ["1 days", "2 days", "3 days"]
    native_df = native_pd.DataFrame(data, index=native_pd.to_timedelta(idx))
    snow_df = pd.DataFrame(data, index=pd.to_timedelta(idx))
    with SqlCounter(query_count=query_count, join_count=join_count):
        eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.loc[key])


@pytest.mark.parametrize(
    "key, expected_result",
    [
        (
            slice(None, "4 days"),
            native_pd.DataFrame(
                data={
                    "A": [1, 2, 3, 10],
                    "B": [4, 5, 6, 11],
                    "C": [7, 8, 9, 12],
                },
                index=native_pd.to_timedelta(
                    ["1 days", "2 days", "3 days", "25 hours"]
                ),
            ),
        ),
        (
            slice(None, "4 days", 2),
            native_pd.DataFrame(
                data={
                    "A": [1, 3],
                    "B": [4, 6],
                    "C": [7, 9],
                },
                index=native_pd.to_timedelta(["1 days", "3 days"]),
            ),
        ),
        (
            slice("1 day", "2 days"),
            native_pd.DataFrame(
                data={
                    "A": [1, 2],
                    "B": [4, 5],
                    "C": [7, 8],
                },
                index=native_pd.to_timedelta(["1 days", "2 days"]),
            ),
        ),
        (
            slice("1 day 1 hour", "2 days 2 hours", -1),
            native_pd.DataFrame(
                data={
                    "A": [10, 3],
                    "B": [11, 6],
                    "C": [12, 9],
                },
                index=native_pd.to_timedelta(["1 days 1 hour", "3 days"]),
            ),
        ),
    ],
)
@sql_count_checker(query_count=2)
def test_df_loc_get_with_timedelta_behavior_difference(key, expected_result):
    # In these test cases, native pandas raises a KeyError but Snowpark pandas works correctly.
    data = {
        "A": [1, 2, 3, 10],
        "B": [4, 5, 6, 11],
        "C": [7, 8, 9, 12],
    }
    idx = ["1 days", "2 days", "3 days", "25 hours"]
    native_df = native_pd.DataFrame(data, index=native_pd.to_timedelta(idx))
    snow_df = pd.DataFrame(data, index=pd.to_timedelta(idx))

    with pytest.raises(KeyError):
        # The error message is usually of the form KeyError: Timedelta('4 days 23:59:59.999999999').
        native_df.loc[key]

    actual_result = snow_df.loc[key]
    assert_frame_equal(actual_result, expected_result)


@sql_count_checker(query_count=3, join_count=1)
def test_df_loc_get_with_timedeltaindex_key():
    data = {
        "A": [1, 2, 3],
        "B": [4, 5, 6],
        "C": [7, 8, 9],
    }
    idx = ["1 days", "2 days", "3 days"]
    native_df = native_pd.DataFrame(data, index=native_pd.to_timedelta(idx))
    snow_df = pd.DataFrame(data, index=pd.to_timedelta(idx))
    key = ["1 days", "3 days"]
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.loc[
            native_pd.to_timedelta(key)
            if isinstance(df, native_pd.DataFrame)
            else pd.to_timedelta(key)
        ],
    )


@pytest.mark.xfail(reason="SNOW-1653219 None key does not work with timedelta index")
@sql_count_checker(query_count=2)
def test_df_loc_get_with_timedelta_and_none_key():
    data = {
        "A": [1, 2, 3],
        "B": [4, 5, 6],
        "C": [7, 8, 9],
    }
    idx = ["1 days", "2 days", "3 days"]
    snow_df = pd.DataFrame(data, index=pd.to_timedelta(idx))
    # Compare with an empty DataFrame, since native pandas raises a KeyError.
    expected_df = native_pd.DataFrame()
    assert_frame_equal(snow_df.loc[None], expected_df, check_column_type=False)


@sql_count_checker(query_count=2, join_count=4)
@pytest.mark.parametrize("index", [list("ABC"), [0, 1, 2]])
def test_df_loc_set_row_from_series(index):
    native_df = native_pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=list("ABC"))
    snow_df = pd.DataFrame(native_df)

    def locset(df):
        series = (
            pd.Series([1, 4, 9], index=index)
            if isinstance(df, pd.DataFrame)
            else native_pd.Series([1, 4, 9], index=index)
        )
        df.loc[1] = series
        return df

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        locset,
    )


@pytest.mark.parametrize("row_obj", [[0, 1, 2], native_pd.Index([0, 1, 2])])
def test_df_loc_full_set_row_from_list_like(row_obj):
    native_df = native_pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=list("ABC"))
    snow_df = pd.DataFrame(native_df)

    def locset(df):
        obj = (
            row_obj
            if isinstance(df, native_pd.DataFrame) or isinstance(row_obj, list)
            else pd.Index([0, 1, 2])
        )
        df.loc[:] = obj
        return df

    query_count = 1 if isinstance(row_obj, list) else 4
    with SqlCounter(query_count=query_count):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            locset,
        )


@pytest.mark.xfail(reason="SNOW-1709762: Need to fix boolean indexing")
@sql_count_checker(query_count=2, join_count=1)
def test_df_loc_full_set_row_from_series_using_series_column_key():
    native_df = native_pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=list("ABC"))
    snow_df = pd.DataFrame(native_df)

    def locset(df):
        key = native_pd.Series([True, False, True], index=list("ABC"))
        obj = (
            native_pd.Series([1, 3, 5])
            if isinstance(df, native_pd.DataFrame)
            else pd.Series([1, 3, 5])
        )
        if isinstance(df, pd.DataFrame):
            key = pd.Series(key)
        df.loc[:, key] = obj
        return df

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        locset,
    )


@sql_count_checker(query_count=2, join_count=1)
@pytest.mark.parametrize(
    "index, expected_result",
    [
        ([3, 4, 5], native_pd.DataFrame([[None] * 3] * 2)),
        ([0, 1, 2], native_pd.DataFrame([[1, 4, 9]] * 2)),
    ],
)
def test_df_loc_full_set_row_from_series_pandas_errors_default_columns(
    index, expected_result
):
    native_df = native_pd.DataFrame([[1, 2, 3], [4, 5, 6]])
    snow_df = pd.DataFrame(native_df)

    with pytest.raises(ValueError, match="setting an array element with a sequence."):
        native_df.loc[:] = native_pd.Series([1, 4, 9], index=index)

    snow_df.loc[:] = pd.Series([1, 4, 9], index=index)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, expected_result)


@sql_count_checker(query_count=2, join_count=1)
@pytest.mark.parametrize("series_index", [list("ABC"), list("ABC")[::-1]])
def test_df_loc_full_set_row_from_series_pandas_errors_string_columns(series_index):
    native_df = native_pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=list("ABC"))
    snow_df = pd.DataFrame(native_df)

    with pytest.raises(ValueError, match="setting an array element with a sequence."):
        native_df.loc[:] = native_pd.Series([1, 4, 9], index=series_index)

    snow_df.loc[:] = pd.Series([1, 4, 9], index=series_index)
    if series_index == list("ABC"):
        expected_result = native_pd.DataFrame([[1, 4, 9]] * 2, columns=list("ABC"))
    else:
        expected_result = native_pd.DataFrame(
            [[1, 4, 9][::-1]] * 2, columns=list("ABC")
        )

    assert_snowpark_pandas_equal_to_pandas(snow_df, expected_result)


@sql_count_checker(query_count=0)
def test_df_loc_invalid_key():
    # Bug fix: SNOW-1320674
    native_df = native_pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    snow_df = pd.DataFrame(native_df)

    def op(df):
        df["C"] = df["A"] / df["D"]

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        op,
        expect_exception=True,
        expect_exception_type=KeyError,
        expect_exception_match="D",
    )


@pytest.mark.parametrize(
    "key",
    [
        list("ABC"),
        list("CBA"),
    ],
)
@pytest.mark.parametrize("convert_key_to_series", [True, False])
@pytest.mark.parametrize("row_loc", [None, 0])
def test_df_loc_set_series_value(key, convert_key_to_series, row_loc):
    native_df = native_pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=list("ABC"))
    snow_df = pd.DataFrame(native_df)
    query_count = 2
    key_sorted = key == list("ABC")
    if row_loc is not None:
        if convert_key_to_series:
            join_count = 6
        else:
            join_count = 4
    else:
        if convert_key_to_series:
            join_count = 3
        else:
            join_count = 1

    if convert_key_to_series:
        query_count = 3
        snow_key = pd.Series(key)
        native_key = native_pd.Series(key)
    else:
        snow_key = native_key = key
    with SqlCounter(query_count=query_count, join_count=join_count):
        if row_loc is None:
            snow_df.loc[:, snow_key] = pd.Series([1, 4, 9], index=list("ABC"))
            # This is a bug in pandas. Issue filed here: https://github.com/pandas-dev/pandas/issues/59933
            with pytest.raises(
                ValueError, match="setting an array element with a sequence."
            ):
                native_df.loc[:, native_key] = native_pd.Series(
                    [1, 4, 9], index=list("ABC")
                )
            # We differ from pandas here because we ignore the index of the value when the key is a pandas object.
            if key_sorted or isinstance(snow_key, list):
                native_df = native_pd.DataFrame([[1, 4, 9]] * 2, columns=list("ABC"))
            else:
                native_df = native_pd.DataFrame(
                    [[1, 4, 9][::-1]] * 2, columns=list("ABC")
                )
        else:
            snow_df.loc[row_loc, snow_key] = pd.Series([1, 4, 9], index=list("ABC"))
            native_df.loc[row_loc, native_key] = native_pd.Series(
                [1, 4, 9], index=list("ABC")
            )
            # We differ from pandas here because we ignore the index of the value when the key is a pandas object.
            if not key_sorted and convert_key_to_series:
                native_df = native_pd.DataFrame(
                    [[9, 4, 1], [4, 5, 6]], columns=list("ABC")
                )

        assert_snowpark_pandas_equal_to_pandas(snow_df, native_df)


@pytest.mark.parametrize(
    "key",
    [
        slice("A", "B"),
        slice("C", "A", -1),
    ],
)
@pytest.mark.parametrize("row_loc", [None, 0])
def test_df_loc_set_series_value_slice_key(key, row_loc):
    native_df = native_pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=list("ABC"))
    snow_df = pd.DataFrame(native_df)
    query_count = 2
    if row_loc is not None:
        join_count = 4
    else:
        join_count = 1

    with SqlCounter(query_count=query_count, join_count=join_count):
        if row_loc is None:
            snow_df.loc[:, key] = pd.Series([1, 4, 9], index=list("ABC"))
            if key.start == "A":
                # The pandas bug does not apply to this codepath since we only set one column.
                # Instead, there is another difference from pandas behavior, where pandas
                # sets every value to NaN despite the labels matching.
                native_df = native_pd.DataFrame(
                    [[1, 4, 3], [1, 4, 6]], columns=list("ABC")
                )
            else:
                # This is a bug in pandas. Issue filed here: https://github.com/pandas-dev/pandas/issues/59933
                with pytest.raises(
                    ValueError, match="setting an array element with a sequence."
                ):
                    native_df.loc[:, key] = native_pd.Series(
                        [1, 4, 9], index=list("ABC")
                    )
                # Since the key is a slice, we rely on the index values of the Series item for label matching.
                native_df = native_pd.DataFrame([[1, 4, 9]] * 2, columns=list("ABC"))
        else:
            snow_df.loc[row_loc, key] = pd.Series([1, 4, 9], index=list("ABC"))
            native_df.loc[row_loc, key] = native_pd.Series([1, 4, 9], index=list("ABC"))
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, native_df)


@sql_count_checker(query_count=3)
def test_fix_1829928():
    vars = [
        -0.974507,
        0.407267,
        -0.035405,
        0.578839,
        0.286799,
        1.096326,
        -1.911032,
        0.583056,
        0.244446,
        0.118878,
    ]
    targets = [1, 0, 0, 1, 0, 1, 0, 1, 1, 0]
    native_df = native_pd.DataFrame(data={"Variable A": vars, "target": targets})

    df = pd.DataFrame(native_df)

    native_df.loc[:, "test"] = native_pd.qcut(
        native_df["Variable A"], 10, labels=False, duplicates="drop"
    )

    df.loc[:, "test"] = pd.qcut(df["Variable A"], 10, labels=False, duplicates="drop")

    assert_frame_equal(df, native_df, check_dtype=False)
