#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import functools
import numbers
import random

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from modin.pandas import Series
from pandas._libs.lib import is_bool, is_scalar
from pandas.errors import IndexingError

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin.extensions.utils import try_convert_index_to_native
from tests.integ.modin.frame.test_loc import (
    diff2native_negative_row_inputs,
    negative_snowpark_pandas_input_keys,
    row_inputs,
)
from tests.integ.modin.utils import (
    assert_series_equal,
    assert_snowpark_pandas_equal_to_pandas,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
    generate_a_random_permuted_list_exclude_self,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

EMPTY_LIST_LIKE_VALUES = [
    [],
    native_pd.Index([]),
    np.array([]),
    native_pd.Series([]),
]

SERIES_AND_LIST_LIKE_KEY_AND_ITEM_VALUES = [
    # series
    native_pd.Series([random.randint(0, 6) for _ in range(7)]),
    # list-like
    native_pd.Index([random.randint(0, 6) for _ in range(7)]),
]

# Values that are scalars or behave like scalar keys and items.
SCALAR_LIKE_VALUES = [0, "xyz", None, 3.14]

# Data types for row key and item values with loc set.
LIST_LIKE_AND_SERIES_DATA_TYPES = [
    "index",
    "ndarray",
    "index with name",
    "list",
    "series",
    "series with name",
    "series with non-default index",
]


@pytest.fixture
def convert_data_to_data_type():
    def converter(data, data_type):
        if "list" in data_type:
            return data
        elif "series" in data_type:
            if data_type == "series with non-default index":
                index = [
                    3,  # 3 does not exist in the default row_key index, so it will be skipped
                    2,
                    1,
                ]
                return native_pd.Series(data, index=index[: len(data)])
            elif data_type == "series with name":
                name = "xyz"  # name does not matter
                return native_pd.Series(data, name=name)
            else:
                return native_pd.Series(data)
        elif "index" in data_type:
            name = "random name"  # name does not matter
            if data_type == "index with name":
                return pd.Index(data, name=name)
            else:
                return pd.Index(data)
        elif data_type == "ndarray":
            return np.array(data)

    return converter


@pytest.fixture(params=[True, False])
def use_default_index(request):
    return request.param


@pytest.fixture(params=["series", "list", "array", "index"], scope="module")
def key_type(request):
    return request.param


@pytest.mark.parametrize(
    "key",
    row_inputs,
)
def test_series_loc_get_return_series(
    key, str_index_snowpark_pandas_series, str_index_native_series
):
    with SqlCounter(query_count=2 if is_scalar(key) else 1):
        eval_snowpark_pandas_result(
            str_index_snowpark_pandas_series,
            str_index_native_series,
            lambda df: df.loc[key],
        )


@pytest.mark.parametrize(
    "key,error_type,error_msg",
    diff2native_negative_row_inputs,
)
@sql_count_checker(query_count=0)
def test_series_loc_get_negative_diff2native(
    key, error_type, error_msg, str_index_snowpark_pandas_series
):
    with pytest.raises(
        error_type,
        match=error_msg,
    ):
        _ = str_index_snowpark_pandas_series.loc[key]


@pytest.mark.parametrize(
    "key",
    negative_snowpark_pandas_input_keys,
)
@sql_count_checker(query_count=0)
def test_series_loc_get_negative_snowpark_pandas_input(
    key,
    str_index_snowpark_pandas_series,
    negative_loc_snowpark_pandas_input_map,
    str_index_native_series,
):
    eval_snowpark_pandas_result(
        str_index_snowpark_pandas_series,
        str_index_native_series,
        lambda df: df.loc[negative_loc_snowpark_pandas_input_map[key][0]]
        if isinstance(df, Series)
        else df.loc[negative_loc_snowpark_pandas_input_map[key][1]],
        expect_exception=True,
    )


def _deterministic_standard_normal(size, seed=0):
    np.random.seed(0)
    return np.random.normal(size=size)


@pytest.mark.parametrize(
    "series,key",
    [
        (native_pd.Series([1, 2, 3]), 2),
        (native_pd.Series(["A", "B", "C", "D", "E"]), 3),
        (native_pd.Series(_deterministic_standard_normal(1000)), 458),
        (
            native_pd.Series(["A", "B", "C", "D", "E"], index=[5, 3, 2, 4, 1]),
            4,
        ),
        (native_pd.Series([5, 3, 2, 4, 1], index=["A", "B", "C", "D", "E"]), "D"),
        (
            native_pd.Series(
                list(range(5)),
                index=pd.MultiIndex.from_tuples(
                    list(zip(["A", "B", "C", "D", "E"], range(5)))
                ),
            ),
            random.choice(list(zip(["A", "B", "C", "D", "E"], range(5)))),
        ),
    ],
)
def test_series_loc_get_basic(series, key):
    query_count = 2

    # If the series is big it won't be inlined, so there is extra overhead in creating and dropping temporary tables
    # that we expect to happen for this test.
    expect_high_count = len(series) > 100
    high_count_reason = None
    if expect_high_count:
        query_count += 6
        high_count_reason = "SNOW-998609: Snowpark pandas Series wrapping another series results in duplicate queries"

    # returns single element for series
    with SqlCounter(
        query_count=query_count,
        high_count_expected=expect_high_count,
        high_count_reason=high_count_reason,
    ):
        assert pd.Series(series).loc[key] == series.loc[key]


@sql_count_checker(query_count=1, join_count=0)
def test_series_loc_get_all_rows():
    data = [1, 2, 3]
    columns = ["A"]

    snow_df = pd.DataFrame(data=data, columns=columns)
    native_df = native_pd.DataFrame(data=data, columns=columns)

    def apply_loc(df):
        df.loc[:, "A"] = 0
        return df

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: apply_loc(df),
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
def test_series_loc_get_key_bool_series_with_aligned_indices(key, use_default_index):
    # aligned indices means both row_pos and index are exactly match
    if use_default_index:
        index = None
    else:
        # index can have null values and duplicates
        index = native_pd.Index(["a", "a", None, "b", "b"], name="index")
    native_series = native_pd.Series([1, 2, 3, 4, 5], index=index)
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda s: s.loc[pd.Series(key, index=index, dtype="bool")]
        if isinstance(s, pd.Series)
        else s.loc[native_pd.Series(key, index=index, dtype="bool")],
    )


@pytest.mark.parametrize(
    "key",
    [
        [True] * 5,
        [False] * 5,
        [random.choice([True, False]) for _ in range(5)],
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_series_loc_get_key_bool_series_with_unaligned_and_distinct_indices(
    key, use_default_index
):
    # unaligned and distinct indices: e.g., [1,2,3,4,5] vs [5,4,3,2,1]
    if use_default_index:
        index = None
        native_key_index = native_pd.Index(np.random.permutation(range(5)))
        key_index = pd.Index(native_key_index)

    else:
        index_value = ["a", "b", "c", "d", "e"]
        index = native_pd.Index(index_value, name="index")
        native_key_index = native_pd.Index(
            generate_a_random_permuted_list_exclude_self(index_value), name="index"
        )
        key_index = pd.Index(native_key_index)
    native_series = native_pd.Series([1, 2, 3, 4, 5], index=index)
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda s: s.loc[pd.Series(key, index=key_index, dtype="bool")]
        if isinstance(s, pd.Series)
        else s.loc[native_pd.Series(key, index=native_key_index, dtype="bool")],
    )


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
def test_series_loc_get_key_bool(key, key_type, default_index_native_series):
    def loc_helper(ser):
        if isinstance(ser, native_pd.Series):
            # If native pandas Series, truncate the series and key.
            _ser = ser[: len(key)]
            _key = key[: len(_ser)]
        else:
            _key, _ser = key, ser

        # Convert key to the required type.
        if key_type == "index":
            _key = (
                pd.Index(_key, dtype=bool)
                if isinstance(_ser, pd.Series)
                else native_pd.Index(_key, dtype=bool)
            )
        elif key_type == "ndarray":
            _key = np.array(_key, dtype=bool)
        elif key_type == "series":
            _key = (
                pd.Series(_key)
                if isinstance(_ser, pd.Series)
                else native_pd.Series(_key)
            )

        return _ser.loc[_key]

    default_index_series = pd.Series(default_index_native_series)
    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            default_index_series,
            default_index_native_series,
            loc_helper,
        )


@pytest.mark.parametrize(
    "row",
    [
        lambda x: [1, 3],
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_df_loc_get_callable_key(row):
    native_series = native_pd.Series([1, 2, 3, 4, 5])
    eval_snowpark_pandas_result(
        pd.Series(native_series),
        native_series,
        lambda df: df.loc[row],
    )


@sql_count_checker(query_count=1, join_count=2)
def test_series_loc_get_key_bool_series_with_unaligned_and_duplicate_indices():
    # index can have null values and duplicates
    key = [True] * 5
    index_value = ["a", "a", None, "b", "b"]
    index = native_pd.Index(index_value, name="index")
    native_series = native_pd.Series([1, 2, 3, 4, 5], index=index)

    permuted_index_value = generate_a_random_permuted_list_exclude_self(index_value)
    key_index = pd.Index(permuted_index_value, dtype="string")
    native_key_index = native_pd.Index(permuted_index_value, dtype="string")
    snow_series = pd.Series(native_series)
    series_key = pd.Series(key, index=key_index, dtype="bool")
    native_series_key = native_pd.Series(key, index=native_key_index, dtype="bool")

    # Note:
    # pandas: always raise IndexingError when indices with duplicates are not aligned
    # Snowpark pandas: perform outer join on index and no error will be raised
    with pytest.raises(IndexingError):
        native_series.loc[native_series_key]

    assert_series_equal(
        snow_series.loc[series_key],
        native_pd.Series(
            [1, 1, 2, 2, 3, 4, 4, 5, 5],
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
@sql_count_checker(query_count=1, join_count=2)
def test_series_loc_get_key_bool_series_with_mismatch_index_len(key, use_default_index):
    if use_default_index:
        index = None
        key_index = np.random.permutation(len(key))
    else:
        index = ["a", "b", "c", "d", "e", "a1", "b1", "c1", "d1", "e1"]
        key_index = native_pd.Index(
            np.random.permutation(index[: len(key)]), dtype="string"
        )
        index = np.random.permutation(index[:5])
    native_series = native_pd.DataFrame([1, 2, 3, 4, 5], index=index)
    snow_series = pd.DataFrame(native_series)
    native_series_key = native_pd.Series(key, index=key_index, dtype="bool")

    series_key = pd.Series(key, index=pd.Index(key_index), dtype="bool")
    if len(key) < 5:
        # Native pandas raises error if any index from native_df is not in the key; when no missing index exists, native
        # pandas will perform as expected even though the key includes out-of-bound index
        with pytest.raises(IndexingError):
            native_series.loc[native_series_key]
        # Snowpark pandas does not raise error but select the index existing in the key. So the behavior is same as
        # the native one if the missing ones are dropped from native_df
        native_series = native_series.drop(
            index=[i for i in native_series.index if i not in native_series_key.index]
        )
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda df: df.loc[series_key]
        if isinstance(df, pd.DataFrame)
        else df.loc[native_series_key],
    )


@sql_count_checker(query_count=2, join_count=2)
def test_series_loc_get_key_scalar(
    default_index_snowpark_pandas_series, default_index_native_series
):
    key = random.choice(range(0, len(default_index_native_series)))
    # squeeze triggers additional queries
    snow = default_index_snowpark_pandas_series.loc[key]
    expected = default_index_native_series.loc[key]
    if isinstance(expected, tuple):
        # Snowpark pandas exposes tuple to list
        expected = list(expected)
    assert snow == expected


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
def test_series_loc_get_key_non_boolean_series(
    native_series_key,
    key_type,
    default_index_snowpark_pandas_series,
    default_index_native_series,
):
    def loc_helper(s, index_name=None):
        def type_convert(key, is_snow_type):
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

        return s.loc[type_convert(native_series_key, isinstance(s, pd.Series))]

    # default index
    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_series,
            default_index_native_series,
            loc_helper,
        )

    # non default index
    non_default_index_native_series = (
        default_index_native_series.reset_index()
        .astype({"index": "Int64"})
        .set_index("index")
        .squeeze()
    )
    non_default_index_snowpark_pandas_series = pd.Series(
        non_default_index_native_series
    )
    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            non_default_index_snowpark_pandas_series,
            non_default_index_native_series,
            functools.partial(loc_helper, index_name="index"),
            check_index_type=False,
        )

    # non default index with duplicates and null
    dup_native_series = native_pd.concat(
        [
            non_default_index_native_series,
            non_default_index_native_series,
            non_default_index_native_series.reindex([[None] * 7]),
        ]
    )
    dup_snowpandas_series = pd.Series(dup_native_series)
    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            dup_snowpandas_series,
            dup_native_series,
            functools.partial(loc_helper, index_name="index"),
            check_index_type=False,
        )

    # use key with null values
    native_series_key = native_pd.concat([native_series_key.astype("Int64"), None])
    non_default_index_native_series = (
        default_index_native_series.reset_index()
        .astype({"index": "Int64"})
        .set_index("index")
        .squeeze()
    )

    dup_native_series = native_pd.concat(
        [
            non_default_index_native_series,
            non_default_index_native_series,
            non_default_index_native_series.reindex([[None] * 7]),
        ]
    )
    dup_snowpandas_series = pd.Series(dup_native_series)
    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            dup_snowpandas_series,
            dup_native_series,
            functools.partial(loc_helper, index_name="index"),
            check_index_type=False,
        )


@pytest.mark.parametrize("monotonic_decreasing", [False, True])
# to_pandas with variant columns added one more query
# TODO: SNOW-933782 should resolve it
@sql_count_checker(query_count=1)
def test_series_loc_get_key_slice(
    monotonic_decreasing,
    default_index_snowpark_pandas_series,
    default_index_native_series,
):
    seed = [None, -10, -3, -1, 1, 3, 10]
    key = slice(random.choice(seed), random.choice(seed), random.choice(seed))

    if monotonic_decreasing:
        native_series = default_index_native_series[::-1]
        snow_series = pd.Series(native_series)
    else:
        native_series = default_index_native_series
        snow_series = default_index_snowpark_pandas_series

    eval_snowpark_pandas_result(snow_series, native_series, lambda s: s.loc[key])


@pytest.fixture(scope="function")
def multiindex_series():
    tuples = [
        ("mark i", "mark v"),
        ("mark i", "mark vi"),
        ("sidewinder", "mark i"),
        ("sidewinder", "mark ii"),
        ("viper", "mark ii"),
        ("viper", "mark iii"),
    ]
    index = pd.MultiIndex.from_tuples(tuples)
    values = [12, 2, 0, 4, 10, 20]

    return native_pd.Series(values, index=index)


@pytest.mark.parametrize(
    "loc_with_slice",
    [
        lambda s: s.loc["mark i":"sidewinder"],
        lambda s: s.loc[("mark i", "mark v"):],
        lambda s: s.loc[("mark i",):],
        lambda s: s.loc[("mark i", "mark v"):("sidewinder", "mark i")],
        lambda s: s.loc["mark i":("sidewinder", "mark i")],
        lambda s: s.loc["mark i":"sidewinder":2],
        lambda s: s.loc["sidewinder":"mark i":-2],
        lambda s: s.loc["mark v":"mark vi"],
    ],
)
@sql_count_checker(query_count=1)
def test_mi_series_loc_get_slice_key(multiindex_series, loc_with_slice):
    s = pd.Series(multiindex_series)
    eval_snowpark_pandas_result(
        s,
        multiindex_series,
        loc_with_slice,
        check_index_type=False,
    )


@pytest.mark.parametrize(
    "key, native_error",
    [
        # scalar key behavior: prefix match plus drop level
        ["mark i", None],
        [(("mark i",)), None],
        [("mark i", "mark vi"), None],
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
def test_mi_series_loc_get_non_boolean_list_key(multiindex_series, key, native_error):
    s = pd.Series(multiindex_series)
    if isinstance(key, tuple) or is_scalar(key):
        query_count, join_count = 1, 0
        if isinstance(key, tuple) and len(key) == 2:
            # multiindex full lookup requires squeeze to run
            query_count += 1
    else:
        # other list like
        query_count, join_count = 1, 1
    with SqlCounter(query_count=query_count, join_count=join_count):
        if native_error:
            with pytest.raises(native_error):
                _ = multiindex_series.loc[key]
            assert s.loc[key].empty
        else:
            if isinstance(key, tuple) and len(key) == 2:
                print(s.loc[key])
                print(multiindex_series.loc[key])
            else:
                eval_snowpark_pandas_result(
                    s,
                    multiindex_series,
                    lambda s: s.loc[key],
                )


@pytest.mark.parametrize(
    "row_key",
    [
        native_pd.Series([False, True, True]),
        native_pd.Series(
            [
                0,  # 0 does not exist in item, so the row values will be set to NULL
                1,
                2,
            ],
            name="ccc",  # name does not matter
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
        native_pd.Series(
            [2, 1, 0, 0, 1, 2], index=[10, 0, 30, 40, 50, 100]
        ),  # series with non-default index
    ],
)
def test_series_loc_set_series_row_key_and_series_item(row_key):
    # series.loc[series key] = series item
    # ------------------------------------
    series = native_pd.Series([1, 2, 3], name="abc")
    item = native_pd.Series(
        [10, 20, 30],
        index=[
            3,  # 3 does not exist in the row key, so it will be skipped
            2,
            1,
        ],
        name="xyz",  # name does not matter
    )

    def loc_set_helper(s):
        if isinstance(s, native_pd.Series):
            s.loc[row_key] = item
        else:
            s.loc[pd.Series(row_key)] = pd.Series(item)

    expected_join_count = (
        2 if len(row_key) > 0 and all(isinstance(i, bool) for i in row_key) else 3
    )
    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            pd.Series(series), series, loc_set_helper, inplace=True
        )


@pytest.mark.parametrize(
    "row_key",
    [
        [True, False, True],
        [
            0,  # 0 does not exist in item, so the row values will be set to NULL
            1,
            2,
        ],
        [2, 0, 2],  # duplicates with no order
    ],
)
@pytest.mark.parametrize("key_type", LIST_LIKE_AND_SERIES_DATA_TYPES)
@pytest.mark.parametrize("item_type", LIST_LIKE_AND_SERIES_DATA_TYPES)
def test_series_loc_set_series_and_list_like_row_key_and_item(
    row_key, key_type, item_type, convert_data_to_data_type
):  # series.loc[series/list-like key] = series/list-like item
    # --------------------------------------------------------
    # Length of key must match length of item in native pandas.
    series = native_pd.Series([1, 2, 3], name="abc")
    item = [10, 20, 30]

    expected_join_count = 3
    if all(isinstance(i, bool) for i in row_key):
        if item_type.startswith("series"):
            expected_join_count = 2
        else:
            expected_join_count = 4

    # With a boolean key, the number of items provided must match the number of True values in the key in pandas.
    if is_bool(row_key[0]):
        item = item[:2]

        if key_type == "series with non-default index":
            # The boolean series row keys (e.g., pd.Series([True, False, True], index=[9, 5, 1])) are valid as row keys
            # only if the index of the row key and that of the series for assignment match. The series is recreated with
            # the same non-default index as the row_key to prevent errors.
            series = native_pd.Series([1, 2, 3], index=[3, 2, 1], name="abc")

    def loc_set_helper(s):
        # Convert key and item to the required types.
        _row_key = convert_data_to_data_type(row_key, key_type)
        _item = convert_data_to_data_type(item, item_type)

        if isinstance(s, pd.Series):
            # Convert row key and item to Snowpark pandas if required.
            _row_key = (
                pd.Series(_row_key)
                if isinstance(_row_key, native_pd.Series)
                else _row_key
            )
            _item = pd.Series(_item) if isinstance(_item, native_pd.Series) else _item
        else:
            _row_key = try_convert_index_to_native(_row_key)
            _item = try_convert_index_to_native(_item)
        s.loc[_row_key] = _item

    query_count = 1
    # 5 extra queries: sum of two cases below
    if item_type.startswith("index") and key_type.startswith("index"):
        query_count = 6
    # 4 extra queries: 1 query to convert item index to pandas in loc_set_helper, 2 for iter, and 1 for to_list
    elif item_type.startswith("index"):
        query_count = 5
    # 1 extra query to convert to series to setitem
    elif key_type.startswith("index"):
        query_count = 2
    with SqlCounter(query_count=query_count, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            pd.Series(series), series, loc_set_helper, inplace=True
        )


def test_series_loc_set_dataframe_item_negative(key_type):
    series = native_pd.Series([1, 2, 3])
    row_key = [
        0,  # 0 does not exist in item, so the row values will be set to NULL
        1,
        2,
    ]
    item = native_pd.DataFrame(
        [[10, 20, 30], [40, 50, 60], [70, 80, 90]],
        columns=["A", "B", "C"],
        index=[
            3,  # 3 does not exist in the row key, so it will be skipped
            2,
            1,
        ],
    )

    def key_converter(s):
        _row_key = row_key
        # Convert key to the required type.
        if key_type == "index":
            _row_key = (
                pd.Index(_row_key)
                if isinstance(s, pd.Series)
                else native_pd.Index(_row_key)
            )
        elif key_type == "ndarray":
            _row_key = np.array(_row_key)
        elif key_type == "series":
            _row_key = (
                pd.Series(_row_key)
                if isinstance(s, pd.Series)
                else native_pd.Series(_row_key)
            )
        return _row_key

    def loc_set_helper(s):
        row_key = key_converter(s)
        if isinstance(s, native_pd.Series):
            s.loc[try_convert_index_to_native(row_key)] = item
        else:
            s.loc[pd.Series(row_key)] = pd.DataFrame(item)

    with SqlCounter(query_count=0):
        eval_snowpark_pandas_result(
            pd.Series(series),
            series,
            loc_set_helper,
            inplace=True,
            expect_exception=True,
            expect_exception_type=ValueError,
            expect_exception_match="Incompatible indexer with DataFrame",
        )


@pytest.mark.parametrize(
    "row_key, item_values",
    [
        # Test single row (existing) and combinations of existing/new columns
        ("a", 99),
        # Test single row (new / not existing) and combinations of existing/new columns
        ("x", 94),
    ],
)
@pytest.mark.parametrize(
    "ser_index",
    [
        # Test with unique index values
        ["a", "b", "c", "d"],
        # Test with duplicate index values
        ["a", "a", "c", "d"],
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_series_loc_set_scalar_row_key_enlargement(row_key, item_values, ser_index):
    data = [1, 2, 3, 4]

    snow_ser = pd.Series(data, index=ser_index)
    native_ser = native_pd.Series(data, index=ser_index)

    def set_loc_helper(ser):
        ser.loc[row_key] = item_values

    eval_snowpark_pandas_result(snow_ser, native_ser, set_loc_helper, inplace=True)


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
def test_series_loc_set_key_slice(start, stop, step):
    ser_data = ["x", "y", "z", "w", "u"]
    idx = [0, 1, 2, 3, 4]

    native_ser = native_pd.Series(
        ser_data, dtype="str", index=pd.Index(idx, dtype="int32")
    )
    snow_ser = pd.Series(native_ser)

    key = slice(start, stop, step)

    def set_loc_helper(ser):
        ser.loc[key] = "new"

    query_count, join_count = 1, 2
    if key == slice(None):
        join_count = 0
    with SqlCounter(query_count=query_count, join_count=join_count):
        eval_snowpark_pandas_result(snow_ser, native_ser, set_loc_helper, inplace=True)


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
def test_series_loc_set_key_slice_with_series(start, stop, step):
    key = slice(start, stop, step)

    ser_data = ["x", "y", "z", "w", "u"]
    item_data = ["A", "B", "C", "D", "E"]
    idx = [0, 1, 2, 3, 4]
    native_ser = native_pd.Series(ser_data)
    snow_ser = pd.Series(ser_data)

    slice_len = len(native_pd.Series(idx).loc[key])

    native_item_ser = native_pd.Series(
        item_data[:slice_len],
        dtype="str",
        index=pd.Index(idx[:slice_len], dtype="int32"),
    )

    def set_loc_helper(ser):
        ser.loc[key] = (
            pd.Series(native_item_ser)
            if isinstance(ser, pd.Series)
            else native_item_ser
        )

    if slice_len == 0:
        # pandas can fail in this case, so we skip call loc for it, see more below in
        # test_series_loc_set_key_slice_with_series_item_pandas_bug
        set_loc_helper(snow_ser)
        # snow_ser should not change when slice_len = 0
        with SqlCounter(query_count=1):
            assert_snowpark_pandas_equal_to_pandas(snow_ser, native_ser)
    else:
        native_res = set_loc_helper(native_ser)
        if is_scalar(native_res):
            with SqlCounter(query_count=0):
                snow_res = set_loc_helper(snow_ser)
                assert snow_res == native_res
        else:
            with SqlCounter(query_count=1, join_count=4):
                snow_res = set_loc_helper(snow_ser)
                assert_series_equal(snow_res, native_res)


@pytest.mark.parametrize(
    "start, stop, step, pandas_fail", [[1, -1, None, True], [10, None, None, False]]
)
@sql_count_checker(query_count=2, join_count=3)
def test_series_loc_set_key_slice_with_series_item_pandas_bug(
    start, stop, step, pandas_fail
):
    key = slice(start, stop, step)

    ser_data = ["x", "y", "z", "w", "u"]
    item_data = ["A", "B", "C", "D", "E"]
    idx = [0, 1, 2, 3, 4]
    native_ser = native_pd.Series(ser_data)
    snow_ser = pd.Series(ser_data)

    slice_len = len(native_pd.Series(idx).loc[key])
    assert slice_len == 0

    native_item_ser = native_pd.Series(
        item_data[:slice_len],
        dtype="str",
        index=pd.Index(idx[:slice_len], dtype="int32"),
    )

    def set_loc_helper(ser):
        ser.loc[key] = (
            pd.Series(native_item_ser)
            if isinstance(ser, pd.Series)
            else native_item_ser
        )

    # pandas has bug when slice is None
    if pandas_fail:
        # File /opt/homebrew/Caskroom/miniconda/base/envs/snowpandas/lib/python3.8/site-packages/pandas/core/indexers/utils.py:181, in check_setitem_lengths(indexer, value, values)
        #     178 if is_list_like(value):
        #     179     if len(value) != length_of_indexer(indexer, values) and values.ndim == 1:
        #     180         # In case of two dimensional value is used row-wise and broadcasted
        # --> 181         raise ValueError(
        #     182             "cannot set using a slice indexer with a "
        #     183             "different length than the value"
        #     184         )
        #     185     if not len(value):
        #     186         no_op = True
        #
        # ValueError: cannot set using a slice indexer with a different length than the value
        with pytest.raises(
            ValueError,
            match="cannot set using a slice indexer with a different length than the value",
        ):
            set_loc_helper(native_ser)
        set_loc_helper(snow_ser)
        assert_snowpark_pandas_equal_to_pandas(snow_ser, native_ser)
    else:
        eval_snowpark_pandas_result(snow_ser, native_ser, set_loc_helper, inplace=True)


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("key", EMPTY_LIST_LIKE_VALUES)
@pytest.mark.parametrize(
    "item", EMPTY_LIST_LIKE_VALUES[:-1]
)  # ignore last element: empty series
def test_series_loc_set_with_empty_key_and_empty_item_negative(
    key,
    item,
    default_index_native_series,
):
    # series.loc[empty list-like/series key] = empty list-like item
    # -------------------------------------------------------------
    # In native pandas, there is no change to the Series:
    # 0             1
    # 1           1.1
    # 2          True
    # 3             a
    # 4    2021-01-01
    # 5          (1,)
    # 6           [1]
    # dtype: object
    #
    # In Snowpark pandas we raise a ValueError because performing the check on the frontend to mimic pandas' behavior
    # makes the code more complex and there are more cases to handle.

    native_ser = default_index_native_series.copy()
    snowpark_ser = pd.Series(native_ser)

    err_msg = "The length of the value/item to set is empty"
    with pytest.raises(ValueError, match=err_msg):
        native_ser.loc[try_convert_index_to_native(key)] = try_convert_index_to_native(
            item
        )
        snowpark_ser.loc[
            pd.Series(key) if isinstance(key, native_pd.Series) else key
        ] = item
        assert_series_equal(snowpark_ser, native_ser)


@sql_count_checker(query_count=1, join_count=3)
@pytest.mark.parametrize("key", EMPTY_LIST_LIKE_VALUES)
def test_series_loc_set_with_empty_key_and_empty_series_item(
    key,
    default_index_native_series,
):
    # series.loc[empty list-like/series key] = empty series item
    # ----------------------------------------------------------
    # In native pandas, there is no change to the Series:
    # 0             1
    # 1           1.1
    # 2          True
    # 3             a
    # 4    2021-01-01
    # 5          (1,)
    # 6           [1]
    # dtype: object
    #
    # Snowpark pandas mirrors this behavior.

    native_ser = default_index_native_series.copy()
    snowpark_ser = pd.Series(native_ser)
    item = native_pd.Series([])

    native_ser.loc[try_convert_index_to_native(key)] = item
    snowpark_ser.loc[
        pd.Series(key) if isinstance(key, native_pd.Series) else key
    ] = pd.Series(item)
    assert_snowpark_pandas_equal_to_pandas(snowpark_ser, native_ser)


@sql_count_checker(query_count=1, join_count=2)
@pytest.mark.parametrize("key", EMPTY_LIST_LIKE_VALUES)
def test_series_loc_set_with_empty_key_and_scalar_item(
    key,
    default_index_native_series,
):
    # series.loc[empty list-like/series key] = scalar item
    # ----------------------------------------------------
    # In native pandas, there is no change to the Series:
    # 0             1
    # 1           1.1
    # 2          True
    # 3             a
    # 4    2021-01-01
    # 5          (1,)
    # 6           [1]
    # dtype: object
    #
    # Snowpark pandas mirrors this behavior. If a string scalar item is used, the rest of the values in the series are
    # converted to strings, like:
    # >>> series.loc[[]] = "a"
    # 0             1
    # 1           1.1
    # 2          true   <--- converted to string from boolean
    # 3             a
    # 4    2021-01-01
    # 5           [1]
    # 6           [1]
    # dtype: object

    native_ser = default_index_native_series.copy()
    snowpark_ser = pd.Series(native_ser)
    item = 32

    native_ser.loc[try_convert_index_to_native(key)] = item
    snowpark_ser.loc[
        pd.Series(key) if isinstance(key, native_pd.Series) else key
    ] = item
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snowpark_ser, native_ser)


@sql_count_checker(query_count=0, join_count=0)
@pytest.mark.parametrize("key", EMPTY_LIST_LIKE_VALUES)
@pytest.mark.parametrize(
    "item", [native_pd.Index([random.randint(0, 6) for _ in range(7)])]
)
def test_series_loc_set_with_empty_key_and_list_like_item_negative(
    key,
    item,
    default_index_native_series,
):
    # series.loc[empty list-like/series key] = list-like item
    # -------------------------------------------------------
    # In native pandas, there is no change to the Series:
    # >>> series
    # 0             1
    # 1           1.1
    # 2          True
    # 3             a
    # 4    2021-01-01
    # 5          (1,)
    # 6           [1]
    # dtype: object
    # >>> series.loc[[]] = [1, 2, 3]
    # >>> series
    # 0             1
    # 1           1.1
    # 2          True
    # 3             a
    # 4    2021-01-01
    # 5          (1,)
    # 6           [1]
    # dtype: object
    # In Snowpark pandas we raise a ValueError because performing the check on the frontend to mimic pandas' behavior
    # makes the code more complex and there are more cases to handle.

    native_ser = default_index_native_series.copy()
    snowpark_ser = pd.Series(native_ser)

    err_msg = (
        "cannot set using a list-like indexer with a different length than the value"
    )
    with pytest.raises(ValueError, match=err_msg):
        native_ser.loc[key] = item
        snowpark_ser.loc[
            pd.Series(key) if isinstance(key, native_pd.Series) else key
        ] = (pd.Series(item) if isinstance(item, native_pd.Series) else item)
        assert_series_equal(snowpark_ser, native_ser)


@sql_count_checker(query_count=1, join_count=3)
@pytest.mark.parametrize("key", EMPTY_LIST_LIKE_VALUES)
@pytest.mark.parametrize(
    "item", [native_pd.Series([random.randint(0, 6) for _ in range(7)])]
)
def test_series_loc_set_with_empty_key_and_series_item(
    key,
    item,
    default_index_native_series,
):
    # series.loc[empty list-like/series key] = series item
    # ----------------------------------------------------
    # In native pandas, there is no change to the Series:
    # 0             1
    # 1           1.1
    # 2          True
    # 3             a
    # 4    2021-01-01
    # 5          (1,)
    # 6           [1]
    # dtype: object
    #
    # Snowpark pandas mirrors this behavior.

    native_ser = default_index_native_series.copy()
    snowpark_ser = pd.Series(native_ser)

    native_ser.loc[try_convert_index_to_native(key)] = item
    snowpark_ser.loc[pd.Series(key) if isinstance(key, native_pd.Series) else key] = (
        pd.Series(item) if isinstance(item, native_pd.Series) else item
    )
    assert_snowpark_pandas_equal_to_pandas(snowpark_ser, native_ser)


def test_empty_series_col_loc():
    native_ser = native_pd.Series()
    snow_ser = pd.Series(native_ser)

    def col_loc(series):
        series.loc[:, 0] = 1

    with SqlCounter(query_count=0):
        eval_snowpark_pandas_result(
            snow_ser,
            native_ser,
            col_loc,
            inplace=True,
            expect_exception=IndexingError,
            expect_exception_match="Too many indexers",
        )


@pytest.mark.parametrize("index", [True, False], ids=["with_index", "without_index"])
class TestEmptySeriesLoc:
    @sql_count_checker(query_count=1)
    def test_empty_series_loc_slice(self, index):
        kwargs = {}
        if index:
            kwargs["index"] = [0, 1, 2]

        def row_loc_slice(series):
            series.loc[:] = 1

        native_ser = native_pd.Series(**kwargs)
        snow_ser = pd.Series(native_ser)
        eval_snowpark_pandas_result(
            snow_ser,
            native_ser,
            row_loc_slice,
            check_index_type=False,
            inplace=True,
        )

    def test_empty_series_loc_scalar(self, index):
        kwargs = {}
        if index:
            kwargs["index"] = [0, 1, 2]

        def row_loc(series):
            series.loc[0] = 1

        native_ser = native_pd.Series(**kwargs)
        snow_ser = pd.Series(native_ser)
        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snow_ser,
                native_ser,
                row_loc,
                inplace=True,
            )

        if index:

            def row_loc_outside_index(series):
                series.loc[11] = 1

            native_ser = native_pd.Series(**kwargs)
            snow_ser = pd.Series(native_ser)
            with SqlCounter(query_count=1):
                eval_snowpark_pandas_result(
                    snow_ser,
                    native_ser,
                    row_loc_outside_index,
                    inplace=True,
                )


@pytest.mark.parametrize("key", SERIES_AND_LIST_LIKE_KEY_AND_ITEM_VALUES)
@pytest.mark.parametrize("item", [range(7)])
@sql_count_checker(query_count=0)
def test_series_loc_set_series_list_like_key_and_range_like_item_negative(
    key, item, default_index_native_series
):
    # series.loc[series/list-like key] = range-like item
    # --------------------------------------------------
    # Ranges are treated like lists. This case is not implemented yet.
    # Example:
    # >>> series = pd.Series(["a", "b", "c", "d"])
    # >>> series.loc[pd.Series([1, 2])] = range(5, 30, 23)
    # >>> series
    # 0     a
    # 1     5
    # 2    28
    # 3     d
    # dtype: object

    snowpark_ser = pd.Series(default_index_native_series)
    err_msg = "Currently do not support Series or list-like keys with range-like values"
    with pytest.raises(NotImplementedError, match=err_msg):
        snowpark_ser.loc[key] = item


@pytest.mark.parametrize("key", SCALAR_LIKE_VALUES)
@pytest.mark.parametrize("item", [native_pd.Series(["a", "abc", "ab", "abcd"])])
@sql_count_checker(query_count=0)
def test_series_loc_set_scalar_key_and_series_item_negative(
    key, item, mixed_type_index_native_series_mixed_type_index
):
    # series.loc[scalar key] = series item
    # ------------------------------------
    # Using a scalar key and series item. Like the previous test, the item is assigned to only one element in the
    # series at index `key`. The item behaves like a scalar value even though it's a Series object. This feature is
    # currently not supported in Snowpark pandas - raises ValueError.
    #
    # Example:
    # >>> series = pd.Series(["a", "b"])
    # >>> series.loc["c"] = series     <-- does not work because it causes infinite recursion
    # >>> series.loc["c"] = pd.Series(["a", "b"])
    # >>> series
    # 0                a
    # 1                b
    # c    0    a      <-- the series item is assigned as a single value to a particular index label
    #      1    b
    #      dtype: object
    # dtype: object

    snowpark_ser = pd.Series(mixed_type_index_native_series_mixed_type_index)
    err_msg = "Currently do not support setting cell with list-like values"
    with pytest.raises(NotImplementedError, match=err_msg):
        snowpark_ser.loc[key] = pd.Series(item)


@pytest.mark.parametrize("key", SCALAR_LIKE_VALUES)
@pytest.mark.parametrize("item", [native_pd.DataFrame({"A": [34, 35], "B": [76, 77]})])
@sql_count_checker(query_count=0)
def test_series_loc_set_scalar_key_and_df_item_mixed_types_negative(
    key, item, mixed_type_index_native_series_mixed_type_index
):
    # series.loc[scalar key] = df item
    # --------------------------------
    # Using a scalar key and df item. Like the previous test, the item is assigned to only one element in the
    # series at index `key`. The item behaves like a scalar value even though it's a DataFrame object. This feature is
    # currently not supported in Snowpark pandas - raises ValueError.
    #
    # Example:
    # >>> series = pd.Series(["a", "b"])
    # >>> series.loc["c"] = pd.DataFrame({"A": [34, 35], "B": [76, 77]})
    # >>> series
    # 0                   a
    # 1                   b
    # c        A   B      <-- the df item is assigned as a single value to a particular index label
    #      0  34  76
    #      1  35  77
    # dtype: object

    snowpark_ser = pd.Series(mixed_type_index_native_series_mixed_type_index)
    err_msg = "Incompatible indexer with DataFrame"
    with pytest.raises(ValueError, match=err_msg):
        snowpark_ser.loc[key] = pd.DataFrame(item)  # nested df


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("key", [1, 12, [1, 2, 3], native_pd.Series([0, 4, 5])])
def test_series_loc_set_slice_item_negative(key, default_index_native_series):
    # series.loc[array-like/scalar key] = slice item
    # ----------------------------------------------
    # Here, slice is treated like a scalar object and assigned as itself to given key(s). This behavior is currently
    # not supported in Snowpark pandas.
    #
    # Example:
    # >>> series = pd.Series(["a", "b", "c", "d"])
    # >>> series.loc[range(2)] = slice(20, 30, 40)
    # >>> series
    # 0    slice(20, 30, 40)
    # 1    slice(20, 30, 40)
    # 2                    c
    # 3                    d
    # dtype: object

    snowpark_ser = pd.Series(default_index_native_series)
    item = slice(20, 30, 40)
    err_msg = (
        "Currently do not support assigning a slice value as if it's a scalar value"
    )
    with pytest.raises(NotImplementedError, match=err_msg):
        snowpark_ser.loc[
            pd.Series(key) if isinstance(key, native_pd.Series) else key
        ] = item


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
def test_series_loc_set_boolean_key(key, index):
    # series.loc[True/False key] = scalar item
    # ----------------------------------------
    item = 99

    data = list(range(len(index)))

    native_ser = native_pd.Series(data, index=index)
    snow_ser = pd.Series(native_ser)

    snow_ser.loc[key] = item

    # In pandas, setitem for 0/False and 1/True will potentially set multiple values or fail to set at all if neither
    # keys already exist in the index.  In Snowpark pandas we treat 0 & False, and 1 & True as distinct values, so
    # they are independently settable, whether they exist or do not already exist in the series index.
    try:
        key_index = [str(v) for v in native_ser.index].index(str(key))
        native_ser.iloc[key_index] = item
    except ValueError:
        native_ser = native_pd.concat(
            [native_ser, native_pd.Series([item], index=[key])]
        )

    assert_series_equal(snow_ser, native_ser, check_dtype=False)


@sql_count_checker(query_count=0, join_count=0)
@pytest.mark.parametrize("item", [1.2, None, ["a", "b", "c"]])
def test_series_loc_set_df_key_negative(item, default_index_native_series):
    # series.loc[df key] = any item
    # -----------------------------
    # A DataFrame is an invalid key when setting items in a Series.
    df_key = native_pd.DataFrame([[1, 2]])

    native_ser = default_index_native_series
    snowpark_ser = pd.Series(native_ser)

    # Native pandas error verification.
    err_msg = "'int' object is not iterable"
    with pytest.raises(TypeError, match=err_msg):
        native_ser.loc[df_key] = item

    # Snowpark pandas error verification.
    err_msg = "Data cannot be a DataFrame"
    with pytest.raises(ValueError, match=err_msg):
        snowpark_ser.loc[pd.DataFrame(df_key)] = item
        assert_series_equal(snowpark_ser, native_ser)


@pytest.mark.parametrize(
    "key, item", [(lambda x: x < 3, [1, 2]), (lambda x: x == 0, 8.7)]
)
def test_series_loc_set_lambda_key(key, item):
    # series.loc[lambda key] = any item
    # ---------------------------------
    # Example:
    # >>> series = pd.Series(["a", "b", "c", "d", "e", "f"])
    # >>> series.loc[lambda x: x < "d"] = "new val"
    # >>> series
    # 0    new val
    # 1    new val
    # 2    new val
    # 3          d
    # 4          e
    # 5          f
    # dtype: object
    data = {"a": 1, "b": 2, "c": 3}
    snowpark_ser = pd.Series(data)
    native_ser = native_pd.Series(data)

    native_ser.loc[key] = item
    snowpark_ser.loc[key] = item

    # Join is performed when the item is list-like - join index and list-like item for assignment.
    # If item is scalar, no join is performed.
    with SqlCounter(query_count=1, join_count=3 if isinstance(item, list) else 0):
        assert_series_equal(snowpark_ser, native_ser, check_dtype=False)


def test_series_loc_set_callable_key():
    # series.loc[callable key] = any item
    # -----------------------------------
    # Example:
    # >>> series = pd.Series(["a", "b", "c", "d", "e", "f"])
    # >>> series.loc[series < "d"] = "new val"
    # >>> series
    # 0    new val
    # 1    new val
    # 2    new val
    # 3          d
    # 4          e
    # 5          f
    # dtype: object
    ser = native_pd.Series(list(range(10)))

    # Scalar item
    # -----------
    native_ser = ser.copy()
    snowpark_ser = pd.Series(native_ser)

    # set every element divisible by 4 to 3
    native_ser.loc[((native_ser + native_ser) % 4) == 0] = 3
    snowpark_ser.loc[((snowpark_ser + snowpark_ser) % 4) == 0] = 3

    # Using 3.14 instead of 3 in the above test, Snowpark pandas series has a mix of Decimal objects and floats in the
    # result but native pandas is only floats.
    with SqlCounter(query_count=1, join_count=0):
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snowpark_ser, native_ser
        )

    # Series item
    # -----------
    native_ser = ser.copy()
    snowpark_ser = pd.Series(native_ser)

    # set every element divisible by 3 to Series([10, 11, 12, 13])
    native_ser.loc[native_ser % 3 == 0] = native_pd.Series([10, 11, 12, 13])
    snowpark_ser.loc[snowpark_ser % 3 == 0] = pd.Series([10, 11, 12, 13])

    # Join is performed when the item is list-like - join index and list-like item for assignment.
    # If item is scalar, no join is performed.
    with SqlCounter(query_count=1, join_count=1):
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snowpark_ser, native_ser
        )


@sql_count_checker(query_count=0, join_count=0)
@pytest.mark.parametrize("key", [3, "t", -3.555])
@pytest.mark.parametrize("item", [[1, 2, 3], ["a", "b", "c"], [26, "z", 2.6]])
def test_series_loc_set_with_scalar_key_and_list_like_item(
    key,
    item,
    default_index_native_series,
):
    # series.loc[scalar key] = list-like item
    # ---------------------------------------
    # Feature not yet implemented in Snowpark pandas.
    # In native pandas:
    # >>> series
    # 0             1
    # 1           1.1
    # 2          True
    # 3             a
    # 4    2021-01-01
    # 5          (1,)
    # 6           [1]
    # dtype: object
    #
    # Using key that is present in the index:
    # >>> series.loc[3] = [1, 2, 3]
    # >>> series
    # 0             1
    # 1           1.1
    # 2          True
    # 3     [1, 2, 3]
    # 4    2021-01-01
    # 5          (1,)
    # 6           [1]
    # dtype: object
    #
    # Using key not in the index:
    # >>> series.loc["t"] = ["a", "b", "c"]
    # >>> series
    # 0             1
    # 1           1.1
    # 2          True
    # 3     [1, 2, 3]
    # 4    2021-01-01
    # 5          (1,)
    # 6           [1]
    # t     [a, b, c]
    # dtype: object

    native_ser = default_index_native_series.copy()
    snowpark_ser = pd.Series(native_ser)

    err_msg = "Currently do not support setting cell with list-like values"
    with pytest.raises(NotImplementedError, match=err_msg):
        native_ser.loc[key] = item
        snowpark_ser.loc[key] = item
        assert_series_equal(snowpark_ser, native_ser)


@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.parametrize("key", SCALAR_LIKE_VALUES)
@pytest.mark.parametrize("item", SCALAR_LIKE_VALUES)
def test_series_loc_set_with_scalar_key_and_scalar_item(
    key,
    item,
):
    # series.loc[scalar key] = scalar item
    # ------------------------------------
    # In Snowpark pandas, when the index, data, key, or item are of different types, type casting issues can occur.
    # >>> native_ser = native_pd.Series(["a", "b", "c", "d", "e", "xyz", 23],
    # ...              index=list(["0", "1", "2", "3", "4", "5", "6"]))
    # >>> snowpark_ser = pd.Series(native_ser)
    # >>> key = 0  # int value
    # >>> item = 0  # int value
    # Example 1:
    # >>> native_ser.loc[0] = 0
    # '0'      'a'
    # '1'      'b'
    # '2'      'c'
    # '3'      'd'
    # '4'      'e'
    # '5'    'xyz'
    # '6'       23
    #   0        0   <---- new element in series
    # dtype: object
    # In native pandas above, the key 0 is added as a new value to the index which is int 0, the rest of the values are
    # unchanged/retain type. The behavior of native pandas is to update the data at key if it exists or create a new
    # index value with item as the new data.
    # >>> snowpark_ser.loc[0] = 0
    # 0.00000      0  <----- updated the original index "0" instead of making a new one
    # 1.00000      b
    # 2.00000      c
    # 3.00000      d
    # 4.00000      e
    # 5.00000    xyz
    # 6.00000     23
    # dtype: object
    # In Snowpark pandas, all index values are converted to np.int64 object values before assignment. So, index "0" is
    # turned into a numeric value and treated as new index, therefore "0" is updated instead of creating a new 0 key.

    # Sometimes, in Snowpark pandas if a string item is used, all the data in the series is converted to string type.
    # In the case below, Snowpark pandas and native pandas have the same index (change or retain index in the same way)
    # but the values in the series are different after locset.
    # >>> native_ser = native_pd.Series(["a", "b", "c", "d", "e", "xyz", 23],
    # ...              index=list(["0", "1", "2", "3", "4", "5", "6"]))
    # >>> snowpark_ser = pd.Series(native_ser)
    # >>> key = "0"  # str value
    # >>> item = "xyz"  # str value
    # Example 2:
    # >>> native_ser.loc["0"] = "xyz"
    # '0'      'xyz'
    # '1'      'b'
    # '2'      'c'
    # '3'      'd'
    # '4'      'e'
    # '5'    'xyz'
    # '6'       23
    # dtype: object
    # In native pandas above, the value at "0" is updated to by "xyz", the rest of the values stay the same.
    # >>> snowpark_ser.loc["0"] = "xyz"
    # '0'      'xyz'
    # '1'      'b'
    # '2'      'c'
    # '3'      'd'
    # '4'      'e'
    # '5'    'xyz'
    # '6'     '23'  <----- turned into string
    # dtype: object
    # In Snowpark pandas, the int values are converted to string because of Snowflake implicit type casting; 23 turns
    # into "23".

    # Using different series below to avoid Snowflake implicit type casting behavior or emulate casting behavior from
    # Snowflake in native pandas.
    if isinstance(item, str) and isinstance(key, numbers.Number):
        # Snowflake tries to cast the index to numeric in this case - therefore use default index.
        native_ser = native_pd.Series(["a", "b", "c", "d", "e", "xyz", "23"])
    elif isinstance(item, str) and (key is None or isinstance(key, str)):
        # Snowflake tries to cast the series and index to string in this case - therefore use a string series.
        native_ser = native_pd.Series(
            ["a", "b", "c", "d", "e", "xyz", "23"],
            index=list(["0", "1", "2", "3", "4", "5", "6"]),
        )
    elif key is None or isinstance(key, str):
        # Without the 23 as int in the series, Snowflake tries to cast the whole series to a numeric type or a string
        # type. Avoid this by using a mixed series.
        native_ser = native_pd.Series(
            ["a", "b", "c", "d", "e", "xyz", 23],
            index=list(["0", "1", "2", "3", "4", "5", "6"]),
        )
    else:
        # Without the 0 as int in the series, Snowflake tries to cast the whole series' index to a numeric type.
        # Avoid this by using a mixed index.
        native_ser = native_pd.Series(
            ["a", "b", "c", "d", "e", "xyz", 23],
            index=list([0, "1", "2", "3", "4", "5", "6"]),
        )
    snowpark_ser = pd.Series(native_ser)
    native_ser.loc[key] = item
    snowpark_ser.loc[key] = item
    assert_series_equal(snowpark_ser, native_ser, check_index_type=False)


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "ops",
    [
        lambda s: s["2010"],
        lambda s: s.loc["2010-11"],
        lambda s: s["2010":"2011-10"],
    ],
)
def test_series_partial_string_indexing(ops):
    rng = native_pd.date_range("2010-10-01", "2011-12-31", freq="BME")
    native_ts = native_pd.Series(np.random.randn(len(rng)), index=rng)
    snowpark_ts = pd.Series(native_ts)

    # need to set check_freq=False since Snowpark pandas index's freq is always null
    eval_snowpark_pandas_result(snowpark_ts, native_ts, ops, check_freq=False)


@pytest.mark.parametrize(
    "ops, error",
    [
        [lambda s: s["2010"], True],
        [lambda s: s.loc["2010-11"], True],
        [lambda s: s["2010":"2011-10"], False],
    ],
)
def test_series_non_partial_string_indexing_cases(ops, error):
    # These are string values which should use exact match not partial string indexing
    rng = native_pd.date_range("2010-10-01", "2011-12-31", freq="BME").astype(str)
    native_str = native_pd.Series(np.random.randn(len(rng)), index=rng)
    snowpark_str = pd.Series(native_str)

    if error:
        with pytest.raises(KeyError):
            ops(native_str)
        with SqlCounter(query_count=1, join_count=1):
            assert len(ops(snowpark_str)) == 0
    else:
        with SqlCounter(query_count=1):
            # need to set check_freq=False since Snowpark pandas index's freq is always null
            eval_snowpark_pandas_result(snowpark_str, native_str, ops, check_freq=False)


@sql_count_checker(query_count=2)
def test_series_partial_string_indexing_behavior_diff():
    native_series_minute = native_pd.Series(
        [1, 2, 3],
        native_pd.DatetimeIndex(
            ["2011-12-31 23:59:00", "2012-01-01 00:00:00", "2012-01-01 00:02:00"]
        ),
    )
    series_minute = pd.Series(native_series_minute)

    # In partial string indexing, pandas check the resolution of the dataframe and the string resolution, if the
    # resolution is the same, pandas performs an exact match and return the value directly
    assert native_series_minute.index.resolution == "minute"
    native_res = native_series_minute["2011-12-31 23:59"]
    assert native_res == 1

    # While Snowpark pandas index does not maintain resolution, so it always return a series in this case:
    snow_res = series_minute["2011-12-31 23:59"]
    assert_series_equal(
        snow_res,
        native_pd.Series(
            [1],
            native_pd.DatetimeIndex(["2011-12-31 23:59:00"]),
        ),
        check_dtype=False,
    )

    # Similar to other cases, pandas raise error for out-of-bound key; while Snowpark pandas will return empty result.
    with pytest.raises(KeyError):
        native_series_minute["2022"]

    assert len(series_minute["2022"]) == 0


@sql_count_checker(query_count=1, join_count=1)
def test_series_loc_set_none():
    # Note that pandas does not support df.loc[None,:] like the series does here.
    native_s = native_pd.Series([1, 2, 3])

    def loc_set_helper(s):
        s.loc[None] = 100

    eval_snowpark_pandas_result(
        pd.Series(native_s), native_s, loc_set_helper, inplace=True
    )


@pytest.mark.parametrize(
    "key, query_count, join_count",
    [
        (
            "1 day",
            2,
            4,
        ),  # 1 join from series creation (double counted), 1 join from squeeze, 1 join from to_pandas during eval
        (
            native_pd.to_timedelta("1 day"),
            2,
            4,
        ),  # 1 join from series creation (double counted), 1 join from squeeze, 1 join from to_pandas during eval
        (["1 day", "3 days"], 1, 2),
        ([True, False, False], 1, 2),
        (slice(None, "4 days"), 1, 1),
        (slice(None, "4 days", 2), 1, 1),
        (slice("1 day", "2 days"), 1, 1),
        (slice("1 day 1 hour", "2 days 2 hours", 1), 1, 1),
    ],
)
def test_series_loc_get_with_timedelta(key, query_count, join_count):
    data = ["A", "B", "C"]
    idx = ["1 days", "2 days", "3 days"]
    native_ser = native_pd.Series(data, index=native_pd.to_timedelta(idx))
    snow_ser = pd.Series(data, index=pd.to_timedelta(idx))

    # Perform loc.
    with SqlCounter(query_count=query_count, join_count=join_count):
        snow_res = snow_ser.loc[key]
        native_res = native_ser.loc[key]
        if is_scalar(key):
            assert snow_res == native_res
        else:
            assert_series_equal(snow_res, native_res)


@pytest.mark.parametrize(
    "key, expected_result",
    [
        (
            slice(None, "4 days"),
            native_pd.Series(
                ["A", "B", "C", "D"],
                index=native_pd.to_timedelta(
                    ["1 days", "2 days", "3 days", "1 day 1 hour"]
                ),
            ),
        ),
        (
            slice(None, "4 days", 2),
            native_pd.Series(
                ["A", "C"], index=native_pd.to_timedelta(["1 day", "3 days"])
            ),
        ),
        (
            slice("1 day", "2 days"),
            native_pd.Series(
                ["A", "B"], index=native_pd.to_timedelta(["1 days", "2 days"])
            ),
        ),
        (
            slice("1 day 1 hour", "2 days 2 hours", -1),
            native_pd.Series(
                ["D", "C"], index=native_pd.to_timedelta(["1 day 1 hour", "3 days"])
            ),
        ),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_series_loc_get_with_timedelta_behavior_difference(key, expected_result):
    data = ["A", "B", "C", "D"]
    idx = ["1 days", "2 days", "3 days", "25 hours"]
    native_ser = native_pd.Series(data, index=native_pd.to_timedelta(idx))
    snow_ser = pd.Series(data, index=pd.to_timedelta(idx))

    with pytest.raises(KeyError):
        # The error message is usually of the form KeyError: Timedelta('4 days 23:59:59.999999999').
        native_ser.loc[key]

    actual_result = snow_ser.loc[key]
    assert_series_equal(actual_result, expected_result)


@sql_count_checker(query_count=2, join_count=2)
def test_series_loc_get_with_timedeltaindex_key():
    data = ["A", "B", "C"]
    idx = ["1 days", "2 days", "3 days"]
    native_ser = native_pd.Series(data, index=native_pd.to_timedelta(idx))
    snow_ser = pd.Series(data, index=pd.to_timedelta(idx))

    # Perform loc.
    key = ["1 days", "3 days"]
    snow_res = snow_ser.loc[pd.to_timedelta(key)]
    native_res = native_ser.loc[native_pd.to_timedelta(key)]
    assert_series_equal(snow_res, native_res)


@pytest.mark.xfail(reason="SNOW-1653219 None key does not work with timedelta index")
@sql_count_checker(query_count=2)
def test_series_loc_get_with_timedelta_and_none_key():
    data = ["A", "B", "C"]
    idx = ["1 days", "2 days", "3 days"]
    snow_ser = pd.Series(data, index=pd.to_timedelta(idx))
    # Compare with an empty Series, since native pandas raises a KeyError.
    expected_ser = native_pd.Series()
    assert_series_equal(snow_ser.loc[None], expected_ser)
