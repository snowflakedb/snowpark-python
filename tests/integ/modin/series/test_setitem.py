#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import numbers
import random
import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas._libs.lib import is_bool, is_list_like

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.modin.plugin.extensions.utils import try_convert_index_to_native
from tests.integ.modin.utils import (
    assert_series_equal,
    assert_snowpark_pandas_equal_to_pandas,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

# General note
# ------------
# - This file aims to document all possible combinations/behaviors with Series.__setitem__ and difference between
#   Series.__setitem__ and loc setitem.
#
# - Snowpark pandas code can cause type coercion (implicit casting) issues if:
#   - a comparison between two different types is made, or
#   - an item of a certain value is assigned to a column with a different data type.
#   Type coercion is usually done through an IFF/coalesce statement. Sometimes this coercion works, sometimes it fails.
#
# - the IFF statement in set_frame_2d_labels_with_scalar_row() in indexing_utils.py is the source of the type casting
#   issue. This might be taken care of if it is a significant use case.
#
# - Cases where a slice item can be assigned as if it's a scalar value (series[key] = slice item) are not supported.
#   This is because we need a way to represent a Python slice object in Snowflake. This is tested in
#   `test_series_setitem_array_like_key_and_scalar_item_mixed_types()`and should raise NotImplementedError.
#
# - Series, DataFrame items which behave like a scalar item (object is assigned to a single label) raise ValueError
#   since they result in nest series or df.
# - DataFrame key raises ValueError.
# - slice value that behaves like a scalar value raises ValueError.
# _ Cases that raise NotImplementedError:
#   - list-like or Series key with range-like or slice items
#   - boolean cases listed below

# - More information: https://docs.snowflake.com/en/sql-reference/data-type-conversion#implicit-casting-coercion


# Boolean cases
# -------------
# These cases are not supported due to type casting issues in Snowflake. This because the boolean key can be
# compared with non-boolean values: Snowflake tries to cast non-boolean values to boolean type and fails.
#
# Below is the code used to generate boolean list-likes/series.
# def generate_random_boolean_list():
#     return [random.choice([True, False]) for _ in range(14)]
#
# values that can be used as key and item should be added to SERIES_AND_LIST_LIKE_KEY_AND_ITEM_VALUES_{NO|WITH}_DUPLICATES:
# native_pd.Series([random.choice([True, False]) for _ in range(7)]),
# np.array([random.choice([True, False]) for _ in range(7)]),
#
# BOOLEAN_ARRAY_LIKE_VALUES = [
#     generate_random_boolean_list(),
#     native_pd.Index(generate_random_boolean_list()),
#     np.array(generate_random_boolean_list()),
# ]


# BEHAVIOR TABLE
# --------------
# Table which records different series, scalar/list item, and int key behavior. All behavior should match this.
# series[scalar key] = scalar/list item
# series' index and scalar key must be of the same type, and key needs to be present in the index.
PRINT_TABLE = False  # If True, a new BEHAVIOR_TABLE is printed.
BEHAVIOR_TABLE = """
Series locset int key and scalar/list item behavior.
+--------------+----------------+----------------------+--------------------------------------------------------------+
| Series type  | Item type      | Exception type       | Error message                                                |
+--------------+----------------+----------------------+--------------------------------------------------------------+
| int          | float          | -                    | No error: Matches native pandas.                             |
| int          | string         | SnowparkSQLException | Numeric value 'writing' is not recognized                    |
| int          | timestamp      | SnowparkSQLException | Can not convert parameter '"values"."__reduced___' of type [ |
| -            | -              | -                    | NUMBER(38,0)] into expected type [TIMESTAMP_NTZ(9)]          |
| int          | int list       | ValueError           | setting an array element with a sequence.                    |
| int          | string list    | NotImplementedError  | Currently do not support setting cell with list-like values  |
| int          | float list     | ValueError           | setting an array element with a sequence.                    |
| int          | timestamp list | NotImplementedError  | Currently do not support setting cell with list-like values  |
| float        | int            | -                    | No error: Matches native pandas.                             |
| float        | string         | SnowparkSQLException | Numeric value 'writing' is not recognized                    |
| float        | timestamp      | SnowparkSQLException | Can not convert parameter '"values"."__reduced___' of type [ |
| -            | -              | -                    | FLOAT] into expected type [TIMESTAMP_NTZ(9)]                 |
| float        | int list       | ValueError           | setting an array element with a sequence.                    |
| float        | string list    | NotImplementedError  | Currently do not support setting cell with list-like values  |
| float        | float list     | ValueError           | setting an array element with a sequence.                    |
| float        | timestamp list | NotImplementedError  | Currently do not support setting cell with list-like values  |
| string       | int            | SnowparkSQLException | Numeric value 'love' is not recognized                       |
| string       | float          | SnowparkSQLException | Numeric value 'love' is not recognized                       |
| string       | timestamp      | SnowparkSQLException | Timestamp 'love' is not recognized                           |
| string       | int list       | NotImplementedError  | Currently do not support setting cell with list-like values  |
| string       | string list    | NotImplementedError  | Currently do not support setting cell with list-like values  |
| string       | float list     | NotImplementedError  | Currently do not support setting cell with list-like values  |
| string       | timestamp list | NotImplementedError  | Currently do not support setting cell with list-like values  |
| timestamp    | int            | SnowparkSQLException | Can not convert parameter '"values"."__reduced___' of type [ |
| -            | -              | -                    | TIMESTAMP_NTZ(9)] into expected type [NUMBER(1,0)]           |
| timestamp    | float          | SnowparkSQLException | Can not convert parameter '"values"."__reduced___' of type [ |
| -            | -              | -                    | TIMESTAMP_NTZ(9)] into expected type [NUMBER(2,1)]           |
| timestamp    | string         | SnowparkSQLException | Timestamp 'writing' is not recognized                        |
| timestamp    | int list       | NotImplementedError  | Currently do not support setting cell with list-like values  |
| timestamp    | string list    | NotImplementedError  | Currently do not support setting cell with list-like values  |
| timestamp    | float list     | NotImplementedError  | Currently do not support setting cell with list-like values  |
| timestamp    | timestamp list | ValueError           | Could not convert object to NumPy datetime                   |
+--------------+----------------+----------------------+--------------------------------------------------------------+
"""


# Values that are scalars or behave like scalar keys and items.
SCALAR_LIKE_VALUES = [0, "xyz", None, 3.14]

# Values that behave like arrays/lists not all cases are covered here, should be covered in series/test_loc.py
ARRAY_LIKE_VALUES = [
    native_pd.Index(["a", "b", "c", "b"]),
    np.array([-2, -1, -1, True]),
    range(-2, 3),
    slice(-100, 2),
]

SERIES_AND_LIST_LIKE_KEY_AND_ITEM_VALUES_NO_DUPLICATES = [
    # series
    native_pd.Series([1, 4, 0, 2, 3]),
    # list-like - not all cases are covered here, should be covered in series/test_loc.py
    native_pd.Index([4, 6, 0, 5, 3]),
]

SERIES_AND_LIST_LIKE_KEY_AND_ITEM_VALUES_WITH_DUPLICATES = [
    # series
    native_pd.Series([1, 4, 1, 0, 2, 3, 2]),
    # list-like - not all cases are covered here, should be covered in series/test_loc.py
    native_pd.Index([4, 6, 0, 6, 0, 5, 4]),
]

EMPTY_LIST_LIKE_VALUES = [
    [],
    np.array([]),
    native_pd.Index([]),
    native_pd.Series([]),
]

DATA_FOR_STRING_KEY_TYPE_CHECKING_TESTS = [
    list(range(5)),  # only int
    [3.3333, 1, 2, 3, 4],  # int + float
    [1, "a", 2, 3, 4],  # int + string
    [True, 1, 2, 3, 4],  # True + int
    [False, "a", "b", "c", "d"],  # bool + string
    [["a", 20, 30], 1, 2, 3, 4],  # mixed list + int
    [[0], [1], [2], [3], [4]],  # only list
    [0, [1], [2], [3], [4]],  # int + list
    [[0], "a", [2], [3], [4]],  # list + string
    [["a", 20, 30], 1, 2, "a", 0],  # mixed
]


# Note: if you are modifying these tests, please update the SQL counter and differentiate between native pandas and
# Snowpark pandas behavior with an example.


@pytest.mark.parametrize(
    "key, item",
    [
        ("a", 35),  # "a" is present in the index
        ("z", 35),  # "z" is not present in the index
        (None, 35),  # None scalar
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_series_setitem_scalar_key_and_scalar_item(
    key, item, default_index_native_int_series
):
    # series[scalar key] = scalar item
    # --------------------------------
    # This is the simplest case for series[scalar key] = scalar item: the key and series' index have the same type.
    # Snowflake performs type coercion/implicit casting when the key and index have different types. This is
    # illustrated in the tests below.
    #
    # Example:
    # >>> series = pd.Series([2, 4, 6], index=["a", "b", "c"])
    # >>> series["z"] = 65
    # >>> series
    # a     2
    # b     4
    # c     6
    # z    65     <-- new index label created with item as the value
    # dtype: int64

    native_ser = default_index_native_int_series.copy()
    snowpark_ser = pd.Series(native_ser)

    # Assign item and compare results.
    native_ser[key] = item
    snowpark_ser[key] = item
    assert_series_equal(snowpark_ser, native_ser, check_dtype=False)


@pytest.mark.xfail(reason="SNOW-979584 decide whether to support mixed type")
@pytest.mark.parametrize(
    "key, item",
    [
        ("xyz", 0),
        ("a", None),
        ("xyz", 3.14),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_series_setitem_scalar_key_and_scalar_item_mixed_type_series(
    key, item, mixed_type_index_native_series_mixed_type_index
):
    # series[scalar key] = scalar item
    # --------------------------------
    # Here, we test series[key] = item where item and key behave like scalar values, i.e. only one element in the
    # series (at index `key`) is updated (or created if not present in series). The value set is `item`.
    #
    # Example:
    # >>> series = pd.Series(["a", "b"])
    # >>> series["xyz"] = slice(100, 400, 100)
    # >>> series
    # "0"                         a     <-- Snowflake type system turns the index column into string type
    # "1"                         b
    # "xyz"    slice(100, 400, 100)
    # dtype: object

    native_ser = mixed_type_index_native_series_mixed_type_index.copy()
    snowpark_ser = pd.Series(native_ser)

    # Assign item and compare results.
    native_ser[key] = item
    snowpark_ser[key] = item
    assert_series_equal(snowpark_ser, native_ser)


@pytest.mark.xfail(reason="TODO SNOW-979584 decide whether to support mixed type")
@pytest.mark.parametrize("item", ["abc", 3.14, None, np.nan])
@sql_count_checker(query_count=1, join_count=1)
def test_series_setitem_none_key_and_scalar_item_mixed_type_series(
    item, mixed_type_index_native_series_mixed_type_index
):
    # series[None] = scalar item
    # --------------------------
    # This test is similar to the previous one, using None as the key instead.
    #
    # Example:
    # >>> series = pd.Series([1.1, True, 1.2], index=["a", None, 23])
    # >>> series[None] = "a"
    # >>> series
    #    0     1.1
    # None       a    <-- value at label None is assigned the item
    #    2     1.2
    # dtype: object

    native_ser = mixed_type_index_native_series_mixed_type_index.copy()
    snowpark_ser = pd.Series(native_ser)

    # Assign item and compare results.
    native_ser[None] = item
    snowpark_ser[None] = item
    assert_series_equal(snowpark_ser, native_ser)


@pytest.mark.parametrize(
    "key, item",
    [
        (0, "a"),
        ("xyz", "a"),
        (3.14, "a"),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_series_setitem_scalar_key_and_scalar_item_mixed_type_series_type_coercion(
    key, item, mixed_type_index_native_series_mixed_type_index
):
    # series[scalar key] = scalar item
    # --------------------------------
    # This test highlights the Snowflake type coercion (implicit type casting) caused due to IFF.
    # In cases where the key and item are scalar and the Series has values of different types, Snowpark pandas and
    # native pandas exhibit different behavior.
    #
    # In Snowpark pandas, the types of all values in the Series and its index can be coerced (implicitly cast) which
    # results in the types of values being changed (usually from variant to boolean). However, in native pandas, these
    # types are retained. This behavior difference is due to the IFF statement (in set_frame_2d_labels_with_scalar_row()
    # in indexing_utils.py) which is comparing index labels. The IFF statement coerces types based on Snowflake rules.
    # In this case, the values are implicitly converted to string type.
    # https://docs.snowflake.com/en/sql-reference/data-type-conversion#implicit-casting-coercion
    #
    # Example:
    # In native pandas,
    # >>> series = native_pd.Series([1.1, True, 1.2])
    # >>> series[0] = "a"
    # >>> series
    # 0       a      # string type
    # 1    True      # boolean type
    # 2     1.2      # float type
    # dtype: object
    #
    # In Snowpark pandas,
    # >>> series = pd.Series([1.1, True, 1.2])
    # >>> series[0] = "a"
    # >>> series
    # 0       a      # string type
    # 1    true      # string type (coerced to string from boolean)
    # 2     1.2      # string type (coerced to string from float)
    # dtype: object

    native_ser = mixed_type_index_native_series_mixed_type_index.copy()
    snowpark_ser = pd.Series(native_ser)

    with pytest.raises(AssertionError, match="are different"):
        native_ser[key] = item
        snowpark_ser[key] = item
        # The series are not equal since the types of their values changed.
        assert_snowpark_pandas_equal_to_pandas(snowpark_ser, native_ser)


# +------------------------------------------+
# | series[scalar boolean key] = scalar item |
# +------------------------------------------+
# In general, native pandas treats True and 1 as the same label and False and 0 as the same label. Snowpark pandas
# generally treats True and 1, and False and 0 as different labels.
#
# These are the four cases of setitem (LHS + RHS) in the box above. They are covered in separate tests below:
# (1) Boolean scalar key is explicitly mentioned in the series index.
# (2) The series has a default index, i.e., 0 and 1 are present in the index. True and False are absent.
# (3) The series has both the boolean value and its corresponding int present in the series' index simultaneously.
#     Native pandas treats (0 and False) and (1 and True) as the same label (duplicates).
#     duplicate_index = [0, 1, False, True]
# (4) The index of the series is mutually exclusive with `duplicate_index`. The series' index does not have any values
#     from `duplicate_index`.


# TODO: SNOW-986548 fix where key is False, row is missed in this case
@pytest.mark.parametrize("key", [True, False])
@pytest.mark.parametrize("item", SCALAR_LIKE_VALUES)
@sql_count_checker(query_count=1, join_count=1)
def test_series_setitem_boolean_key_and_scalar_item_label_updated(key, item):
    # series[scalar boolean key] = scalar item
    # ----------------------------------------
    # CASE 1: Label updated: True/False present in the series' index with True/False key respectively.
    # ------------------------------------------------------------------------------------------------
    # Snowpark pandas matches native pandas behavior in most cases except when strings are involved.
    # This test highlights the Snowflake coercion caused due to IFF for the case series[scalar key] = bool scalar.
    # Here, Snowflake tries to coerce (implicitly cast) other types when updating the values in the series.
    #
    # This can arise when two values of different types are compared by Snowflake through IFF statements, or a value of
    # typeA (e.g. int) gets assigned to a value's spot of typeB (e.g. string).
    # Snowpark pandas logic.
    #
    # Relevant information:
    # ref: https://docs.snowflake.com/en/sql-reference/data-type-conversion#implicit-casting-coercion

    # Examples:
    # When an int item is used:
    # In both native pandas and Snowpark pandas:
    # In Snowpark pandas,
    # >>> series = pd.Series([1.1, "hello", 1.2], index=["a", True, "b"])
    # >>> series[True] = 34
    # >>> series
    # a       1.1
    # True     34
    # b       1.2
    # dtype: object

    # When a string item is used:
    # In native pandas,
    # >>> series = native_pd.Series([1.1, "hello", 1.2], index=["a", True, "b"])
    # >>> series[True] = "xyz"
    # >>> series
    # a       1.1   <--- float
    # True    xyz
    # b       1.2   <--- float
    # dtype: object
    #
    # In Snowpark pandas,
    # >>> series = pd.Series([1.1, "hello", 1.2], index=["a", True, "b"])
    # >>> series[True] = "xyz"
    # >>> series
    # a       1.1   <--- converted to string from float
    # True    xyz
    # b       1.2   <--- converted to string from float
    # dtype: object

    # series with numeric and string values
    native_ser = native_pd.Series(
        [1.1, "hello", 1.2, 34], index=["a", True, "b", False]
    )
    snowpark_ser = pd.Series(native_ser)

    # perform setitem
    native_ser[key] = item
    snowpark_ser[key] = item

    # All the cases except where key is "xyz" match native pandas. "xyz" causes the remaining values in the series to
    # be cast the string type.
    if item != "xyz":
        # match native pandas; compare with native pandas behavior
        assert_series_equal(snowpark_ser, native_ser, check_dtype=False)

    else:
        # This is the expected output after Snowpark pandas assigns a string item. All the values in the series are
        # converted to string. `expected_ser` is native pandas object to simplify assertion below.
        if key is True:
            expected_ser = native_pd.Series(
                ["1.1", item, "1.2", "34"], index=["a", True, "b", False]
            )
        else:  # False key
            expected_ser = native_pd.Series(
                ["1.1", "hello", "1.2", item], index=["a", True, "b", False]
            )

        # does not match native pandas; verify expected behavior
        assert_series_equal(snowpark_ser, expected_ser, check_dtype=False)


@pytest.mark.parametrize("key", [True, False])
@pytest.mark.parametrize("item", SCALAR_LIKE_VALUES)
def test_series_setitem_boolean_key_and_scalar_item_case2_numeric_index(key, item):
    # series[scalar boolean key] = scalar item
    # ----------------------------------------
    # CASE 2: numeric index with True/False key (index that contains 0/1 but True and False are absent).
    # --------------------------------------------------------------------------------------------------
    # In native pandas, if True/False is not present in the labels but is used as the key, a KeyError is raised.
    # However, True/False is a label in the index, pandas treats that as a regular label and assigns values to it.
    # True and 1 are not treated as the same label if True is absent. False and 0 are not treated as the same label
    # if False is absent.
    # Example:
    # >>> series = native_pd.Series([1.1, "hello", 1.2])
    # >>> series[True] = "xyz"
    # KeyError: 'cannot use a single bool to index into setitem'
    #
    # In Snowpark pandas, Snowflake always tries to cast all the boolean values to the provided key, in this case a
    # boolean value. Snowflake casts numeric types to boolean types with the following logic: 0 is converted to False,
    # all other values are converted to True. This test verifies that behavior.
    # Example:
    # >>> series = pd.Series([1.1, "hello", 1.2, [0, 3], 12, []])
    # >>> series[True] = "new"
    # >>> series
    # False    1.1   <--- float value converted to string type
    # True     new        because of Snowflake type coercion
    # True     new
    # True     new
    # True     new
    # True     new
    # dtype: object
    # In the example above, all the labels except 0 are converted to True and all their values are "new". Same behavior
    # with False as the key.
    #
    # Relevant information:
    # ref: https://docs.snowflake.com/en/sql-reference/data-type-conversion#implicit-casting-coercion

    if key is False and item == "xyz":
        # skip test for now: weird behavior: result looks fine, their types are weird.
        # False      xyz
        # True     hello
        # True     [0,3]
        # True        12
        # dtype: object
        # Types of each element:
        # <class 'str'>
        # <class 'modin.pandas.series.Series'>
        # <class 'modin.pandas.series.Series'>
        # <class 'modin.pandas.series.Series'>
        with SqlCounter(query_count=0):
            return

    # create the series and assign item
    series_data = [1.1, "hello", [0, 3], 12]
    # using default index since it is all numeric values; any numeric index should have the same behavior
    snowpark_ser = pd.Series(data=series_data)
    snowpark_ser[key] = item

    # create the expected series - turn the index into True and False labels and assign item accordingly
    index = [False, True, True, True]
    data = []
    for idx, label in enumerate(index):
        if label == key:
            data.append(item)
        else:
            data.append(series_data[idx])

    if isinstance(item, str):
        # All the non-string values are implicitly cast to string by Snowflake
        data = [val if isinstance(val, str) else str(val) for val in data]

    expected_ser = native_pd.Series(data=data, index=index)

    with SqlCounter(query_count=1, join_count=1):
        # verify that the result is correct
        assert_series_equal(snowpark_ser, expected_ser)


@pytest.mark.parametrize("key", [True, False])
@pytest.mark.parametrize("item", SCALAR_LIKE_VALUES)
@sql_count_checker(query_count=1, join_count=1)
def test_series_setitem_boolean_key_and_scalar_item_case2_non_numeric_index(key, item):
    # series[scalar boolean key] = scalar item
    # ----------------------------------------
    # CASE 2: non-numeric index with True/False key (index that contains 0/1 but True and False are absent).
    # ------------------------------------------------------------------------------------------------------
    # Here, the index needs to not have all numeric elements, otherwise it behaves like the previous test.
    # A new label (True/False) is created in the series and item is assigned to it.
    # Example:
    # In native pandas:
    # >>> series = native_pd.Series([1.1, "hello", [0, 3], 12], index=[0, 1, 2, []])
    # >>> series[True] = "new"
    # >>> series
    # 0        1.1
    # 1        new
    # 2     [0, 3]
    # []        12
    # dtype: object
    # In Snowpark pandas:
    # >>> series = pd.Series([1.1, "hello", [0, 3], 12], index=[0, 1, 2, []])
    # >>> series[True] = "new"
    # >>> series
    # 0         1.1
    # 1       hello
    # 2       [0,3]
    # []         12
    # True      new   <--- True is added as a new label/row
    # In Snowpark pandas, True is treated as a label that is not present in the Series and is appended at the end.
    # Same behavior with False as the key.
    # Native pandas treats True and 1, and False and 0 as the same labels in this case -- no new value is appended but
    # the values at the labels 0/1 are updated.
    #
    # Edge case: np.nan as a label:
    # Exact same behavior as None as a label with np.nan being converted to None. Native pandas fails with
    # KeyError: 'cannot use a single bool to index into setitem' for both np.nan and None labels.

    # create the series with non-default index and assign item
    data = [1.1, "hello", [0, 3], 12]
    index = [0, "a", 2, [1, 2, 3]]
    snowpark_ser = pd.Series(data=data, index=index)
    snowpark_ser[key] = item

    # create the expected series: append new label at the end
    if isinstance(item, str):
        # all the non-string values in the series are implicitly cast to strings by Snowflake
        data = ["1.1", "hello", "[0,3]", "12", item]
    else:
        # new label and its value are appended to the series' index and data
        data.append(item)
    # index types remain the same/are not cast to different types
    index.append(key)
    expected_ser = native_pd.Series(data=data, index=index)

    # verify that the result is correct
    assert_series_equal(snowpark_ser, expected_ser)


@pytest.mark.parametrize("key", [0, 1])
@pytest.mark.parametrize("item", SCALAR_LIKE_VALUES)
@sql_count_checker(query_count=1, join_count=1)
def test_series_setitem_boolean_key_and_scalar_item_case3(
    key, item, native_series_with_duplicate_boolean_index
):
    # series[scalar boolean key] = scalar item
    # ----------------------------------------
    # CASE 3: The series has both (0 and False) and (1 and True) in its index.
    # ------------------------------------------------------------------------
    # In native pandas, if 0 and False or 1 and True are present as labels of the series index together, 0 and 1 are
    # treated as the labels from False and True respectively, and vice versa.
    # >>> series = native_pd.Series([1.1, "hello", 1.2, [1, 2]], index=[0, 1, False, True])
    # >>> series[True] = "xyz"    <--- series[1] = "xyz" does the same thing
    # >>> series
    # 0        1.1
    # 1        xyz    <--- True updates the label 1 as well
    # False    1.2
    # True     xyz    <--- updated
    # dtype: object
    #
    # Snowpark errors if True/False is used as the key. Currently, Snowpark pandas treats (0 and False) and (1 and True)
    # as different/unique labels. So only one element is updated.
    #
    # EDGE CASE: `ser[1] = None` here behaves just like native pandas does. It updates the values at labels 1 and True.
    # However, `ser[0]` = None` only updates values at the label 0. Native pandas does both 0 and False. In fact,
    # In every single test here (and most probably in most cases) Snowpark pandas only matches 1 with 1, and 0 with 0,
    # i.e., any item != None used will exhibit this behavior.

    # Like the other tests, there is a possibility for type coercion.
    # Relevant information:
    # ref: https://docs.snowflake.com/en/sql-reference/data-type-conversion#implicit-casting-coercion

    native_ser = native_series_with_duplicate_boolean_index.copy()
    snowpark_ser = pd.Series(native_ser)

    err_msg = "Series are different"
    native_ser[key] = item
    snowpark_ser[key] = item
    if key == 1 and item is None:
        # Edge case that passes - Snowpark pandas and native pandas treat 0 and False, and 1 and True as equivalent
        # labels.
        assert_series_equal(snowpark_ser, native_ser)
    else:
        with pytest.raises(AssertionError, match=err_msg):
            # The series are not equal since native pandas treats 0 and False, and 1 and True as equivalent labels but
            # Snowpark pandas does not.
            assert_series_equal(snowpark_ser, native_ser)


@pytest.mark.xfail(
    reason="Snowflake type casting error caused due to comparison of different types with IFF.",
    strict=False,
)
@pytest.mark.parametrize("key", [True, False])
@pytest.mark.parametrize("item", SCALAR_LIKE_VALUES)
@sql_count_checker(query_count=0)
def test_series_setitem_boolean_key_and_scalar_item_case4_negative(
    key, item, default_index_native_int_series
):
    # series[scalar boolean key] = scalar item (negative case)
    # --------------------------------------------------------
    # All tests should have the error: SnowparkSQLException: "Boolean value 'a' is not recognized". This is because of
    # Snowflake's type coercion. Here Snowflake is trying to cast a string to boolean type and fails.
    # Relevant information:
    # ref: https://docs.snowflake.com/en/sql-reference/data-type-conversion#implicit-casting-coercion

    # CASE 4: The series' index does not contain values in [0, 1, True, False].
    # -------------------------------------------------------------------------
    # In native pandas, True/False is absent in the series' index and is passed in as a key, it errors: it can't use a
    # scalar bool to index the series. Same as 0 or 1 being in the index.
    # >>> series = native_pd.Series(["hello", 1.2, [1, 2]], index=["a", "b", "c"])
    # >>> series[True] = "xyz"
    # KeyError: 'cannot use a single bool to index into setitem'
    # dtype: object
    #
    # Snowpark has a coercion/implicit type casting issue similar to the previous tests.
    # >>> series = pd.Series(["hello", 1.2, [1, 2]], index=["a", "b", "c"])
    # >>> series[True] = "xyz"
    # SnowparkSQLException: Boolean value 'a' is not recognized

    native_ser = default_index_native_int_series.copy()
    snowpark_ser = pd.Series(native_ser)

    # Snowpark series is compared with unmodified native pandas series to trigger computation. Here, Snowpark errors
    # because Snowflake can't convert the string index labels to boolean type - done to compare with the True/False key.
    snowpark_ser[key] = item
    assert_series_equal(snowpark_ser, native_ser)


@pytest.mark.xfail(
    reason="Snowflake type casting error caused due to comparison of different types with IFF.",
    strict=False,
)
@pytest.mark.parametrize("key", SCALAR_LIKE_VALUES)
@pytest.mark.parametrize("item", [True, False])
@sql_count_checker(query_count=0)
def test_series_setitem_scalar_key_and_boolean_item_mixed_type_series_negative(
    key, item, mixed_type_index_native_series_mixed_type_index
):
    # series[scalar key] = scalar boolean item
    # ----------------------------------------
    # This test highlights the Snowflake coercion caused due to IFF for the case series[scalar key] = bool scalar.
    # Explanation in tests above. Here, Snowflake tries to coerce (implicitly cast) other types (like variant in
    # Snowflake) to boolean, and fails.
    #
    # All tests should fail with the error: SnowparkSQLException: "Failed to cast variant value <value> to BOOLEAN".
    #
    # Relevant information:
    # IFF statement in set_frame_2d_labels_with_scalar_row() in indexing_utils.py.
    # ref: https://docs.snowflake.com/en/sql-reference/data-type-conversion#implicit-casting-coercion
    #
    # Example:
    # In native pandas,
    # >>> series = native_pd.Series([1.1, "hello", 1.2])
    # >>> series[0] = True
    # >>> series
    # 0     True      # boolean type
    # 1    hello      # string type
    # 2      1.2      # float type
    # dtype: object
    #
    # In Snowpark pandas,
    # >>> series = pd.Series([1.1, "hello", 1.2])
    # >>> series[0] = True
    # SnowparkSQLException: Failed to cast variant value "hello" to BOOLEAN

    native_ser = mixed_type_index_native_series_mixed_type_index.copy()
    snowpark_ser = pd.Series(native_ser)

    # Assign item and compare results.
    native_ser[key] = item
    snowpark_ser[key] = item
    assert_series_equal(snowpark_ser, native_ser)


@pytest.mark.parametrize("key", ["a", "z"])
@pytest.mark.parametrize("item", [[2, 3, 4], native_pd.Series(["a", "b"])])
@sql_count_checker(query_count=0)
def test_series_setitem_scalar_key_and_array_like_and_series_item(key, item):
    # series[scalar key] = array-like/series item
    # -------------------------------------------
    # This is the simplest case for series[scalar key] = array-like item. `item` here is an array-like/series value
    # but is assigned to only one element at index label `key`, just like a scalar value would be assigned.
    #
    # pandas Example:
    # >>> series = pd.Series([2, 4, 6], index=["a", "b", "c"])
    # >>> series["z"] = [3, 4, 6]
    # >>> series
    # a            2
    # b            4
    # c            6
    # z    [3, 4, 6]
    # dtype: object
    #
    # >>> series["c"] = [3, 4, 6]
    # ValueError: setting an array element with a sequence.
    # But series["c"] = ["3", "4", "6"] works.
    #
    # >>> series["z"] = pd.Series([3, 4, 6])
    # >>> series
    # a              2
    # b              4
    # c              6
    # z    0    3        <--- series is assigned to a single element
    #      1    4
    #      2    6
    #      dtype: int64
    # dtype: object
    #
    # >>> series["z"] = pd.Series([3, 4, 6])
    # ValueError: setting an array element with a sequence.
    # But series["c"] = pd.Series(["3", "4", "6"]) works.
    #
    # Snowpark pandas does not support those cases now.
    # TODO: SNOW-991872 support set array values

    native_ser = native_pd.Series([2, 4, 6], index=["a", "b", "c"])
    snowpark_ser = pd.Series(native_ser)

    # Assign item and compare results.
    if key in native_ser and not isinstance(item[0], str):
        # existing index
        with pytest.raises(ValueError):
            native_ser[key] = item
    else:
        # non-existing index
        native_ser[key] = item

    with pytest.raises((ValueError, NotImplementedError)):
        snowpark_ser[key] = (
            pd.Series(item) if isinstance(item, native_pd.Series) else item
        )
        snowpark_ser.to_pandas()


@pytest.mark.parametrize("key", SCALAR_LIKE_VALUES)
@pytest.mark.parametrize("item", ARRAY_LIKE_VALUES)
def test_series_setitem_scalar_key_and_array_like_item_mixed_types(
    key, item, mixed_type_index_native_series_mixed_type_index
):
    # series[scalar key] = array-like item
    # ------------------------------------
    # Like the previous test, this test performs series[key] = item. `item` here is an array-like value but is assigned
    # to only one element at index label `key`, just like a scalar value would be assigned.
    #
    # Example:
    # >>> series = pd.Series(["a", "b"], index=[1, False])
    # >>> series[False] = range(-2, 3)
    # >>> series
    # 1                        a
    # False    (-2, -1, 0, 1, 2)
    # dtype: object
    # Normally, False as the key would error since you cannot use single boolean values as indexers but since False was
    # already set as a part of the index, it can be used to index the series.
    if isinstance(item, native_pd.Index):
        item = pd.Index(item)
    with SqlCounter(query_count=1 if isinstance(item, pd.Index) else 0):
        native_ser = mixed_type_index_native_series_mixed_type_index.copy()
        snowpark_ser = pd.Series(native_ser)

        # Assign item and compare results.
        with pytest.raises(NotImplementedError):
            native_ser[key] = try_convert_index_to_native(item)
            snowpark_ser[key] = item
            assert_series_equal(snowpark_ser, native_ser)


@pytest.mark.parametrize("key", SCALAR_LIKE_VALUES)
@pytest.mark.parametrize("item", [native_pd.Series(["a", "abc", "ab", "abcd"])])
@sql_count_checker(query_count=0)
def test_series_setitem_scalar_key_and_series_item_negative(
    key, item, mixed_type_index_native_series_mixed_type_index
):
    # series[scalar key] = series item
    # --------------------------------
    # Using a scalar key and series item. Like the previous test, the item is assigned to only one element in the
    # series at index `key`. The item behaves like a scalar value even though it's a Series object. This feature is
    # currently not supported in Snowpark pandas - raises ValueError.
    #
    # Example:
    # >>> series = pd.Series(["a", "b"])
    # >>> series["c"] = series     <-- does not work because it causes infinite recursion
    # >>> series["c"] = pd.Series(["a", "b"])
    # >>> series
    # 0                a
    # 1                b
    # c    0    a      <-- the series item is assigned as a single value to a particular index label
    #      1    b
    #      dtype: object
    # dtype: object

    snowpark_ser = pd.Series(mixed_type_index_native_series_mixed_type_index)
    err_msg = "Scalar key incompatible with Snowpark pandas Series value"
    with pytest.raises(ValueError, match=err_msg):
        snowpark_ser[key] = pd.Series(item)


@pytest.mark.parametrize("key", SCALAR_LIKE_VALUES)
@pytest.mark.parametrize("item", [native_pd.DataFrame({"A": [34, 35], "B": [76, 77]})])
@sql_count_checker(query_count=0)
def test_series_setitem_scalar_key_and_df_item_mixed_types_negative(
    key, item, mixed_type_index_native_series_mixed_type_index
):
    # series[scalar key] = df item
    # ----------------------------
    # Using a scalar key and df item. Like the previous test, the item is assigned to only one element in the
    # series at index `key`. The item behaves like a scalar value even though it's a DataFrame object. This feature is
    # currently not supported in Snowpark pandas - raises ValueError.
    #
    # Example:
    # >>> series = pd.Series(["a", "b"])
    # >>> series["c"] = pd.DataFrame({"A": [34, 35], "B": [76, 77]})
    # >>> series
    # 0                   a
    # 1                   b
    # c        A   B      <-- the df item is assigned as a single value to a particular index label
    #      0  34  76
    #      1  35  77
    # dtype: object

    snowpark_ser = pd.Series(mixed_type_index_native_series_mixed_type_index)
    err_msg = "Scalar key incompatible with Snowpark pandas DataFrame value"
    with pytest.raises(ValueError, match=err_msg):
        snowpark_ser[key] = pd.DataFrame(item)  # nested df


@pytest.mark.parametrize("key", [[1, 2, 3], native_pd.Series([5, 6, 3])])
@pytest.mark.parametrize("item", ["abc", 24])
def test_series_setitem_array_like_and_series_key_and_scalar_item(
    key, item, default_index_native_series
):
    # series[array-like key] = scalar item
    # ------------------------------------
    # This is the simplest case for series[array-like/series key] = scalar item. The expected behavior is that
    # multiple index labels specified by `key` have `item` assigned to them all.
    #
    # In Snowpark pandas, the types of all values in the Series and its index can be coerced (implicitly cast) which
    # results in the types of values being changed (usually from variant to boolean). However, in native pandas, these
    # types are retained. This behavior difference is due to the IFF statement (in set_frame_2d_labels_with_scalar_row()
    # in indexing_utils.py) which is comparing index labels. The IFF statement coerces types based on Snowflake rules.
    # In this case, the values are implicitly converted to string type.
    # https://docs.snowflake.com/en/sql-reference/data-type-conversion#implicit-casting-coercion
    #
    # Example:
    # >>> series = pd.Series(["a", "b", "c", "d"])
    # >>> series[[2, 0, 1]] = "xyz"
    # >>> series
    # 0    xyz
    # 1    xyz
    # 2    xyz
    # 3      d
    # dtype: object
    native_ser = default_index_native_series.copy()
    snowpark_ser = pd.Series(native_ser)

    # Assign item and compare results.
    native_ser[key] = item
    snowpark_ser[pd.Series(key) if isinstance(key, native_pd.Series) else key] = item
    with SqlCounter(query_count=1, join_count=2):
        if item == 24:
            # Cases that pass - Snowpark pandas and native pandas assign values to the same labels. The int item here
            # does not trigger type coercion.
            assert_snowpark_pandas_equal_to_pandas(snowpark_ser, native_ser)
        else:  # item = "abc"
            # The series are not equal since the types of their values changed.
            err_msg = "Series are different"
            with pytest.raises(AssertionError, match=err_msg):
                assert_snowpark_pandas_equal_to_pandas(snowpark_ser, native_ser)


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("key", [1, 12, [1, 2, 3], native_pd.Series([0, 4, 5])])
def test_series_setitem_slice_item_negative(key, default_index_native_series):
    # series[array-like/scalar key] = slice item
    # ------------------------------------------
    # Here, slice is treated like a scalar object and assigned as itself to given key(s). This behavior is currently
    # not supported in Snowpark pandas.
    #
    # Example:
    # >>> series = pd.Series(["a", "b", "c", "d"])
    # >>> series[range(2)] = slice(20, 30, 40)
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
        snowpark_ser[
            pd.Series(key) if isinstance(key, native_pd.Series) else key
        ] = item


@pytest.mark.parametrize("item", [0, ["a", "b", "c"], native_pd.Series([1, 2])])
@sql_count_checker(query_count=0)
def test_series_setitem_df_key_negative(item, default_index_native_series):
    # series[df key] = any item
    # -------------------------
    # Should raise a ValueError since this should be impossible.
    snowpark_ser = pd.Series(default_index_native_series)
    key = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    err_msg = "Snowpark pandas DataFrame cannot be used as an indexer with Series"
    with pytest.raises(ValueError, match=err_msg):
        snowpark_ser[
            pd.Series(key) if isinstance(key, native_pd.Series) else key
        ] = item


@pytest.mark.parametrize("key", ARRAY_LIKE_VALUES)  # + BOOLEAN_ARRAY_LIKE_VALUES)
@pytest.mark.parametrize("item", SCALAR_LIKE_VALUES)
# Parameters commented here due to reasons described at the top.
# @sql_count_checker(query_count=1, join_count=2)
def test_series_setitem_array_like_key_and_scalar_item_mixed_types(
    key, item, mixed_type_index_native_series_mixed_type_index
):
    # series[array-like key] = scalar item
    # ------------------------------------
    # Using an array-like key and scalar values. The expected behavior is that multiple index labels specified by `key`
    # have `item` assigned to them all. In Snowpark pandas, like in an IFF statement, Snowflake implictly casts (coerces
    # types) during comparison, this can result in either all the elements in the original series to turn into string
    # type or raises an error because it failed to perform this casting operation. Slice objects can behave like scalar
    # objects where each label in the key is assigned `slice item`. This slice object behavior is not supported in
    # Snowpark pandas because currently there is no way to represent a Python slice object in Snowflake.
    #
    # Example:
    # Behavior matches
    # ----------------
    # >>> series = pd.Series(["a", "b", "c", "d"])
    # >>> series[range(2)] = "abc"
    # >>> series
    # 0    abc
    # 1    abc
    # 2      c
    # 3      d
    # dtype: object
    #
    # Behavior mismatch
    # -----------------
    # >>> series = pd.Series(["a", "b", "c", "d"])
    # >>> series[range(2)] = 23
    # Native pandas behavior:
    # >>> series
    # 0    23
    # 1    23
    # 2     c
    # 3     d
    # dtype: object
    # Using a numeric key here raises a type coercion error in Snowpark pandas. This is most likely because Snowflake
    # failed to cast the string key to a numeric value.
    # >>> series[range(2)] = 23
    # SnowparkSQLException: Numeric value 'c' is not recognized
    #
    # Native pandas behavior:
    # >>> series = pd.Series(["a", "b", True, "d"])
    # >>> series[range(1)] = "ab"
    # >>> series
    # 0      23
    # 1       b
    # 2    True   <-- retains boolean type
    # 3       d
    # dtype: object
    # Snowpark pandas behavior:
    # >>> series = pd.Series(["a", "b", True, "d"])
    # >>> series[range(1)] = 23
    # >>> series
    # 0      ab
    # 1       b
    # 2    true   <-- string type (coerced to string from boolean)
    # 3       d
    # dtype: object

    if isinstance(key, native_pd.Index):
        key = pd.Index(key)

    native_ser = mixed_type_index_native_series_mixed_type_index.copy()
    snowpark_ser = pd.Series(native_ser)

    # Assign item.
    native_ser[try_convert_index_to_native(key)] = item
    snowpark_ser[key] = item

    err_msg = "Series are different"
    if not isinstance(key, slice) and any(x == 1 for x in key):
        # If the key contains the int 1, native pandas treats this as True but Snowpark pandas sees them as different
        # labels. This does not happen in all cases that contain 1 (like the elif statement below).
        with SqlCounter(query_count=1, join_count=2):
            with pytest.raises(AssertionError, match=err_msg):
                assert_series_equal(snowpark_ser, native_ser, check_dtype=False)

    elif (isinstance(key, pd.Index) and item == "xyz") or (
        isinstance(key, slice) and (item is None or item == "xyz")
    ):
        # In these three cases, Snowpark pandas works and the values are logically correct. However, Snowpark pandas
        # casts all values in the series to string which results in a type mismatch when compared to native pandas.
        # Here, native pandas treats 1 and True as different labels, like Snowpark pandas does.
        # 1. key = Index(['a', 'b', 'c', 'b'], dtype='object'), item = 'xyz'
        # 2. key = slice(-100, 2, None), item = 'xyz'
        # 3. key = slice(-100, 2, None), item = None
        with SqlCounter(query_count=1, join_count=2):
            with pytest.raises(AssertionError, match=err_msg):
                assert_series_equal(snowpark_ser, native_ser, check_dtype=False)

    else:
        with SqlCounter(query_count=1, join_count=2):
            # In all other cases, native pandas and Snowpark pandas behavior matches.
            assert_series_equal(snowpark_ser, native_ser, check_dtype=False)


@pytest.mark.skip(
    "slice item doesn't work - not JSON serializable. Everything else runs but needs optimization."
)
@pytest.mark.parametrize(
    "key",
    [
        native_pd.Series([1, 5, 3]),
        native_pd.Series(
            [random.choice([True, False]) for _ in range(random.randint(14, 24))]
        ),
    ],
)
@pytest.mark.parametrize("item", SCALAR_LIKE_VALUES + [slice(10, 20, 30)])
def test_series_setitem_series_key_and_scalar_item(
    key, item, default_index_native_series
):
    # series[series key] = scalar item
    # -------------------------------------
    # Using a series key and a scalar item. The expected behavior is that multiple index labels specified by the
    # series `key` have `item` assigned to them. Let `data` be the series which calls __setitem__. A boolean series
    # `key` of any length >= `data`'s index length is valid provided `key`'s index matches the index of `data`.
    #
    # Example:
    # >>> series = pd.Series(["a", "b", "c", "d"])
    # >>> series[pd.Series([1, 2])] = "stringval"
    # >>> series
    # 0            a
    # 1    stringval
    # 2    stringval
    # 3            d
    # dtype: object

    native_ser = default_index_native_series.copy()
    snowpark_ser = pd.Series(native_ser)

    # Behavior does not match native pandas due to type coercion, so Series don't match in most cases.
    # Relevant information:
    # IFF statement in set_frame_2d_labels_with_scalar_row() in indexing_utils.py.
    # ref: https://docs.snowflake.com/en/sql-reference/data-type-conversion#implicit-casting-coercion

    err_msg = "Series are different"
    qc = 2 if is_bool(key[0]) else 3
    with SqlCounter(query_count=qc, join_count=1):
        native_ser[key] = item
        snowpark_ser[key] = item
        if isinstance(item, numbers.Number):
            assert_series_equal(snowpark_ser, native_ser)
        else:
            with pytest.raises(AssertionError, match=err_msg):
                assert_series_equal(snowpark_ser, native_ser)


@pytest.mark.parametrize(
    "key", SERIES_AND_LIST_LIKE_KEY_AND_ITEM_VALUES_WITH_DUPLICATES
)
@pytest.mark.parametrize(
    "item",
    SERIES_AND_LIST_LIKE_KEY_AND_ITEM_VALUES_WITH_DUPLICATES,
)
@sql_count_checker(query_count=1, join_count=3)
def test_series_setitem_series_list_like_item_key_and_item_with_duplicates(
    key, item, default_index_native_series
):
    # series[series/list-like key] = series/list-like item
    # ----------------------------------------------------
    # Example:
    # >>> series = pd.Series(["a", "b", "c", "d"])
    # >>> series[pd.Series([1, 2])] = pd.Index(["abc", "xyz"])
    # >>> series
    # 0      a
    # 1    abc
    # 2    xyz
    # 3      d
    # dtype: object

    native_ser = default_index_native_series.copy()
    snowpark_ser = pd.Series(native_ser)

    # Assign item and compare results.
    native_ser[key] = item
    snowpark_ser[pd.Series(key) if isinstance(key, native_pd.Series) else key] = (
        pd.Series(item) if isinstance(item, native_pd.Series) else item
    )
    assert_snowpark_pandas_equal_to_pandas(snowpark_ser, native_ser)


# matching_item_row_by_label is False here.
@pytest.mark.parametrize("key", SERIES_AND_LIST_LIKE_KEY_AND_ITEM_VALUES_NO_DUPLICATES)
@pytest.mark.parametrize(
    "item",
    SERIES_AND_LIST_LIKE_KEY_AND_ITEM_VALUES_NO_DUPLICATES,
)
@sql_count_checker(query_count=1, join_count=3)
def test_series_setitem_series_list_like_item_key_and_item_no_duplicates(
    key, item, default_index_native_series
):
    # series[series/list-like key] = series/list-like item
    # ----------------------------------------------------
    # Example:
    # >>> series = pd.Series(["a", "b", "c", "d"])
    # >>> series[pd.Series([1, 2])] = pd.Index(["abc", "xyz"])
    # >>> series
    # 0      a
    # 1    abc
    # 2    xyz
    # 3      d
    # dtype: object
    native_ser = default_index_native_series.copy()
    snowpark_ser = pd.Series(native_ser)

    # Assign item and compare results.
    native_ser[key] = item
    snowpark_ser[pd.Series(key) if isinstance(key, native_pd.Series) else key] = (
        pd.Series(item) if isinstance(item, native_pd.Series) else item
    )
    assert_snowpark_pandas_equal_to_pandas(snowpark_ser, native_ser)


@pytest.mark.parametrize("key", [range(2, 6, 2), slice(1, 7, 3)])
@pytest.mark.parametrize("item", [native_pd.Series(["abc", 21])])
@sql_count_checker(query_count=1, join_count=3)
def test_series_setitem_range_like_key_and_series_list_like_item(
    key, item, default_index_native_series
):
    # series[range-like key] = series/list-like item
    # ----------------------------------------------
    # Ranges are treated like lists.
    # Example:
    # >>> series = pd.Series(["a", "b", "c", "d"])
    # >>> series[range(1, 3)] = pd.Index(["abc", "xyz"])
    # >>> series
    # 0      a
    # 1    abc
    # 2    xyz
    # 3      d
    # dtype: object
    #
    # Cases like `item = np.array([True, False])]` are not supported due to type casting issues in Snowflake. Error
    # arises because the boolean key is being compared with non-boolean values: Snowflake tries to cast non-boolean
    # values to boolean type and fails.
    # Relevant information:
    # IFF statement in set_frame_2d_labels_with_scalar_row() in indexing_utils.py.
    # ref: https://docs.snowflake.com/en/sql-reference/data-type-conversion#implicit-casting-coercion

    native_ser = default_index_native_series.copy()
    snowpark_ser = pd.Series(native_ser)

    # Assign item and compare results.
    # __setitem__/loc set behavior with Series values is broken in native pandas after 2.x
    # https://github.com/pandas-dev/pandas/issues/51386
    # Instead, we use iloc set to mimic the expected behavior
    native_ser.iloc[key] = item
    snowpark_ser[key] = pd.Series(item) if isinstance(item, native_pd.Series) else item
    assert_snowpark_pandas_equal_to_pandas(snowpark_ser, native_ser)


@pytest.mark.parametrize(
    "key",
    SERIES_AND_LIST_LIKE_KEY_AND_ITEM_VALUES_NO_DUPLICATES
    + SERIES_AND_LIST_LIKE_KEY_AND_ITEM_VALUES_WITH_DUPLICATES,
)
@pytest.mark.parametrize("item", [range(7)])
@sql_count_checker(query_count=0)
def test_series_setitem_series_list_like_key_and_range_like_item_negative(
    key, item, default_index_native_series
):
    # series[series/list-like key] = range-like item
    # ----------------------------------------------
    # Ranges are treated like lists. This case is not implemented yet.
    # Example:
    # >>> series = pd.Series(["a", "b", "c", "d"])
    # >>> series[pd.Series([1, 2])] = range(5, 30, 23)
    # >>> series
    # 0     a
    # 1     5
    # 2    28
    # 3     d
    # dtype: object

    snowpark_ser = pd.Series(default_index_native_series)
    err_msg = "Currently do not support Series or list-like keys with range-like values"
    with pytest.raises(NotImplementedError, match=err_msg):
        snowpark_ser[key] = item


@sql_count_checker(query_count=1, join_count=0)
def test_setitem_lambda_series():
    # series[lambda key] = scalar
    # ---------------------------
    data = {"a": 1, "b": 2, "c": 3}
    snow_ser = pd.Series(data)
    native_ser = native_pd.Series(data)

    def helper(ser):
        ser[lambda x: x < 2] = 8

    eval_snowpark_pandas_result(snow_ser, native_ser, helper, inplace=True)


def test_series_setitem_lambda_key():
    # series[lambda key] = scalar/list-like/series item
    # -------------------------------------------------
    # Using a lambda as the key and scalar, list-like, and series as the item.
    #
    # Example:
    # Expected native pandas behavior:
    # >>> series = pd.Series(["a", "b", "c", "d", "e", "f"])
    # >>> series[lambda x: x < "d"] = "new val"
    # >>> series
    # 0    new val
    # 1    new val
    # 2    new val
    # 3          d
    # 4          e
    # 5          f
    # dtype: object

    ser = native_pd.Series(list(range(10)))

    # set every element in series to 99 where element > 22
    native_ser = ser.copy()
    snowpark_ser = pd.Series(native_ser)
    # Assign item and compare results.
    native_ser[lambda x: x > 22] = 99
    snowpark_ser[lambda x: x > 22] = 99
    with SqlCounter(query_count=1):
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snowpark_ser, native_ser
        )

    # set every element divisible by 4 to 3
    native_ser = ser.copy()
    snowpark_ser = pd.Series(native_ser)
    # Assign item and compare results.
    native_ser[lambda z: ((z + z) % 4) == 0] = 3
    snowpark_ser[lambda z: ((z + z) % 4) == 0] = 3
    # Using 3.14 instead of 3 in the above test, Snowpark pandas series has a mix of Decimal objects and floats in the
    # result but native pandas is only floats.
    with SqlCounter(query_count=1):
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snowpark_ser, native_ser
        )

    # Weird case with native pandas:
    # If the item is a Series, first native pandas applies the lambda to the `series` index and collects these labels.
    # Then pandas tries to match these labels with the ones from the index of the item. If there is a match, a value is
    # assigned. If there is no match, NaN is assigned.

    # set every element divisible by 3 to Series([10, 11, 12, 13])
    native_ser = ser.copy()
    snowpark_ser = pd.Series(native_ser)
    native_ser[lambda y: y % 3 == 0] = native_pd.Series([10, 11, 12, 13])
    snowpark_ser[lambda y: y % 3 == 0] = pd.Series([10, 11, 12, 13])
    with SqlCounter(query_count=1):
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snowpark_ser, native_ser
        )


@pytest.mark.xfail(
    reason="Snowflake type casting error caused due to comparison of different types with IFF.",
    strict=False,
)
@sql_count_checker(query_count=0)
def test_series_setitem_lambda_key_string_compare():
    # series[lambda key] = scalar/list-like/series item
    # -------------------------------------------------
    # Using a lambda as the key and scalar, list-like, and series as the item. The tests here fail with the error:
    # SnowparkSQLException: "Numeric value 'a' is not recognized". This is most likely because Snowflake
    # failed to cast the string 'a' to a numeric value.
    #
    # Example:
    # Expected native pandas behavior:
    # >>> series = pd.Series(["a", "b", "c", "d", "e", "f"])
    # >>> series[lambda x: x < "d"] = "new val"
    # >>> series
    # 0    new val
    # 1    new val
    # 2    new val
    # 3          d
    # 4          e
    # 5          f
    # dtype: object

    # set every element in series to 99 where element > "c"
    native_ser = native_pd.Series(["a", "b", "c", "d", "e", "f"])
    snowpark_ser = pd.Series(native_ser)
    # Assign item and compare results.
    native_ser[lambda x: x > "c"] = 99
    snowpark_ser[lambda x: x > "c"] = 99
    assert_series_equal(snowpark_ser, native_ser)

    # set every element divisible by 3 to "a"
    native_ser = native_pd.Series(list(range(10)))
    snowpark_ser = pd.Series(native_ser)
    # Assign item and compare results.
    native_ser[lambda y: y % 3 == 0] = native_pd.Series(["a", "b", "c", "d"])
    snowpark_ser[lambda y: y % 3 == 0] = pd.Series(["a", "b", "c", "d"])
    assert_series_equal(snowpark_ser, native_ser)

    # set every element divisible by 4 to "bc"
    native_ser = native_pd.Series(list(range(10)))
    snowpark_ser = pd.Series(native_ser)
    # Assign item and compare results.
    native_ser[lambda z: ((z + z) % 4) == 0] = "bc"
    snowpark_ser[lambda z: ((z + z) % 4) == 0] = "bc"
    assert_series_equal(snowpark_ser, native_ser)


def test_series_setitem_comparator_key():
    # series[comparator key] = scalar/list-like/series item
    # ------------------------------------------------------
    # Using a lambda as the key and scalar, list-like, and series as the item. Series behavior in Snowpark pandas does
    # not completely match native pandas at the moment.
    #
    # Example:
    # Expected native pandas behavior:
    # >>> series = pd.Series(["a", "b", "c", "d", "e", "f"])
    # >>> series[lambda x: x < "d"] = "new val"
    # >>> series
    # 0    new val
    # 1    new val
    # 2    new val
    # 3          d
    # 4          e
    # 5          f
    # dtype: object

    ser = native_pd.Series(list(range(10)))

    # set every element in series to 99 where element > 22
    native_ser = ser.copy()
    snowpark_ser = pd.Series(native_ser)
    # Assign item and compare results.
    native_ser[native_ser > 22] = 99
    snowpark_ser[snowpark_ser > 22] = 99
    with SqlCounter(query_count=1):
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snowpark_ser, native_ser
        )

    # set every element which has a remainder of 1 when divided by 4 to 3.
    native_ser = ser.copy()
    snowpark_ser = pd.Series(native_ser)
    # Assign item and compare results.
    native_ser[((native_ser + native_ser) % 4) == 1] = 3
    snowpark_ser[((snowpark_ser + snowpark_ser) % 4) == 1] = 3
    # Using 3.14 instead of 3 in the above test, Snowpark pandas series has a mix of Decimal objects and floats in the
    # result but native pandas is only floats.
    with SqlCounter(query_count=1):
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snowpark_ser, native_ser
        )

    # Weird case with native pandas:
    # If the item is a Series, first native pandas applies the lambda to the `series` index and collects these labels.
    # Then pandas tries to match these labels with the ones from the index of the item. If there is a match, a value is
    # assigned. If there is no match, NaN is assigned.

    # set every element divisible by 3 to Series([10, 11, 12, 13])
    native_ser = ser.copy()
    snowpark_ser = pd.Series(native_ser)
    native_ser[native_ser % 3 == 0] = native_pd.Series([10, 11, 12, 13])
    snowpark_ser[snowpark_ser % 3 == 0] = pd.Series([10, 11, 12, 13])
    with SqlCounter(query_count=1):
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snowpark_ser, native_ser
        )


@pytest.mark.xfail(
    reason="Snowflake type casting error caused due to comparison of different types with IFF.",
    strict=False,
)
@sql_count_checker(query_count=5)
def test_series_setitem_comparator_key_string_compare():
    # series[comparator key] = scalar/list-like/series item
    # -----------------------------------------------------
    # Using a comparator/condition as the key and scalar, list-like, and series as the item. The tests here fail with
    # the error: SnowparkSQLException: "Numeric value 'a' is not recognized". This is most likely because Snowflake
    # failed to cast the string 'a' to a numeric value.
    #
    # Example:
    # >>> series = pd.Series(list(range(8)))
    # >>> series[series > 4]
    # 5    5
    # 6    6
    # 7    7
    # dtype: int64
    # >>> series[series > 4] = 2
    # >>> series
    # 0    0
    # 1    1
    # 2    2
    # 3    3
    # 4    4
    # 5    2
    # 6    2
    # 7    2
    # dtype: int64

    # set every element in series to 99 where element > "c"
    native_ser = native_pd.Series(["a", "b", "c", "d", "e", "f"])
    snowpark_ser = pd.Series(native_ser)
    # Assign item and compare results.
    native_ser[native_ser > "c"] = 99
    snowpark_ser[snowpark_ser > "c"] = 99
    assert_series_equal(snowpark_ser, native_ser)

    # set every element divisible by 3 to Series(["a", "b", "c", "d"])
    native_ser = native_pd.Series(list(range(10)))
    snowpark_ser = pd.Series(native_ser)
    # Assign item and compare results.
    native_ser[native_ser % 3 == 0] = native_pd.Series(["a", "b", "c", "d"])
    snowpark_ser[snowpark_ser % 3 == 0] = pd.Series(["a", "b", "c", "d"])
    assert_series_equal(snowpark_ser, native_ser)

    # set every element divisible by 4 to "bc"
    native_ser = native_pd.Series(list(range(10)))
    snowpark_ser = pd.Series(native_ser)
    # Assign item and compare results.
    native_ser[((native_ser + native_ser) % 4) == 1] = "bc"
    snowpark_ser[((snowpark_ser + snowpark_ser) % 4) == 1] = "bc"
    assert_series_equal(snowpark_ser, native_ser)


def test_series_setitem_series_behavior_that_deviates_from_loc_set(
    default_index_native_series,
):
    # Test to highlight the behavior difference with data series' index and item's index for loc set and __setitem__
    # --------------------------------------------------------------------------------------------------------------
    # If value is a Series, value's index doesn't matter/is ignored. However, loc setitem matches the key's
    # index with value's index. To emulate this behavior, treat the Series as if it is matching by position.
    #
    # For example,
    # With __setitem__, the index of value does not matter.
    # >>> series = pd.Series([1, 2, 3], index=["a", "b", "c"])
    # >>> series[["a", "b"]] = pd.Series([9, 8])
    # a    9
    # b    8
    # c    3
    # dtype: int64
    # value = pd.Series([9, 8], index=["foo", "bar"]) also produces same result as above.
    #
    # However, with loc setitem, index matters.
    # >>> series.loc[["a", "b"]] = pd.Series([9, 8])
    # a    NaN
    # b    NaN
    # c    3.0
    # dtype: float64
    #
    # >>> series.loc[["a", "b"]] = pd.Series([9, 8], index=["a", "b"])
    # a    9
    # b    8
    # c    3
    # dtype: int64
    # Due to the behavior above, loc setitem can work with any kind of value regardless of length.
    # With __setitem__, the length of the value must match length of the key. Currently, loc setitem can
    # handle this with boolean keys.
    index = ["a", "b", "c", "d", "e", "f"]
    snowpark_ser = pd.Series(list(range(6)), index=index)

    # If item is a Series, item's index doesn't matter/is ignored. It should set the elements at index "b" and "c" to
    # 20 and 30 respectively.
    snowpark_setitem_ser = snowpark_ser.copy()
    snowpark_setitem_ser[["b", "c"]] = pd.Series([20, 30])

    expected = native_pd.Series([0, 20, 30, 3, 4, 5], index=index)

    with SqlCounter(query_count=1):
        assert_series_equal(snowpark_setitem_ser, expected, check_dtype=False)

    expected_with_nan = native_pd.Series([0, None, None, 3, 4, 5], index=index)
    # loc setitem matches the data's index with item's index. So the elements at "b" and "c" are set to NaN.
    snowpark_locset_ser = snowpark_ser.copy()
    snowpark_locset_ser.loc[["b", "c"]] = pd.Series([20, 30], index=["x", "y"])
    with SqlCounter(query_count=1):
        assert_series_equal(snowpark_locset_ser, expected_with_nan, check_dtype=False)

    snowpark_locset_ser = snowpark_ser.copy()
    # This will fail because join string "b", "c" with item's 0, 1 int index is not recognized in Snowflake
    snowpark_locset_ser.loc[["b", "c"]] = pd.Series([20, 30])

    with SqlCounter(query_count=0):
        with pytest.raises(SnowparkSQLException):
            snowpark_locset_ser.to_pandas()

    # However, using a series key with the index set to match the key used, the __setitem__ behavior is replicated.
    snowpark_locset_ser2 = snowpark_ser.copy()
    snowpark_locset_ser2.loc[["b", "c"]] = pd.Series([20, 30], index=["b", "c"])

    # They should be equal.
    with SqlCounter(query_count=1):
        assert_series_equal(snowpark_locset_ser2, expected, check_dtype=False)


@pytest.mark.parametrize("key", EMPTY_LIST_LIKE_VALUES)
@pytest.mark.parametrize(
    "item", EMPTY_LIST_LIKE_VALUES[:-2]
)  # ignore last element: empty series
def test_series_setitem_with_empty_key_and_empty_item_negative(
    key,
    item,
    default_index_native_series,
):
    # series[empty list-like/series key] = empty list-like item
    # ---------------------------------------------------------
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

    if isinstance(key, native_pd.Index):
        snowpark_key = pd.Index(key)
    else:
        snowpark_key = key

    with SqlCounter(query_count=0):

        err_msg = "The length of the value/item to set is empty"
        with pytest.raises(ValueError, match=err_msg):
            native_ser[key] = item
            snowpark_ser[
                pd.Series(snowpark_key)
                if isinstance(snowpark_key, native_pd.Series)
                else snowpark_key
            ] = item
            assert_series_equal(snowpark_ser, native_ser)


@pytest.mark.parametrize("key", EMPTY_LIST_LIKE_VALUES)
def test_series_setitem_with_empty_key_and_empty_series_item(
    key,
    default_index_native_series,
):
    # series[empty list-like/series key] = empty series item
    # ------------------------------------------------------
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

    if isinstance(key, native_pd.Index):
        snowpark_key = pd.Index(key)
    else:
        snowpark_key = key

    with SqlCounter(query_count=1):
        native_ser[key] = item
        snowpark_ser[
            pd.Series(snowpark_key)
            if isinstance(snowpark_key, native_pd.Series)
            else snowpark_key
        ] = pd.Series(item)
        assert_snowpark_pandas_equal_to_pandas(snowpark_ser, native_ser)


@pytest.mark.parametrize("key", EMPTY_LIST_LIKE_VALUES)
def test_series_setitem_with_empty_key_and_scalar_item(
    key,
    default_index_native_series,
):
    # series[empty list-like/series key] = scalar item
    # -------------------------------------------------
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
    # >>> series[[]] = "a"
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

    if isinstance(key, native_pd.Index):
        snowpark_key = pd.Index(key)
    else:
        snowpark_key = key

    with SqlCounter(query_count=1, join_count=2):
        native_ser[key] = item
        snowpark_ser[
            pd.Series(snowpark_key)
            if isinstance(snowpark_key, native_pd.Series)
            else snowpark_key
        ] = item
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snowpark_ser, native_ser
        )


@sql_count_checker(query_count=0, join_count=0)
@pytest.mark.parametrize("key", EMPTY_LIST_LIKE_VALUES)
@pytest.mark.parametrize("item", SERIES_AND_LIST_LIKE_KEY_AND_ITEM_VALUES_NO_DUPLICATES)
def test_series_setitem_with_empty_key_and_series_and_list_like_item_negative(
    key,
    item,
    default_index_native_series,
):
    # series[empty list-like/series key] = list-like/series item
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
    # In Snowpark pandas we raise a ValueError because performing the check on the frontend to mimic pandas' behavior
    # makes the code more complex and there are more cases to handle.

    native_ser = default_index_native_series.copy()
    snowpark_ser = pd.Series(native_ser)

    err_msg = (
        "cannot set using a list-like indexer with a different length than the value"
    )
    with pytest.raises(ValueError, match=err_msg):
        native_ser[try_convert_index_to_native(key)] = item
        snowpark_ser[pd.Series(key) if isinstance(key, native_pd.Series) else key] = (
            pd.Series(item) if isinstance(item, native_pd.Series) else item
        )
        assert_series_equal(snowpark_ser, native_ser)


@pytest.mark.parametrize("index", [True, False], ids=["with_index", "without_index"])
class TestEmptySeries:
    # empty_series
    # ------------
    # An empty series differs based on what its index is.
    #
    # If an index is not specified/default index:
    # >>> series = pd.Series()
    # >>> series
    # Series([], dtype: float64)   <-- the default dtype for Series will be object in the future
    #
    # If an index is specified:
    # >>> series = pd.Series(index=[0, 1, 2])
    # >>> series
    # 0   NaN
    # 1   NaN
    # 2   NaN
    # dtype: float64   <-- the default dtype for Series will be object in the future

    def test_empty_series_setitem_slice(self, index):
        # empty_series[slice key] = scalar item
        # -------------------------------------
        # using key = slice(None), all items should be updated. If no values in the Series are present (default index),
        # the series is unchanged --> remains empty.
        #
        # If an index is not specified/default index:
        # >>> series = pd.Series()
        # >>> series[slice(None)] = 1
        # >>> series
        # Series([], dtype: float64)
        #
        # If an index is specified:
        # >>> series = pd.Series(index=[0, 1, 2])
        # >>> series[slice(None)] = 1
        # >>> series
        # 0    1.0   <-- all the values in the series are changed from NaN to 1.0
        # 1    1.0
        # 2    1.0
        # dtype: float64

        kwargs = {}
        if index:
            kwargs["index"] = [0, 1, 2]
        native_ser = native_pd.Series(**kwargs)
        snow_ser = pd.Series(native_ser)

        def setitem_slice(series):
            series[:] = 1

        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snow_ser,
                native_ser,
                setitem_slice,
                inplace=True,
            )

    @pytest.mark.parametrize("key", [0, 11])
    def test_empty_series_setitem_scalar(self, index, key):
        # empty_series[scalar key] = scalar item
        # --------------------------------------
        # using a scalar key, only the value at the label that matches the scalar key should be updated.
        kwargs = {}
        if index:
            kwargs["index"] = [0, 1, 2]
        native_ser = native_pd.Series(**kwargs)
        snow_ser = pd.Series(native_ser)

        def setitem_scalar(series):
            series[key] = 1

        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snow_ser,
                native_ser,
                setitem_scalar,
                inplace=True,
            )


@pytest.mark.parametrize("data", DATA_FOR_STRING_KEY_TYPE_CHECKING_TESTS)
@pytest.mark.parametrize("item", [12, -8.999])
def test_series_setitem_check_type_behavior_with_string_key_and_number_scalar_item(
    data, item
):
    # series[string key] = int/float item
    # -----------------------------------
    # - If assigning value where a list used to exist, Snowflake will raise an error like: SnowparkSQLException:
    #   "Can not convert parameter '"values"."__reduced___o42q"' of type [ARRAY] into expected type [NUMBER(1,0)]".
    #
    # - If a float item is assigned to a series with only int float values, all the values are converted to very
    #   specific Decimal objects -- precision and number of digits matter with Decimal objects. The values are correct
    #   and the type is maintained in a different format but is ultimately different from native pandas.
    #
    # - Assigning a value to any other type retains the type; no casting.
    #
    # Example:
    # >>> series = pd.Series([["a", 20, 30], 1, 2, "a"], index=["xyz", "A", "B", "C"])
    # >>> series
    # xyz    [a, 20, 30]
    # A                1
    # B                2
    # C                a
    # dtype: object
    # >>> series["xyz"] = 12
    # >>> series
    # xyz    12
    # A       1
    # B       2
    # C       a
    # dtype: object

    # STRING KEY
    key = "xyz"

    # Create a string index - because we need the label "xyz" to be present, Snowflake will try to cast everything to a
    # string type - easier to compare with native pandas if it is a string index.
    index = ["xyz", "A", "B", "C", "D"]
    # Create series with data and the index below
    native_ser = native_pd.Series(data=data, index=index)
    snowpark_ser = pd.Series(native_ser)

    # Check whether all values in the data are lists:
    all_values_are_lists = all(isinstance(val, list) for val in data)

    # Assign item at key and compare
    native_ser[key] = item
    snowpark_ser[key] = item

    if all_values_are_lists:
        # Snowflake will error because it is trying to set an int item to a column of ARRAY type.
        err_msg = "Can not convert parameter"
        with SqlCounter(query_count=0, join_count=0):
            with pytest.raises(SnowparkSQLException, match=err_msg):
                assert_series_equal(snowpark_ser, native_ser, check_dtype=False)
    else:
        # All other cases match native pandas behavior
        with SqlCounter(query_count=1, join_count=1):
            assert_series_equal(snowpark_ser, native_ser, check_dtype=False)


@pytest.mark.parametrize("item", [True, False])
def test_series_setitem_check_type_behavior_with_string_key_and_boolean_scalar_item(
    item,
):
    # series[string key] = True/False item
    # ------------------------------------
    # column = column where item is assigned to.
    # - Success if values can be cast to boolean type.
    #   This is the only successful case. Snowflake tries to cast the values in the "assignee" column to a boolean type;
    #   it works with columns of numeric or string type but fails in all cases where the column type is VARIANT, ARRAY,
    #   and the rest. On success (values can be cast to boolean type), all values except 0 are turned into True
    #   while 0 is turned into False. This behavior is shown in the example in CASE 1 below.
    #
    # In the rest of the cases, Snowflake fails to cast the column type to boolean and raises a SnowparkSQLException:
    # - "Can not convert parameter '"values"."__reduced___agzk"' of type [ARRAY] into expected type [BOOLEAN]" when
    #     the item is being assigned to a spot where a list exists and the series only contains lists.
    #
    # - "Failed to cast variant value [1] to BOOLEAN" when the item is being assigned to a row in a column of type
    #   VARIANT.

    # STRING KEY
    key = "xyz"

    # Create a string index - because we need the label "xyz" to be present, Snowflake will try to cast everything to a
    # string type - easier to compare with native pandas if it is a string index.
    index = ["xyz", "A", "B", "C", "D"]

    # CASE 1: Snowflake casting is successful, numeric or string type columns:
    # ------------------------------------------------------------------------
    data = [0, 1.2, 3, 5, 0]
    # Create series with data and the index below
    native_ser = native_pd.Series(data=data, index=index)
    snowpark_ser = pd.Series(native_ser)

    # Assign item at key
    native_ser[key] = item
    snowpark_ser[key] = item

    # Snowpark pandas does not raise any errors for this case but has weird behavior. Values 0 are set to False and
    # the rest of the values are set to True - the whole series is turned into a boolean series.
    # >>> series = pd.Series([1.3, "a", 23], index=["a", "b", "c"])
    # >>> series["a"] = True
    # >>> series
    # a    True
    # b    True
    # c    True
    # dtype: bool
    with SqlCounter(query_count=1, join_count=1):
        err_msg = "Series are different"
        with pytest.raises(AssertionError, match=err_msg):
            assert_series_equal(snowpark_ser, native_ser, check_dtype=False)

    # Compare with expected results - 0 is turned to False, rest are turned to True.
    expected_data = [False if val == 0 else True for val in native_ser]
    expected_ser = native_pd.Series(data=expected_data, index=index)
    assert_series_equal(snowpark_ser, expected_ser, check_dtype=False)

    # CASE 2: Snowflake casting is unsuccessful, variant column:
    # ----------------------------------------------------------
    data = [1, "a", 2, 3, 4]
    # Create series with data and the index below
    native_ser = native_pd.Series()
    snowpark_ser = pd.Series(data=data, index=index)

    # Assign item at key
    native_ser[key] = item
    snowpark_ser[key] = item

    err_msg = "Failed to cast variant value"
    with SqlCounter(query_count=0, join_count=0):
        with pytest.raises(SnowparkSQLException, match=err_msg):
            assert_series_equal(snowpark_ser, native_ser, check_dtype=False)

    # CASE 3: Snowflake casting is unsuccessful, array column:
    # --------------------------------------------------------
    data = [[0], [1], [2], [3], [4]]
    # Create series with data and the index below
    native_ser = native_pd.Series(data=data, index=index)
    snowpark_ser = pd.Series(native_ser)

    # Assign item at key
    native_ser[key] = item
    snowpark_ser[key] = item

    err_msg = "Can not convert parameter"
    with SqlCounter(query_count=0, join_count=0):
        with pytest.raises(SnowparkSQLException, match=err_msg):
            assert_series_equal(snowpark_ser, native_ser, check_dtype=False)


@pytest.mark.parametrize("data", DATA_FOR_STRING_KEY_TYPE_CHECKING_TESTS)
def test_series_setitem_check_type_behavior_with_string_key_and_string_scalar_item(
    data,
):
    # series[string key] = string item
    # --------------------------------
    # - When the all the values in the series are of int/float type, original value at key is int/float type,
    #   Snowflake assumes that the item passed in is also going to be some number type. It tries to cast the string item
    #   to numeric type and fails with error: SnowparkSQLException: "Numeric value 'string!' is not recognized".
    #
    # - In all other cases, Snowflake's type casting is in the opposite direction: if the item is a string, it will try
    #   to cast the rest of the value in the series to be string as well.
    #
    # - If all the values in series are of string type, this is the only case Snowpark pandas exactly matches native
    #   pandas. Assignment/performing locset is successful only when the original value at key can be cast to string.
    #
    # - In the cases where the value cannot be cast to a string, as in the case of a column of ARRAY type, Snowflake
    #   raises the error: SnowparkSQLException: "Can not convert parameter '"values"."__reduced___jgjz"' of type [ARRAY]
    #   into expected type [VARCHAR(6)]".

    # STRING KEY AND ITEM
    key = "xyz"
    item = "string!"

    # Create a string index - because we need the label "xyz" to be present, Snowflake will try to cast everything to a
    # string type - easier to compare with native pandas if it is a string index.
    index = ["xyz", "A", "B", "C", "D"]
    # Create series with data and the index below
    native_ser = native_pd.Series(data=data, index=index)
    snowpark_ser = pd.Series(native_ser)

    # Check if all the values in the series are numeric
    all_values_are_numeric = all(
        isinstance(val, numbers.Number) and not is_bool(val) for val in data
    )
    # Check if all the values in the series are array-like
    all_values_are_array_like = all(is_list_like(val) for val in data)

    # Assign item at key
    native_ser[key] = item
    snowpark_ser[key] = item

    if all_values_are_numeric:
        with SqlCounter(query_count=0, join_count=0):
            # If all the values in the series are numeric, Snowflake tries to cast the string to a numeric value and fails
            err_msg = re.escape("Numeric value 'string!' is not recognized")
            with pytest.raises(SnowparkSQLException, match=err_msg):
                assert_series_equal(snowpark_ser, native_ser, check_dtype=False)

    elif all_values_are_array_like:
        with SqlCounter(query_count=0, join_count=0):
            # If all the values in the series are array-like, Snowflake tries to cast them to string and fails
            err_msg = re.escape("Can not convert parameter")
            with pytest.raises(SnowparkSQLException, match=err_msg):
                assert_series_equal(snowpark_ser, native_ser, check_dtype=False)

    else:
        # Snowflake successfully casts all the values in the series to string. Native pandas retains the value types.
        err_msg = "Series are different"

        # If the rest of the data apart from value at label is a string type, ignore below error checking.
        will_raise_error = not all(isinstance(val, str) for val in data[1:])
        if will_raise_error:
            with pytest.raises(AssertionError, match=err_msg):
                assert_series_equal(snowpark_ser, native_ser, check_dtype=False)

        expected_data = [str(val) for val in native_ser]
        expected_ser = native_pd.Series(data=expected_data, index=index)
        with SqlCounter(query_count=1, join_count=1):
            assert_series_equal(snowpark_ser, expected_ser, check_dtype=False)


def test_series_setitem_value_length_is_short():
    native_series = native_pd.Series([1, 2, 3, 4], index=pd.Index(["a", "b", "c", "d"]))
    snow_series = pd.Series(native_series)

    with pytest.raises(
        ValueError,
        match="cannot set using a list-like indexer with a different length than the value",
    ):
        native_series[["a", "b", "c"]] = native_pd.Series([0, 1])

    def setitem_helper(series):
        if isinstance(series, native_pd.Series):
            series[["a", "b", "c", "d"]] = native_pd.Series([0, 1, 1, 1])
        else:
            series[["a", "b", "c", "d"]] = pd.Series([0, 1])

    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_series,
            native_series,
            setitem_helper,
            inplace=True,
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
@sql_count_checker(query_count=1, join_count=3)
def test_series_setitem_key_slice_with_series(start, stop, step):
    key = slice(start, stop, step)

    ser_data = ["x", "y", "z", "w", "u"]
    item_data = ["A", "B", "C", "D", "E"]
    idx = [0, 1, 2, 3, 4]
    native_ser = native_pd.Series(ser_data)
    snow_ser = pd.Series(ser_data)

    slice_len = len(native_pd.Series(idx)[key])

    native_item_ser = native_pd.Series(
        item_data[:slice_len],
        dtype="str",
        index=native_pd.Index(idx[:slice_len], dtype="int32"),
    )

    def set_loc_helper(ser):
        ser[key] = (
            pd.Series(native_item_ser)
            if isinstance(ser, pd.Series)
            else native_item_ser
        )

    if step is not None and step < 0:
        set_loc_helper(snow_ser)
        # pandas may fail when step is negative, so we convert slice key to list key to make it work
        key = native_ser.index[key]
        set_loc_helper(native_ser)
        assert_snowpark_pandas_equal_to_pandas(snow_ser, native_ser)

    elif slice_len == 0:
        # pandas can fail in this case, so we skip call loc for it, see more below in
        # test_series_loc_set_key_slice_with_series_item_pandas_bug
        set_loc_helper(snow_ser)
        # snow_ser should not change when slice_len = 0
        assert_snowpark_pandas_equal_to_pandas(snow_ser, native_ser)
    else:
        eval_snowpark_pandas_result(snow_ser, native_ser, set_loc_helper, inplace=True)


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

    data = list(range(len(index)))

    native_ser = native_pd.Series(data, index=index)
    snow_ser = pd.Series(native_ser)

    snow_ser[key] = item

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


def series_setitem_generate_type_behavior_table():
    # Behavior comparison of Snowpark pandas and native pandas
    # --------------------------------------------------------
    # Generate dict which records the behavior of Snowpark pandas and native pandas, with exception details and casting
    # information.
    # Can access values as dict[series][key][item] --> will produce a nested dict

    # List of type of data and data to create series with for behavior comparison with pandas. Any data can be added:
    # - all data below will be used as item types
    # - non-list data will be used as series types.
    # Format: ("type <list>", [data])
    types_and_data = [
        ("int", [23, 0, 9, -2, 100]),
        ("float", [3.14, -0.999, 5.4, 100.001, -27.0008]),
        ("string", ["I", "love", "writing", "Snowpark pandas", "tests!"]),
        (
            "timestamp",
            [
                pd.Timestamp("2017-01-03"),
                pd.Timestamp("2017-01-03"),
                pd.Timestamp("2017-01-03"),
                pd.Timestamp("2018-01-03"),
                pd.Timestamp("2019-01-03"),
            ],
        ),
        ("int list", [[0, 1], [3, 4], [6, 7], [-9, -10], [-12, -13]]),
        (
            "string list",
            [
                ["one", "two", "3"],
                ["four", "five", "6"],
                ["seven", "eight", "9"],
                ["0", "one", "two"],
                ["ten", "11", "12"],
            ],
        ),
        ("float list", [[3.14], [9.8], [6.667], [3.000], [1.414]]),
        (
            "timestamp list",
            [[pd.Timestamp("2017-01-03")], [pd.Timestamp("2017-01-04")]] * 5,
        ),
        # Data below is commented out/unused to reduce the size of dict generated.
        # ("empty list", [[]] * 5),
        # ("None", [None] * 5),
        # ("NaN", [np.nan] * 5),
    ]

    # Index mapping for different key types - used for series generation.
    index_dict = {
        "int": list(range(5)),
        "float": [1.001, 2.002, 3.003, 4.004, 5.005],
        "string": list(str(i) for i in range(5)),
    }

    # The key mapping for different key types.
    keys_dict = {"int": 0, "float": 1.001, "string": "1"}
    # Create a list of data to help with printing.
    table_data = []
    # Remove any list types for series data.
    series_types_and_data = [
        (_type, _data) for _type, _data in types_and_data if "list" not in _type
    ]

    # Try all possible combinations of series, key, and item types to record behavior.

    # Create a dict to hold type behavior data.
    # series_type -> item_type_to_key_dict
    series_type_to_item_dict = {}
    for series_type, series_data in series_types_and_data:

        # item_type -> key_type_to_behavior_dict
        item_type_to_key_dict = {}
        for item_type, item_list in types_and_data:

            # Skip combinations with same series type and item type - behavior should match native pandas.
            if item_type == series_type:
                continue

            # key_type -> (exception type, error message)
            key_type_to_err_msg_dict = {}
            for key_type in ["int"]:
                # , "float", "string"]: not generating data for float and string to reduce dict size.
                # Get the key and corresponding index.
                key = keys_dict[key_type]
                index = index_dict[key_type]

                # Create the series.
                native_ser = native_pd.Series(series_data, index=index)
                snowpark_ser = pd.Series(native_ser)

                # Pick item from the list of items and assign to series.
                item = item_list[2]

                # Record behavior.
                clean_err_msg = "-"
                err_type = "-"
                try:
                    native_ser[key] = item
                    snowpark_ser[key] = item

                    # If a float item is being assigned to an int series, Snowflake converts all series values to a
                    # Decimal object. Convert all these values to float before comparison to see if we have the
                    # correct results.
                    if series_type == "int" and item_type == "float":
                        snowpark_ser = pd.Series([float(val) for val in snowpark_ser])

                    # Compare the Snowpark pandas and native pandas results.
                    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                        snowpark_ser, native_ser
                    )

                except Exception as exception:
                    # Values used to print table.
                    err_msg = str(exception).strip()
                    err_type = type(exception).__name__

                    # Remove the random jargon '__reduced___lhgj' to prevent random output.
                    if "__reduced" in err_msg:
                        red_loc = err_msg.find("__reduced___")
                        err_msg = (
                            err_msg[: red_loc + 12]
                            + err_msg[min(red_loc + 17, len(err_msg) - 1) :]
                        )

                    # If error is known and generates custom error message based on key/item values, truncate it.
                    known_err_msg = [
                        "Can not convert parameter",
                        "Series values are different",
                        "Series.index are different",
                        "Numeric value ",
                        "Failed to cast variant value",
                        "Timestamp '",
                    ]
                    clean_err_msg = err_msg
                    for snippet in known_err_msg:
                        if snippet in err_msg:
                            clean_err_msg = err_msg[err_msg.find(snippet) :]

                else:
                    # Snowpark pandas and native pandas match! Therefore, type is retained.
                    err_type = "-"
                    clean_err_msg = "No error: Matches native pandas."

                finally:
                    # key_type -> (exception type, error message)
                    entry = [err_type, clean_err_msg]
                    key_type_to_err_msg_dict[key_type] = entry

                    # Add extra data to table for printing.
                    row = [series_type, item_type] + entry
                    table_data.append(row)

            # item_type -> key_type_dict
            item_type_to_key_dict[item_type] = key_type_to_err_msg_dict
        # series_type -> item_type_dict
        series_type_to_item_dict[series_type] = item_type_to_key_dict

    # For dict only:
    # print(series_type_to_item_dict)

    # PRINTING
    # --------
    # Table is printed in output window.
    if PRINT_TABLE:
        # Table header.
        col_list = [
            "Series type",
            "Item type",
            "Exception type",
            "Error message",
        ]
        num_cols = len(col_list)

        # Print the header.
        # +-------------+------------+
        # | Series type | Item type  | . . .
        # +-------------+------------+

        # Create the horizontal lines and whitespace in the table based on column width below. For future, key type is 9
        len_name_no_space = [12, 14, 20, 60]

        # Format each column and concatenate them with '+'.
        horizontal_line = "+"  # starting char
        for dash_len in len_name_no_space:
            border_segment = "-" * (dash_len + 2) + "+"
            horizontal_line = horizontal_line + border_segment

        # Create the heading row.
        formatted_row = "| "
        for i in range(num_cols):
            col_name = str(col_list[i])
            format_logic = "{: <" + str(len_name_no_space[i]) + "}"
            table_value = format_logic.format(col_name)
            formatted_row = formatted_row + table_value + " | "
        heading_row = formatted_row

        # Print table name.
        print("\nSeries locset int key and scalar/list item behavior.")
        # Print header.
        print(horizontal_line)
        print(heading_row)
        print(horizontal_line)

        # Print rows.
        for row_data in table_data:
            # Print all data except for the last one - error message.
            formatted_row = "| "
            for i in range(num_cols - 1):
                col_name = str(row_data[i])
                format_logic = "{: <" + str(len_name_no_space[i]) + "}"
                table_value = format_logic.format(col_name)
                formatted_row = formatted_row + table_value + " | "
            # Print first line of error message.
            err_msg = row_data[-1]
            err_msg_format_logic = "{: <" + str(len_name_no_space[-1]) + "}"
            formatted_err_msg = (
                err_msg_format_logic.format(err_msg[: len_name_no_space[-1]]) + " | "
            )
            print(formatted_row + formatted_err_msg)

            # If error message is too long, print multi-line error message.
            if len(err_msg) > len_name_no_space[-1]:  # multi-line
                # Create empty row.
                empty_row = "| "
                for i in range(num_cols - 1):
                    format_logic = "{: <" + str(len_name_no_space[i]) + "}"
                    table_value = format_logic.format("-")
                    empty_row = empty_row + table_value + " | "
                while len(err_msg) > len_name_no_space[-1]:
                    err_msg = err_msg[len_name_no_space[-1] :]
                    formatted_err_msg = (
                        err_msg_format_logic.format(err_msg[: len_name_no_space[-1]])
                        + " | "
                    )
                    print(empty_row + formatted_err_msg)

        # Print table bottom.
        print(horizontal_line)

    # Used for verifying whether recorded table is up-to-date.
    return series_type_to_item_dict


@sql_count_checker(no_check=True)
def test_behavior_table_is_up_to_date():
    # Test that checks whether the behavior table is correct.
    # No SQL count checking is done since this test only verifies whether the contents of BEHAVIOR_TABLE are correct.
    # locset is run to collect the data but the data itself is not verified by this test - it is just recorded. Other
    # locset tests verify locset functionality, this method checks whether the documented behavior is up-to-date.

    series_type_to_item_dict = series_setitem_generate_type_behavior_table()

    # Read the string, use it to index the dict, and compare data.
    # Split string into rows.
    lines_in_table = BEHAVIOR_TABLE.splitlines()

    prev_err_msg = ""  # used for verification
    prev_row_data = ""  # used for the assert error message
    for line in lines_in_table:
        # All data lines begin with | as the column boundary
        clean_row_data = []
        if len(line) > 0 and "|" == line[0]:
            row_data = line.split("| ")
            # Each value in row_data has whitespace and "|" after it - remove it.
            for data in row_data:
                data = data[:-1].strip()
                if data != "":
                    clean_row_data.append(data)
            row_data = clean_row_data

            # If this row is an error msg overflow line, the last entry should be non-empty. All other entries are "-".
            if all(data == "-" for data in row_data[-1]) and len(row_data[-1]) > 0:
                if row_data[-1] not in prev_err_msg:
                    raise AssertionError(
                        f"The error message is different for: {prev_row_data} - update table"
                    )
            else:
                # This row is a new entry. The fields in order are: "Series type", "Item type", "Exception type",
                # and "Error message".
                series_type = row_data[0]
                item_type = row_data[1]
                err_type = row_data[2]
                err_msg = row_data[3]

                key_type = (
                    "int"  # no longer included in the table, here for future reference
                )

                if series_type in ["int", "float", "string"]:
                    # Data for generating error messages.
                    curr_row_data = {
                        "series_type": series_type,
                        "item_type": item_type,
                        "key_type": key_type,
                    }
                    (expected_err_type, expected_err_msg,) = series_type_to_item_dict[
                        series_type
                    ][item_type][key_type]

                    # Verify that table and new data values match.
                    if err_type != expected_err_type:
                        raise AssertionError(
                            f"Exception raised: table: {err_type}, expected: {expected_err_type} for {curr_row_data}."
                        )

                    # The error message for each combination is truncated over multiple rows, check whether all parts
                    # of the error message match, through existing row traversal.
                    if err_msg not in expected_err_msg:
                        raise AssertionError(
                            f"Error message is wrong for {curr_row_data}. table: {err_msg}, "
                            f"expected: {expected_err_msg[:100]}"
                            + (". . ." if len(expected_err_msg) > 100 else "")
                        )
                        prev_row_data = curr_row_data
                        prev_err_msg = expected_err_msg


@sql_count_checker(query_count=2, join_count=2)
def test_series_setitem_int_key():
    # pandas series setitem with int key is similar to loc set in most cases:
    # E.g., set index with label 3 to 100
    native_s = native_pd.Series([0, 0, 0], index=[1, 2, 3])
    native_s[3] = 100
    assert_series_equal(native_s, native_pd.Series([0, 0, 100], index=[1, 2, 3]))

    # E.g., set index with label 5 to 100, since 5 is not in the existing indices, enlargement is allowed.
    native_s = native_pd.Series([0, 0, 0], index=[1, 2, 3])
    native_s[5] = 100
    assert_series_equal(native_s, native_pd.Series([0, 0, 0, 100], index=[1, 2, 3, 5]))

    # If the int key does not exist and is a valid position, it will be treated as
    # an iloc set and set the element at the specified position. However, if the index is a float64,
    # as of pandas 2.X, the series will be enlarged with the new key added.
    native_s = native_pd.Series([0, 0, 0], index=["1", "2", "3"])
    native_s[0] = 100
    assert_series_equal(native_s, native_pd.Series([100, 0, 0], index=["1", "2", "3"]))

    native_s = native_pd.Series([0, 0, 0], index=[1.1, 1.2, 1.3])
    native_s[0] = 100
    assert_series_equal(
        native_s, native_pd.Series([0, 0, 0, 100], index=[1.1, 1.2, 1.3, 0])
    )

    native_s = native_pd.Series([0, 0, 0], index=[1, 2, 3])
    native_s[0] = 100
    assert_series_equal(native_s, native_pd.Series([0, 0, 0, 100], index=[1, 2, 3, 0]))

    # Snowpark pandas always treats series setitem as loc set. It matches the behavior of the future version of pandas.
    s = pd.Series([0, 0, 0], index=[1, 2, 3])
    s[0] = 100
    assert_snowpark_pandas_equal_to_pandas(
        s, native_pd.Series([0, 0, 0, 100], index=[1, 2, 3, 0]), check_dtype=False
    )

    s = pd.Series([0, 0, 0], index=[1.1, 2.2, 3.3])
    s[0] = 100
    assert_snowpark_pandas_equal_to_pandas(
        s, native_pd.Series([0, 0, 0, 100], index=[1.1, 2.2, 3.3, 0]), check_dtype=False
    )
