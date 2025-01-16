#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import assert_index_equal
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize(
    "index, type",
    [
        # convert int to type
        (native_pd.Index([1, 2, 3, 4], dtype=np.int64), np.float64),
        (native_pd.Index([1, 2, 3, 4], dtype=np.int64, name="imaginary numbers"), bool),
        (native_pd.Index([10, 20, 90, 80], dtype=np.int64), str),
        # convert float to type
        (native_pd.Index([1.2, 2, -0.1113, 4.07], dtype=float), np.int64),
        (native_pd.Index([1.111, 2.222, -3.3333, -4.4444, 0], dtype=np.float64), bool),
        # convert str to type
        (
            native_pd.Index(["a", "b", "c"], dtype=str, name="pandas.Index the Great!"),
            "object",
        ),
        (native_pd.Index(["1", "2", "3"], dtype="string"), np.int64),
        (native_pd.Index(["-2", "-5.6", "-0.008"], dtype="string"), np.float64),
        (native_pd.Index(["True", "False", "True", "False"], dtype=str), bool),
        (native_pd.Index(["a", "b", "c", None], dtype=str), bool),
        # convert bool to type
        (native_pd.Index([True, False, True, False], dtype=bool), str),
        (native_pd.Index([True, True, True, False], dtype=bool), np.int8),
        (native_pd.Index([True, False, False, False], dtype=bool), np.float64),
        # convert object to type
        (native_pd.Index(["a", "b", "c", 1, 2, 4], dtype="O"), str),
        (native_pd.Index([1, 2, 3, 4], dtype="O"), np.int64),
        (native_pd.Index([1.11, 2.1111, 3.0002, 4.111], dtype=object), np.float64),
        (native_pd.Index(["2024-01-01 10:00:00"], dtype=object), "datetime64[ns]"),
    ],
)
def test_index_astype(index, type):
    snow_index = pd.Index(index)
    with SqlCounter(query_count=1):
        assert_index_equal(snow_index.astype(type), index.astype(type))


@pytest.mark.parametrize(
    "index, type",
    [
        # convert int to type
        (native_pd.Index([1, 2, 3, 4, 5], dtype=np.int64), np.float64),
        (
            native_pd.Index([1, 2, 3, 4, 5], dtype=np.int64, name="imaginary numbers"),
            bool,
        ),
        (native_pd.Index([10, 20, 90, 80, 20], dtype=np.int64), str),
        # convert float to type
        (native_pd.Index([1.2, 2, -0.1113, 4.07, 0.111], dtype=np.float64), np.int64),
        (native_pd.Index([1.111, 2.222, -3.3333, -4.4444, 0], dtype=np.float64), bool),
        # convert str to type
        (
            native_pd.Index(
                ["a", "b", "c", "d", "e"], dtype=str, name="pandas.Index the Great!"
            ),
            "object",
        ),
        (native_pd.Index(["1", "2", "3", "4", "5"], dtype="string"), np.int64),
        (
            native_pd.Index(["-2", "-5.6", "-0.008", "3.14", "-6.28"], dtype="string"),
            np.float64,
        ),
        (native_pd.Index(["True", "False", "True", "True", "False"], dtype=str), bool),
        (native_pd.Index(["a", "b", "c", None, "d"], dtype=str), bool),
        # convert bool to type
        (native_pd.Index([True, False, True, False, False], dtype=bool), str),
        (native_pd.Index([True, True, True, False, False], dtype=bool), np.int8),
        (native_pd.Index([True, False, False, False, True], dtype=bool), np.float64),
        # convert object to type
        (native_pd.Index(["a", "b", "c", 1, 2], dtype="O"), str),
        (native_pd.Index([1, 2, 3, 4, 5], dtype="O"), np.int64),
        (
            native_pd.Index([1.11, 2.1111, 3.0002, 4.111, 5.001], dtype=object),
            np.float64,
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_index_df_columns_astype(index, type):
    native_df = native_pd.DataFrame([[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]], columns=index)
    snow_df = pd.DataFrame(native_df)
    native_index = native_df.columns.astype(type)
    snow_index = snow_df.columns.astype(type)
    assert_index_equal(snow_index, native_index)


@pytest.mark.parametrize("from_type", [str, np.int64, np.float64, object, bool])
@pytest.mark.parametrize("to_type", [str, np.int64, np.float64, object, bool])
def test_index_astype_empty_index(from_type, to_type):
    native_index = native_pd.Index([], dtype=from_type)
    snow_index = pd.Index(native_index)
    with SqlCounter(query_count=1):
        # exact=False is used because of a discrepancy in the "inferred_type" attribute
        # when to_type is bool between Snowpark pandas (empty) and native pandas (bool).
        assert_index_equal(
            snow_index.astype(to_type), native_index.astype(to_type), exact=False
        )


@pytest.mark.parametrize(
    "index, type, err_msg",
    [
        (
            native_pd.Index(["1", "2", "not a number", "a"], dtype=str),
            float,
            "Numeric value 'not a number' is not recognized",
        ),
        (
            native_pd.Index(["a", "b", "c"], dtype=str, name="pandas.Index the Great!"),
            int,
            "Numeric value 'a' is not recognized",
        ),
        (
            native_pd.Index(
                ["a", "b", "c", 1, 2, 4], dtype="O", name="pandas.Index the Great!"
            ),
            int,
            'Failed to cast variant value "a" to FIXED',
        ),
        (
            native_pd.Index([True, "b", "c", 1, 2, None, 4], dtype="O"),
            bool,
            'Failed to cast variant value "b" to BOOLEAN',
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_index_astype_negative(index, type, err_msg):
    # The first three cases raise a ValueError in pandas but the last case works in pandas.
    snow_index = pd.Index(index)
    with pytest.raises(SnowparkSQLException, match=err_msg):
        print(snow_index.astype(type))


@sql_count_checker(query_count=2)
def test_index_astype_float_rounding_behavior_difference():
    # Snowflake rounds a float to the closest integer but pandas always towards 0.
    native_index = native_pd.Index(
        [1.2, 2, -0.1113, 4.5197, -9.0009, -9.999], dtype=float
    )
    snow_index = pd.Index(native_index)
    with pytest.raises(AssertionError):
        assert_index_equal(snow_index.astype(np.int64), native_index.astype(np.int64))
    # The result in native pandas is [1, 2, 0, 5, -9, -10]
    # but in Snowpark pandas is [1, 2, 0, 4, -9, -9]
    expected_result = native_pd.Index([1, 2, 0, 5, -9, -10], dtype=np.int64)
    assert_index_equal(snow_index.astype(np.int64), expected_result)


@sql_count_checker(query_count=4)
def test_index_astype_bool_nan_none():
    # In Snowflake, np.nan and None are treated as NULL values. In pandas, np.nan is treated
    # as a float value and None is treated as a NULL value. This causes a discrepancy when an
    # Index is being converted from any type to bool.
    native_index = native_pd.Index(["a", "b", "c", None, np.nan], dtype=str)
    snow_index = pd.Index(native_index)
    with pytest.raises(AssertionError):
        assert_index_equal(snow_index.astype(bool), native_index.astype(bool))
    expected_result = native_pd.Index([True, True, True, False, False], dtype=bool)
    assert_index_equal(snow_index.astype(bool), expected_result)

    # Another case where this arises is when a float Index with "None" in it is used. pandas
    # converts None to NaN during Index creation and thus leads to this difference.
    native_index = native_pd.Index(
        [1.111, 2.222, -3.3333, -4.4444, None, 0], dtype=float
    )
    snow_index = pd.Index(native_index)
    with pytest.raises(AssertionError):
        assert_index_equal(snow_index.astype(bool), native_index.astype(bool))
    expected_result = native_pd.Index(
        [True, True, True, True, False, False], dtype=bool
    )
    assert_index_equal(snow_index.astype(bool), expected_result)


@sql_count_checker(query_count=2)
def test_index_astype_float_to_string():
    native_index = native_pd.Index([1, 2, 3.4, 4], dtype=float)
    snow_index = pd.Index(native_index)
    # When astype() is called on the native pandas Index, the index values
    # are first converted to float, and these float values are turned into
    # strings.
    # The result in native pandas is ["1.0", "2.0", "3.4", "4.0"]
    # but in Snowpark pandas is ["1", "2", "3.4", "4"]
    with pytest.raises(AssertionError):
        assert_index_equal(snow_index.astype(str), native_index.astype(str))
    expected_result = native_pd.Index(["1", "2", "3.4", "4"], dtype=str)
    assert_index_equal(snow_index.astype(str), expected_result)


@sql_count_checker(query_count=1)
def test_index_astype_failure_snow_1480906():
    # TODO: SNOW-1480906 - ticket tracks this specific issue.
    # TODO: SNOW-1514565 - this bug is most likely caused due to a known issue with apply.
    native_index = native_pd.Index([1, 2, 3], dtype=int)
    snow_index = pd.Index(native_index)
    with pytest.raises(AssertionError):
        assert_index_equal(snow_index.astype("object"), native_index.astype("object"))
    assert snow_index.dtype == np.int64
