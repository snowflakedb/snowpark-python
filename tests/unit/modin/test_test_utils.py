#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import contextlib
from decimal import Decimal

import numpy as np
import pandas as native_pd
import pytest
from pandas.io.formats.printing import PrettyDict
from pytest import param

from tests.integ.modin.utils import assert_dicts_equal, assert_values_equal


class A:
    def __lt__(self, other):
        return False

    def __eq__(self, other):
        return True

    def __hash__(self):
        return id(self)

    def __repr__(self):
        # Always give the same __repr__. Otherwise, each pytest-xdist process
        # gets its own A() object with a different repr like
        # "test_test_utils.A object at 0x7ff7008d8880" and pytest-xdist cannot
        # run the tests in parallel (see SNOW-1000116).
        return "a_object"


a_object = A()
numpy_nan_variable = np.nan
float_nan_variable = float("nan")


@pytest.mark.parametrize(
    "actual,expected,expect_equals",
    [
        ({0: 1}, PrettyDict({0: 1}), False),
        (PrettyDict({0: 1}), {0: 1}, False),
        ({0: 1}, {}, False),
        (
            {1: native_pd.DataFrame([[1, 2], [3, 4]]), 2: "string"},
            {1: native_pd.DataFrame([[1, 2], [3, 4]]), 2: "string"},
            True,
        ),
        ({1: native_pd.Series([1, 2])}, {1: native_pd.Series([1, 2])}, True),
        ({1: native_pd.Series([1, 2])}, {1: native_pd.DataFrame([1, 2])}, False),
        (
            # Ignore dtypes on index objects
            {1: native_pd.Index([1, 2], dtype="float64")},
            {1: native_pd.Index([1, 2], dtype="int64")},
            True,
        ),
        ({1: native_pd.Index([1])}, {1: native_pd.Index([1])}, True),
        # begin nans in values tests
        ({1: None}, {1: np.nan}, False),
        ({1: np.nan}, {1: np.nan}, True),
        ({1: float_nan_variable}, {1: numpy_nan_variable}, True),
        ({1: numpy_nan_variable}, {1: float_nan_variable}, True),
        ({1: numpy_nan_variable}, {1: numpy_nan_variable}, True),
        ({1: float_nan_variable}, {1: float_nan_variable}, True),
        ({1: native_pd.Index([None])}, {1: native_pd.Index([None])}, True),
        ({1: native_pd.Index([np.nan])}, {1: native_pd.Index([np.nan])}, True),
        # Ignore dtypes on index objects
        ({1: native_pd.Index([np.nan])}, {1: native_pd.Index([None])}, True),
        # end nans in values tests
        param({1: "a", 2: "b"}, {2: "b", 1: "a"}, False, id="keys_out_of_order"),
        # begin cases with nan and None keys.
        ({numpy_nan_variable: 3}, {numpy_nan_variable: 3}, True),
        ({np.nan: 3}, {np.nan: 3}, True),
        # Compare float_nan_variable to itself, but also compare
        # float('nan') to another float('nan') in another test case,
        # because float('nan') is not float('nan') but float_nan_variable
        # is float_nan_variable
        ({float_nan_variable: 3}, {float_nan_variable: 3}, True),
        ({float("nan"): 3}, {float("nan"): 3}, True),
        ({None: 0}, {None: 0}, True),
        # end cases with nan and None keys.
        # this is a pathological case to catch comparing
        # dictionaries with itertools.zip_longest() without
        # passing fillval, or using zip() without strict=True.
        # The shorter dictionary's items() gets padded with `None`,
        # which is equal to A(). This test failed at
        # commit eb2eaafbc247161239ba5b18c60d86a3270aa930
        ({None: 0, a_object: 0}, {None: 0}, False),
        # begin cases with wrong types
        (dict(), 1, False),
        (dict(), dict(), True),
        (dict(), [], False),
        ([], dict(), False),
        # end cases with wrong types
    ],
    ids=lambda v: str(v),
)
def test_assert_dicts_equal(actual, expected, expect_equals):
    with contextlib.nullcontext() if expect_equals else pytest.raises(AssertionError):
        assert_dicts_equal(actual, expected)


@pytest.mark.parametrize(
    "actual,expected,expect_equals",
    [
        (1, native_pd.DataFrame([1]), False),
        (native_pd.Series(1), native_pd.DataFrame([1]), False),
        (native_pd.DataFrame([1], dtype=object), native_pd.DataFrame([1]), False),
        (native_pd.DataFrame([1]), native_pd.DataFrame([1]), True),
        (native_pd.DataFrame([1, 2]), native_pd.DataFrame([1, 2]), True),
        (native_pd.DataFrame([None]), native_pd.DataFrame([None]), True),
        (native_pd.DataFrame([None]), native_pd.DataFrame([np.nan]), False),
        (native_pd.DataFrame([np.nan]), native_pd.DataFrame([None]), False),
        (native_pd.DataFrame([]), native_pd.DataFrame([None]), False),
        (native_pd.DataFrame([None]), native_pd.DataFrame([]), False),
        (native_pd.DataFrame(), native_pd.DataFrame(), True),
        (1, native_pd.Series(1), False),
        (native_pd.Series(1, dtype=object), native_pd.Series(1), False),
        (native_pd.Series(1), native_pd.Series(1), True),
        (native_pd.Series([1, None]), native_pd.Series([1, None]), True),
        (
            native_pd.Index([1, 2], dtype="float64"),
            native_pd.Index([1, 2], dtype="int64"),
            False,
        ),
        (native_pd.Index([1, 2]), native_pd.Index([3]), False),
        (np.array([1]), 1, False),
        (1, np.array([1]), False),
        (np.array([1, np.nan, 2]), np.array([1.00, np.nan, 2.0]), True),
        (1, [1], False),
        ([1, 2], [1, 2], True),
        ([1, None], [1], False),
        ([1, None], [1, None], True),
        ((1, float("nan")), (1, float("nan")), True),
        ((), [], False),
        ((1, 2), [1, 2], False),
        ({}, (), False),
        ({1, 2}, {2, 1}, True),
        ({1}, {}, False),
        (None, None, True),
        (np.nan, np.nan, True),
        (native_pd.NaT, native_pd.NaT, True),
        (float("nan"), float("nan"), True),
        (None, np.nan, False),
        (np.nan, None, False),
        (None, float("nan"), False),
        (float("nan"), None, False),
        (native_pd.NA, np.nan, False),
        (np.nan, native_pd.NA, False),
        (native_pd.NaT, np.nan, False),
        (np.nan, native_pd.NaT, False),
        (1, 1.0, True),
        (1.0, 1, True),
        (1.5, 1.5, True),
        (1.5, 1.6, False),
        (Decimal("1e-20"), 0, False),
        (0, Decimal("1e-20"), False),
        (Decimal("1e-20"), Decimal("1e-20"), True),
        ("ac", "bc", False),
        ("ac", "ac", True),
        (native_pd.Timestamp(1), native_pd.Timestamp(1), True),
        (
            native_pd.Timestamp(1, tz="America/Los_Angeles"),
            native_pd.Timestamp(1, tz="America/Los_Angeles"),
            True,
        ),
        (
            native_pd.Timestamp(1),
            native_pd.Timestamp(1, tz="America/Los_Angeles"),
            False,
        ),
        (
            native_pd.Timestamp(1, tz="America/Los_Angeles"),
            native_pd.Timestamp(1),
            False,
        ),
        (native_pd.Timedelta(days=1), native_pd.Timedelta(days=1), True),
        (native_pd.Timedelta(days=1), native_pd.Timedelta(days=2), False),
    ],
    ids=lambda v: str(v),
)
def test_assert_values_equal(actual, expected, expect_equals):
    with contextlib.nullcontext() if expect_equals else pytest.raises(AssertionError):
        assert_values_equal(actual, expected)
