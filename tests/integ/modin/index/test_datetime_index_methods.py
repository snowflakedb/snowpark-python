#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import (
    assert_frame_equal,
    assert_index_equal,
    assert_series_equal,
    eval_snowpark_pandas_result,
)


@sql_count_checker(query_count=0)
def test_datetime_index_construction():
    # create from native pandas datetime index.
    index = native_pd.DatetimeIndex(["2021-01-01", "2021-01-02", "2021-01-03"])
    snow_index = pd.Index(index)
    assert isinstance(snow_index, pd.DatetimeIndex)

    # create from query compiler with timestamp type.
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=index)
    snow_index = df.index
    assert isinstance(snow_index, pd.DatetimeIndex)

    # create from snowpark pandas datetime index.
    snow_index = pd.Index(pd.DatetimeIndex([123]))
    assert isinstance(snow_index, pd.DatetimeIndex)


@pytest.mark.parametrize("data", [[1, 99], [21.3, 2.1], ["2014-09-01", "2021-01-01"]])
@sql_count_checker(query_count=1)
def test_datetime_index_construction_from_other_types(data):
    snow_index = pd.DatetimeIndex(pd.Index(data))
    native_index = native_pd.DatetimeIndex(native_pd.Index(data))
    assert_index_equal(snow_index, native_index)


@pytest.mark.parametrize(
    "data", [native_pd.Index([True, False]), native_pd.to_timedelta(["1d", "2d"])]
)
@sql_count_checker(query_count=0)
def test_datetime_index_construction_from_other_types_negative(data):
    msg = re.escape(f"dtype {data.dtype} cannot be converted to datetime64[ns]")
    with pytest.raises(TypeError, match=msg):
        pd.DatetimeIndex(pd.Index(data))
    with pytest.raises(TypeError, match=msg):
        native_pd.DatetimeIndex(data)


@sql_count_checker(query_count=0)
def test_datetime_index_construction_negative():
    # Try to create datatime index query compiler with int type.
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    msg = "DatetimeIndex can only be created from a query compiler with TimestampType"
    with pytest.raises(ValueError, match=msg):
        pd.DatetimeIndex(query_compiler=df._query_compiler)


@sql_count_checker(query_count=0)
@pytest.mark.parametrize(
    "kwargs",
    [
        {"freq": "M"},
        {"tz": "UTC"},
        {"normalize": True},
        {"closed": "left"},
        {"ambiguous": "infer"},
        {"dayfirst": True},
        {"yearfirst": True},
        {"dtype": "int"},
        {"copy": True},
        {"name": "abc"},
    ],
)
def test_non_default_args(kwargs):
    idx = pd.DatetimeIndex(["2014-01-01 10:00:00"])

    name = list(kwargs.keys())[0]
    value = list(kwargs.values())[0]
    msg = f"Non-default argument '{name}={value}' when constructing Index with query compiler"
    with pytest.raises(AssertionError, match=msg):
        pd.DatetimeIndex(query_compiler=idx._query_compiler, **kwargs)


@sql_count_checker(query_count=6)
def test_index_parent():
    """
    Check whether the parent field in Index is updated properly.
    """
    native_idx1 = native_pd.DatetimeIndex(["2024-01-01"], name="id1")
    native_idx2 = native_pd.DatetimeIndex(["2024-01-01", "2024-01-02"], name="id2")

    # DataFrame case.
    df = pd.DataFrame({"A": [1]}, index=native_idx1)
    snow_idx = df.index
    assert_frame_equal(snow_idx._parent._parent, df)
    assert_index_equal(snow_idx, native_idx1)

    # Series case.
    s = pd.Series([1, 2], index=native_idx2, name="zyx")
    snow_idx = s.index
    assert_series_equal(snow_idx._parent._parent, s)
    assert_index_equal(snow_idx, native_idx2)


dt_properties = pytest.mark.parametrize(
    "property_name",
    [
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "second",
        "microsecond",
        "nanosecond",
        "date",
        "time",
        "dayofyear",
        "day_of_year",
        "dayofweek",
        "day_of_week",
        "weekday",
        "quarter",
        "is_month_start",
        "is_month_end",
        "is_quarter_start",
        "is_quarter_end",
        "is_year_start",
        "is_year_end",
        "is_leap_year",
    ],
)


def _dt_property_comparator(snow_res, native_res):
    # Native pandas returns a list for few properties like date, is_*_start,
    # is_*_end.
    if not isinstance(native_res, native_pd.Index):
        native_res = native_pd.Index(native_res)
        # Replace NaT with None for comparison.
        native_res = native_res.where(native_res.notnull(), None)
    assert_index_equal(snow_res, native_res, exact=False)


@dt_properties
@sql_count_checker(query_count=1)
def test_dt_property_with_tz(property_name):
    native_index = native_pd.DatetimeIndex(
        [
            "2014-04-04 23:56:01.000000001",
            "2014-07-18 21:24:02.000000002",
            "2015-11-22 22:14:03.000000003",
            "2015-11-23 20:12:04.1234567890",
            pd.NaT,
        ],
        tz="US/Eastern",
    )
    snow_index = pd.DatetimeIndex(native_index)
    eval_snowpark_pandas_result(
        snow_index,
        native_index,
        lambda i: getattr(i, property_name),
        comparator=_dt_property_comparator,
    )


@dt_properties
@pytest.mark.parametrize(
    "freq", ["d", "h", "min", "s", "y", "m", "D", "3m", "ms", "us", "ns"]
)
@sql_count_checker(query_count=1)
def test_dt_properties(property_name, freq):
    native_index = native_pd.date_range(start="2021-01-01", periods=5, freq=freq)
    native_index = native_index.append(native_pd.DatetimeIndex([pd.NaT]))
    snow_index = pd.DatetimeIndex(native_index)

    eval_snowpark_pandas_result(
        snow_index,
        native_index,
        lambda i: getattr(i, property_name),
        comparator=_dt_property_comparator,
    )


@pytest.mark.parametrize("property", ["timetz", "tz", "freqstr", "inferred_freq"])
@sql_count_checker(query_count=0)
def test_dt_property_not_implemented(property):
    snow_index = pd.DatetimeIndex(["2021-01-01", "2021-01-02", "2021-01-03"])
    msg = f"Snowpark pandas does not yet support the property DatetimeIndex.{property}"
    with pytest.raises(NotImplementedError, match=msg):
        getattr(snow_index, property)


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("method", ["day_name", "month_name"])
def test_day_month_name(method):
    native_index = native_pd.date_range("2020-05-01", periods=5, freq="17D")
    snow_index = pd.date_range("2020-05-01", periods=5, freq="17D")
    eval_snowpark_pandas_result(
        snow_index,
        native_index,
        lambda s: getattr(s, method)(),
    )


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("method", ["day_name", "month_name"])
def test_day_month_name_negative(method):
    snow_index = pd.date_range("2020-05-01", periods=5, freq="17D")
    msg = f"Snowpark pandas method DatetimeIndex.{method} does not yet support the 'locale' parameter"
    with pytest.raises(NotImplementedError, match=msg):
        getattr(snow_index, method)(locale="pt_BR.utf8")


@sql_count_checker(query_count=1)
def test_normalize():
    native_index = native_pd.date_range(start="2021-01-01", periods=5, freq="7h")
    native_index = native_index.append(native_pd.DatetimeIndex([pd.NaT]))
    snow_index = pd.DatetimeIndex(native_index)
    eval_snowpark_pandas_result(
        snow_index,
        native_index,
        lambda i: i.normalize(),
    )


@pytest.mark.parametrize(
    "datetime_index_value",
    [
        ["2014-04-04 23:56:20", "2014-07-18 21:24:30", "2015-11-22 22:14:40"],
        ["04/04/2014", "07/18/2013", "11/22/2015"],
        ["2014-04-04 23:56", pd.NaT, "2014-07-18 21:24", "2015-11-22 22:14", pd.NaT],
        [
            pd.Timestamp(2017, 1, 1, 12),
            pd.Timestamp(2018, 2, 1, 10),
            pd.Timestamp(2000, 2, 1, 10),
        ],
    ],
)
@pytest.mark.parametrize("func", ["round", "floor", "ceil"])
@pytest.mark.parametrize("freq", ["1d", "2d", "1h", "2h", "1min", "2min", "1s", "2s"])
def test_floor_ceil_round(datetime_index_value, func, freq):
    native_index = native_pd.DatetimeIndex(datetime_index_value)
    snow_index = pd.DatetimeIndex(native_index)
    if func == "round" and "s" in freq:
        with SqlCounter(query_count=0):
            msg = f"Snowpark pandas method DatetimeIndex.round does not yet support the 'freq={freq}' parameter"
            with pytest.raises(NotImplementedError, match=msg):
                snow_index.round(freq=freq)
    else:
        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snow_index, native_index, lambda i: getattr(i, func)(freq)
            )


@pytest.mark.parametrize("func", ["floor", "ceil", "round"])
@pytest.mark.parametrize(
    "freq, ambiguous, nonexistent",
    [
        ("1w", "raise", "raise"),
        ("1h", "infer", "raise"),
        ("1h", "NaT", "raise"),
        ("1h", np.array([True, True, False]), "raise"),
        ("1h", "raise", "shift_forward"),
        ("1h", "raise", "shift_backward"),
        ("1h", "raise", "NaT"),
        ("1h", "raise", pd.Timedelta("1h")),
        ("1w", "infer", "shift_forward"),
    ],
)
@sql_count_checker(query_count=0)
def test_floor_ceil_round_negative(func, freq, ambiguous, nonexistent):
    datetime_index_value = [
        "2014-04-04 23:56",
        pd.NaT,
        "2014-07-18 21:24",
        "2015-11-22 22:14",
        pd.NaT,
    ]
    native_index = native_pd.DatetimeIndex(datetime_index_value)
    snow_index = pd.DatetimeIndex(native_index)
    msg = f"Snowpark pandas method DatetimeIndex.{func} does not yet support"
    with pytest.raises(NotImplementedError, match=msg):
        getattr(snow_index, func)(
            freq=freq, ambiguous=ambiguous, nonexistent=nonexistent
        )
