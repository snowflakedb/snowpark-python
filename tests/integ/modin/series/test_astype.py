#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import re
from datetime import date, time
from itertools import product

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas import DatetimeTZDtype
from pandas.core.arrays.boolean import BooleanDtype
from pandas.core.arrays.floating import Float32Dtype, Float64Dtype
from pandas.core.arrays.integer import (
    Int8Dtype,
    Int16Dtype,
    Int32Dtype,
    Int64Dtype,
    UInt8Dtype,
    UInt16Dtype,
    UInt32Dtype,
    UInt64Dtype,
)
from pandas.core.arrays.string_ import StringDtype
from pandas.core.dtypes.common import is_extension_array_dtype

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin._internal.type_utils import (
    NUMPY_SNOWFLAKE_TYPE_PAIRS,
    PANDAS_EXT_SNOWFLAKE_TYPE_PAIRS,
    TypeMapper,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.types import _FractionalType, _IntegralType
from tests.integ.modin.utils import (
    assert_series_equal,
    assert_snowpark_pandas_equal_to_pandas,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


def basic_types():
    supported_dtypes = [
        "boolean" if isinstance(p[0], BooleanDtype) else p[0]
        for p in (NUMPY_SNOWFLAKE_TYPE_PAIRS + PANDAS_EXT_SNOWFLAKE_TYPE_PAIRS)
        if p[0] != np.object_
    ]
    # include python types
    supported_dtypes += [int, float, str]
    return supported_dtypes


BASIC_ASTYPE_CASES = list(product(basic_types(), basic_types()))


EXTENSION_TYPE_TO_NUMPY_DTYPE = {
    "boolean": np.bool_,
    Float32Dtype(): np.float64,
    Float64Dtype(): np.float64,
    Int64Dtype(): np.int64,
    UInt64Dtype(): np.uint64,
    Int32Dtype(): np.int32,
    UInt32Dtype(): np.uint32,
    Int16Dtype(): np.int16,
    UInt16Dtype(): np.uint16,
    Int8Dtype(): np.int8,
    UInt8Dtype(): np.uint8,
    StringDtype(): np.object_,
}


def get_expected_dtype(to_dtype):
    # special handling for expected dtype, i.e, the expected result from series.dtype
    if to_dtype is str:
        expected_dtype = np.object_
    elif is_extension_array_dtype(to_dtype):
        expected_dtype = EXTENSION_TYPE_TO_NUMPY_DTYPE[to_dtype]
    else:
        expected_dtype = to_dtype
    expected_dtype = np.dtype(expected_dtype)
    if isinstance(TypeMapper.to_snowflake(expected_dtype), _IntegralType):
        expected_dtype = np.dtype("int64")
    elif isinstance(TypeMapper.to_snowflake(expected_dtype), _FractionalType):
        expected_dtype = np.dtype("float64")
    return expected_dtype


def get_expected_to_pandas_dtype(to_dtype, expected_dtype):
    if to_dtype is str or to_dtype == "string[python]":
        to_pandas_dtype = str
    else:
        to_pandas_dtype = expected_dtype
    return to_pandas_dtype


@pytest.mark.parametrize("from_dtype, to_dtype", BASIC_ASTYPE_CASES)
def test_astype_basic(from_dtype, to_dtype):
    seed = [0, 1, 2, 3]
    if from_dtype == "boolean":
        # seed used for boolean
        seed = [True, False, False, True]

    expected_dtype = get_expected_dtype(to_dtype)

    # verify snowpark pandas raise similar exceptions as pandas in the following cases
    type_error_msg = "cannot be converted"
    if (
        isinstance(from_dtype, StringDtype) or from_dtype is str
    ) and to_dtype == "datetime64[ns]":
        with SqlCounter(query_count=1):
            # Snowpark pandas use Snowflake auto format detection and the behavior can be different from native pandas
            assert_snowpark_pandas_equal_to_pandas(
                pd.Series(seed, dtype=from_dtype).astype(to_dtype),
                native_pd.Series(
                    [
                        native_pd.Timestamp("1970-01-01 00:00:00"),
                        native_pd.Timestamp("1970-01-01 00:00:01"),
                        native_pd.Timestamp("1970-01-01 00:00:02"),
                        native_pd.Timestamp("1970-01-01 00:00:03"),
                    ]
                ),
            )
            with pytest.raises(ValueError, match="day is out of range for month"):
                # use dypte=str instead of StringDType since the SP fallback will create the original dataframe in str
                native_pd.Series(seed, dtype=str).astype(expected_dtype)
    elif from_dtype == "datetime64[ns]" and "float" in str(to_dtype).lower():
        with SqlCounter(query_count=0):
            with pytest.raises(TypeError, match=type_error_msg):
                pd.Series(seed, dtype=from_dtype).astype(to_dtype).to_pandas()
            with pytest.raises(TypeError, match="Cannot cast"):
                native_pd.Series(seed, dtype=from_dtype).astype(expected_dtype)
    else:
        with SqlCounter(query_count=1):
            # verify snowpark pandas series.dtype result
            s = pd.Series(seed, dtype=from_dtype).astype(to_dtype)
            assert s.dtype == expected_dtype

            # verify snowpark pandas series.to_pandas() result
            expected_to_pandas = native_pd.Series(seed, dtype=from_dtype).astype(
                get_expected_to_pandas_dtype(to_dtype, expected_dtype)
            )
            if from_dtype in (
                float,
                np.float64,
                np.float16,
                np.float32,
                Float32Dtype(),
                Float64Dtype(),
            ) and (to_dtype == "string[python]" or to_dtype is str):
                # snowpark pandas prints float value 1.0 to string "1" while pandas prints "1.0"
                expected_to_pandas = expected_to_pandas.astype(np.float64).apply(
                    lambda x: f"{x:.0f}"
                )
            # verify the values in the series are equal
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                s,
                expected_to_pandas,
                check_datetimelike_compat=True,
            )


@pytest.mark.parametrize("from_dtype", basic_types())
@pytest.mark.parametrize(
    "to_tz",
    [
        "UTC",
        "Asia/Tokyo",
        "America/Los_Angeles",
    ],
)
def test_astype_to_DatetimeTZDtype(from_dtype, to_tz):
    to_dtype = f"datetime64[ns, {to_tz}]"
    offset_map = {
        "UTC": "UTC",
        "Asia/Tokyo": "UTC+09:00",
        "America/Los_Angeles": "UTC-08:00",
    }
    seed = (
        [True, False, False, True]
        # if isinstance(from_dtype, BooleanDtype)
        if from_dtype == "boolean"
        else [0, 1, 2, 3]
    )

    if from_dtype == np.bool_ or from_dtype == "boolean":
        error_msg = "cannot be converted to datetime64"
        with SqlCounter(query_count=0):
            with pytest.raises(TypeError, match=error_msg):
                pd.Series(seed, dtype=from_dtype).astype(to_dtype).to_pandas()
            with pytest.raises(TypeError, match=error_msg):
                native_pd.Series(seed, dtype=from_dtype).astype(to_dtype)
    elif isinstance(from_dtype, StringDtype) or from_dtype is str:
        # Snowpark pandas use Snowflake auto format detection and the behavior can be different from native pandas
        with SqlCounter(query_count=1):
            assert_snowpark_pandas_equal_to_pandas(
                pd.Series(seed, dtype=from_dtype).astype(to_dtype),
                native_pd.Series(
                    [
                        native_pd.Timestamp("1970-01-01 00:00:00", tz="UTC").tz_convert(
                            offset_map[to_tz]
                        ),
                        native_pd.Timestamp("1970-01-01 00:00:01", tz="UTC").tz_convert(
                            offset_map[to_tz]
                        ),
                        native_pd.Timestamp("1970-01-01 00:00:02", tz="UTC").tz_convert(
                            offset_map[to_tz]
                        ),
                        native_pd.Timestamp("1970-01-01 00:00:03", tz="UTC").tz_convert(
                            offset_map[to_tz]
                        ),
                    ]
                ),
            )
            with pytest.raises(ValueError, match="day is out of range for month"):
                # use dypte=str instead of StringDType
                native_pd.Series(seed, dtype=str).astype(to_dtype)
    else:
        with SqlCounter(query_count=2):
            s = pd.Series(seed, dtype=from_dtype).astype(to_dtype)
            assert s.dtype == DatetimeTZDtype(tz=offset_map[to_tz])
            #
            # native_pd.Series([0,1,2], dtype="float64").astype("datetime64[ns, Asia/Tokyo]")
            # 0             1970-01-01 00:00:00+09:00
            # 1   1970-01-01 00:00:00.000000001+09:00
            # 2   1970-01-01 00:00:00.000000002+09:00
            # dtype: datetime64[ns, Asia/Tokyo]
            #
            # The result is wrong and also with warning in pandas:
            # /var/folders/nw/8qf9s1jd01q15skcgp93r2p80000gn/T/ipykernel_3495/2932902506.py:1: FutureWarning: The behavior
            # of DatetimeArray._from_sequence with a timezone-aware dtype and floating-dtype data is deprecated. In a future
            # version, this data will be interpreted as nanosecond UTC timestamps instead of wall-times, matching the
            # behavior with integer dtypes. To retain the old behavior, explicitly cast to 'datetime64[ns]' before passing
            # the data to pandas. To get the future behavior, first cast to 'int64'.
            # Snowpark pandas does the right way to convert it to int64 first and then convert to datetime64.
            #
            # pd.Series([0,1,2], dtype="int64").astype("datetime64[ns, Asia/Tokyo]")
            # 0             1970-01-01 09:00:00+09:00
            # 1   1970-01-01 09:00:00.000000001+09:00
            # 2   1970-01-01 09:00:00.000000002+09:00
            # dtype: datetime64[ns, Asia/Tokyo]
            if "float" in str(from_dtype).lower():
                from_dtype = "int64"

            if from_dtype == "datetime64[ns]":
                # Native pandas after 2.0 disallows using astype to convert from timzone-naive to timezone-aware
                # This remains valid in Snowflake, so Snowpark pandas performs the conversion anyway
                with pytest.raises(
                    TypeError,
                    match="Cannot use .astype to convert from timezone-naive dtype to timezone-aware dtype.",
                ):
                    native_pd.Series(seed, dtype=from_dtype).astype(to_dtype)
                expected_to_pandas = (
                    native_pd.Series(seed, dtype=from_dtype)
                    .dt.tz_localize("UTC")
                    .dt.tz_convert(offset_map[to_tz])
                )
            else:
                expected_to_pandas = (
                    native_pd.Series(seed, dtype=from_dtype)
                    .astype(to_dtype)
                    .dt.tz_convert(offset_map[to_tz])
                )
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                s,
                expected_to_pandas,
                check_datetimelike_compat=True,
            )


@pytest.mark.parametrize(
    "from_tz",
    [
        "UTC",
        "Asia/Tokyo",
        "America/Los_Angeles",
    ],
)
def test_astype_from_DatetimeTZDtype_to_datetime64(from_tz):
    from_dtype = f"datetime64[ns, {from_tz}]"
    to_dtype = "datetime64[ns]"
    native = native_pd.Series([0, 1, 2, 3], dtype=from_dtype)
    snow = pd.Series(native)
    expected_dtype = get_expected_dtype(to_dtype)
    with SqlCounter(query_count=1):
        s = snow.astype(to_dtype)
        assert s.dtype == expected_dtype
        # Native pandas after 2.0 disallows using astype to convert from timzone-aware to timezone-naive
        # This remains valid in Snowflake, so Snowpark pandas performs the conversion anyway
        with pytest.raises(
            TypeError,
            match="Cannot use .astype to convert from timezone-aware dtype to timezone-naive dtype.",
        ):
            native.astype(to_dtype)
        expected_to_pandas = native.dt.tz_convert(None)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snow.astype(to_dtype),
            expected_to_pandas,
            check_datetimelike_compat=True,
        )


@sql_count_checker(query_count=1)
def test_astype_uint():
    # Snowflake’s numeric types are always signed. So astype(‘uint8’) in this case has no effect on the original series.
    s = pd.Series([-1, 0, 1]).astype("uint8")
    assert s.dtype == "int64"
    assert_snowpark_pandas_equal_to_pandas(
        s, native_pd.Series([-1, 0, 1]), check_dtype=False
    )


@sql_count_checker(query_count=2)
def test_astype_none():
    # s = pd.Series([3.14, np.nan, None], dtype="float64").astype(str)
    # expected = pd.Series(["3.14", np.nan, None], dtype=str)
    # assert_series_equal(s, expected)
    # # verify to_pandas results too
    # assert_series_equal(s, native_pd.Series(["3.14", np.nan, None], dtype=str))
    #
    # # pandas will raise ValueError when casting a series with null values to int
    # with pytest.raises(ValueError):
    #     native_pd.Series([1, 2, 3, None]).astype(int)
    # # but snowpark pandas allows it since Snowflake backend support null values
    # s = pd.Series([1, 2, 3, None]).astype(int)
    # assert s.dtype == "int64"
    # assert_series_equal(s, native_pd.Series([1, 2, 3, None]))

    # date with none
    s = pd.Series([date(year=2011, month=1, day=1)] * 3 + [None, np.nan, pd.NA]).astype(
        bool
    )
    s2 = pd.Series([True] * 3 + [None, np.nan, pd.NA]).astype(bool)
    assert_series_equal(s, s2)


@pytest.mark.parametrize("to_dtype", basic_types())
@pytest.mark.parametrize(
    "seed",
    [
        [date(year=2011, month=1, day=1)] * 3,
        [time(0), time(1), time(2)],
    ],
)
def test_python_date_and_time(seed, to_dtype):
    s = pd.Series(seed)
    native = native_pd.Series(seed)
    expected_dtype = get_expected_dtype(to_dtype)
    expected_to_pandas_dtype = get_expected_to_pandas_dtype(to_dtype, expected_dtype)

    to_dtype_str = str(to_dtype).lower()
    if (
        "int" in to_dtype_str
        or "float" in to_dtype_str
        or ("datetime64" in to_dtype_str and isinstance(seed[0], time))
    ):
        with SqlCounter(query_count=0):
            with pytest.raises(TypeError):
                native.astype(to_dtype)
            with pytest.raises(TypeError, match="cannot be converted"):
                s.astype(to_dtype).to_pandas()
    else:
        with SqlCounter(query_count=1):
            snow = s.astype(to_dtype)
            assert snow.dtype == expected_dtype
            expected_to_pandas = native.astype(expected_to_pandas_dtype)
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                snow,
                expected_to_pandas,
                check_datetimelike_compat=True,
            )


@pytest.mark.parametrize(
    "seed",
    [
        [time(0), time(1), time(2)],
        [date(year=2011, month=1, day=1)] * 3,
    ],
)
def test_python_datetime_astype_DatetimeTZDtype(seed):
    to_dtype = "datetime64[ns, UTC]"
    s = pd.Series(seed)
    native = native_pd.Series(seed)
    if isinstance(seed[0], time):
        with SqlCounter(query_count=0):
            with pytest.raises(TypeError):
                native.astype(to_dtype)
            with pytest.raises(TypeError, match="cannot be converted"):
                s.astype(to_dtype).to_pandas()
    else:
        with SqlCounter(query_count=2):
            snow = s.astype(to_dtype)
            assert snow.dtype == to_dtype
            expected_to_pandas = native.astype(to_dtype)
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                snow,
                expected_to_pandas,
                check_datetimelike_compat=True,
            )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "data",
    [[12345678, 9], [12345678, 2.6], [True, False], [1, "2"]],
    ids=["int", "float", "boolean", "object"],
)
def test_astype_to_timedelta(data):
    native_series = native_pd.Series(data)
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda series: series.astype("timedelta64[ns]")
    )


@sql_count_checker(query_count=0)
def test_astype_to_timedelta_negative():
    native_datetime_series = native_pd.Series(
        data=[pd.to_datetime("2000-01-01"), pd.to_datetime("2001-01-01")]
    )
    snow_datetime_series = pd.Series(native_datetime_series)
    with SqlCounter(query_count=0):
        with pytest.raises(
            TypeError,
            match=re.escape("Cannot cast DatetimeArray to dtype timedelta64[ns]"),
        ):
            native_datetime_series.astype("timedelta64[ns]")
        with pytest.raises(
            TypeError,
            match=re.escape(
                "dtype datetime64[ns] cannot be converted to timedelta64[ns]"
            ),
        ):
            snow_datetime_series.astype("timedelta64[ns]")
    with SqlCounter(query_count=0):
        snow_string_series = pd.Series(data=["2 days, 3 minutes"])
        with pytest.raises(
            NotImplementedError,
            match=re.escape("dtype object cannot be converted to timedelta64[ns]"),
        ):
            snow_string_series.astype("timedelta64[ns]")


@sql_count_checker(query_count=0)
def test_astype_object():
    assert pd.Series([1, 2, 3], dtype="int16").astype(np.object_).dtype == "object"


@sql_count_checker(query_count=0)
def test_astype_copy():
    s1 = pd.Series([1, 2, 3])
    s2 = s1.astype(str, copy=False)
    assert s1.dtype == "object"
    assert s2 is None


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            ["2001-10-24", "2002-11-11"],
            native_pd.Series(["2001-10-24", "2002-11-11"]).astype("datetime64[ns]"),
        ),
        (
            ["2001-10-24", "2002-11-116"],
            native_pd.Series(["2001-10-24", "2002-11-116"]),
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_astype_errors_ignore_not_implemented(data, expected):
    s1 = pd.Series(data)
    msg = "Snowpark pandas astype API doesn't yet support errors == 'ignore'"
    with pytest.raises(NotImplementedError, match=msg):
        s1.astype("datetime64[ns]", errors="ignore")


@sql_count_checker(query_count=0)
def test_astype_int64_warning(caplog):
    caplog.clear()
    WarningMessage.printed_warnings.clear()
    with caplog.at_level(logging.WARNING):
        pd.Series(["2001", "2002"]).astype("int16")
        assert "Snowpark pandas API auto cast all integers to int64" in caplog.text
