#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.pandas as pd
from tests.integ.modin.series.test_astype import (
    basic_types,
    get_expected_dtype,
    get_expected_to_pandas_dtype,
)
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import assert_frame_equal, assert_series_equal
from tests.utils import Utils


@sql_count_checker(query_count=1)
def test_series_input():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [2.4, 2.5, 3.1]})
    astype = pd.Series({"a": "str", "b": "str"})
    ret = df.astype(astype)
    print(ret.dtypes)
    assert_series_equal(
        ret.dtypes, native_pd.Series({"a": np.object_, "b": np.object_})
    )


@sql_count_checker(query_count=1)
def test_input_negative():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [2.4, 2.5, 3.1]})
    with pytest.raises(KeyError, match="not found in columns"):
        df.astype({"a": str, "c": str})

    native_astype = native_pd.Series({"a": str, "c": str})
    with pytest.raises(TypeError, match="Please convert this to Snowpark pandas"):
        df.astype(native_astype)

    astype = pd.Series(["str", "str"], index=["a", "a"])
    with pytest.raises(
        ValueError, match="The new Series of types must have a unique index"
    ):
        df.astype(astype)


@pytest.mark.parametrize("to_dtype", basic_types())
def test_astype_from_timestamp_ltz(session, to_dtype):
    test_table_name = "test_astype_from_timestamp_ltz"
    col_name_type = "timestamp_ltz timestamp_ltz"
    Utils.create_table(session, test_table_name, col_name_type, is_temporary=True)
    session.sql(
        f"insert into {test_table_name} values ('2023-01-01 00:00:01.000000001'), ('2023-12-31 23:59:59.999999999')"
    ).collect()
    snow = pd.read_snowflake(test_table_name)
    native = snow.to_pandas()
    expected_dtype = get_expected_dtype(to_dtype)
    expected_to_pandas_dtype = get_expected_to_pandas_dtype(to_dtype, expected_dtype)
    if to_dtype == "datetime64[ns]":
        # Native pandas after 2.0 disallows using astype to convert from timzone-aware to timezone-naive
        # This remains valid in Snowflake, so Snowpark pandas performs the conversion anyway
        with SqlCounter(query_count=1):
            assert_frame_equal(
                snow.astype(to_dtype),
                native.map(lambda col: col.tz_convert("UTC").tz_localize(None)),
                check_dtype=False,
                check_datetimelike_compat=True,
                check_index_type=False,
            )
        with pytest.raises(
            TypeError,
            match="Cannot use .astype to convert from timezone-aware dtype to timezone-naive dtype.",
        ):
            native.astype(expected_to_pandas_dtype)
    elif "float" in str(to_dtype).lower():
        with SqlCounter(query_count=0):
            with pytest.raises(TypeError, match="cannot be converted"):
                snow.astype(to_dtype).to_pandas()
            with pytest.raises(TypeError, match="Cannot cast DatetimeArray to dtype"):
                native.astype(expected_to_pandas_dtype)
    else:
        with SqlCounter(query_count=1):
            s = snow.astype(to_dtype)
            assert s.dtypes[0] == expected_dtype
            expected_to_pandas = native.astype(expected_to_pandas_dtype)
            assert_frame_equal(
                snow.astype(to_dtype),
                expected_to_pandas,
                check_dtype=False,
                check_datetimelike_compat=True,
                check_index_type=False,
            )
