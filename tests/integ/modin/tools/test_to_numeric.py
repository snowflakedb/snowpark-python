#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import contextlib
import logging
import re
from datetime import date, time

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import (
    assert_series_equal,
    assert_snowpark_pandas_equal_to_pandas,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import Utils


@pytest.fixture(params=[None, "integer", "signed", "unsigned", "float"])
def downcast(request):
    return request.param


@pytest.fixture(
    params=[
        "ignore",
        "raise",
        "coerce",
    ]
)
def errors(request):
    return request.param


@pytest.mark.parametrize(
    "input,dtype,expected_dtype",
    [
        (None, None, "float64"),
        # for the following 3 parameter sets, note that pandas converts an
        # empty non-numeric series to int64, but we always convert to double
        # for non-numeric types, e.g., string, because it is nontrivial to
        # check whether the values are integer only, and we don't want to do a
        # length check.
        ([], None, "float64"),
        ([], str, "float64"),
        ([], "object", "float64"),
        ([], "datetime64[ns]", "int64"),
        ([1, 2, 3], None, "int64"),
        ([1.3, 2.2, -3.14], None, "float64"),
        # string
        (
            ["1", "2", "3"],
            None,
            "float64",
        ),  # <- deviate from pandas' behavior, pandas returns i64
        (["1.1", "-3.14", "-3"], None, "float64"),
        # variant
        (["1.1", "2", -3], None, "float64"),
        (["1.0", "", -3], None, "float64"),
        # Bool is regarded as numeric.
        ([True, False, True, True], None, "bool"),
        (
            [1.3, np.nan, 2.2, None, pd.NA, -3.14],
            None,
            "float64",
        ),  # <- deviate from pandas' behavior, pandas returns object
        param([native_pd.Timedelta(1)], None, "int64", id="timedelta"),
    ],
)
@sql_count_checker(query_count=1)
def test_series_to_numeric(input, dtype, expected_dtype):
    snow_series = pd.Series(input, dtype=dtype)
    native_series = native_pd.Series(input, dtype=dtype)

    # When input is None, the snow series index dtype is object which will trigger an extra query to get its value type
    # in to_pandas(). This will be improved in SNOW-933782
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda s: pd.to_numeric(s)
        if isinstance(s, pd.Series)
        else native_pd.to_numeric(s),
    )
    assert pd.to_numeric(snow_series).dtype == expected_dtype


@pytest.mark.parametrize(
    "input, dtype",
    [
        (1, "int64"),
        (1.3, "float64"),
        ("1.1", "float64"),
        ("-3", "float64"),
        ("1.0", "float64"),
        ("", "float64"),
        (None, "float64"),
        (np.nan, "float64"),
        (np.inf, "float64"),
        ("inf", "float64"),
        (True, "bool"),
    ],
)
@sql_count_checker(query_count=1)
def test_scalar_to_numeric(input, dtype):
    snow = pd.to_numeric(input)
    assert snow.dtype == dtype
    native = native_pd.to_numeric(input)
    if isinstance(snow, np.float64):
        assert (np.isnan(snow) and np.isnan(native)) or snow == pytest.approx(native)
    else:
        assert snow == native


@sql_count_checker(query_count=1)
def test_scalar_timedelta_to_numeric():
    # Test this case separately because of a bug in pandas: https://github.com/pandas-dev/pandas/issues/59944
    input = native_pd.Timedelta(1)
    with pytest.raises(TypeError, match=re.escape("Invalid object type at position 0")):
        native_pd.to_numeric(input)
    assert pd.to_numeric(input) == 1


@sql_count_checker(query_count=1)
def test_downcast_ignored(downcast, caplog):
    caplog.clear()
    with caplog.at_level(logging.DEBUG):
        pd.to_numeric("1", downcast=downcast)
    if downcast:
        assert "downcast is ignored in Snowflake backend" in caplog.text
    else:
        assert "downcast is ignored in Snowflake backend" not in caplog.text


@sql_count_checker(query_count=1)
def test_nan_to_numeric():
    # snowpark pandas can handle "nan" correctly but native pandas does not
    input = "nan"
    assert np.isnan(pd.to_numeric(input))
    with pytest.raises(
        ValueError, match=f'Unable to parse string "{input}" at position 0'
    ):
        native_pd.to_numeric(input)


@pytest.fixture(params=[47393996303418497800, 100000000000000000000])
def large_val(request):
    return request.param


@sql_count_checker(query_count=1)
def test_really_large_scalar(large_val):
    snow = pd.to_numeric(large_val)
    native = native_pd.to_numeric(large_val)
    if isinstance(snow, np.float64):
        assert snow == pytest.approx(native)
    else:
        assert snow == native


def test_to_numeric_errors(errors):
    input = ["apple", "1.0", "2", -3]

    if errors == "raise":
        with SqlCounter(query_count=0):
            eval_snowpark_pandas_result(
                pd.Series(input),
                native_pd.Series(input),
                lambda s: pd.to_numeric(s, errors=errors)
                if isinstance(s, pd.Series)
                else native_pd.to_numeric(s, errors=errors),
                expect_exception=True,
                expect_exception_type=SnowparkSQLException,
                assert_exception_equal=False,  # pandas raise ValueError instead
                expect_exception_match="Numeric value 'apple' is not recognized",
            )
    else:
        with SqlCounter(query_count=0 if errors == "ignore" else 1), pytest.raises(
            NotImplementedError
        ) if errors == "ignore" else contextlib.nullcontext():
            eval_snowpark_pandas_result(
                pd.Series(input),
                native_pd.Series(input),
                lambda s: pd.to_numeric(s, errors=errors)
                if isinstance(s, pd.Series)
                else native_pd.to_numeric(s, errors=errors),
            )


@pytest.mark.parametrize(
    "input",
    [
        ["apple", "1.0", "2", -3],
        ["1", "NULL", "3"],
    ],
)
@pytest.mark.parametrize(
    "errors, expected_query_count",
    [
        ["ignore", 0],
        ["coerce", 0],
    ],
)
def test_to_numeric_errors_dtype(input, errors, expected_query_count):
    with SqlCounter(query_count=expected_query_count), pytest.raises(
        NotImplementedError
    ) if errors == "ignore" else contextlib.nullcontext():
        ret = pd.to_numeric(input, errors=errors)
        # since invalid parsing will be treated as null, the dtype will be float64
        assert ret.dtype == np.dtype("float64")


@sql_count_checker(query_count=0)
def test_to_numeric_errors_invalid():
    input = ["apple", "1.0", "2", -3]
    invalid = "invalid"
    eval_snowpark_pandas_result(
        pd.Series(input),
        native_pd.Series(input),
        lambda s: pd.to_numeric(s, errors=invalid)
        if isinstance(s, pd.Series)
        else native_pd.to_numeric(s, errors=invalid),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="invalid error value specified",
    )


@sql_count_checker(query_count=2)
def test_list():
    ser = ["1", "-3.14", "7"]
    res = pd.to_numeric(ser)
    expected = pd.Series([1, -3.14, 7])
    assert_series_equal(res, expected)


@sql_count_checker(query_count=2)
def test_tuple():
    tup = ("1", "-3.14", "7")
    res = pd.to_numeric(tup)
    expected = pd.Series([1, -3.14, 7])
    assert_series_equal(res, expected)


@pytest.mark.parametrize(
    "input, expected_dtype",
    [
        (np.array(["1", "-3.14", "7"]), "float64"),
        (np.array([-1, 0, 1], dtype=np.int8), "int64"),
        (native_pd.array([-1, 0, 1, None], dtype=pd.Int64Dtype()), "float64"),
    ],
)
@sql_count_checker(query_count=1)
def test_1darray(input, expected_dtype):
    res = pd.to_numeric(input)
    expected = native_pd.Series(input).astype(expected_dtype)
    assert_snowpark_pandas_equal_to_pandas(res, expected)


@sql_count_checker(query_count=0)
def test_2darray():
    arr = np.array([["1", "-3.14", "7"]])
    with pytest.raises(
        TypeError, match="arg must be a list, tuple, 1-d array, or Series"
    ):
        pd.to_numeric(arr)


@sql_count_checker(query_count=0)
def test_type_check():
    # see gh-11776
    df = pd.DataFrame({"a": [1, -3.14, 7], "b": ["4", "5", "6"]})
    with pytest.raises(
        TypeError, match="arg must be a list, tuple, 1-d array, or Series"
    ):
        pd.to_numeric(df)


def test_datetime_like(errors):
    input = native_pd.date_range("20130101", periods=3)
    with SqlCounter(query_count=0 if errors == "ignore" else 1), pytest.raises(
        NotImplementedError
    ) if errors == "ignore" else contextlib.nullcontext():
        eval_snowpark_pandas_result(
            pd.Series(input),
            native_pd.Series(input),
            lambda s: pd.to_numeric(s, errors=errors)
            if isinstance(s, pd.Series)
            else native_pd.to_numeric(s, errors=errors),
        )


@pytest.mark.parametrize(
    "col_name_type, samples, native_series",
    [
        (
            "obj object",
            [
                """select PARSE_JSON(' { "key1": "value1", "key2": NULL } ')""",
                """select PARSE_JSON(' { "key1": 23, "key2": 23.0 } ')""",
            ],
            native_pd.Series(
                np.array(
                    [{"key1": "value1", "key2": None}, {"key1": 23, "key2": 23.0}]
                ),
                name="OBJ",
            ),
        ),
        (
            "arr array",
            [
                "select ARRAY_CONSTRUCT(1,2,3, ARRAY_CONSTRUCT(1,2,3))",
                "select ARRAY_CONSTRUCT(1,2,3, ARRAY_CONSTRUCT(1,2,3))",
            ],
            native_pd.Series(["val", "val"], name="ARR",).apply(
                lambda val: [1, 2, 3, [1, 2, 3]]
            ),  # use `applymap` to create an array cell
        ),
        (
            "date date",
            "values ('2016-05-01'::date), ('2016-05-02'::date)",
            native_pd.Series([date(2016, 5, 1), date(2016, 5, 2)], name="DATE"),
        ),
        (
            "time time",
            "values ('00:00:01'::time), ('23:59:59'::time)",
            native_pd.Series([time(0, 0, 1), time(23, 59, 59)], name="TIME"),
        ),
        (
            "bin binary",
            "values ('48454C50'),('48454C50')",
            native_pd.Series(
                [b"HELP", b"HELP"], index=native_pd.Index([0, 1]), name="BIN"
            ),
        ),
    ],
)
@pytest.mark.parametrize("errors", ["ignore", "raise", "coerce"])
def test_unsupported_types(
    session,
    test_table_name,
    col_name_type,
    samples,
    native_series,
    errors,
):
    Utils.create_table(session, test_table_name, col_name_type, is_temporary=True)
    if not isinstance(samples, list):
        samples = [samples]
    for sample in samples:
        session.sql(f"insert into {test_table_name} {sample}").collect()
    series = pd.read_snowflake(test_table_name, enforce_ordering=True).squeeze()
    with SqlCounter(query_count=0 if errors == "raise" else 1):
        eval_snowpark_pandas_result(
            series,
            native_series,
            lambda s: pd.to_numeric(s, errors=errors)
            if isinstance(s, pd.Series)
            else native_pd.to_numeric(s, errors=errors),
            expect_exception=errors in (None, "raise"),
            expect_exception_type=TypeError,
            assert_exception_equal=False,
            expect_exception_match="Invalid object type",
        )
