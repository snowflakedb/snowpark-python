#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import datetime
import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result

TEST_DATA = [
    "a%_.*?|&^$bc",
    "abcxyz",
    "xyzabc",
    "axyzbc",
    "xabcyz",
    "xyz|%_.*?|&^$",
    "xyzxyz",
    "XYZ",
    "abc\\nxyzabc",
    "abcxyz\\nabc",
    "  \\t\\nabc\\t\\f xyz\\tabc",
    "xy\\nz",
    "abc\nxyzabc",
    "abcxyz\nabc",
    "  \t\n\fabc\t\f xyz\tabc",
    "xy\nz",
    "a",
    "aba",
    " \t\r\n\f",
    "",
    None,
]


@pytest.mark.parametrize("func", ["startswith", "endswith"])
@pytest.mark.parametrize(
    "pat",
    [
        "",
        "xyz",
        "uvw",
        ("xyz",),
        ("uvw", "xyz"),
        ("uvw",),
        ("xyz", 1),
        ("uvw", 1),
        (1, 2),
        (("xyz",),),
        ((1,),),
        "%_.*?|&^$",
    ],
)
@pytest.mark.parametrize("na", [None, np.nan, native_pd.NA, True, False])
@sql_count_checker(query_count=1)
def test_str_startswith_endswith(func, pat, na):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser, native_ser, lambda ser: getattr(ser.str, func)(pat, na=na)
    )


@pytest.mark.parametrize("func", ["startswith", "endswith"])
@pytest.mark.parametrize("pat", [1, True, datetime.date(2019, 12, 4), ["xyz"]])
@sql_count_checker(query_count=0)
def test_str_startswith_endswith_invalid_pattern(func, pat):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: getattr(ser.str, func)(pat=pat),
        expect_exception=True,
        expect_exception_match="expected a string or tuple, not",
    )


@pytest.mark.parametrize("func", ["startswith", "endswith"])
@pytest.mark.parametrize("na", [1, "klm", datetime.date(2019, 12, 4), [True]])
@sql_count_checker(query_count=0)
def test_str_startswith_endswith_invlaid_na(func, na):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas doesn't support non-bool 'na' argument",
    ):
        getattr(snow_ser.str, func)(pat="xyz", na=na)


@pytest.mark.parametrize(
    "pat",
    [
        "",
        "xyz",
        "uvw",
        "%_.*?|&^$",
        r"x.[za]",
        r"(.?:abc|xyz)[^abcxyz]",
        r"a|b|c",
    ],
)
@pytest.mark.parametrize("case", [True, False])
@pytest.mark.parametrize("flags", [0, re.IGNORECASE])
@pytest.mark.parametrize("na", [None, np.nan, native_pd.NA, True, False])
@pytest.mark.parametrize("regex", [True, False])
@sql_count_checker(query_count=1)
def test_str_contains(pat, case, flags, na, regex):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.str.contains(pat, case=case, flags=flags, na=na, regex=regex),
    )


@pytest.mark.parametrize("na", [1, "klm", datetime.date(2019, 12, 4), [True]])
@sql_count_checker(query_count=0)
def test_str_contains_invlaid_na(na):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas doesn't support non-bool 'na' argument",
    ):
        snow_ser.str.contains(pat="xyz", na=na)


@pytest.mark.parametrize(
    "pat",
    [
        "",
        "xyz",
        "^xyz",
        "xyz$",
        "uvw",
        "%_.*?|&^$",
        r".",
        r"\\",
        r"[a-z]{3}",
    ],
)
@pytest.mark.parametrize("flags", [0, re.IGNORECASE, re.MULTILINE, re.DOTALL])
@sql_count_checker(query_count=1)
def test_str_count(pat, flags):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser, native_ser, lambda ser: ser.str.count(pat, flags=flags)
    )


@pytest.mark.parametrize(
    "to_strip", [None, np.nan, "", " ", "abcxyz", "zyxcba", "^$", "\nz"]
)
@sql_count_checker(query_count=1)
def test_str_strip(to_strip):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser, native_ser, lambda ser: ser.str.strip(to_strip=to_strip)
    )


@pytest.mark.parametrize("to_strip", [1, -2.0])
@sql_count_checker(query_count=0)
def test_str_strip_neg(to_strip):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas doesn't support non-str 'to_strip' argument",
    ):
        snow_ser.str.strip(to_strip=to_strip)


@pytest.mark.parametrize("pat", ["xyz", "uv", "|", r".", r"[a-z]{3}"])
@pytest.mark.parametrize("repl", ["w"])
@pytest.mark.parametrize("n", [2, 1, -1])
@pytest.mark.parametrize("case", [None, True, False])
@pytest.mark.parametrize("flags", [0, re.IGNORECASE, re.MULTILINE, re.DOTALL])
@pytest.mark.parametrize("regex", [True, False])
@sql_count_checker(query_count=1)
def test_str_replace(pat, repl, n, case, flags, regex):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.str.replace(
            pat=pat, repl=repl, n=n, case=case, flags=flags, regex=regex
        ),
    )


@pytest.mark.parametrize(
    "pat, repl, n, error",
    [
        (None, "a", 1, NotImplementedError),
        (re.compile("a"), "a", 1, NotImplementedError),
        (-2.0, "a", 1, NotImplementedError),
        ("a", lambda m: m.group(0)[::-1], 1, NotImplementedError),
        ("a", 1, 1, TypeError),
        ("a", "a", "a", NotImplementedError),
        ("a", "a", 0, NotImplementedError),
    ],
)
@sql_count_checker(query_count=0)
def test_str_replace_neg(pat, n, repl, error):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(error):
        snow_ser.str.replace(pat=pat, repl=repl, n=n)


@pytest.mark.parametrize("pat", [None, "a", "|", "%"])
@pytest.mark.parametrize("n", [None, np.NaN, 3, 2, 1, 0, -1, -2])
@sql_count_checker(query_count=0)
def test_str_split(pat, n):
    snow_ser = pd.Series(TEST_DATA)
    with pytest.raises(
        NotImplementedError, match="split is not yet implemented for Series.str"
    ):
        snow_ser.str.split(pat=pat, n=n)


@sql_count_checker(query_count=0)
def test_str_split_negative():
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(
        NotImplementedError, match="split is not yet implemented for Series.str"
    ):
        eval_snowpark_pandas_result(
            snow_ser,
            native_ser,
            lambda ser: ser.str.split(pat=None, n=None, expand=False, regex=None),
        )


@pytest.mark.parametrize("regex", [None, True])
@pytest.mark.xfail(
    reason="Snowflake SQL's split function does not support regex", strict=True
)
def test_str_split_regex(regex):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.str.split(pat=".*", n=-1, expand=False, regex=regex),
    )


@pytest.mark.parametrize(
    "pat, n, expand",
    [
        ("", 1, False),
        (re.compile("a"), 1, False),
        (-2.0, 1, False),
        ("a", "a", False),
        ("a", 1, True),
    ],
)
@sql_count_checker(query_count=0)
def test_str_split_neg(pat, n, expand):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(
        NotImplementedError, match="split is not yet implemented for Series.str"
    ):
        snow_ser.str.split(pat=pat, n=n, expand=expand, regex=False)


@pytest.mark.parametrize("func", ["islower", "isupper", "lower", "upper"])
@sql_count_checker(query_count=1)
def test_str_no_params(func):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser, native_ser, lambda ser: getattr(ser.str, func)()
    )


@sql_count_checker(query_count=0)
def test_str_isdigit_negative():
    snow_ser = pd.Series(TEST_DATA)
    with pytest.raises(
        NotImplementedError, match="isdigit is not yet implemented for Series.str"
    ):
        snow_ser.str.isdigit()


@pytest.mark.parametrize(
    "data",
    [
        [1, 2, 3, 4, 5],
        [1.1, 2.0, None, 4.0, 5.3],
    ],
)
@sql_count_checker(query_count=0)
def test_str_invalid_dtypes(data):
    native_ser = native_pd.Series(data)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.str,
        expect_exception=True,
        expect_exception_match="Can only use .str accessor with string values!",
    )


@sql_count_checker(query_count=1)
def test_str_len():
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(snow_ser, native_ser, lambda ser: ser.str.len())


@pytest.mark.parametrize(
    "items",
    [
        ["FOO", "BAR", "Blah", "blurg"],
        ["this TEST", "THAT", "test", "fInAl tEsT here"],
        ["1", "*this", "%THAT", "4*FINAL test"],
    ],
)
@sql_count_checker(query_count=1)
def test_str_capitalize_valid_input(items):
    snow_series = pd.Series(items, dtype=object)
    native_series = native_pd.Series(items, dtype=object)
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda series: series.str.capitalize()
    )


@pytest.mark.parametrize(
    "items",
    [
        [np.nan, "foo", np.nan, "fInAl tEsT here"],
        [np.nan, np.nan, np.nan],
        [np.nan, "str1", None, "STR2"],
        [None, None, None],
        ["", "", ""],
        [np.nan, "1.0", None, "tHIs"],
        [None, "foo", None, "bar"],
    ],
)
@sql_count_checker(query_count=1)
def test_str_capitalize_nan_none_empty_input(items):
    snow_series = pd.Series(items, dtype=object)
    native_series = native_pd.Series(items, dtype=object)
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda series: series.str.capitalize()
    )


@pytest.mark.parametrize(
    "items",
    [
        ["FOO", "BAR", "Blah", "blurg"],
        ["this TEST", "THAT", "test", "fInAl tEsT here"],
        ["T", "Q a", "B P", "BA P", "Ba P"],
        ["1", "*this", "%THAT", "4*FINAL test"],
        [
            "Crash",
            "course",
            "###Crash",
            "###course",
            "### Crash",
            "### Crash ###",
            "### Crash Course ###",
            "###crash Course ###",
            "###Crash Course###",
            "crash Course",
            "Crash course",
        ],
    ],
)
@sql_count_checker(query_count=1)
def test_str_title_valid_input(items):
    snow_series = pd.Series(items, dtype=object)
    native_series = native_pd.Series(items, dtype=object)
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda series: series.str.title()
    )


@pytest.mark.parametrize(
    "items",
    [
        [np.nan, "foo", np.nan, "fInAl tEsT here"],
        [np.nan, np.nan, np.nan],
        [np.nan, "str1", None, "STR2"],
        [None, None, None],
        ["", "", ""],
        [np.nan, "1.0", None, "tHIs"],
        [None, "foo", None, "bar"],
    ],
)
@sql_count_checker(query_count=1)
def test_str_title_nan_none_empty_input(items):
    snow_series = pd.Series(items, dtype=object)
    native_series = native_pd.Series(items, dtype=object)
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda series: series.str.title()
    )


@pytest.mark.parametrize(
    "items",
    [
        ["Foo", "BAR", "Blah", "blurg"],
        ["this TEST", "That", "test", "Final Test Here"],
        ["T", "Q a", "B P", "BA P", "Ba P"],
        ["1", "*This", "%THAT", "4*FINAL test"],
        [
            "Crash",
            "course",
            "###Crash",
            "###course",
            "### Crash",
            "### Crash ###",
            "### Crash Course ###",
            "###crash Course ###",
            "###Crash Course###",
            "crash Course",
            "Crash course",
        ],
    ],
)
@sql_count_checker(query_count=1)
def test_str_istitle_valid_input(items):
    snow_series = pd.Series(items, dtype=object)
    native_series = native_pd.Series(items, dtype=object)
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda series: series.str.istitle()
    )


@pytest.mark.parametrize(
    "items",
    [
        [np.nan, "Foo", np.nan, "fInAl tEsT here", "Final Test Here"],
        [np.nan, np.nan, np.nan],
        [np.nan, "Str1", None, "STR2"],
        [None, None, None],
        ["", "", ""],
        [np.nan, "1.0", None, "tHIs"],
        [None, "foo", None, "bar"],
    ],
)
@sql_count_checker(query_count=1)
def test_str_istitle_nan_none_empty_input(items):
    snow_series = pd.Series(items, dtype=object)
    native_series = native_pd.Series(items, dtype=object)
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda series: series.str.istitle()
    )
