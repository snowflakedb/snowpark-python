#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import re
import sys

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

from snowflake.snowpark._internal.utils import TempObjectType
import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_series_equal,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

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
    1,
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
        snow_ser, native_ser, lambda ser: ser.str.count(pat=pat, flags=flags)
    )


@pytest.mark.parametrize("i", [None, -100, -2, -1, 0, 1, 2, 100])
@sql_count_checker(query_count=1)
def test_str_get(i):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.str.get(i=i),
    )


@pytest.mark.parametrize("i", [None, -100, -2, -1, 0, 1, 2, 100])
@sql_count_checker(query_count=1)
def test_str_get_list(i):
    native_ser = native_pd.Series([["a", "b"], ["c", "d", None], None, []])
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(snow_ser, native_ser, lambda ser: ser.str.get(i=i))


@pytest.mark.parametrize(
    "data",
    [
        [{"a": "x", "b": "y"}, {"c": None}, {None: "z"}, None, {}],
        [{"a": 1, "b": 2}, {"c": None}, {None: 3}, None, {}],
    ],
)
@pytest.mark.parametrize("i", ["", "a", "b", "c", "d"])
@sql_count_checker(query_count=1)
def test_str_get_dict(i, data):
    native_ser = native_pd.Series(data=data)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(snow_ser, native_ser, lambda ser: ser.str.get(i=i))


@pytest.mark.parametrize(
    "data, i",
    [
        (["a", "b"], 1.2),
        (["a", "b"], "a"),
        ([[1, 2]], "a"),
        ([{"a": "x"}], 1),
    ],
)
@sql_count_checker(query_count=0)
def test_str_get_neg(data, i):
    native_ser = native_pd.Series(data)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas method 'Series.str.get' doesn't yet support 'i' argument of types other than ",
    ):
        snow_ser.str.get(i=i)


@pytest.mark.parametrize(
    "key",
    [
        None,
        [1, 2],
        (1, 2),
        {1: "a", 2: "b"},
        -100,
        -2,
        -1,
        0,
        1,
        2,
        100,
        slice(None, None, None),
        slice(0, -1, 1),
        slice(-1, 0, -1),
        slice(0, -1, 2),
        slice(-1, 0, -2),
        slice(-100, 100, 2),
        slice(100, -100, -2),
    ],
)
@sql_count_checker(query_count=1)
def test_str___getitem__(key):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.str[key],
    )


@sql_count_checker(query_count=0)
def test_str___getitem___zero_step():
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(
        ValueError,
        match="slice step cannot be zero",
    ):
        snow_ser.str[slice(None, None, 0)]


@sql_count_checker(query_count=0)
def test_str___getitem___string_key():
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas string indexing doesn't yet support keys of types other than int when the data column contains strings",
    ):
        snow_ser.str["a"]


@pytest.mark.parametrize(
    "key",
    [
        None,
        [1, 2],
        (1, 2),
        {1: "a", 2: "b"},
        -100,
        -2,
        -1,
        0,
        1,
        2,
        100,
        slice(None, None, None),
        slice(0, -1, 1),
        slice(-100, 100, 1),
    ],
)
@sql_count_checker(query_count=1)
def test_str___getitem___list(key):
    native_ser = native_pd.Series([["a", "b"], ["c", "d", None], None, []])
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.str[key],
    )


@sql_count_checker(query_count=0)
def test_str___getitem___list_neg():
    native_ser = native_pd.Series([["a", "b"], ["c", "d", None], None, []])
    snow_ser = pd.Series(native_ser)
    with pytest.raises(
        NotImplementedError,
        match="does not yet support 'step!=1' for list values",
    ):
        snow_ser.str[slice(None, None, 2)]


@pytest.mark.parametrize(
    "data",
    [
        [{"a": "x", "b": "y"}, {"c": None}, {None: "z"}, None, {}],
        [{"a": 1, "b": 2}, {"c": None}, {None: 3}, None, {}],
    ],
)
@pytest.mark.parametrize(
    "key",
    [
        "a",
        "b",
        "c",
        "d",
        slice(None, None, None),
        slice(0, -1, 1),
        slice(-100, 100, 1),
    ],
)
@sql_count_checker(query_count=1)
def test_str___getitem___dict(data, key):
    # pandas has an incompatibility bug with python 3.12 that should be fixed in pandas 3.0
    # https://github.com/pandas-dev/pandas/issues/57500
    if sys.version_info.minor == 12:
        pytest.skip(reason="Skipping test for python 3.12")

    native_ser = native_pd.Series(data=data)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.str[key],
    )


@pytest.mark.parametrize(
    "data, key",
    [
        (["a", "b"], 1.2),
        (["a", "b"], "a"),
        ([[1, 2]], "a"),
        ([{"a": "x"}], 1),
    ],
)
@sql_count_checker(query_count=0)
def test_str___getitem___neg(data, key):
    native_ser = native_pd.Series(data=data)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas string indexing doesn't yet support keys of types other than ",
    ):
        snow_ser.str[key]


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
@sql_count_checker(query_count=1)
def test_str_match(pat, case, flags, na):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.str.match(pat, case=case, flags=flags, na=na),
    )


@pytest.mark.parametrize("na", [1, "klm", datetime.date(2019, 12, 4), [True]])
@sql_count_checker(query_count=0)
def test_str_match_invlaid_na(na):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas method 'Series.str.match' does not support non-bool 'na' argument",
    ):
        snow_ser.str.match(pat="xyz", na=na)


@pytest.mark.parametrize("start", [None, -100, -2, -1, 0, 1, 2, 100])
@pytest.mark.parametrize("stop", [None, -100, -2, -1, 0, 1, 2, 100])
@pytest.mark.parametrize("step", [None, -100, -2, -1, 1, 2, 100])
@sql_count_checker(query_count=1)
def test_str_slice(start, stop, step):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.str.slice(start=start, stop=stop, step=step),
    )


@pytest.mark.parametrize("start", [None, -100, -2, -1, 0, 1, 2, 100])
@pytest.mark.parametrize("stop", [None, -100, -2, -1, 0, 1, 2, 100])
@pytest.mark.parametrize("step", [None, 1])
@sql_count_checker(query_count=1)
def test_str_slice_list(start, stop, step):
    native_ser = native_pd.Series([["a", "b"], ["c", "d", None], None, []])
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.str.slice(start=start, stop=stop, step=step),
    )


@pytest.mark.parametrize("start", [None, -100, -2, -1, 0, 1, 2, 100])
@pytest.mark.parametrize("stop", [None, -100, -2, -1, 0, 1, 2, 100])
@pytest.mark.parametrize("step", [None, 1])
@sql_count_checker(query_count=1)
def test_str_slice_dict(start, stop, step):
    # pandas has an incompatibility bug with python 3.12 that should be fixed in pandas 3.0
    # https://github.com/pandas-dev/pandas/issues/57500
    if sys.version_info.minor == 12:
        pytest.skip(reason="Skipping test for python 3.12")

    native_ser = native_pd.Series(
        [{"a": "x", "b": "y"}, {"c": None}, {None: "z"}, None, {}]
    )
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.str.slice(start=start, stop=stop, step=step),
    )


@sql_count_checker(query_count=0)
def test_str_slice_neg():
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(
        ValueError,
        match="slice step cannot be zero",
    ):
        snow_ser.str.slice(start=None, stop=None, step=0)


@sql_count_checker(query_count=0)
def test_str_slice_list_neg():
    native_ser = native_pd.Series([["a", "b"], ["c", "d", None], None, []])
    snow_ser = pd.Series(native_ser)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas method 'Series.str.slice' does not yet support 'step!=1' for list values",
    ):
        snow_ser.str.slice(start=None, stop=None, step=2)


@pytest.mark.parametrize("func", ["strip", "lstrip", "rstrip"])
@pytest.mark.parametrize(
    "to_strip", [None, np.nan, "", " ", "abcxyz", "zyxcba", "^$", "\nz"]
)
@sql_count_checker(query_count=1)
def test_str_strip_variants(func, to_strip):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser, native_ser, lambda ser: getattr(ser.str, func)(to_strip=to_strip)
    )


@pytest.mark.parametrize("func", ["strip", "lstrip", "rstrip"])
@pytest.mark.parametrize("to_strip", [1, -2.0])
@sql_count_checker(query_count=0)
def test_str_strip_variants_neg(func, to_strip):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(
        NotImplementedError,
        match=f"Snowpark pandas Series.str.{func} does not yet support non-str 'to_strip' argument",
    ):
        getattr(snow_ser.str, func)(to_strip=to_strip)


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


@pytest.mark.parametrize(
    "pat", [None, "a", "ab", "abc", "non_occurrence_pat", "|", "%"]
)
@pytest.mark.parametrize("n", [None, np.nan, 3, 2, 1, 0, -1, -2])
@sql_count_checker(query_count=1)
def test_str_split_expand_false(pat, n):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.str.split(pat=pat, n=n, expand=False, regex=None),
    )


@pytest.mark.parametrize("pat", [None, "a", "ab", "abc", "no_occurrence_pat", "|", "%"])
@pytest.mark.parametrize("n", [None, np.nan, 3, 2, 1, 0, -1, -2])
@sql_count_checker(query_count=2)
def test_str_split_expand_true(pat, n):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.str.split(pat=pat, n=n, expand=True, regex=None),
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
    "pat, n, error",
    [
        ("", 1, ValueError),
        (re.compile("a"), 1, NotImplementedError),
        (-2.0, 1, NotImplementedError),
        ("a", "a", NotImplementedError),
    ],
)
@sql_count_checker(query_count=0)
def test_str_split_neg(pat, n, error):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(error):
        snow_ser.str.split(pat=pat, n=n, expand=False, regex=False)


@pytest.mark.parametrize("func", ["isdigit", "islower", "isupper", "lower", "upper"])
@sql_count_checker(query_count=1)
def test_str_no_params(func):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser, native_ser, lambda ser: getattr(ser.str, func)()
    )


@pytest.mark.parametrize("func", ["center", "ljust", "rjust"])
@pytest.mark.parametrize("width", [-1, 0, 1, 10, 100])
@pytest.mark.parametrize("fillchar", [" ", "#"])
@sql_count_checker(query_count=1)
def test_str_center_ljust_rjust(func, width, fillchar):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: getattr(ser.str, func)(width=width, fillchar=fillchar),
    )


@pytest.mark.parametrize("func", ["center", "ljust", "rjust"])
@pytest.mark.parametrize(
    "width, fillchar",
    [
        (None, " "),
        ("ten", " "),
        (10, ""),
        (10, "ab"),
        (10, None),
        (10, 10),
    ],
)
@sql_count_checker(query_count=0)
def test_str_center_ljust_rjust_neg(func, width, fillchar):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(TypeError):
        getattr(snow_ser.str, func)(width=width, fillchar=fillchar)


@pytest.mark.parametrize("width", [-1, 0, 1, 10, 100])
@pytest.mark.parametrize("side", ["left", "right", "both"])
@pytest.mark.parametrize("fillchar", [" ", "#"])
@sql_count_checker(query_count=1)
def test_str_pad(width, side, fillchar):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.str.pad(width=width, side=side, fillchar=fillchar),
    )


@pytest.mark.parametrize(
    "width, side, fillchar, error",
    [
        (None, "both", " ", TypeError),
        ("ten", "both", " ", TypeError),
        (10, None, " ", ValueError),
        (10, 10, " ", ValueError),
        (10, "invalid", " ", ValueError),
        (10, "both", "", TypeError),
        (10, "both", "ab", TypeError),
        (10, "both", None, TypeError),
        (10, "both", 10, TypeError),
    ],
)
@sql_count_checker(query_count=0)
def test_str_pad_neg(width, side, fillchar, error):
    native_ser = native_pd.Series(TEST_DATA)
    snow_ser = pd.Series(native_ser)
    with pytest.raises(error):
        snow_ser.str.pad(width=width, side=side, fillchar=fillchar)


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


@sql_count_checker(query_count=1)
def test_str_len_list():
    native_ser = native_pd.Series([["a", "b"], ["c", "d", None], None, []])
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(snow_ser, native_ser, lambda ser: ser.str.len())


@pytest.mark.parametrize("enable_sql_simplifier", [True, False])
def test_str_len_list_coin_base(session, enable_sql_simplifier):
    session.sql_simplifier_enabled = enable_sql_simplifier
    expected_udf_count = 2
    if session.sql_simplifier_enabled:
        expected_udf_count = 1
    with SqlCounter(query_count=3, udf_count=expected_udf_count):
        from tests.utils import Utils

        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        Utils.create_table(
            session, table_name, "SHARED_CARD_USERS array", is_temporary=True
        )
        session.sql(
            f"""insert into {table_name} (SHARED_CARD_USERS) SELECT PARSE_JSON('["Apple", "Pear", "Cabbage"]')"""
        ).collect()
        session.sql(f"insert into {table_name} values (NULL)").collect()

        df = pd.read_snowflake(table_name)
        # Follow read_snowflake with a sort operation to ensure that ordering is stable and tests are not flaky.
        df = df.sort_values(df.columns.to_list(), ignore_index=True)

        def compute_num_shared_card_users(x):
            """
            Helper function to compute the number of shared card users

            Input:
            - x: the array with the users

            Output: Number of shared card users
            """
            if x:
                return len(x)
            else:
                return 0

        # The following two methods for computing the final result should be identical.

        # The first one uses `Series.str.len` followed by `Series.fillna`.
        str_len_res = df["SHARED_CARD_USERS"].str.len().fillna(0)

        # The second one uses `Series.apply` and a user defined function.
        apply_res = df["SHARED_CARD_USERS"].apply(
            lambda x: compute_num_shared_card_users(x)
        )

        assert_series_equal(str_len_res, apply_res, check_dtype=False)


@pytest.mark.parametrize(
    "data",
    [
        [{"a": "x", "b": "y"}, {"c": None}, {None: "z"}, None, {}],
        [{"a": 1, "b": 2}, {"c": None}, {None: 3}, None, {}],
    ],
)
@sql_count_checker(query_count=1)
def test_str_len_dict(data):
    native_ser = native_pd.Series(data=data)
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(snow_ser, native_ser, lambda ser: ser.str.len())


@pytest.mark.parametrize(
    "items",
    [
        ["FOO", "BAR", "Blah", "blurg", 1],
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
        [np.nan, "foo", np.nan, "fInAl tEsT here", 1],
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
        ["FOO", "BAR", "Blah", "blurg", 1],
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
        [np.nan, "foo", np.nan, "fInAl tEsT here", 1],
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
        ["Foo", "BAR", "Blah", "blurg", 1],
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
        [np.nan, "Foo", np.nan, "fInAl tEsT here", "Final Test Here", 1],
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
