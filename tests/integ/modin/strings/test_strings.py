#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equal_to_pandas,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

# NOTE: Many tests in this file previously fell back to native pandas in stored procedures. These
# have since been updated to expect NotImplementedError, and the original "expected" result has
# been left as a comment to make properly implementing these methods easier.


# TODO (SNOW-863786): import whole pandas/tests/strings/test_strings.py
@pytest.mark.parametrize("pattern", [0, True, native_pd.Series(["foo", "bar"])])
@sql_count_checker(query_count=0)
def test_startswith_endswith_non_str_patterns(pattern):
    ser = pd.Series(["foo", "bar"])
    if isinstance(pattern, native_pd.Series):
        pattern = pd.Series(pattern)

    msg = f"expected a string or tuple, not {type(pattern).__name__}"
    with pytest.raises(TypeError, match=msg):
        ser.str.startswith(pattern)
    with pytest.raises(TypeError, match=msg):
        ser.str.endswith(pattern)


@sql_count_checker(query_count=1)
def test_count():
    ser = pd.Series(["foo", "foofoo", np.nan, "foooofooofommmfoo"], dtype=object)

    result = ser.str.count("f[o]+")
    expected = native_pd.Series([1, 2, np.nan, 4], dtype=np.float64)
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.parametrize(
    "repeat, expected_result_data",
    [
        (3, ["aaa", "bbb", np.nan, "ccc", np.nan, "ddd"]),
        ([1, 2, 3, 4, 5, 6], ["a", "bb", np.nan, "cccc", np.nan, "dddddd"]),
    ],
)
@sql_count_checker(query_count=0)
def test_repeat(repeat, expected_result_data):
    ser = pd.Series(["a", "b", np.nan, "c", np.nan, "d"], dtype=object)

    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.repeat",
    ):
        ser.str.repeat(repeat)
    # expected = native_pd.Series(expected_result_data, dtype=object)


@pytest.mark.parametrize("arg, repeat", [[None, 4], ["b", None]])
@sql_count_checker(query_count=0)
def test_repeat_with_null(arg, repeat):
    ser = pd.Series(["a", arg], dtype=object)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.repeat",
    ):
        ser.str.repeat([3, repeat])
    # expected = native_pd.Series(["aaa", None], dtype=object)


@sql_count_checker(query_count=0)
def test_empty_str_empty_cat():
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.cat",
    ):
        assert pd.Series(dtype=object).str.cat() == ""


@sql_count_checker(query_count=0)
def test_empty_df_float_raises():
    with pytest.raises(AttributeError):
        pd.Series(dtype="float64").str.cat()


@sql_count_checker(query_count=1)
def test_empty_str_self_cat():
    ser = pd.Series(dtype=object)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.cat",
    ):
        ser.str.cat(ser)


@pytest.mark.parametrize(
    "fn",
    [
        (lambda ser: ser.str.title()),
        (lambda ser: ser.str.count("a")),
        (lambda ser: ser.str.contains("a")),
        (lambda ser: ser.str.startswith("a")),
        (lambda ser: ser.str.endswith("a")),
        (lambda ser: ser.str.lower()),
        (lambda ser: ser.str.upper()),
        (lambda ser: ser.str.replace("a", "b")),
    ],
)
def test_empty_str_methods(fn, query_count=1, sproc_count=0):
    with SqlCounter(query_count=query_count, sproc_count=sproc_count):
        eval_snowpark_pandas_result(
            pd.Series(dtype=object),
            native_pd.Series(dtype=object),
            fn,
            comparator=assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
        )


@pytest.mark.parametrize(
    "method, expected, query_count",
    [
        pytest.param(
            "isalnum",
            [True, True, True, True, True, False, True, True, False, False],
            0,
        ),
        pytest.param(
            "isalpha",
            [True, True, True, False, False, False, True, False, False, False],
            0,
        ),
        (
            "isdigit",
            [False, False, False, True, False, False, False, True, False, False],
            2,
        ),
        pytest.param(
            "isnumeric",
            [False, False, False, True, False, False, False, True, False, False],
            0,
        ),
        pytest.param(
            "isspace",
            [False, False, False, False, False, False, False, False, False, True],
            0,
        ),
        (
            "islower",
            [False, True, False, False, False, False, False, False, False, False],
            2,
        ),
        (
            "isupper",
            [True, False, False, False, True, False, True, False, False, False],
            2,
        ),
        (
            "istitle",
            [True, False, True, False, True, False, False, False, False, False],
            2,
        ),
    ],
)
def test_ismethods(method, expected, query_count):
    data = ["A", "b", "Xy", "4", "3A", "", "TT", "55", "-", "  "]
    native_ser = native_pd.Series(data, dtype=object)
    ser = pd.Series(data, dtype=object)

    expected = native_pd.Series(expected, dtype=bool)
    with SqlCounter(query_count=query_count):
        if query_count == 0:  # indicates unimplemented method
            with pytest.raises(
                NotImplementedError,
                match=f"Snowpark pandas does not yet support the method Series.str.{method}",
            ):
                result = getattr(ser.str, method)()
        else:
            result = getattr(ser.str, method)()
            assert_snowpark_pandas_equal_to_pandas(result, expected)

            # compare with standard library
            expected = [getattr(item, method)() for item in native_ser]
            assert list(result.to_pandas()) == expected


@pytest.mark.parametrize(
    "method, expected",
    [
        ("isnumeric", [False, True, True, False, True, True, False]),
        ("isdecimal", [False, True, False, False, False, True, False]),
    ],
)
@sql_count_checker(query_count=0)
def test_isnumeric_unicode(method, expected):
    # 0x00bc: ¼ VULGAR FRACTION ONE QUARTER
    # 0x2605: ★ not number
    # 0x1378: ፸ ETHIOPIC NUMBER SEVENTY
    # 0xFF13: ３ Em 3
    data = ["A", "3", "¼", "★", "፸", "３", "four"]
    # native_ser = native_pd.Series(data, dtype=object)
    ser = pd.Series(data, dtype=object)
    with pytest.raises(
        NotImplementedError,
        match=f"Snowpark pandas does not yet support the method Series.str.{method}",
    ):
        getattr(ser.str, method)()
    # expected = native_pd.Series(expected, dtype=bool)

    # compare with standard library
    # expected = [getattr(item, method)() for item in native_ser]


@pytest.mark.parametrize(
    "method, expected",
    [
        ("isnumeric", [False, np.nan, True, False, np.nan, True, False]),
        ("isdecimal", [False, np.nan, False, False, np.nan, True, False]),
    ],
)
@sql_count_checker(query_count=0)
def test_isnumeric_unicode_missing(method, expected):
    values = ["A", np.nan, "¼", "★", np.nan, "３", "four"]
    ser = pd.Series(values, dtype=object)
    # expected = native_pd.Series(expected, dtype=object)
    with pytest.raises(
        NotImplementedError,
        match=f"Snowpark pandas does not yet support the method Series.str.{method}",
    ):
        getattr(ser.str, method)()


@sql_count_checker(query_count=0)
def test_split_join_roundtrip():
    native_ser = native_pd.Series(["a_b_c", "c_d_e", np.nan, "f_g_h"], dtype=object)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.join",
    ):
        pd.Series(native_ser).str.split("_").str.join("_")


@sql_count_checker(query_count=1)
def test_len():
    ser = pd.Series(
        ["foo", "fooo", "fooooo", np.nan, "fooooooo", "foo\n", "あ"],
        dtype=object,
    )
    result = ser.str.len()
    expected = native_pd.Series([3, 4, 6, np.nan, 8, 4, 1], dtype=np.float64)
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.parametrize(
    "method,sub,start,end,expected",
    [
        ("index", "EF", None, None, [4, 3, 1, 0]),
        ("rindex", "EF", None, None, [4, 5, 7, 4]),
        ("index", "EF", 3, None, [4, 3, 7, 4]),
        ("rindex", "EF", 3, None, [4, 5, 7, 4]),
        ("index", "E", 4, 8, [4, 5, 7, 4]),
        ("rindex", "E", 0, 5, [4, 3, 1, 4]),
    ],
)
@sql_count_checker(query_count=0)
def test_index(method, sub, start, end, expected):
    data = ["ABCDEFG", "BCDEFEF", "DEFGHIJEF", "EFGHEF"]
    # native_obj = native_pd.Series(data, dtype=object)
    obj = pd.Series(data, dtype=object)
    # expected = native_pd.Series(expected, dtype=np.int8)
    with pytest.raises(
        NotImplementedError,
        match=f"Snowpark pandas does not yet support the method Series.str.{method}",
    ):
        getattr(obj.str, method)(sub, start, end)

    # compare with standard library
    # expected = [getattr(item, method)(sub, start, end) for item in native_obj]


@sql_count_checker(query_count=0)
def test_index_not_found_raises():
    obj = pd.Series(["ABCDEFG", "BCDEFEF", "DEFGHIJEF", "EFGHEF"], dtype=object)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.index",
    ):
        obj.str.index("DE")


@pytest.mark.parametrize("method", ["index", "rindex"])
@sql_count_checker(query_count=0)
def test_index_raises_not_implemented_error(method):
    obj = pd.Series([], dtype=object)
    msg = f"Snowpark pandas does not yet support the method Series.str.{method}"

    with pytest.raises(NotImplementedError, match=msg):
        getattr(obj.str, method)("sub")


@pytest.mark.parametrize(
    "method, exp",
    [
        ["index", [1, 1, 0]],
        ["rindex", [3, 1, 2]],
    ],
)
@sql_count_checker(query_count=0)
def test_index_missing(method, exp):
    ser = pd.Series(["abcb", "ab", "bcbe", np.nan], dtype=object)

    # expected = native_pd.Series(exp + [np.nan], dtype=np.float64)
    with pytest.raises(
        NotImplementedError,
        match=f"Snowpark pandas does not yet support the method Series.str.{method}",
    ):
        getattr(ser.str, method)("b")


@pytest.mark.parametrize(
    "start, stop, step, expected",
    [
        (2, 5, None, ["foo", "bar", np.nan, "baz"]),
        (0, 3, -1, ["", "", np.nan, ""]),
        (None, None, -1, ["owtoofaa", "owtrabaa", np.nan, "xuqzabaa"]),
        (3, 10, 2, ["oto", "ato", np.nan, "aqx"]),
        (3, 0, -1, ["ofa", "aba", np.nan, "aba"]),
    ],
)
@sql_count_checker(query_count=1)
def test_slice(start, stop, step, expected):
    ser = pd.Series(["aafootwo", "aabartwo", np.nan, "aabazqux"], dtype=object)
    result = ser.str.slice(start, stop, step)
    expected = native_pd.Series(expected, dtype=object)
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.parametrize(
    "start,stop,repl,expected",
    [
        (2, 3, None, ["shrt", "a it longer", "evnlongerthanthat", "", np.nan]),
        (2, 3, "z", ["shzrt", "a zit longer", "evznlongerthanthat", "z", np.nan]),
        (2, 2, "z", ["shzort", "a zbit longer", "evzenlongerthanthat", "z", np.nan]),
        (2, 1, "z", ["shzort", "a zbit longer", "evzenlongerthanthat", "z", np.nan]),
        (-1, None, "z", ["shorz", "a bit longez", "evenlongerthanthaz", "z", np.nan]),
        (None, -2, "z", ["zrt", "zer", "zat", "z", np.nan]),
        (6, 8, "z", ["shortz", "a bit znger", "evenlozerthanthat", "z", np.nan]),
        (-10, 3, "z", ["zrt", "a zit longer", "evenlongzerthanthat", "z", np.nan]),
    ],
)
@sql_count_checker(query_count=0)
def test_slice_replace(start, stop, repl, expected):
    ser = pd.Series(
        ["short", "a bit longer", "evenlongerthanthat", "", np.nan],
        dtype=object,
    )
    # expected = native_pd.Series(expected, dtype=object)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.slice_replace",
    ):
        ser.str.slice_replace(start, stop, repl)


@pytest.mark.parametrize(
    "method, exp",
    [
        ["lstrip", ["aa   ", "bb \n", np.nan, "cc  "]],
        ["rstrip", ["  aa", " bb", np.nan, "cc"]],
    ],
)
@sql_count_checker(query_count=1)
def test_lstrip_rstrip(method, exp):
    ser = pd.Series(["  aa   ", " bb \n", np.nan, "cc  "], dtype=object)

    result = getattr(ser.str, method)()
    expected = native_pd.Series(exp, dtype=object)
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.parametrize(
    "prefix, expected", [("a", ["b", " b c", "bc"]), ("ab", ["", "a b c", "bc"])]
)
@sql_count_checker(query_count=0)
def test_removeprefix(prefix, expected):
    ser = pd.Series(["ab", "a b c", "bc"], dtype=object)
    # ser_expected = native_pd.Series(expected, dtype=object)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.removeprefix",
    ):
        ser.str.removeprefix(prefix)


@pytest.mark.parametrize(
    "suffix, expected", [("c", ["ab", "a b ", "b"]), ("bc", ["ab", "a b c", ""])]
)
@sql_count_checker(query_count=0)
def test_removesuffix(suffix, expected):
    ser = pd.Series(["ab", "a b c", "bc"], dtype=object)
    # ser_expected = native_pd.Series(expected, dtype=object)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.removesuffix",
    ):
        ser.str.removesuffix(suffix)


@sql_count_checker(query_count=0)
def test_encode_decode():
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.encode",
    ):
        pd.Series(["a", "b", "a\xe4"], dtype=object).str.encode("utf-8")
    # expected = ser.to_pandas().str.decode("utf-8")
    # result = ser.str.decode("utf-8")


@pytest.mark.parametrize(
    "form, expected",
    [
        ("NFKC", ["ABC", "ABC", "123", np.nan, "アイエ"]),
        ("NFC", ["ABC", "ＡＢＣ", "１２３", np.nan, "ｱｲｴ"]),
    ],
)
@sql_count_checker(query_count=0)
def test_normalize(
    form,
    expected,
):
    ser = pd.Series(
        ["ABC", "ＡＢＣ", "１２３", np.nan, "ｱｲｴ"],
        index=["a", "b", "c", "d", "e"],
        dtype=object,
    )
    # expected = native_pd.Series(
    #     expected, index=["a", "b", "c", "d", "e"], dtype=object
    # )
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.normalize",
    ):
        ser.str.normalize(form)


@pytest.mark.parametrize(
    "width, data, expected_data",
    [
        (3, ["-1", "1", "1000", 10, np.nan], ["-01", "001", "1000", np.nan, np.nan]),
        (5, ["-2", "+5"], ["-0002", "+0005"]),
    ],
)
@sql_count_checker(query_count=0)
def test_zfill(width, data, expected_data):
    # https://github.com/pandas-dev/pandas/issues/20868
    value = pd.Series(data)
    # expected = native_pd.Series(expected_data)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.zfill",
    ):
        value.str.zfill(width)


@sql_count_checker(query_count=0)
def test_zfill_with_leading_sign():
    value = pd.Series(["-cat", "-1", "+dog"])
    # expected = native_pd.Series(["-0cat", "-0001", "+0dog"])
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.zfill",
    ):
        value.str.zfill(5)


@pytest.mark.parametrize(
    "key, expected_result",
    [
        ("name", ["Hello", "Goodbye", None]),
        ("value", ["World", "Planet", "Sea"]),
    ],
)
@sql_count_checker(query_count=1)
def test_get_with_dict_label(key, expected_result):
    # GH47911
    s = pd.Series(
        [
            {"name": "Hello", "value": "World"},
            {"name": "Goodbye", "value": "Planet"},
            {"value": "Sea"},
        ]
    )
    result = s.str.get(key)
    expected = native_pd.Series(expected_result)
    assert_snowpark_pandas_equal_to_pandas(result, expected)
