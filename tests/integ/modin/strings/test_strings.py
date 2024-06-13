#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equal_to_pandas,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_series,
    eval_snowpark_pandas_result,
)
from tests.utils import running_on_public_ci


# This whole suite is skipped in ci run because those are tests for unsupported
# APIs, which is time-consuming, and it will run the daily jenkins job.
@pytest.fixture(scope="module", autouse=True)
def skip(pytestconfig):
    if running_on_public_ci():
        pytest.skip(
            "Disable series str tests for public ci",
            allow_module_level=True,
        )


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


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.parametrize(
    "repeat, expected_result_data",
    [
        (3, ["aaa", "bbb", np.nan, "ccc", np.nan, "ddd"]),
        ([1, 2, 3, 4, 5, 6], ["a", "bb", np.nan, "cccc", np.nan, "dddddd"]),
    ],
)
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_repeat(repeat, expected_result_data):
    ser = pd.Series(["a", "b", np.nan, "c", np.nan, "d"], dtype=object)

    result = ser.str.repeat(repeat)
    expected = native_pd.Series(expected_result_data, dtype=object)
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.parametrize("arg, repeat", [[None, 4], ["b", None]])
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_repeat_with_null(arg, repeat):
    ser = pd.Series(["a", arg], dtype=object)
    result = ser.str.repeat([3, repeat])
    expected = native_pd.Series(["aaa", None], dtype=object)
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_empty_str_empty_cat():
    assert pd.Series(dtype=object).str.cat() == ""


@sql_count_checker(query_count=1, join_count=1)
def test_empty_df_float_raises():
    with pytest.raises(AttributeError):
        pd.Series(dtype="float64").str.cat()


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@sql_count_checker(query_count=10, fallback_count=1, sproc_count=1)
def test_empty_str_self_cat():
    # The query count is higher in this test because of the creation of a temp table for the
    # second series argument being passed in as argument to the cat sproc
    # Related: SNOW-960061

    eval_snowpark_pandas_result(
        pd.Series(dtype=object),
        native_pd.Series(dtype=object),
        lambda ser: ser.str.cat(ser),
        comparator=assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    )


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
    "method, expected, query_count, sproc_count",
    [
        pytest.param(
            "isalnum",
            [True, True, True, True, True, False, True, True, False, False],
            9,
            1,
            marks=pytest.mark.xfail(
                reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
                strict=True,
                raises=RuntimeError,
            ),
        ),
        pytest.param(
            "isalpha",
            [True, True, True, False, False, False, True, False, False, False],
            9,
            1,
            marks=pytest.mark.xfail(
                reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
                strict=True,
                raises=RuntimeError,
            ),
        ),
        (
            "isdigit",
            [False, False, False, True, False, False, False, True, False, False],
            2,
            0,
        ),
        pytest.param(
            "isnumeric",
            [False, False, False, True, False, False, False, True, False, False],
            9,
            1,
            marks=pytest.mark.xfail(
                reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
                strict=True,
                raises=RuntimeError,
            ),
        ),
        pytest.param(
            "isspace",
            [False, False, False, False, False, False, False, False, False, True],
            9,
            1,
            marks=pytest.mark.xfail(
                reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
                strict=True,
                raises=RuntimeError,
            ),
        ),
        (
            "islower",
            [False, True, False, False, False, False, False, False, False, False],
            2,
            0,
        ),
        (
            "isupper",
            [True, False, False, False, True, False, True, False, False, False],
            2,
            0,
        ),
        (
            "istitle",
            [True, False, True, False, True, False, False, False, False, False],
            2,
            0,
        ),
    ],
)
def test_ismethods(method, expected, query_count, sproc_count):
    data = ["A", "b", "Xy", "4", "3A", "", "TT", "55", "-", "  "]
    native_ser = native_pd.Series(data, dtype=object)
    ser = pd.Series(data, dtype=object)

    expected = native_pd.Series(expected, dtype=bool)
    with SqlCounter(query_count=query_count, sproc_count=sproc_count):
        result = getattr(ser.str, method)()
        assert_snowpark_pandas_equal_to_pandas(result, expected)

        # compare with standard library
        expected = [getattr(item, method)() for item in native_ser]
        assert list(result.to_pandas()) == expected


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.parametrize(
    "method, expected",
    [
        ("isnumeric", [False, True, True, False, True, True, False]),
        ("isdecimal", [False, True, False, False, False, True, False]),
    ],
)
@sql_count_checker(query_count=9, fallback_count=1, sproc_count=1)
def test_isnumeric_unicode(method, expected):
    # 0x00bc: ¼ VULGAR FRACTION ONE QUARTER
    # 0x2605: ★ not number
    # 0x1378: ፸ ETHIOPIC NUMBER SEVENTY
    # 0xFF13: ３ Em 3
    data = ["A", "3", "¼", "★", "፸", "３", "four"]
    native_ser = native_pd.Series(data, dtype=object)
    ser = pd.Series(data, dtype=object)
    expected = native_pd.Series(expected, dtype=bool)
    result = getattr(ser.str, method)()
    assert_snowpark_pandas_equal_to_pandas(result, expected)

    # compare with standard library
    expected = [getattr(item, method)() for item in native_ser]
    assert list(result.to_pandas()) == expected


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.parametrize(
    "method, expected",
    [
        ("isnumeric", [False, np.nan, True, False, np.nan, True, False]),
        ("isdecimal", [False, np.nan, False, False, np.nan, True, False]),
    ],
)
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_isnumeric_unicode_missing(method, expected):
    values = ["A", np.nan, "¼", "★", np.nan, "３", "four"]
    ser = pd.Series(values, dtype=object)
    expected = native_pd.Series(expected, dtype=object)
    result = getattr(ser.str, method)()
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@sql_count_checker(query_count=9, fallback_count=1, sproc_count=1)
def test_split_join_roundtrip():
    ser = pd.Series(["a_b_c", "c_d_e", np.nan, "f_g_h"], dtype=object)
    result = ser.str.split("_").str.join("_")
    expected = ser.to_pandas().astype(object)
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@sql_count_checker(query_count=1, fallback_count=0, sproc_count=0)
def test_len():
    ser = pd.Series(
        ["foo", "fooo", "fooooo", np.nan, "fooooooo", "foo\n", "あ"],
        dtype=object,
    )
    result = ser.str.len()
    expected = native_pd.Series([3, 4, 6, np.nan, 8, 4, 1], dtype=np.float64)
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
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
@sql_count_checker(query_count=9, fallback_count=1, sproc_count=1)
def test_index(method, sub, start, end, expected):
    data = ["ABCDEFG", "BCDEFEF", "DEFGHIJEF", "EFGHEF"]
    native_obj = native_pd.Series(data, dtype=object)
    obj = pd.Series(data, dtype=object)
    expected = native_pd.Series(expected, dtype=np.int8)
    result = getattr(obj.str, method)(sub, start, end)

    assert_snowpark_pandas_equal_to_pandas(result, expected)

    # compare with standard library
    expected = [getattr(item, method)(sub, start, end) for item in native_obj]
    assert list(result.to_pandas()) == expected


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@sql_count_checker(query_count=4)
def test_index_not_found_raises():
    obj = pd.Series(["ABCDEFG", "BCDEFEF", "DEFGHIJEF", "EFGHEF"], dtype=object)
    with pytest.raises(SnowparkSQLException):
        obj.str.index("DE")


@pytest.mark.parametrize("method", ["index", "rindex"])
@sql_count_checker(query_count=0)
def test_index_raises_not_implemented_error(method):
    obj = pd.Series([], dtype=object)
    msg = f"{method} is not yet implemented for Series.str"

    with pytest.raises(NotImplementedError, match=msg):
        getattr(obj.str, method)("sub")


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.parametrize(
    "method, exp",
    [
        ["index", [1, 1, 0]],
        ["rindex", [3, 1, 2]],
    ],
)
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_index_missing(method, exp):
    ser = pd.Series(["abcb", "ab", "bcbe", np.nan], dtype=object)

    result = getattr(ser.str, method)("b")
    expected = native_pd.Series(exp + [np.nan], dtype=np.float64)
    assert_snowpark_pandas_equal_to_pandas(result, expected)


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


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
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
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_slice_replace(start, stop, repl, expected):
    ser = pd.Series(
        ["short", "a bit longer", "evenlongerthanthat", "", np.nan],
        dtype=object,
    )
    expected = native_pd.Series(expected, dtype=object)
    result = ser.str.slice_replace(start, stop, repl)
    assert_snowpark_pandas_equal_to_pandas(result, expected)


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


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.parametrize(
    "prefix, expected", [("a", ["b", " b c", "bc"]), ("ab", ["", "a b c", "bc"])]
)
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_removeprefix(prefix, expected):
    ser = pd.Series(["ab", "a b c", "bc"], dtype=object)
    result = ser.str.removeprefix(prefix)
    ser_expected = native_pd.Series(expected, dtype=object)
    assert_snowpark_pandas_equal_to_pandas(result, ser_expected)


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.parametrize(
    "suffix, expected", [("c", ["ab", "a b ", "b"]), ("bc", ["ab", "a b c", ""])]
)
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_removesuffix(suffix, expected):
    ser = pd.Series(["ab", "a b c", "bc"], dtype=object)
    result = ser.str.removesuffix(suffix)
    ser_expected = native_pd.Series(expected, dtype=object)
    assert_snowpark_pandas_equal_to_pandas(result, ser_expected)


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@sql_count_checker(query_count=16, fallback_count=2, sproc_count=2)
def test_encode_decode():
    ser = pd.Series(["a", "b", "a\xe4"], dtype=object).str.encode("utf-8")
    result = ser.str.decode("utf-8")

    expected = ser.to_pandas().str.decode("utf-8")
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.parametrize(
    "form, expected",
    [
        ("NFKC", ["ABC", "ABC", "123", np.nan, "アイエ"]),
        ("NFC", ["ABC", "ＡＢＣ", "１２３", np.nan, "ｱｲｴ"]),
    ],
)
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_normalize(
    form,
    expected,
):
    ser = pd.Series(
        ["ABC", "ＡＢＣ", "１２３", np.nan, "ｱｲｴ"],
        index=["a", "b", "c", "d", "e"],
        dtype=object,
    )
    expected = native_pd.Series(expected, index=["a", "b", "c", "d", "e"], dtype=object)
    result = ser.str.normalize(form)
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.parametrize(
    "width, data, expected_data",
    [
        (3, ["-1", "1", "1000", 10, np.nan], ["-01", "001", "1000", np.nan, np.nan]),
        (5, ["-2", "+5"], ["-0002", "+0005"]),
    ],
)
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_zfill(width, data, expected_data):
    # https://github.com/pandas-dev/pandas/issues/20868
    value = pd.Series(data)
    result = value.str.zfill(width)
    expected = native_pd.Series(expected_data)
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_zfill_with_leading_sign():
    value = pd.Series(["-cat", "-1", "+dog"])
    expected = native_pd.Series(["-0cat", "-0001", "+0dog"])
    result = value.str.zfill(5)
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.parametrize(
    "key, expected_result",
    [
        ("name", ["Hello", "Goodbye", None]),
        ("value", ["World", "Planet", "Sea"]),
    ],
)
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
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
    assert_snowpark_pandas_equal_to_pandas(result, expected, check_dtype=False)


@pytest.mark.parametrize(
    "data, table",
    [
        (
            # Simple 1-element mapping
            ["aaaaa", "bbbaaa", "cafdsaf;lh"],
            str.maketrans("a", "b"),
        ),
        (
            # Mapping with mixed str, unicode code points, and Nones
            ["aaaaa", "fjkdsajk", "cjghgjqk", "yubikey"],
            str.maketrans({ord("a"): "A", ord("f"): None, "y": "z", "k": None}),
        ),
        (
            # Mapping with special characters
            [
                "Peña",
                "Ordoñez",
                "Raúl",
                "Ibañez",
                "François",
                "øen",
                "2πr = τ",
                "München",
            ],
            str.maketrans(
                {
                    "ñ": "n",
                    "ú": "u",
                    "ç": "c",
                    "ø": "o",
                    "τ": "t",
                    "π": "p",
                    "ü": "u",
                }
            ),
        ),
        (
            # Mapping with compound emojis. Each item in the series renders as a single emoji,
            # but is actually 4 characters. Calling `len` on each element correctly returns 4.
            # https://unicode.org/emoji/charts/emoji-zwj-sequences.html
            # Inputs:
            # - "head shaking horizontally" = 1F642 + 200D + 2194 + FE0F
            # - "heart on fire" = 2764 + FE0F + 200D + 1F525
            # - "judge" = 1F9D1 + 200D + 2696 + FE0F
            # Outputs:
            # - "head shaking vertically" = 1F642 + 200D + 2195 + FE0F
            # - "mending heart" = 2764 + FE0F + 200D + 1FA79
            # - "health worker" = 1F91D1 + 200D + 2605 + FE0F
            ["😶‍🌫️", "❤️‍🔥", "🧑‍⚖️"],
            {
                0x2194: 0x2195,
                0x1F525: 0x1FA79,
                0x2696: 0x2605,
            },
        ),
    ],
)
@sql_count_checker(query_count=1)
def test_translate(data, table):
    eval_snowpark_pandas_result(
        *create_test_series(data), lambda ser: ser.str.translate(table)
    )


@sql_count_checker(query_count=1)
def test_translate_without_maketrans():
    # pandas requires all table keys to be unicode ordinal values, and does not know how to handle
    # string keys that were not converted to ordinals via `ord` or `str.maketrans`. Since Snowflake
    # SQL uses strings in its mappings, we accept string keys as well as ordinals.
    data = ["aaaaa", "fjkdsajk", "cjghgjqk", "yubikey"]
    table = {ord("a"): "A", ord("f"): None, "y": "z", "k": None}
    snow_ser = pd.Series(data)
    assert_snowpark_pandas_equal_to_pandas(
        snow_ser.str.translate(table),
        native_pd.Series(data).str.translate(str.maketrans(table)),
    )
    # Mappings for "y" and "k" are ignored if not passed through str.maketrans because they are
    # not unicode ordinals
    assert (
        not native_pd.Series(data)
        .str.translate(table)
        .equals(native_pd.Series(data).str.translate(str.maketrans(table)))
    )


@pytest.mark.parametrize(
    "table",
    [
        {"😶‍🌫️": "a"},  # This emoji key is secretly 4 code points
        {"aa": "a"},  # Key is 2 chars
        # Mapping 1 char to multiple is valid in vanilla pandas, but we don't support this
        {"a": "😶‍🌫️"},  # This emoji value is secretly 4 code points
        {"a": "aa"},  # Value is 2 chars
    ],
)
@sql_count_checker(query_count=0)
def test_translate_invalid_mappings(table):
    data = ["aaaaa", "fjkdsajk", "cjghgjqk", "yubikey"]
    # native pandas silently treats all of these cases as no-ops. However, since Snowflake SQL uses
    # strings as mappings instead of a dict construct, passing these arguments to the equivalent
    # SQL argument would either cause an inscrutable error or unexpected changes to the output series.
    native_ser, snow_ser = *create_test_series(data)
    native_ser.str.translate(table)
    with pytest.raises(ValueError):
        snow_ser.str.translate(table)
