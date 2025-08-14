#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Callable
from unittest import mock

import pandas as native_pd
import pytest

from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)


@mock.patch("modin.core.dataframe.algebra.default2pandas.StrDefault.register")
def test_str_cat_no_others(mock_str_register, mock_series):
    result_query_compiler = mock.create_autospec(SnowflakeQueryCompiler)
    result_query_compiler.to_pandas.return_value = native_pd.DataFrame(["abc"])
    return_callable = mock.create_autospec(Callable)
    return_callable.return_value = result_query_compiler
    mock_str_register.return_value = return_callable
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.cat",
    ):
        mock_series.str.cat()


@pytest.mark.parametrize(
    "func, func_name",
    [
        (lambda s: s.str.casefold(), "casefold"),
        (lambda s: s.str.cat(["a", "b", "d", "foo"], na_rep="-"), "cat"),
        (lambda s: s.str.decode("utf-8"), "decode"),  # TODO fixme for modin 0.35.0
        (lambda s: s.str.encode("utf-8"), "encode"),
        (lambda s: s.str.rsplit("_", n=1), "rsplit"),
        (lambda s: s.str.join("_"), "join"),
        (lambda s: s.str.zfill(8), "zfill"),
        (lambda s: s.str.wrap(3), "wrap"),
        (lambda s: s.str.slice_replace(start=3, stop=5, repl="abc"), "slice_replace"),
        (lambda s: s.str.findall("ab"), "findall"),
        (lambda s: s.str.extract("(ab)", expand=False), "extract"),
        (lambda s: s.str.extractall("(ab)", flags=1), "extractall"),
        (lambda s: s.str.partition("|", expand=False), "partition"),
        (lambda s: s.str.removeprefix("t"), "removeprefix"),
        (lambda s: s.str.removesuffix("a"), "removesuffix"),
        (lambda s: s.str.repeat("a"), "repeat"),
        (lambda s: s.str.rpartition(","), "rpartition"),
        (lambda s: s.str.find("abc"), "find"),
        (lambda s: s.str.rfind("abc"), "rfind"),
        (lambda s: s.str.index("abc", start=1), "index"),
        (lambda s: s.str.rindex("abc", start=1), "rindex"),
        (lambda s: s.str.swapcase(), "swapcase"),
        (lambda s: s.str.normalize("NFC"), "normalize"),
        (lambda s: s.str.isalnum(), "isalnum"),
        (lambda s: s.str.isalpha(), "isalpha"),
        (lambda s: s.str.isnumeric(), "isnumeric"),
        (lambda s: s.str.isdecimal(), "isdecimal"),
    ],
)
def test_str_methods_with_series_return(func, func_name, mock_series):
    with pytest.raises(
        NotImplementedError,
        match=f"Snowpark pandas does not yet support the method Series.str.{func_name}",
    ):
        func(mock_series)


@pytest.mark.parametrize(
    "func, func_name",
    [
        (lambda s: s.str.get_dummies(), "get_dummies"),
        (lambda s: s.str.extract("(ab)", expand=True), "extract_expand"),
        (lambda s: s.str.extract("(ab)(cd)", expand=False), "extract_groups"),
        (lambda s: s.str.partition(","), "partition"),
    ],
)
def test_str_methods_with_dataframe_return(func, func_name, mock_series):
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.",
    ):
        func(mock_series)


@pytest.mark.skip(reason="APIs not yet implemented")
@pytest.mark.parametrize(
    "func, error_type, error_message",
    [
        (
            lambda s: s.str.rsplit(""),
            ValueError,
            r"rsplit\(\) requires a non-empty pattern match.",
        ),
        (
            lambda s: s.str.join(sep=None),
            AttributeError,
            "'NoneType' object has no attribute 'join'",
        ),
        (lambda s: s.str.wrap(-1), ValueError, r"invalid width -1 \(must be > 0\)"),
        (
            lambda s: s.str.count(12),
            TypeError,
            "first argument must be string or compiled pattern",
        ),
        (
            lambda s: s.str.findall(12),
            TypeError,
            "first argument must be string or compiled pattern",
        ),
        (
            lambda s: s.str.match(12),
            TypeError,
            "first argument must be string or compiled pattern",
        ),
        (lambda s: s.str.partition(sep=""), ValueError, "empty separator"),
        (lambda s: s.str.rpartition(sep=""), ValueError, "empty separator"),
        (lambda s: s.str.find(sub=111), TypeError, "expected a string object, not int"),
        (
            lambda s: s.str.index(sub=111),
            TypeError,
            "expected a string object, not int",
        ),
        (
            lambda s: s.str.rindex(sub=111),
            TypeError,
            "expected a string object, not int",
        ),
    ],
)
def test_methods_with_error_raise(mock_series, func, error_type, error_message):
    with pytest.raises(error_type, match=error_message):
        func(mock_series)
