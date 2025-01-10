#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas._libs.lib import no_default
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.fixture
def snow_df():
    return pd.DataFrame(
        {"col1": ["one", "two", "three", pd.NA], "col2": ["abc", "pqr", "xyz", None]}
    )


@pytest.mark.parametrize(
    "to_replace, value",
    [
        ("one", "ONE"),  # scalar -> scalar
        ("on", "ON"),  # scalar prefix-> scalar
        ("ne", "NE"),  # scalar suffix-> scalar
        ("n", "N"),  # scalar infix-> scalar
        ("o|t", "_"),  # scalar with '|' (meaningful for regex) -> scalar
        ("one", pd.NA),  # scalar -> NULL
        ("one", None),  # scalar -> None
        (pd.NA, "ONE"),  # NULL -> scalar
        (pd.NaT, "ONE"),  # NULL -> scalar
        (np.nan, "ONE"),  # NULL -> scalar
        (["one"], ["ONE"]),  # list -> list
        ("four", "FOUR"),  # no matching value
        (["one", "two"], ["two", "one"]),  # swap values
        (["one", "two"], "not_three"),  # list -> scalar
        (["one", "two"], ("ONE", "TWO")),  # list -> tuple
        (("one", "two"), ["ONE", "TWO"]),  # tuple -> list
        ({"one": "ONE", "two": "TWO"}, no_default),  # dict -> no_default
    ],
)
@pytest.mark.parametrize("regex", [True, False])
@sql_count_checker(query_count=2)
def test_replace_all_columns(to_replace, value, regex, snow_df):
    eval_snowpark_pandas_result(
        snow_df,
        snow_df.to_pandas(),
        lambda df: df.replace(to_replace, value, regex=regex),
    )


@pytest.mark.parametrize(
    "to_replace, value",
    [
        ("^on.", "ONE"),  # scalar -> scalar
        ("^on.", pd.NA),  # scalar -> NULL
        ("^on.", None),  # scalar -> None
        (["^on."], ["ONE"]),  # list -> list
        ("^fou.", "FOUR"),  # no matching value
        ([r"^on.$", "^tw."], ["two", "one"]),  # swap values
        (["^on.", "^tw."], "not_three"),  # list -> scalar
        (["^on.", "^tw."], ("ONE", "TWO")),  # list -> tuple
        (["^on.", "^tw."], ["ONE", "TWO"]),  # list -> list
        ({"^on.": "ONE", "^tw.": "TWO"}, no_default),  # dict -> no_default
    ],
)
@sql_count_checker(query_count=4)
def test_replace_regex(to_replace, value, snow_df):
    eval_snowpark_pandas_result(
        snow_df,
        snow_df.to_pandas(),
        lambda df: df.replace(to_replace, value, regex=True),
    )
    eval_snowpark_pandas_result(
        snow_df,
        snow_df.to_pandas(),
        lambda df: df.replace(regex=to_replace, value=value),
    )


@sql_count_checker(query_count=2)
def test_replace_regex_capture_groups():
    snow_df = pd.DataFrame(["foo1", "bar34", "56baz78"])
    eval_snowpark_pandas_result(
        snow_df,
        snow_df.to_pandas(),
        lambda df: df.replace(to_replace=r"^([a-z]+)(\d*)", value=r"\1", regex=True),
    )


@pytest.mark.parametrize(
    "to_replace, value",
    [
        ("one", {"col1": "A_ONE", "col2": "B_ONE"}),  # scalar -> dict
        ("abc", {"col1": "A_ABC", "col2": "B_ABC"}),  # scalar -> dict
        ({"col1": "one", "col2": "abc"}, "NEW"),  # dict -> scalar
        ({"col1": "one", "col2": "abc"}, None),  # dict -> None
        ({"col1": "one", "col2": "abc"}, pd.NA),  # dict -> NULL
        ({"col1": ["one", "two"], "col2": "abc"}, "NEW"),  # dict with list -> scalar
        ({"col1": ("one", "two"), "col2": "abc"}, "NEW"),  # dict with tuple -> scalar
        (
            {"col1": ("one", "two"), "col2": "abc"},
            {"col1": ("ONE", "TWO")},
        ),  # dict -> dict
        ({"col1": ["one", "two"], "col2": "abc"}, {"col1": "NEW"}),  # dict -> dict
        ({"col1": {"one": "ONE"}, "col2": {"abc": "ABC"}}),  # nested dict -> no_default
        (pd.NA, {"col1": "one", "col2": "abc"}),  # NULL -> dict
    ],
)
@sql_count_checker(query_count=2)
def test_replace_selected_columns(to_replace, value, snow_df):
    eval_snowpark_pandas_result(
        snow_df, snow_df.to_pandas(), lambda df: df.replace(to_replace, value)
    )


@sql_count_checker(query_count=2)
def test_replace_selected_columns_mixed_types():
    df = pd.DataFrame(
        {"A": [0, 1, 2, 3, 4], "B": [5, 6, 7, 8, 9], "C": ["a", "b", "c", "d", "e"]}
    )
    eval_snowpark_pandas_result(
        df, df.to_pandas(), lambda d: d.replace({"A": {0: 100, 4: 400}})
    )


@sql_count_checker(query_count=0)
def test_replace_method_negative(snow_df):
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas replace API does not support 'method' parameter",
    ):
        snow_df.replace("abc", "ABC", method="pad")


@sql_count_checker(query_count=0)
def test_replace_limit_negative(snow_df):
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas replace API does not support 'limit' parameter",
    ):
        snow_df.replace("abc", "ABC", limit=10)


@pytest.mark.parametrize(
    "pandas_df",
    [
        native_pd.DataFrame([pd.Timedelta(1)]),
        native_pd.DataFrame([1], index=[pd.Timedelta(2)]),
    ],
)
@pytest.mark.xfail(strict=True, raises=NotImplementedError)
def test_replace_frame_with_timedelta_index_or_column_negative(pandas_df):
    eval_snowpark_pandas_result(
        *create_test_dfs(
            pandas_df,
        ),
        lambda df: df.replace({1: 3})
    )


@pytest.mark.xfail(strict=True, raises=NotImplementedError)
@pytest.mark.parametrize(
    "kwargs",
    [
        param(
            {"to_replace": {1: native_pd.Timedelta(1)}},
            id="to_replace_dict_with_pandas_timedelta",
        ),
        param(
            {"to_replace": {1: np.timedelta64(1)}},
            id="to_replace_dict_with_numpy_timedelta",
        ),
        param(
            {"to_replace": {1: datetime.timedelta(days=1)}},
            id="to_replace_dict_with_datetime_timedelta",
        ),
        param(
            {"to_replace": 1, "value": native_pd.Timedelta(1)},
            id="value_timedelta_scalar",
        ),
        param(
            {"to_replace": [1, 2], "value": [native_pd.Timedelta(1), 3]},
            id="value_timedelta_list",
        ),
    ],
)
def test_replace_integer_value_with_timedelta_negative(kwargs):
    eval_snowpark_pandas_result(
        *create_test_dfs(
            [1],
        ),
        lambda df: df.replace(**kwargs)
    )


@sql_count_checker(query_count=0)
def test_replace_no_value_negative(snow_df):
    # pandas will not raise error instead uses 'pad' method to replace values.
    with pytest.raises(ValueError, match="Explicitly specify the new values instead"):
        snow_df.replace(to_replace="abc")


@sql_count_checker(query_count=1)
def test_non_bool_regex_negative(snow_df):
    eval_snowpark_pandas_result(
        snow_df,
        snow_df.to_pandas(),
        lambda df: df.replace(to_replace="abc", value="ABC", regex="abc"),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="'to_replace' must be 'None' if 'regex' is not a bool",
    )


@pytest.mark.parametrize(
    "to_replace, value",
    [
        (["abc", "xyz"], ["NEW"]),
        (["abc", "xyz"], ["ABC", "XYZ", "NEW"]),
    ],
)
@sql_count_checker(query_count=1)
def test_replace_length_mismatch_negative(snow_df, to_replace, value):
    eval_snowpark_pandas_result(
        snow_df,
        snow_df.to_pandas(),
        lambda df: df.replace(to_replace, value),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="Replacement lists must match in length",
    )


@pytest.mark.parametrize(
    "to_replace, value, regex",
    [
        ("one", "ONE", False),  # scalar -> scalar
        (["one"], ["ONE"], False),  # list -> list
        ({"one": "ONE", "two": "TWO"}, no_default, False),  # dict -> no_default
        ("^on.", "ONE", True),  # scalar -> scalar regex
        (["^on."], ["ONE"], True),  # list -> list  regex
        ({"^on.": "ONE", "^tw.": "TWO"}, no_default, True),  # dict -> no_default
    ],
)
@pytest.mark.parametrize(
    "index",
    [
        [[1, 2, 3, 4]],
        [["one", "two", "three", "four"]],
        native_pd.MultiIndex.from_tuples(
            [("one", 1), ("two", 2), ("three", 3), ("four", 4)]
        ),
    ],
)
def test_replace_index(snow_df, to_replace, value, regex, index):
    snow_df = snow_df.set_index(index)
    expected_join_count = 4 if isinstance(index, native_pd.MultiIndex) else 2
    with SqlCounter(query_count=2, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            snow_df,
            snow_df.to_pandas(),
            lambda df: df.replace(to_replace, value, regex=regex),
        )
