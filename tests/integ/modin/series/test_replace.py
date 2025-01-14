#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas._libs.lib import no_default

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.fixture
def snow_series():
    return pd.Series(["one", "two", "three", pd.NA, None], name="col1")


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
def test_replace_all_columns(to_replace, value, snow_series, regex):
    eval_snowpark_pandas_result(
        snow_series,
        snow_series.to_pandas(),
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
def test_replace_regex(to_replace, value, snow_series):
    eval_snowpark_pandas_result(
        snow_series,
        snow_series.to_pandas(),
        lambda df: df.replace(to_replace, value, regex=True),
    )
    eval_snowpark_pandas_result(
        snow_series,
        snow_series.to_pandas(),
        lambda df: df.replace(regex=to_replace, value=value),
    )


@pytest.mark.parametrize(
    "to_replace, value",
    [
        ("one", {"col1": "A_ONE", "col2": "B_ONE"}),  # scalar -> dict
        (pd.NA, {"col1": "A_ABC", "col2": "B_ABC"}),  # NULL -> dict
    ],
)
@sql_count_checker(query_count=1)
def test_replace_value_dict_negative(to_replace, value, snow_series):
    eval_snowpark_pandas_result(
        snow_series,
        snow_series.to_pandas(),
        lambda df: df.replace(to_replace, value),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="In Series.replace 'to_replace' must be None if the 'value' is dict-like",
        assert_exception_equal=False,
    )


@pytest.mark.parametrize(
    "to_replace, value",
    [
        ({"col1": "one", "col2": "abc"}, "NEW"),  # dict -> scalar
        ({"col1": "one", "col2": "abc"}, None),  # dict -> None
        ({"col1": "one", "col2": "abc"}, pd.NA),  # dict -> NULL
    ],
)
@sql_count_checker(query_count=1)
def test_replace_to_replace_dict_negative(to_replace, value, snow_series):
    eval_snowpark_pandas_result(
        snow_series,
        snow_series.to_pandas(),
        lambda df: df.replace(to_replace, value),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="In Series.replace 'to_replace' cannot be dict-like if 'value' is provided",
        assert_exception_equal=False,
    )


@sql_count_checker(query_count=0)
def test_replace_method_negative(snow_series):
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas replace API does not support 'method' parameter",
    ):
        snow_series.replace("abc", "ABC", method="pad")


@sql_count_checker(query_count=0)
def test_replace_limit_negative(snow_series):
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas replace API does not support 'limit' parameter",
    ):
        snow_series.replace("abc", "ABC", limit=10)


@sql_count_checker(query_count=0)
def test_replace_no_value_negative(snow_series):
    # pandas will not raise error instead uses 'pad' method to replace values.
    with pytest.raises(ValueError, match="Explicitly specify the new values instead"):
        snow_series.replace(to_replace="abc")


@sql_count_checker(query_count=1)
def test_non_bool_regex_negative(snow_series):
    eval_snowpark_pandas_result(
        snow_series,
        snow_series.to_pandas(),
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
def test_replace_length_mismatch_negative(snow_series, to_replace, value):
    eval_snowpark_pandas_result(
        snow_series,
        snow_series.to_pandas(),
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
        [[1, 2, 3, 4, 5]],
        [["one", "two", "three", "four", "five"]],
        native_pd.MultiIndex.from_tuples(
            [("one", 1), ("two", 2), ("three", 3), ("four", 4), ("five", 5)]
        ),
    ],
)
def test_replace_index(snow_series, to_replace, value, regex, index):
    snow_series = snow_series.set_axis(index)
    expected_join_count = 4 if isinstance(index, native_pd.MultiIndex) else 2
    with SqlCounter(query_count=2, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            snow_series,
            snow_series.to_pandas(),
            lambda df: df.replace(to_replace, value, regex=regex),
        )
