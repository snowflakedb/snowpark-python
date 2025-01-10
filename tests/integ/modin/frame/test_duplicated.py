#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equal_to_pandas,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize("subset", ["a", ["a"], ["a", "B"], []])
def test_duplicated_with_misspelled_column_name_or_empty_subset(subset):
    df = native_pd.DataFrame({"A": [0, 0, 1], "B": [0, 0, 1], "C": [0, 0, 1]})
    # pandas explicitly check existence of the values from subset while Snowpark pandas does not since duplicated API is
    # using getitem to fetch the column(s)
    with pytest.raises((KeyError, ValueError)):
        df.duplicated(subset)
    expected_res = df.duplicated(["B"]) if "B" in subset else native_pd.Series([])
    query_count = 0
    if subset == []:
        query_count += 1
    with SqlCounter(query_count=query_count):
        if subset == []:
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                pd.DataFrame(df).duplicated(subset),
                expected_res,
            )
        else:
            if isinstance(subset, list):
                if all(label not in df.columns for label in subset):
                    match_str = r"None of .* are in the \[columns\]"
                else:
                    match_str = r".* not in index"
            else:
                match_str = f"\\'{subset}\\'"
            with pytest.raises(KeyError, match=match_str):
                assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                    pd.DataFrame(df).duplicated(subset),
                    expected_res,
                )


@pytest.mark.parametrize(
    "subset, expected",
    [
        ("A", native_pd.Series([False, False, True, False, True])),
        (["A"], native_pd.Series([False, False, True, False, True])),
        (["B"], native_pd.Series([False, False, False, True, True])),
        (["A", "B"], native_pd.Series([False, False, False, False, True])),
        ("C", native_pd.Series([False, False, True, False, True])),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_duplicated_subset(subset, expected):
    df = pd.DataFrame(
        {
            "A": [0, 1, 1, 2, 0],
            "B": ["a", "b", "c", "b", "a"],
            "C": [
                pd.Timedelta(1),
                pd.Timedelta(10),
                pd.Timedelta(1),
                pd.Timedelta(0),
                pd.Timedelta(10),
            ],
        }
    )

    result = df.duplicated(subset=subset)
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.parametrize(
    "keep, expected",
    [
        ("first", native_pd.Series([False, False, True, False, True])),
        ("last", native_pd.Series([True, True, False, False, False])),
        (False, native_pd.Series([True, True, True, False, True])),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_duplicated_keep(keep, expected):
    df = pd.DataFrame({"A": [0, 1, 1, 2, 0], "B": ["a", "b", "b", "c", "a"]})

    result = df.duplicated(keep=keep)
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@sql_count_checker(query_count=1, join_count=2)
def test_duplicated_on_empty_frame():
    # GH 25184

    df = pd.DataFrame(columns=["a", "b"])
    dupes = df.duplicated("a")

    result = df[dupes]
    expected = native_pd.DataFrame(columns=["a", "b"])
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@sql_count_checker(query_count=3, join_count=2)
def test_frame_datetime64_duplicated():
    dates = pd.date_range("2010-07-01", end="2010-08-05").to_series()

    tst = pd.DataFrame({"symbol": "AAA", "date": dates})
    result = tst.duplicated(["date", "symbol"])
    assert (~result).all()

    tst = pd.DataFrame({"date": dates})
    result = tst.date.duplicated()
    assert (~result).all()
