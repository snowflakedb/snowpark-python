#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest


@pytest.mark.parametrize(
    "func, func_name",
    [
        (lambda se: se.groupby("A").__bytes__(), "__bytes__"),
        (lambda se: se.groupby("A").corrwith, "corrwith"),
        (lambda se: se.groupby("A").dtypes, "dtypes"),
        (lambda se: se.groupby("A").pipe(lambda x: x.max() - x.min()), "pipe"),
        (lambda se: se.groupby("A").filter(lambda x: x.mean() > 3), "filter"),
        (lambda se: se.groupby("A").corr(), "corr"),
        (lambda se: se.groupby("A").cov(), "cov"),
        (lambda se: se.groupby("A").cumprod(), "cumprod"),
        (lambda se: se.groupby("A").describe(), "describe"),
        (lambda se: se.groupby("A").diff(), "diff"),
        (lambda se: se.groupby("A").is_monotonic_increasing, "is_monotonic_increasing"),
        (lambda se: se.groupby("A").is_monotonic_decreasing, "is_monotonic_decreasing"),
        (lambda se: se.groupby("A").ngroup(), "ngroup"),
        (lambda se: se.groupby("A").nlargest(4), "nlargest"),
        (lambda se: se.groupby("A").nsmallest(4), "nsmallest"),
        (lambda se: se.groupby("A").nth(5), "nth"),
        (lambda se: se.groupby("A").ohlc(), "ohlc"),
        (lambda se: se.groupby("A").prod(), "prod"),
        (lambda se: se.groupby("A").rolling(2), "rolling"),
        (lambda se: se.groupby("A").sample(n=1, random_state=1), "sample"),
        (lambda se: se.groupby("A").sem(), "sem"),
        (lambda se: se.groupby("A").skew(), "skew"),
        (lambda se: se.groupby("A").take(2), "take"),
        (lambda se: se.groupby("A").expanding(), "expanding"),
        (lambda se: se.groupby("A").hist(), "hist"),
        (lambda se: se.groupby("A").plot(), "plot"),
        (lambda se: se.groupby("A").boxplot("test_group"), "boxplot"),
    ],
)
def test_series_groupby_unsupported_methods_raises(
    mock_series, func, func_name
) -> None:
    msg = f"Snowpark pandas does not yet support the method GroupBy.{func_name}"
    with pytest.raises(NotImplementedError, match=msg):
        func(mock_series)


@pytest.mark.parametrize(
    "func, func_name",
    [
        (lambda df: df.groupby("A").__bytes__(), "__bytes__"),
        (lambda df: df.groupby("A").corrwith, "corrwith"),
        (lambda df: df.groupby("A").dtypes, "dtypes"),
        (lambda df: df.groupby("A").pipe(lambda x: x.max() - x.min()), "pipe"),
        (lambda df: df.groupby("A").filter(lambda x: x.mean() > 3), "filter"),
        (lambda df: df.groupby("A").corr(), "corr"),
        (lambda df: df.groupby("A").cov(), "cov"),
        (lambda df: df.groupby("A").cumprod(), "cumprod"),
        (lambda df: df.groupby("A").describe(), "describe"),
        (lambda df: df.groupby("A").diff(), "diff"),
        (lambda df: df.groupby("A").ngroup(), "ngroup"),
        (lambda df: df.groupby("A").nth(5), "nth"),
        (lambda df: df.groupby("A").ohlc(), "ohlc"),
        (lambda df: df.groupby("A").prod(), "prod"),
        (lambda df: df.groupby("A").sample(n=1, random_state=1), "sample"),
        (lambda df: df.groupby("A").sem(), "sem"),
        (lambda df: df.groupby("A").skew(), "skew"),
        (lambda df: df.groupby("A").take(2), "take"),
        (lambda df: df.groupby("A").expanding(), "expanding"),
        (lambda df: df.groupby("A").hist(), "hist"),
        (lambda df: df.groupby("A").plot(), "plot"),
        (lambda df: df.groupby("A").boxplot("test_group"), "boxplot"),
    ],
)
def test_dataframe_groupby_unsupported_methods_raises(
    mock_dataframe, func, func_name
) -> None:
    msg = f"Snowpark pandas does not yet support the method GroupBy.{func_name}"
    with pytest.raises(NotImplementedError, match=msg):
        func(mock_dataframe)
