#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from collections.abc import Generator
from typing import Callable, Union

import modin.pandas as pd
import pandas as native_pd
import pytest
from _pytest.logging import LogCaptureFixture

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker


def eval_and_validate_unsupported_methods(
    func: Callable,
    func_name: str,
    native_pd_args: list[Union[native_pd.DataFrame, native_pd.Series]],
    caplog: Generator[LogCaptureFixture, None, None],
    inplace: bool = False,
) -> None:
    """
    Apply callable on the given pandas object (native_pd_args) and also the corresponding derived Snowpark pandas objects.
    Verify the following:
    1) Apply callable on the Snowpark pandas objects triggers the default_to_pandas call in SnowflakeQueryCompiler
    2) The results for both Snowpark pandas and pandas matches each other
    """

    # construct the corresponding Snowpark pandas objects function arguments for the given pandas objects
    snow_pd_args = []
    for native_pd_arg in native_pd_args:
        if isinstance(native_pd_arg, native_pd.DataFrame):
            snow_pd_args.append(pd.DataFrame(native_pd_arg))
        else:
            snow_pd_args.append(pd.Series(native_pd_arg))

    native_pd_args = native_pd_args[0] if len(native_pd_args) == 1 else native_pd_args
    snow_pd_args = snow_pd_args[0] if len(snow_pd_args) == 1 else snow_pd_args

    func(native_pd_args)
    with pytest.raises(NotImplementedError):
        func(snow_pd_args)


# unsupported methods for both dataframe and series
UNSUPPORTED_DATAFRAME_SERIES_METHODS = [
    (lambda df: df.cumprod(), "cumprod"),
]

# unsupported methods that can only be applied on dataframe
UNSUPPORTED_DATAFRAME_METHODS = [
    (lambda df: df.cumprod(axis=1), "cumprod"),
]

# unsupported methods that can only be applied on series
# This set triggers SeriesDefault.register
UNSUPPORTED_SERIES_METHODS = [
    (lambda df: df.transform(lambda x: x + 1), "transform"),
]

# unsupported binary operations that can be applied on both dataframe and series
# this set triggers default_to_pandas test with Snowpark pandas objects in arguments
UNSUPPORTED_BINARY_METHODS = [
    # TODO SNOW-862664, support together with combine
    # (lambda dfs: dfs[0].combine(dfs[1], np.minimum, fill_value=1), "combine"),
    (lambda dfs: dfs[0].update(dfs[1]), "update"),
]


# When any unsupported method gets supported, we should run the test to verify (expect failure)
# and remove the corresponding method in the above list.
# When most of the methods are supported, we should run all unsupported methods
@pytest.mark.parametrize(
    "func, func_name",
    UNSUPPORTED_DATAFRAME_SERIES_METHODS + UNSUPPORTED_DATAFRAME_METHODS,
)
@sql_count_checker(query_count=0)
def test_unsupported_dataframe_methods(func, func_name, caplog):
    data = {"a": [1, 2, 3], "b": [4, 5, 6]}
    # Native pandas
    native_df = native_pd.DataFrame(data)
    eval_and_validate_unsupported_methods(func, func_name, [native_df], caplog)


@pytest.mark.parametrize(
    "func, func_name",
    UNSUPPORTED_SERIES_METHODS + UNSUPPORTED_DATAFRAME_SERIES_METHODS,
)
@sql_count_checker(query_count=0)
def test_unsupported_series_methods(func, func_name, caplog) -> None:
    native_series = native_pd.Series([5, 4, 0, 6, 6, 4])
    eval_and_validate_unsupported_methods(func, func_name, [native_series], caplog)


@pytest.mark.parametrize(
    "func, func_name",
    UNSUPPORTED_BINARY_METHODS,
)
@sql_count_checker(query_count=0)
def test_unsupported_dataframe_binary_methods(func, func_name, caplog) -> None:
    # Native pandas
    native_df1 = native_pd.DataFrame([[0, 1], [2, 3]])
    native_df2 = native_pd.DataFrame([[4, 5], [6, 7]])

    eval_and_validate_unsupported_methods(
        func,
        func_name,
        [native_df1, native_df2],
        caplog,
        inplace=bool(func_name == "update"),
    )


@pytest.mark.parametrize(
    "func, func_name",
    UNSUPPORTED_BINARY_METHODS,
)
@sql_count_checker(query_count=0)
def test_unsupported_series_binary_methods(func, func_name, caplog) -> None:
    native_se1 = native_pd.Series([1, 2, 3, 0, 2])
    native_se2 = native_pd.Series([2, 3, 10, 0, 1])

    eval_and_validate_unsupported_methods(
        func,
        func_name,
        [native_se1, native_se2],
        caplog,
        inplace=bool(func_name == "update"),
    )


# This set triggers StrDefault
# The full set of StringMethods test is under tests/integ/modin/strings/
UNSUPPORTED_STR_METHODS = [
    (lambda se: se.str.rfind("a"), "Series.rfind"),
]


@pytest.mark.parametrize(
    "func, func_name",
    UNSUPPORTED_STR_METHODS,
)
@sql_count_checker(query_count=0)
def test_unsupported_str_methods(func, func_name, caplog) -> None:
    native_series = native_pd.Series(["bat.aB", "com.fcc", "foo", "bar"])
    eval_and_validate_unsupported_methods(func, func_name, [native_series], caplog)


# unsupported methods for Index
UNSUPPORTED_INDEX_METHODS = [
    lambda idx: idx.duplicated(),
    lambda idx: idx.drop(),
    lambda idx: idx.union(),
    lambda idx: idx.difference(),
    lambda idx: idx.get_indexer_for(),
    lambda idx: idx.get_level_values(),
    lambda idx: idx.slice_indexer(),
    lambda idx: idx.nbytes(),
    lambda idx: idx.memory_usage(),
    lambda idx: idx.delete(),
    lambda idx: idx.factorize(),
    lambda idx: idx.insert(),
    lambda idx: idx.is_(),
    lambda idx: idx.is_categorical(),
    lambda idx: idx.is_interval(),
    lambda idx: idx.repeat(),
    lambda idx: idx.where(),
    lambda idx: idx.take(),
    lambda idx: idx.putmask(),
    lambda idx: idx.droplevel(),
    lambda idx: idx.fillna(),
    lambda idx: idx.dropna(),
    lambda idx: idx.isna(),
    lambda idx: idx.notna(),
    lambda idx: idx.map(),
    lambda idx: idx.ravel(),
    lambda idx: idx.argsort(),
    lambda idx: idx.searchsorted(),
    lambda idx: idx.shift(),
    lambda idx: idx.append(),
    lambda idx: idx.join(),
    lambda idx: idx.symmetric_difference(),
    lambda idx: idx.asof(),
    lambda idx: idx.asof_locs(),
    lambda idx: idx.get_indexer(),
    lambda idx: idx.get_indexer_non_unique(),
    lambda idx: idx.get_loc(),
    lambda idx: idx.get_slice_bound(),
    lambda idx: idx.isin(),
    lambda idx: idx.slice_locs(),
]


@pytest.mark.parametrize("func", UNSUPPORTED_INDEX_METHODS)
@sql_count_checker(query_count=0)
def test_unsupported_index_methods(func) -> None:
    index = pd.Index([5, 4, 0, 6, 6, 4])
    with pytest.raises(NotImplementedError):
        func(index)
