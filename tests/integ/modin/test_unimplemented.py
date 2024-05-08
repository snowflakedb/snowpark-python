#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from collections.abc import Generator
from typing import Callable, Union

import modin.pandas as pd
import pandas as native_pd
import pytest
from _pytest.logging import LogCaptureFixture

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.modin.plugin.default2pandas.stored_procedure_utils import (
    PACKAGING_REQUIREMENT,
    SNOWPARK_PANDAS_IMPORT,
)
from tests.integ.modin.sql_counter import sql_count_checker


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
    (lambda se: se.is_monotonic_increasing, "property fget:is_monotonic_increasing"),
    (lambda se: se.is_monotonic_decreasing, "property fget:is_monotonic_decreasing"),
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
@sql_count_checker(query_count=1)
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


# This set of method triggers DateTimeDefault
# The full set of DateTimeAccessor test is under tests/integ/modin/series/test_dt_accessor.py
UNSUPPORTED_DT_METHODS = [
    (lambda ds: ds.dt.is_month_start, "property fget:is_month_start"),
    (lambda ds: ds.dt.dayofweek, "property fget:dayofweek"),
]


@pytest.mark.parametrize(
    "func, func_name",
    UNSUPPORTED_DT_METHODS,
)
@sql_count_checker(query_count=0)
def test_unsupported_dt_methods(func, func_name, caplog) -> None:
    datetime_series = native_pd.Series(
        native_pd.date_range("2000-01-01", periods=3, freq="h")
    )
    eval_and_validate_unsupported_methods(func, func_name, [datetime_series], caplog)


# Negative test for SNOW-972740 - Apply on a series changes causes errors in a later transpose
@sql_count_checker(query_count=3, fallback_count=0, sproc_count=0)
def test_fallback_transpose_after_apply_in_stored_proc_negative(session):
    def func(session: Session) -> int:
        df = pd.DataFrame([1, 2, 3])
        # apply followed with transpose inside stored procedure fails to resolve
        # the target path today. This is likely due to how Snowpark pandas is
        # installed today, should be resolved once Snowpark pandas is installed as
        # standard conda library. More investigation is needed.
        # Apply is not an inplace update, here we call df[0] = ... to make sure the
        # final df have the apply subquery.
        df[0] = df[0].apply(lambda x: x)
        df.transpose()
        return 42

    packages = list(session.get_packages().values())
    if "pandas" not in packages:
        packages = [native_pd] + packages
    if "snowflake-snowpark-python" not in packages:
        packages = packages + ["snowflake-snowpark-python"]
    if PACKAGING_REQUIREMENT not in packages:
        packages.append(PACKAGING_REQUIREMENT)
    func_proc = session.sproc.register(
        func,
        imports=[SNOWPARK_PANDAS_IMPORT],
        packages=packages,
    )

    with pytest.raises(SnowparkSQLException) as ex_info:
        assert func_proc() == 42
    assert "Python Interpreter Error" in str(ex_info)


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=SnowparkSQLException,
)
@sql_count_checker(query_count=4, fallback_count=0, sproc_count=1)
def test_sum_in_stored_proc(session):
    def func(session: Session) -> int:
        df = pd.DataFrame([9, 8, 7])
        return df.sum()[0]

    packages = list(session.get_packages().values())
    if "pandas" not in packages:
        packages = [native_pd] + packages
    if "snowflake-snowpark-python" not in packages:
        packages = packages + ["snowflake-snowpark-python"]
    if PACKAGING_REQUIREMENT not in packages:
        packages.append(PACKAGING_REQUIREMENT)
    func_proc = session.sproc.register(
        func,
        imports=[SNOWPARK_PANDAS_IMPORT],
        packages=packages,
    )
    assert func_proc() == 24


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=SnowparkSQLException,
)
@sql_count_checker(query_count=4, fallback_count=0, sproc_count=1)
def test_transpose_in_stored_proc(session):
    def func(session: Session) -> int:
        df = pd.DataFrame([9, 8, 7])
        return df.transpose()[2][0]

    packages = list(session.get_packages().values())
    if "pandas" not in packages:
        packages = [native_pd] + packages
    if "snowflake-snowpark-python" not in packages:
        packages = packages + ["snowflake-snowpark-python"]
    if PACKAGING_REQUIREMENT not in packages:
        packages.append(PACKAGING_REQUIREMENT)
    func_proc = session.sproc.register(
        func,
        imports=[SNOWPARK_PANDAS_IMPORT],
        packages=packages,
    )
    assert func_proc() == 7
