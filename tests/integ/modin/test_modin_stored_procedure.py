#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# Tests to ensure Snowpark pandas can run in stored procedures, and that overrides are properly
# performed. These methods are chosen in particular because we overwrite them using ordinary
# Python means, rather than modin.pandas.api.extensions, and are therefore vulnerable to
# an importlib.reload call and sensitive to import order.
#
# We're not worried about the correctness of results here, just that the returned object is the
# correct type.

import os
from typing import Any, Callable

import modin.pandas as pd
import pytest

import snowflake.snowpark.modin.plugin as plugin  # noqa: F401
from snowflake.snowpark.session import Session
from tests.integ.modin.sql_counter import sql_count_checker
from tests.utils import running_on_public_ci


def run_modin_sproc_func(func: Callable, session: Session) -> Any:
    SNOWPARK_PANDAS_IMPORT = (
        # move from $BASE/tests/integ/modin up to $BASE/src/snowflake
        os.path.join(
            os.path.dirname(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            ),
            "src/snowflake/snowpark",
        ),
        "snowflake.snowpark",
    )
    packages = list(session.get_packages().values())
    PACKAGING_REQUIREMENT = "packaging>=21.0"
    MODIN_REQUIREMENT = "modin==" + plugin.supported_modin_version
    if PACKAGING_REQUIREMENT not in packages:
        packages.append(PACKAGING_REQUIREMENT)
    if MODIN_REQUIREMENT not in packages:
        packages.append(MODIN_REQUIREMENT)
    func_proc = session.sproc.register(
        func,
        imports=[SNOWPARK_PANDAS_IMPORT],
        packages=packages,
    )
    return func_proc()


# !!!
# test_reimport_in_sproc and test_dt_isocalendar_in_sproc are deliberately kept as part of the merge gate
# to ensure no weird import shenanigans
# !!!


@sql_count_checker(query_count=4, sproc_count=1)
def test_reimport_in_sproc(session: Session):
    # Returns a DataFrame
    def func(session: Session) -> str:
        import modin.pandas as pd

        import snowflake.snowpark.modin.plugin as plugin  # noqa: F401

        return type(pd.Series(pd.Timestamp("2020-01-01")).dt.isocalendar())

    assert (
        run_modin_sproc_func(func, session)
        == "<class 'snowflake.snowpark.modin.pandas.dataframe.DataFrame'>"
    )


@sql_count_checker(query_count=4, sproc_count=1)
def test_dt_isocalendar_in_sproc(session: Session):
    # Returns a DataFrame
    def func(session: Session) -> str:
        return type(pd.Series(pd.Timestamp("2020-01-01")).dt.isocalendar())

    assert (
        run_modin_sproc_func(func, session)
        == "<class 'snowflake.snowpark.modin.pandas.dataframe.DataFrame'>"
    )


@pytest.mark.skipif(running_on_public_ci(), reason="slow sproc test")
@sql_count_checker(query_count=4, sproc_count=1)
def test_dt_date_in_sproc(session: Session):
    # Returns a Series
    def func(session: Session) -> str:
        return type(pd.Series(pd.Timestamp("2020-01-01")).dt.date)

    assert (
        run_modin_sproc_func(func, session)
        == "<class 'snowflake.snowpark.modin.pandas.series.Series'>"
    )


@pytest.mark.skipif(running_on_public_ci(), reason="slow sproc test")
@sql_count_checker(query_count=4, sproc_count=1)
def test_str_len_in_sproc(session: Session):
    # Returns a Series
    def func(session: Session) -> str:
        return type(pd.Series("a").str.len())

    assert (
        run_modin_sproc_func(func, session)
        == "<class 'snowflake.snowpark.modin.pandas.series.Series'>"
    )
