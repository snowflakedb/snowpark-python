#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import io

import modin.pandas as pd
import pandas as native_pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker


def _assert_info_lines_equal(modin_info: list[str], pandas_info: list[str]):
    # class is different
    assert modin_info[0] == "<class 'modin.pandas.dataframe.DataFrame'>"
    assert pandas_info[0] == "<class 'pandas.core.frame.DataFrame'>"

    # index is different
    assert "SnowflakeIndex" in modin_info[1]
    assert "RangeIndex" in pandas_info[1]

    assert modin_info[2:-1] == pandas_info[2:-1]

    # memory usage is different
    assert "memory usage" in pandas_info[-1]
    assert modin_info[-1] in ("memory usage: 0.0+ bytes", "memory usage: 0.0 bytes")


@sql_count_checker(query_count=1)
def test_info_verbose_true():
    with io.StringIO() as pandas_buffer, io.StringIO() as modin_buffer:
        data = {"A": [-3, -2, 1], "B": [1, 2, 3]}
        sdf = pd.DataFrame(data)
        pdf = native_pd.DataFrame(data)

        sdf.info(buf=modin_buffer, verbose=True)
        pdf.info(buf=pandas_buffer, verbose=True)

        _assert_info_lines_equal(
            modin_buffer.getvalue().splitlines(),
            pandas_buffer.getvalue().splitlines(),
        )


@sql_count_checker(query_count=0)
def test_info_verbose_false():
    with io.StringIO() as pandas_buffer, io.StringIO() as modin_buffer:
        data = {"A": [-3, -2, 1], "B": [1, 2, 3]}
        sdf = pd.DataFrame(data)
        pdf = native_pd.DataFrame(data)

        sdf.info(buf=modin_buffer, verbose=False)
        pdf.info(buf=pandas_buffer, verbose=False)

        _assert_info_lines_equal(
            modin_buffer.getvalue().splitlines(),
            pandas_buffer.getvalue().splitlines(),
        )


@sql_count_checker(query_count=1)
def test_info_counts_true():
    with io.StringIO() as pandas_buffer, io.StringIO() as modin_buffer:
        data = {"A": [-3, -2, 1], "B": [1, 2, 3]}
        sdf = pd.DataFrame(data)
        pdf = native_pd.DataFrame(data)

        sdf.info(buf=modin_buffer, show_counts=True)
        pdf.info(buf=pandas_buffer, show_counts=True)

        _assert_info_lines_equal(
            modin_buffer.getvalue().splitlines(),
            pandas_buffer.getvalue().splitlines(),
        )


@sql_count_checker(query_count=0)
def test_info_counts_false():
    with io.StringIO() as pandas_buffer, io.StringIO() as modin_buffer:
        data = {"A": [-3, -2, 1], "B": [1, 2, 3]}
        sdf = pd.DataFrame(data)
        pdf = native_pd.DataFrame(data)

        # Minor pandas 2.0 difference, not supported by older modin
        sdf.info(buf=modin_buffer, show_counts=False, null_counts=False)
        pdf.info(buf=pandas_buffer, show_counts=False)

        _assert_info_lines_equal(
            modin_buffer.getvalue().splitlines(),
            pandas_buffer.getvalue().splitlines(),
        )


@sql_count_checker(query_count=1)
def test_info_SNOW_962607():
    """
    Test that with single-column dataframes we still perform a properly
    transposed count
    """
    with io.StringIO() as pandas_buffer, io.StringIO() as modin_buffer:
        data = {"A": [-3, -2, 1]}
        sdf = pd.DataFrame(data)
        pdf = native_pd.DataFrame(data)

        # Minor pandas 2.0 difference, not supported by older modin
        sdf.info(buf=modin_buffer, show_counts=True)
        pdf.info(buf=pandas_buffer, show_counts=True)

        _assert_info_lines_equal(
            modin_buffer.getvalue().splitlines(),
            pandas_buffer.getvalue().splitlines(),
        )
