#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import inspect
import os
import sys

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker


def expect_type_check(df, expected_type: type, expected_class_name: str) -> None:
    """helper function to check whether df adheres to expected type and classname"""
    assert isinstance(
        df, expected_type
    ), "Snowpark pandas expects type {}, but got {}".format(
        str(expected_type), str(type(df))
    )
    class_name_str = f"<class '{expected_class_name}'>"
    assert (
        str(df.__class__) == class_name_str
    ), f"Snowpark pandas expected classname {expected_class_name}, but got {str(df.__class__)}"


@pytest.mark.skipif(
    sys.version_info != (3, 8), reason="stored proc only supported for pyton3.8"
)
def test_class_names_constructors():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    expect_type_check(
        df,
        pd.DataFrame,
        "modin.pandas.dataframe.DataFrame",
    )

    s = pd.Series(index=[1, 2, 3], data=[3, 2, 1])
    expect_type_check(
        s,
        pd.Series,
        "modin.pandas.series.Series",
    )


@sql_count_checker(query_count=0)
def test_class_names_io_entry_points(tmp_path):
    # make sure I/O entry points return correct classes as well
    frame = inspect.currentframe()
    func_name = frame.f_code.co_name
    test_path = os.path.join(tmp_path, f"{func_name}_test.csv")
    with open(test_path, "w") as fp:
        fp.write("a,b,c\n1,2,3\n4,5,6\n7,8,9\n")


@sql_count_checker(query_count=0)
def test_op():
    # make sure operations still return Snowpark pandas
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    df = df[df.a >= 2]
    expect_type_check(
        df,
        pd.DataFrame,
        "modin.pandas.dataframe.DataFrame",
    )


@sql_count_checker(query_count=2)
def test_native_conversion():
    # native pandas -> Snowpark pandas
    ndf = native_pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    expect_type_check(ndf, native_pd.DataFrame, "pandas.core.frame.DataFrame")

    df = pd.DataFrame(ndf)
    expect_type_check(
        df,
        pd.DataFrame,
        "modin.pandas.dataframe.DataFrame",
    )

    # Snowpark pandas -> native pandas
    # convert using modin version (private method, as there is no public method desired)
    ndf = df._to_pandas()
    expect_type_check(ndf, native_pd.DataFrame, "pandas.core.frame.DataFrame")

    # convert using DataFrame way w. public method
    ndf = df.to_pandas()
    expect_type_check(ndf, native_pd.DataFrame, "pandas.core.frame.DataFrame")
