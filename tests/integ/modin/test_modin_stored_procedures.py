#!/usr/bin/env python3

#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
import modin.pandas as pd
import pandas as native_pd

from snowflake.snowpark import Session
from snowflake.snowpark.functions import sproc
from tests.integ.utils.sql_counter import sql_count_checker
from tests.utils import multithreaded_run

pytestmark = [pytest.mark.udf]

# Must pin modin version to match version available in Snowflake Anaconda
SPROC_MODIN_VERSION = "0.33.2"

PACKAGE_LIST = [
    # Note that because we specify `snowflake-snowpark-python` as a package here, it will pick whatever
    # version of the package is available in anaconda, not the latest `main` branch.
    # The behavior of stored procedures with `main` is verified in server-side tests and the stored
    # procedure Jenkins job.
    f"pandas=={native_pd.__version__}",
    f"modin=={SPROC_MODIN_VERSION}",
    "snowflake-snowpark-python",
    "numpy",
]


@sql_count_checker(query_count=4, sproc_count=1)
def test_sproc_head(session):
    @sproc(packages=PACKAGE_LIST)
    def run(session_: Session) -> str:
        df = pd.DataFrame(
            [["a", 2.1, 1], ["b", 4.2, 2], ["c", 6.3, None]],
            columns=["COL_STR", "COL_FLOAT", "COL_INT"],
        )
        snow_df = df.head(2)
        return str(snow_df)

    assert (
        run()
        == "  COL_STR  COL_FLOAT  COL_INT\n0       a        2.1      1.0\n1       b        4.2      2.0"
    )


@sql_count_checker(query_count=4, sproc_count=1)
def test_sproc_dropna(session):
    @sproc(packages=PACKAGE_LIST)
    def run(session_: Session) -> int:
        default_index_snowpark_pandas_df = pd.DataFrame(
            [["a", 2.1, 1], ["b", None, 2], ["c", 6.3, None]],
            columns=["COL_STR", "COL_FLOAT", "COL_INT"],
        )
        snow_df = default_index_snowpark_pandas_df.dropna(subset=["COL_FLOAT"])

        return snow_df.shape[0]

    assert run() == 2


@sql_count_checker(query_count=4, sproc_count=1)
def test_sproc_idx(session):
    @sproc(packages=PACKAGE_LIST)
    def run(session_: Session) -> str:
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        df_result = df["a"]
        return str(df_result)

    assert run() == "0    1\n1    2\n2    3\nName: a, dtype: int64"


@sql_count_checker(query_count=4, sproc_count=1)
def test_sproc_loc(session):
    @sproc(packages=PACKAGE_LIST)
    def run(session_: Session) -> str:
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        df_result = df.loc[df["a"] > 2]
        return str(df_result)

    assert run() == "   a  b\n2  3  z"


@sql_count_checker(query_count=4, sproc_count=1)
def test_sproc_iloc(session):
    @sproc(packages=PACKAGE_LIST)
    def run(session_: Session) -> str:
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        df_result = df.iloc[0, 1]
        return df_result

    assert run() == "x"


@sql_count_checker(query_count=4, sproc_count=1)
def test_sproc_missing_val(session):
    @sproc(packages=PACKAGE_LIST)
    def run(session_: Session) -> int:
        import numpy as np

        df = pd.DataFrame(
            [
                [np.nan, 2, np.nan, 0],
                [3, 4, np.nan, 1],
                [np.nan, np.nan, np.nan, np.nan],
                [np.nan, 3, np.nan, 4],
            ],
            columns=list("ABCD"),
        )
        df_result = df.dropna(how="all")
        return df_result.shape[0]

    assert run() == 3


@sql_count_checker(query_count=4, sproc_count=1)
def test_sproc_type_conv(session):
    @sproc(packages=PACKAGE_LIST)
    def run(session_: Session) -> str:
        df = pd.DataFrame({"int": [1, 2, 3], "str": ["4", "5", "6"]})
        df_result = df.astype(float)["int"].iloc[0]
        return str(df_result)

    assert run() == "1.0"


@sql_count_checker(query_count=8, sproc_count=2)
def test_sproc_binary_ops(session):
    @sproc(packages=PACKAGE_LIST)
    def add(session_: Session) -> str:
        df_1 = pd.DataFrame([[1, 2, 3], [4, 5, 6]])
        df_2 = pd.DataFrame([[6, 7, 8]])
        df_result = df_1.add(df_2)
        return str(df_result)

    @sproc(packages=PACKAGE_LIST)
    def plus(session_: Session) -> str:
        s1 = pd.Series([1, 2, 3])
        s2 = pd.Series([2, 2, 2])
        ser_result = s1 + s2
        return str(ser_result)

    assert add() == "     0    1     2\n0  7.0  9.0  11.0\n1  NaN  NaN   NaN"
    assert plus() == "0    3\n1    4\n2    5\ndtype: int64"


@multithreaded_run()
@sql_count_checker(query_count=8, sproc_count=2)
def test_sproc_agg(session):
    @sproc(packages=PACKAGE_LIST)
    def run_agg(session_: Session) -> str:
        import numpy as np

        df = pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9], [np.nan, np.nan, np.nan]],
            columns=["A", "B", "C"],
        )
        df_result = df.agg(["sum", "min"])
        return str(df_result)

    @sproc(packages=PACKAGE_LIST)
    def run_median(session_: Session) -> str:
        import numpy as np

        df = pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9], [np.nan, np.nan, np.nan]],
            columns=["A", "B", "C"],
        )
        df_result = df.median()
        return str(df_result)

    assert (
        run_agg()
        == "        A     B     C\nsum  12.0  15.0  18.0\nmin   1.0   2.0   3.0"
    )
    assert run_median() == "A    4.0\nB    5.0\nC    6.0\ndtype: float64"


@sql_count_checker(query_count=8, sproc_count=2)
def test_sproc_merge(session):
    @sproc(packages=PACKAGE_LIST)
    def run_merge(session_: Session) -> str:
        df1 = pd.DataFrame(
            {"lkey": ["foo", "bar", "baz", "foo"], "value": [1, 2, 3, 5]}
        )
        df2 = pd.DataFrame(
            {"rkey": ["foo", "bar", "baz", "foo"], "value": [5, 6, 7, 8]}
        )
        df_result = df1.merge(df2, left_on="lkey", right_on="rkey")
        return str(df_result["value_x"])

    @sproc(packages=PACKAGE_LIST)
    def run_join(session_: Session) -> str:
        df = pd.DataFrame(
            {
                "key": ["K0", "K1", "K2", "K3", "K4", "K5"],
                "A": ["A0", "A1", "A2", "A3", "A4", "A5"],
            }
        )
        other = pd.DataFrame({"key": ["K0", "K1", "K2"], "B": ["B0", "B1", "B2"]})
        df_result = df.join(other, lsuffix="_caller", rsuffix="_other")
        return str(df_result["key_other"])

    assert (
        run_merge()
        == "0    1\n1    1\n2    2\n3    3\n4    5\n5    5\nName: value_x, dtype: int64"
    )
    assert run_join() == (
        "0      K0\n1      K1\n2      K2\n3    None\n"
        "4    None\n5    None\nName: key_other, dtype: object"
    )


@sql_count_checker(query_count=4, sproc_count=1)
def test_sproc_groupby(session):
    @sproc(packages=PACKAGE_LIST)
    def run(session_: Session) -> str:
        df = pd.DataFrame(
            {
                "Animal": ["Falcon", "Falcon", "Parrot", "Parrot"],
                "Max Speed": [380.0, 370.0, 24.0, 26.0],
            }
        )
        df_result = df.groupby(["Animal"]).mean()
        return str(df_result)

    assert (
        run()
        == "        Max Speed\nAnimal           \nFalcon      375.0\nParrot       25.0"
    )


@sql_count_checker(query_count=4, sproc_count=1)
def test_sproc_pivot(session):
    @sproc(packages=PACKAGE_LIST)
    def run(session_: Session) -> str:
        df = pd.DataFrame(
            {
                "A": ["foo", "foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar"],
                "B": ["one", "one", "one", "two", "two", "one", "one", "two", "two"],
                "C": [
                    "small",
                    "large",
                    "large",
                    "small",
                    "small",
                    "large",
                    "small",
                    "small",
                    "large",
                ],
                "D": [1, 2, 2, 3, 3, 4, 5, 6, 7],
                "E": [2, 4, 5, 5, 6, 6, 8, 9, 9],
            }
        )
        df_result = pd.pivot_table(
            df, values="D", index=["A", "B"], columns=["C"], aggfunc="sum"
        )
        return str(df_result)

    assert run() == (
        "C        large  small\nA   B                \nbar one    4.0      "
        "5\n    two    7.0      6\nfoo one    4.0      1\n    two    NaN      6"
    )


@sql_count_checker(query_count=4, sproc_count=1)
def test_sproc_apply(session):
    @sproc(packages=PACKAGE_LIST)
    def run(session_: Session) -> str:
        import numpy as np

        df = pd.DataFrame([[2, 0], [3, 7], [4, 9]], columns=["A", "B"])
        df_result = df.apply(np.sum, axis=1)
        return str(df_result)

    assert run() == "0     2\n1    10\n2    13\ndtype: int64"


@sql_count_checker(query_count=4, sproc_count=1)
def test_sproc_applymap(session):
    @sproc(packages=PACKAGE_LIST)
    def run(session_: Session) -> str:
        df = pd.DataFrame([[1, 2.12], [3.356, 4.567]])
        df_result = df.applymap(lambda x: len(str(x)))
        return str(df_result)

    assert run() == "   0  1\n0  3  4\n1  5  5"


@sql_count_checker(query_count=4, sproc_count=1)
def test_sproc_devguide_example(session):
    @sproc(packages=PACKAGE_LIST)
    def run(session_: Session) -> int:
        # Create a Snowpark Pandas DataFrame with sample data.
        df = pd.DataFrame(
            [
                [1, "Big Bear", 8],
                [2, "Big Bear", 10],
                [3, "Big Bear", None],
                [1, "Tahoe", 3],
                [2, "Tahoe", None],
                [3, "Tahoe", 13],
                [1, "Whistler", None],
                ["Friday", "Whistler", 40],
                [3, "Whistler", 25],
            ],
            columns=["DAY", "LOCATION", "SNOWFALL"],
        )
        # Drop rows with null values.
        df = df.dropna()
        # In-place point update to fix data error.
        df.loc[df["DAY"] == "Friday", "DAY"] = 2
        # return [df.iloc[4]["DAY"], df.shape[0]]
        return df.iloc[4]["DAY"]

    assert run() == 2
