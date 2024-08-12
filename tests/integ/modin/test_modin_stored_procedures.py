#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import pytest

try:
    import modin.pandas as pd  # noqa: F401

    is_pandas_available = True
except ImportError:
    is_pandas_available = False

from snowflake.snowpark import Session
from snowflake.snowpark.functions import sproc
from tests.utils import TestFiles, Utils

pytestmark = [
    pytest.mark.udf,
]

tmp_stage_name = Utils.random_stage_name()


@pytest.fixture(scope="module", autouse=True)
def setup(session, resources_path, local_testing_mode):
    test_files = TestFiles(resources_path)
    if not local_testing_mode:
        Utils.create_stage(session, tmp_stage_name, is_temporary=True)
        session.add_packages("snowflake-snowpark-python")
    Utils.upload_to_stage(
        session, tmp_stage_name, test_files.test_sp_py_file, compress=False
    )


def test_sproc_head(session, local_testing_mode):
    @sproc(
        packages=[
            "packaging",
            "pandas==2.2.1",
            "modin==0.28.1",
            "snowflake-snowpark-python",
        ]
    )
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


def test_sproc_dropna(session, local_testing_mode):
    @sproc(
        packages=[
            "packaging",
            "pandas==2.2.1",
            "modin==0.28.1",
            "snowflake-snowpark-python",
        ]
    )
    def run(session_: Session) -> int:
        default_index_snowpark_pandas_df = pd.DataFrame(
            [["a", 2.1, 1], ["b", None, 2], ["c", 6.3, None]],
            columns=["COL_STR", "COL_FLOAT", "COL_INT"],
        )
        snow_df = default_index_snowpark_pandas_df.dropna(subset=["COL_FLOAT"])

        return snow_df.shape[0]

    assert run() == 2


def test_sproc_index(session, local_testing_mode):
    @sproc(
        packages=[
            "packaging",
            "pandas==2.2.1",
            "modin==0.28.1",
            "snowflake-snowpark-python",
        ]
    )
    def idx(session_: Session) -> str:
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        df_result = df["a"]
        return str(df_result)

    @sproc(
        packages=[
            "packaging",
            "pandas==2.2.1",
            "modin==0.28.1",
            "snowflake-snowpark-python",
        ]
    )
    def loc(session_: Session) -> str:
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        df_result = df.loc[df["a"] > 2]
        return str(df_result)

    @sproc(
        packages=[
            "packaging",
            "pandas==2.2.1",
            "modin==0.28.1",
            "snowflake-snowpark-python",
        ]
    )
    def iloc(session_: Session) -> str:
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        df_result = df.iloc[0, 1]
        return df_result

    assert idx() == "0    1\n1    2\n2    3\nName: a, dtype: int64"
    assert loc() == "   a  b\n2  3  z"
    assert iloc() == "x"


def test_sproc_missing_val(session, local_testing_mode):
    @sproc(
        packages=[
            "packaging",
            "pandas==2.2.1",
            "modin==0.28.1",
            "snowflake-snowpark-python",
            "numpy",
        ]
    )
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


def test_sproc_type_conv(session, local_testing_mode):
    @sproc(
        packages=[
            "packaging",
            "pandas==2.2.1",
            "modin==0.28.1",
            "snowflake-snowpark-python",
        ]
    )
    def run(session_: Session) -> str:
        df = pd.DataFrame({"int": [1, 2, 3], "str": ["4", "5", "6"]})
        df_result = df.astype(float)["int"].iloc[0]
        return str(df_result)

    assert run() == "1.0"


def test_sproc_binary_ops(session, local_testing_mode):
    @sproc(
        packages=[
            "pandas==2.2.1",
            "modin==0.28.1",
            "snowflake-snowpark-python",
        ]
    )
    def add(session_: Session) -> str:
        df_1 = pd.DataFrame([[1, 2, 3], [4, 5, 6]])
        df_2 = pd.DataFrame([[6, 7, 8]])
        df_result = df_1.add(df_2)
        return str(df_result)

    @sproc(
        packages=[
            "pandas==2.2.1",
            "modin==0.28.1",
            "snowflake-snowpark-python",
        ]
    )
    def plus(session_: Session) -> str:
        s1 = pd.Series([1, 2, 3])
        s2 = pd.Series([2, 2, 2])
        ser_result = s1 + s2
        return str(ser_result)

    assert add() == "     0    1     2\n0  7.0  9.0  11.0\n1  NaN  NaN   NaN"
    assert plus() == "0    3\n1    4\n2    5\ndtype: int64"


def test_sproc_agg(session, local_testing_mode):
    @sproc(
        packages=[
            "pandas==2.2.1",
            "modin==0.28.1",
            "snowflake-snowpark-python",
            "numpy",
        ]
    )
    def run_agg(session_: Session) -> str:
        import numpy as np

        df = pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9], [np.nan, np.nan, np.nan]],
            columns=["A", "B", "C"],
        )
        df_result = df.agg(["sum", "min"])
        return str(df_result)

    @sproc(
        packages=[
            "pandas==2.2.1",
            "modin==0.28.1",
            "snowflake-snowpark-python",
            "numpy",
        ]
    )
    def run_median(session_: Session) -> str:
        import numpy as np

        df = pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9], [np.nan, np.nan, np.nan]],
            columns=["A", "B", "C"],
        )
        df_result = df.median()
        return str(df_result)

    assert run_median() == "A    4.0\nB    5.0\nC    6.0\ndtype: float64"


def test_sproc_merge(session, local_testing_mode):
    @sproc(
        packages=[
            "pandas==2.2.1",
            "modin==0.28.1",
            "snowflake-snowpark-python",
            "numpy",
        ]
    )
    def run_merge(session_: Session) -> str:
        df1 = pd.DataFrame(
            {"lkey": ["foo", "bar", "baz", "foo"], "value": [1, 2, 3, 5]}
        )
        df2 = pd.DataFrame(
            {"rkey": ["foo", "bar", "baz", "foo"], "value": [5, 6, 7, 8]}
        )
        df_result = df1.merge(df2, left_on="lkey", right_on="rkey")
        return str(df_result["value_x"])

    @sproc(
        packages=[
            "pandas==2.2.1",
            "modin==0.28.1",
            "snowflake-snowpark-python",
            "numpy",
        ]
    )
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


def test_sproc_groupby(session, local_testing_mode):
    @sproc(
        packages=[
            "pandas==2.2.1",
            "modin==0.28.1",
            "snowflake-snowpark-python",
        ]
    )
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


def test_sproc_pivot(session, local_testing_mode):
    @sproc(
        packages=[
            "pandas==2.2.1",
            "modin==0.28.1",
            "snowflake-snowpark-python",
        ]
    )
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


def test_sproc_devguide_example(session, local_testing_mode):
    @sproc(
        packages=[
            "pandas==2.2.1",
            "modin==0.28.1",
            "snowflake-snowpark-python",
        ]
    )
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
