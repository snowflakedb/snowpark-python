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
    def run(session_: Session) -> int:
        default_index_snowpark_pandas_df = pd.DataFrame(
            [["a", 2.1, 1], ["b", 4.2, 2], ["c", 6.3, None]],
            columns=["COL_STR", "COL_FLOAT", "COL_INT"],
        )
        snow_df = default_index_snowpark_pandas_df.head(2)
        return snow_df.shape[0]

    assert run() == 2


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
    def idx(session_: Session) -> int:
        default_index_snowpark_pandas_df = pd.DataFrame(
            {"a": [1, 2, 3], "b": ["x", "y", "z"]}
        )
        snow_df = default_index_snowpark_pandas_df["a"]
        return snow_df.shape[0]

    @sproc(
        packages=[
            "packaging",
            "pandas==2.2.1",
            "modin==0.28.1",
            "snowflake-snowpark-python",
        ]
    )
    def loc(session_: Session) -> int:
        default_index_snowpark_pandas_df = pd.DataFrame(
            {"a": [1, 2, 3], "b": ["x", "y", "z"]}
        )
        snow_df = default_index_snowpark_pandas_df.loc[
            default_index_snowpark_pandas_df["a"] > 2
        ]
        return snow_df.shape[0]

    @sproc(
        packages=[
            "packaging",
            "pandas==2.2.1",
            "modin==0.28.1",
            "snowflake-snowpark-python",
        ]
    )
    def iloc(session_: Session) -> str:
        default_index_snowpark_pandas_df = pd.DataFrame(
            {"a": [1, 2, 3], "b": ["x", "y", "z"]}
        )
        snow_res = default_index_snowpark_pandas_df.iloc[0, 1]
        return snow_res

    assert idx() == 3
    assert loc() == 1
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

        default_index_snowpark_pandas_df = pd.DataFrame(
            [
                [np.nan, 2, np.nan, 0],
                [3, 4, np.nan, 1],
                [np.nan, np.nan, np.nan, np.nan],
                [np.nan, 3, np.nan, 4],
            ],
            columns=list("ABCD"),
        )
        snow_df = default_index_snowpark_pandas_df.dropna(how="all")
        return snow_df.shape[0]

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
        default_index_snowpark_pandas_df = pd.DataFrame(
            {"int": [1, 2, 3], "str": ["4", "5", "6"]}
        )
        snow_df = default_index_snowpark_pandas_df.astype(float)["int"].iloc[0]
        return str(snow_df)

    assert run() == "1.0"
