#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import sys
import pytest
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.resources.test_df_debug_dir.dataframe_generator1 import DataFrameGenerator1
from tests.resources.test_df_debug_dir.dataframe_generator2 import DataFrameGenerator2

from snowflake.snowpark._internal.utils import set_ast_state, AstFlagSource

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="CTE is a SQL feature",
        run=False,
    ),
]


@pytest.fixture(autouse=True)
def setup(request, session):
    original = session.ast_enabled
    set_ast_state(AstFlagSource.TEST, True)
    yield
    set_ast_state(AstFlagSource.TEST, original)


BASE_DEBUG_ERROR = [
    "--- Additional Debug Information ---",
    "Trace of the dataframe operations that could have caused the error",
]
if sys.version_info >= (3, 11):
    LOCATION_PATTERN = r"(\d+:\d+-\d+:\d+)"
else:
    LOCATION_PATTERN = r"(\d+)"

CURR_FILE_PATH = os.path.dirname(__file__)
DATAFRAME_GENERATOR1_PATH = os.path.join(
    os.path.dirname(CURR_FILE_PATH),
    "resources",
    "test_df_debug_dir",
    "dataframe_generator1.py",
)
DATAFRAME_GENERATOR2_PATH = os.path.join(
    os.path.dirname(CURR_FILE_PATH),
    "resources",
    "test_df_debug_dir",
    "dataframe_generator2.py",
)


def test_simple_dataframe_lineage(session):
    df_simple = DataFrameGenerator1(session).simple_dataframe()
    err_df = df_simple.select("does_not_exist")
    if sys.version_info >= (3, 11):
        expected_lineage = [
            f'{CURR_FILE_PATH}|{LOCATION_PATTERN}:  df_simple.select("does_not_exist")',
            f"{DATAFRAME_GENERATOR1_PATH}|{LOCATION_PATTERN}:  self.session.create_dataframe",
        ]
    else:
        expected_lineage = [
            f'{CURR_FILE_PATH}|{LOCATION_PATTERN}:     err_df = df_simple.select("does_not_exist")',
            f"{DATAFRAME_GENERATOR1_PATH}|{LOCATION_PATTERN}:         return self.session.create_dataframe",
        ]

    expected_error_match = ".*".join(BASE_DEBUG_ERROR + expected_lineage)

    with pytest.raises(SnowparkSQLException, match=expected_error_match):
        err_df.collect()


def test_binary_dataframe_lineage(session):
    df_binary = DataFrameGenerator1(session).dataframe_with_union()
    err_df = df_binary.select("does_not_exist")

    if sys.version_info >= (3, 11):
        expected_lineage = [
            f'{CURR_FILE_PATH}|{LOCATION_PATTERN}:  df_binary.select("does_not_exist")',
            f"{DATAFRAME_GENERATOR1_PATH}|{LOCATION_PATTERN}:  df1.union(df2)",
            f"{DATAFRAME_GENERATOR1_PATH}|{LOCATION_PATTERN}:  self.session.create_dataframe",
            f"{DATAFRAME_GENERATOR1_PATH}|{LOCATION_PATTERN}:  self.session.create_dataframe",
        ]
    else:
        expected_lineage = [
            f'{CURR_FILE_PATH}|{LOCATION_PATTERN}:     err_df = df_binary.select("does_not_exist")',
            f"{DATAFRAME_GENERATOR1_PATH}|{LOCATION_PATTERN}:         return df1.union(df2)",
            f"{DATAFRAME_GENERATOR1_PATH}|{LOCATION_PATTERN}:             self.session.create_dataframe",
            f"{DATAFRAME_GENERATOR1_PATH}|{LOCATION_PATTERN}:             self.session.create_dataframe",
        ]

    expected_error_match = ".*".join(BASE_DEBUG_ERROR + expected_lineage)

    with pytest.raises(SnowparkSQLException, match=expected_error_match):
        err_df.collect()


def test_dataframe_lineage_with_describe(session):
    if sys.version_info >= (3, 11):
        expected_lineage = [
            f"{DATAFRAME_GENERATOR1_PATH}|{LOCATION_PATTERN}:  self.session.create_dataframe",
            f'{DATAFRAME_GENERATOR1_PATH}|{LOCATION_PATTERN}:  df2.select("non_existent_column"), on="id", how="inner"',
            f"{DATAFRAME_GENERATOR1_PATH}|{LOCATION_PATTERN}:  self.session.create_dataframe",
        ]
    else:
        expected_lineage = [
            f"{DATAFRAME_GENERATOR1_PATH}|{LOCATION_PATTERN}:         df1 = self.session.create_dataframe",
            f'{DATAFRAME_GENERATOR1_PATH}|{LOCATION_PATTERN}:         return df1.join(df2.select("non_existent_column"), on="id", how="inner")',
            f"{DATAFRAME_GENERATOR1_PATH}|{LOCATION_PATTERN}:         df2 = self.session.create_dataframe",
        ]

    expected_error_match = ".*".join(BASE_DEBUG_ERROR + expected_lineage)
    with pytest.raises(SnowparkSQLException, match=expected_error_match):
        DataFrameGenerator1(session).dataframe_with_join_on_bad_col()


def test_with_loops_and_nested_calls(session):
    df = DataFrameGenerator1(session).dataframe_with_loop()
    err_df = df.select("does_not_exist")

    if sys.version_info >= (3, 11):
        expected_lineage = [
            f'{CURR_FILE_PATH}|{LOCATION_PATTERN}:  df.select("does_not_exist")',
            f'{DATAFRAME_GENERATOR1_PATH}|{LOCATION_PATTERN}:  df.with_column("new_col", df["id"] + i)',
            f"{DATAFRAME_GENERATOR1_PATH}|{LOCATION_PATTERN}:  self.session.create_dataframe",
        ]
    else:
        expected_lineage = [
            f'{CURR_FILE_PATH}|{LOCATION_PATTERN}:     err_df = df.select("does_not_exist")',
            f'{DATAFRAME_GENERATOR1_PATH}|{LOCATION_PATTERN}:             df = df.with_column("new_col", df["id"] + i)',
            f"{DATAFRAME_GENERATOR1_PATH}|{LOCATION_PATTERN}:         df = self.session.create_dataframe",
        ]
    expected_error_match = ".*".join(BASE_DEBUG_ERROR + expected_lineage)
    with pytest.raises(SnowparkSQLException, match=expected_error_match) as exc_info:
        err_df.collect()

    # assert that we only add the debug information once
    assert str(exc_info.value).count("--- Additional Debug Information ---") == 1


def test_env_variable(session):
    LINAGE_ENV_VAR = "SNOWPARK_PYTHON_DATAFRAME_LINEAGE_LENGTH_ON_ERROR"

    df = DataFrameGenerator1(session).dataframe_with_long_operation_chain()
    # unset the env variable
    if LINAGE_ENV_VAR in os.environ:
        del os.environ[LINAGE_ENV_VAR]

    with pytest.raises(SnowparkSQLException) as exc_info:
        df.select("does_not_exist").collect()
    assert str(exc_info.value).count("--- Additional Debug Information ---") == 1
    base_len = len(str(exc_info.value).split("\n"))

    os.environ[LINAGE_ENV_VAR] = "0"
    with pytest.raises(SnowparkSQLException) as exc_info:
        df.select("does_not_exist").collect()
    assert str(exc_info.value).count("--- Additional Debug Information ---") == 1
    assert len(str(exc_info.value).split("\n")) == base_len - 5

    os.environ[LINAGE_ENV_VAR] = "10"
    with pytest.raises(SnowparkSQLException) as exc_info:
        df.select("does_not_exist").collect()
    assert str(exc_info.value).count("--- Additional Debug Information ---") == 1
    assert len(str(exc_info.value).split("\n")) == base_len + 5


def test_multiple_files(session):
    df1 = DataFrameGenerator1(session).simple_dataframe()
    df2 = DataFrameGenerator2(session).simple_dataframe()
    join_df = df1.join(df2, on="id", how="inner")
    err_df = join_df.select("does_not_exist")

    if sys.version_info >= (3, 11):
        expected_lineage = [
            f'{CURR_FILE_PATH}|{LOCATION_PATTERN}:  join_df.select("does_not_exist")',
            f'{CURR_FILE_PATH}|{LOCATION_PATTERN}:  df1.join(df2, on="id", how="inner")',
            f"{DATAFRAME_GENERATOR1_PATH}|{LOCATION_PATTERN}:  self.session.create_dataframe",
            f"{DATAFRAME_GENERATOR2_PATH}|{LOCATION_PATTERN}:  self.session.create_dataframe",
        ]
    else:
        expected_lineage = [
            f'{CURR_FILE_PATH}|{LOCATION_PATTERN}:     err_df = join_df.select("does_not_exist")',
            f'{CURR_FILE_PATH}|{LOCATION_PATTERN}:     join_df = df1.join(df2, on="id", how="inner")',
            f"{DATAFRAME_GENERATOR1_PATH}|{LOCATION_PATTERN}:         return self.session.create_dataframe",
            f"{DATAFRAME_GENERATOR2_PATH}|{LOCATION_PATTERN}:         return self.session.create_dataframe",
        ]
    expected_error_match = ".*".join(BASE_DEBUG_ERROR + expected_lineage)
    with pytest.raises(SnowparkSQLException, match=expected_error_match):
        err_df.collect()
