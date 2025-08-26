#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import re
import sys
from unittest import mock
import pytest
from snowflake.snowpark._internal.debug_utils import (
    SNOWPARK_PYTHON_DATAFRAME_TRANSFORM_TRACE_LENGTH,
)
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.resources.test_debug_utils_dir.dataframe_generator1 import (
    DataFrameGenerator1,
)
from tests.resources.test_debug_utils_dir.dataframe_generator2 import (
    DataFrameGenerator2,
)

from snowflake.snowpark._internal.utils import set_ast_state, AstFlagSource
import snowflake.snowpark.context as context


pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="SnowparkSQLException is not raised in localtesting mode",
        run=False,
    ),
    pytest.mark.skipif(
        "FIPS_TEST" in os.environ,
        reason="SNOW-2204213: Reading source file location is not correct in FIPS mode",
        run=False,
    ),
]


@pytest.fixture(autouse=True)
def setup(request, session):
    original = session.ast_enabled
    context.configure_development_features(enable_dataframe_trace_on_error=True)
    set_ast_state(AstFlagSource.TEST, True)
    if SNOWPARK_PYTHON_DATAFRAME_TRANSFORM_TRACE_LENGTH in os.environ:
        del os.environ[SNOWPARK_PYTHON_DATAFRAME_TRANSFORM_TRACE_LENGTH]
    yield
    context.configure_development_features(enable_dataframe_trace_on_error=False)
    set_ast_state(AstFlagSource.TEST, original)
    if SNOWPARK_PYTHON_DATAFRAME_TRANSFORM_TRACE_LENGTH in os.environ:
        del os.environ[SNOWPARK_PYTHON_DATAFRAME_TRANSFORM_TRACE_LENGTH]


if sys.version_info >= (3, 11):
    LOCATION_PATTERN = r"(\d+:\d+-\d+:\d+)"
else:
    LOCATION_PATTERN = r"(\d+)"

CURR_FILE_PATH = os.path.abspath(__file__)
DATAFRAME_GENERATOR1_PATH = os.path.join(
    os.path.dirname(os.path.dirname(CURR_FILE_PATH)),
    "resources",
    "test_debug_utils_dir",
    "dataframe_generator1.py",
)
DATAFRAME_GENERATOR2_PATH = os.path.join(
    os.path.dirname(os.path.dirname(CURR_FILE_PATH)),
    "resources",
    "test_debug_utils_dir",
    "dataframe_generator2.py",
)

CURR_FILE_WITH_LOC_PATTERN = f"{re.escape(CURR_FILE_PATH + '|')}{LOCATION_PATTERN}"
DF_GEN1_WITH_LOC_PATTERN = (
    f"{re.escape(DATAFRAME_GENERATOR1_PATH + '|')}{LOCATION_PATTERN}"
)
DF_GEN2_WITH_LOC_PATTERN = (
    f"{re.escape(DATAFRAME_GENERATOR2_PATH + '|')}{LOCATION_PATTERN}"
)


def check_df_debug_lineage(expected_lineage, exc_info, total_length):
    assert str(exc_info.value).count("--- Additional Debug Information ---") == 1

    assert (
        re.search(
            re.escape(
                f"Trace of the most recent dataframe operations associated with the error (total {total_length}):"
            ),
            str(exc_info.value),
        )
        is not None
    ), f"Expected trace not found in {str(exc_info.value)}"

    for pattern in expected_lineage:
        assert (
            re.search(pattern, str(exc_info.value)) is not None
        ), f"Pattern {pattern} not found in {str(exc_info.value)}"


def test_simple_dataframe_lineage(session):
    df_simple = DataFrameGenerator1(session).simple_dataframe()
    err_df = df_simple.select("does_not_exist")
    if sys.version_info >= (3, 11):
        expected_lineage = [
            CURR_FILE_WITH_LOC_PATTERN
            + re.escape(': df_simple.select("does_not_exist")'),
            DF_GEN1_WITH_LOC_PATTERN + re.escape(": self.session.create_dataframe"),
        ]
    else:
        expected_lineage = [
            CURR_FILE_WITH_LOC_PATTERN
            + re.escape(':     err_df = df_simple.select("does_not_exist")'),
            DF_GEN1_WITH_LOC_PATTERN
            + re.escape(":         return self.session.create_dataframe"),
        ]

    with pytest.raises(SnowparkSQLException) as exc_info:
        err_df.collect()
    check_df_debug_lineage(expected_lineage, exc_info, 2)


def test_binary_dataframe_lineage(session):
    df_binary = DataFrameGenerator1(session).dataframe_with_union()
    err_df = df_binary.select("does_not_exist")
    total_length = 7 if sys.version_info >= (3, 10) else 4
    os.environ[SNOWPARK_PYTHON_DATAFRAME_TRANSFORM_TRACE_LENGTH] = str(total_length)

    if sys.version_info >= (3, 11):
        expected_lineage = [
            CURR_FILE_WITH_LOC_PATTERN
            + re.escape(': df_binary.select("does_not_exist")'),
            DF_GEN1_WITH_LOC_PATTERN + re.escape(": df1.union(df2)"),
            DF_GEN1_WITH_LOC_PATTERN + re.escape(': sort("letter")'),
            DF_GEN1_WITH_LOC_PATTERN + re.escape(": distinct()"),
            DF_GEN1_WITH_LOC_PATTERN + re.escape(': filter("id > 0")'),
            DF_GEN1_WITH_LOC_PATTERN + re.escape(": self.session.create_dataframe("),
        ]
    elif sys.version_info >= (3, 10) and sys.version_info < (3, 11):
        expected_lineage = [
            CURR_FILE_WITH_LOC_PATTERN
            + re.escape(':     err_df = df_binary.select("does_not_exist")'),
            DF_GEN1_WITH_LOC_PATTERN + re.escape(":         return df1.union(df2)"),
            DF_GEN1_WITH_LOC_PATTERN + re.escape(':             .sort("letter")'),
            DF_GEN1_WITH_LOC_PATTERN + re.escape(":         ).distinct()"),
            DF_GEN1_WITH_LOC_PATTERN + re.escape(':             .filter("id > 0")'),
            DF_GEN1_WITH_LOC_PATTERN
            + re.escape(":             self.session.create_dataframe("),
            DF_GEN1_WITH_LOC_PATTERN
            + re.escape(":         df2 = self.session.create_dataframe("),
        ]
    else:
        expected_lineage = [
            CURR_FILE_WITH_LOC_PATTERN
            + re.escape(':     err_df = df_binary.select("does_not_exist")'),
            DF_GEN1_WITH_LOC_PATTERN + re.escape(":         return df1.union(df2)"),
            DF_GEN1_WITH_LOC_PATTERN
            + re.escape(":             self.session.create_dataframe("),
            DF_GEN1_WITH_LOC_PATTERN
            + re.escape(":         df2 = self.session.create_dataframe("),
        ]

    with pytest.raises(SnowparkSQLException) as exc_info:
        err_df.collect()
    check_df_debug_lineage(expected_lineage, exc_info, total_length)


def test_dataframe_lineage_with_describe(session):
    if sys.version_info >= (3, 11):
        expected_lineage = [
            DF_GEN1_WITH_LOC_PATTERN + re.escape(": self.session.create_dataframe("),
            DF_GEN1_WITH_LOC_PATTERN + re.escape(': df2.select("non_existent_column")'),
        ]
    else:
        expected_lineage = [
            DF_GEN1_WITH_LOC_PATTERN
            + re.escape(":         df2 = self.session.create_dataframe"),
            DF_GEN1_WITH_LOC_PATTERN
            + re.escape(
                ':         return df1.join(df2.select("non_existent_column"), on="id", how="inner")'
            ),
        ]

    with pytest.raises(SnowparkSQLException) as exc_info:
        DataFrameGenerator1(session).dataframe_with_join_on_bad_col()
    check_df_debug_lineage(expected_lineage, exc_info, 2)


def test_with_loops_and_nested_calls(session):
    df = DataFrameGenerator1(session).dataframe_with_loop()
    err_df = df.select("does_not_exist")

    if sys.version_info >= (3, 11):
        expected_lineage = [
            CURR_FILE_WITH_LOC_PATTERN + re.escape(': df.select("does_not_exist")'),
            DF_GEN1_WITH_LOC_PATTERN
            + re.escape(': df.with_column("new_col", df["id"] + i)'),
            DF_GEN1_WITH_LOC_PATTERN + re.escape(": self.session.create_dataframe("),
        ]
    else:
        expected_lineage = [
            CURR_FILE_WITH_LOC_PATTERN
            + re.escape(':     err_df = df.select("does_not_exist")'),
            DF_GEN1_WITH_LOC_PATTERN
            + re.escape(':             df = df.with_column("new_col", df["id"] + i)'),
            DF_GEN1_WITH_LOC_PATTERN
            + re.escape(":         df = self.session.create_dataframe"),
        ]
    with pytest.raises(SnowparkSQLException) as exc_info:
        err_df.collect()
    check_df_debug_lineage(expected_lineage, exc_info, 3)


def test_env_variable(session):
    df = DataFrameGenerator1(session).dataframe_with_long_operation_chain()
    # unset the env variable
    if SNOWPARK_PYTHON_DATAFRAME_TRANSFORM_TRACE_LENGTH in os.environ:
        del os.environ[SNOWPARK_PYTHON_DATAFRAME_TRANSFORM_TRACE_LENGTH]

    with pytest.raises(SnowparkSQLException) as exc_info:
        df.select("does_not_exist").collect()
    assert (
        str(exc_info.value).count(
            "Trace of the most recent dataframe operations associated with the error"
        )
        == 1
    )
    base_len = len(str(exc_info.value).split("\n"))

    os.environ[SNOWPARK_PYTHON_DATAFRAME_TRANSFORM_TRACE_LENGTH] = "0"
    with pytest.raises(SnowparkSQLException) as exc_info:
        df.select("does_not_exist").collect()
    assert (
        str(exc_info.value).count(
            "Trace of the most recent dataframe operations associated with the error"
        )
        == 1
    )
    assert len(str(exc_info.value).split("\n")) == base_len - 5

    os.environ[SNOWPARK_PYTHON_DATAFRAME_TRANSFORM_TRACE_LENGTH] = "10"
    with pytest.raises(SnowparkSQLException) as exc_info:
        df.select("does_not_exist").collect()
    assert (
        str(exc_info.value).count(
            "Trace of the most recent dataframe operations associated with the error"
        )
        == 1
    )
    assert len(str(exc_info.value).split("\n")) == base_len + 5


def test_multiple_files(session):
    df1 = DataFrameGenerator1(session).simple_dataframe()
    df2 = DataFrameGenerator2(session).simple_dataframe()
    join_df = df1.join(df2, on="id", how="inner")
    err_df = join_df.select("does_not_exist")

    if sys.version_info >= (3, 11):
        expected_lineage = [
            CURR_FILE_WITH_LOC_PATTERN
            + re.escape(': join_df.select("does_not_exist")'),
            CURR_FILE_WITH_LOC_PATTERN
            + re.escape(': df1.join(df2, on="id", how="inner")'),
            DF_GEN1_WITH_LOC_PATTERN + re.escape(": self.session.create_dataframe"),
            DF_GEN2_WITH_LOC_PATTERN + re.escape(": self.session.create_dataframe"),
        ]
    else:
        expected_lineage = [
            CURR_FILE_WITH_LOC_PATTERN
            + re.escape(':     err_df = join_df.select("does_not_exist")'),
            CURR_FILE_WITH_LOC_PATTERN
            + re.escape(':     join_df = df1.join(df2, on="id", how="inner")'),
            DF_GEN1_WITH_LOC_PATTERN
            + re.escape(":         return self.session.create_dataframe"),
            DF_GEN2_WITH_LOC_PATTERN
            + re.escape(":         return self.session.create_dataframe"),
        ]

    with pytest.raises(SnowparkSQLException) as exc_info:
        err_df.collect()
    check_df_debug_lineage(expected_lineage, exc_info, 4)


def test_error_in_lineage_extraction_is_safe(session):
    err_df = DataFrameGenerator1(session).simple_dataframe().select("does_not_exist")
    with mock.patch(
        "snowflake.snowpark._internal.analyzer.snowflake_plan.get_df_transform_trace_message",
        side_effect=Exception("test"),
    ):
        with pytest.raises(SnowparkSQLException) as exc_info:
            err_df.collect()
        assert (
            "Trace of the most recent dataframe operations associated with the error"
            not in str(exc_info.value)
        )

        with pytest.raises(SnowparkSQLException) as exc_info:
            DataFrameGenerator1(session).dataframe_with_join_on_bad_col()

        assert (
            "Trace of the most recent dataframe operations associated with the error"
            not in str(exc_info.value)
        )


def test_enable_and_disable_extract_debug_trace(session):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    context.configure_development_features(enable_dataframe_trace_on_error=True)
    with pytest.raises(SnowparkSQLException) as exc_info:
        DataFrameGenerator1(session).simple_dataframe().select("does_not_exist").show()
    assert (
        "Trace of the most recent dataframe operations associated with the error"
        in str(exc_info.value)
    )

    context.configure_development_features(enable_dataframe_trace_on_error=False)
    with pytest.raises(SnowparkSQLException) as exc_info:
        DataFrameGenerator1(session).simple_dataframe().select("does_not_exist").show()
    assert (
        "Trace of the most recent dataframe operations associated with the error"
        not in str(exc_info.value)
    )
