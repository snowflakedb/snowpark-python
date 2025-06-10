#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from unittest.mock import patch

import pytest

from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import (
    DataFrameReference,
    OrderedDataFrame,
    OrderingColumn,
)
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from snowflake.snowpark.types import ColumnIdentifier, IntegerType, StructField


@pytest.fixture(scope="function")
@patch("snowflake.snowpark.dataframe.DataFrame")
def test_query_compiler(mock_dataframe) -> SnowflakeQueryCompiler:
    snowpark_df_schema_fields = (
        StructField(column_identifier=ColumnIdentifier('"a"'), datatype=IntegerType),
        StructField(column_identifier=ColumnIdentifier('"B"'), datatype=IntegerType),
        StructField(column_identifier=ColumnIdentifier('"C"'), datatype=IntegerType),
        StructField(
            column_identifier=ColumnIdentifier('"INDEX"'), datatype=IntegerType
        ),
    )
    mock_dataframe.schema.fields = snowpark_df_schema_fields

    internal_frame = InternalFrame.create(
        ordered_dataframe=OrderedDataFrame(
            DataFrameReference(mock_dataframe),
            ordering_columns=[OrderingColumn('"INDEX"')],
        ),
        data_column_pandas_labels=["a", "B"],
        data_column_pandas_index_names=["(label1, label2)"],
        data_column_snowflake_quoted_identifiers=['"a"', '"B"'],
        index_column_pandas_labels=["INDEX", "C"],
        index_column_snowflake_quoted_identifiers=['"INDEX"', '"C"'],
        data_column_types=None,
        index_column_types=None,
    )

    return SnowflakeQueryCompiler(internal_frame)


def test_get_index_names(test_query_compiler) -> None:
    assert test_query_compiler.get_index_names(axis=0) == ["INDEX", "C"]
    assert test_query_compiler.get_index_names(axis=1) == ["(label1, label2)"]


def test_copy(test_query_compiler) -> None:
    test_query_compiler.snowpark_pandas_api_calls.append({"key1": "value1"})
    copy_qc = test_query_compiler.copy()
    # Verify modin frame and telemetry data is copied.
    assert copy_qc._modin_frame == test_query_compiler._modin_frame
    assert copy_qc.snowpark_pandas_api_calls == [{"key1": "value1"}]

    # Modify copy query compiler
    copy_qc.snowpark_pandas_api_calls.append({"key2": "value2"})
    # Verify original query compiler remains unchanged.
    assert test_query_compiler.snowpark_pandas_api_calls == [{"key1": "value1"}]
