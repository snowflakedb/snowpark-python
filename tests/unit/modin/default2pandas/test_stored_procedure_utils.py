#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import Any, Union
from unittest import mock

import pytest
from modin.pandas import DataFrame, Series

from snowflake.snowpark import Session
from snowflake.snowpark.dataframe import DataFrame as SnowparkDataFrame
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import (
    DataFrameReference,
    OrderedDataFrame,
    OrderingColumn,
)
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from snowflake.snowpark.modin.plugin.default2pandas.stored_procedure_utils import (
    SnowparkPandasObjectPickleData,
    SnowparkPandasObjectType,
    StoredProcedureDefault,
)
from snowflake.snowpark.types import (
    ColumnIdentifier,
    IntegerType,
    StructField,
    StructType,
)


def mock_snowpark_dataframe() -> SnowparkDataFrame:
    fake_snowpark_dataframe = mock.create_autospec(SnowparkDataFrame)
    snowpark_df_schema = StructType(
        [
            StructField(
                column_identifier=ColumnIdentifier('"A"'), datatype=IntegerType
            ),
            StructField(
                column_identifier=ColumnIdentifier('"B"'), datatype=IntegerType
            ),
            StructField(
                column_identifier=ColumnIdentifier('"C"'), datatype=IntegerType
            ),
            StructField(
                column_identifier=ColumnIdentifier('"D"'), datatype=IntegerType
            ),
            StructField(
                column_identifier=ColumnIdentifier('"INDEX"'), datatype=IntegerType
            ),
        ]
    )
    fake_snowpark_dataframe.schema = snowpark_df_schema
    fake_snowpark_dataframe.select.return_value = fake_snowpark_dataframe

    return fake_snowpark_dataframe


@pytest.fixture(scope="module")
def mock_session() -> Session:
    fake_session = mock.create_autospec(Session)
    fake_snowpark_dataframe = mock_snowpark_dataframe()
    fake_session.table.return_value = fake_snowpark_dataframe

    return fake_session


def create_df_test_pickle_data(
    table_name: str, obj_type: SnowparkPandasObjectType
) -> SnowparkPandasObjectPickleData:
    return SnowparkPandasObjectPickleData(
        table_name=table_name,
        object_type=obj_type.name,
        data_column_pandas_labels=["A", "B"],
        data_column_pandas_index_names=[None],
        data_column_snowflake_quoted_identifiers=['"A"', '"B"'],
        index_column_pandas_labels=["INDEX"],
        index_column_snowflake_quoted_identifiers=['"INDEX"'],
        ordering_columns=[OrderingColumn('"INDEX"')],
        row_position_snowflake_quoted_identifier=None,
    )


def create_series_test_pickle_data(table_name: str) -> SnowparkPandasObjectPickleData:
    return SnowparkPandasObjectPickleData(
        table_name=table_name,
        object_type=SnowparkPandasObjectType.SERIES.name,
        data_column_pandas_labels=["C"],
        data_column_pandas_index_names=[None],
        data_column_snowflake_quoted_identifiers=['"C"'],
        index_column_pandas_labels=["INDEX"],
        index_column_snowflake_quoted_identifiers=['"INDEX"'],
        ordering_columns=[OrderingColumn('"INDEX"')],
        row_position_snowflake_quoted_identifier=None,
    )


def mock_snowpark_pandas_object(
    obj_type: SnowparkPandasObjectType,
) -> Union[DataFrame, Series, SnowflakeQueryCompiler]:
    fake_snowpark_dataframe = mock_snowpark_dataframe()
    fake_snowpark_dataframe.write.save_as_table.return_value = None
    ordered_dataframe = OrderedDataFrame(
        DataFrameReference(fake_snowpark_dataframe)
    ).sort(OrderingColumn('"INDEX"'))

    internal_frame = InternalFrame.create(
        ordered_dataframe=ordered_dataframe,
        data_column_pandas_labels=["C"],
        data_column_pandas_index_names=[None],
        data_column_snowflake_quoted_identifiers=['"C"'],
        index_column_pandas_labels=["INDEX"],
        index_column_snowflake_quoted_identifiers=['"INDEX"'],
    )

    query_compiler = SnowflakeQueryCompiler(internal_frame)

    if obj_type == SnowparkPandasObjectType.DATAFRAME:
        return DataFrame(query_compiler=query_compiler)
    if obj_type == SnowparkPandasObjectType.SERIES:
        return Series(query_compiler=query_compiler)

    return query_compiler


def validate_obj_type_recursively(obj: Any, expected_type: Any) -> None:
    """
    Recursively validate the type of the obj and all nested object matches the expected type.

    The data structure of obj must be the same as expected_type, for example, if obj is a list, the
    expected_type must in a list.

    Args:
        obj: the object to check the type for
        expected_type: the expected type for the object

    Raises:
        AssertionException if the type doesn't match
    """
    if isinstance(obj, (list, tuple)) and isinstance(expected_type, (list, tuple)):
        for o, t in zip(obj, expected_type):
            validate_obj_type_recursively(o, t)
    elif isinstance(obj, dict) and isinstance(expected_type, dict):
        for k, v in obj.items():
            validate_obj_type_recursively(v, expected_type[k])
    else:
        assert isinstance(obj, expected_type)


@pytest.mark.parametrize(
    "obj, pickle_data_dict, expected_obj_type",
    [
        (True, {}, bool),
        (
            "temp_table",
            {
                "temp_table": create_df_test_pickle_data(
                    "temp_table", SnowparkPandasObjectType.QUERY_COMPILER
                )
            },
            SnowflakeQueryCompiler,
        ),
        ("temp_table", {}, str),
        (
            ["temp_table", 5, "temp_table1"],
            {
                "temp_table": create_df_test_pickle_data(
                    "temp_table", SnowparkPandasObjectType.DATAFRAME
                ),
                "temp_table2": create_series_test_pickle_data("temp_table2"),
            },
            [DataFrame, int, str],
        ),
        (
            ("temp_table", 5, ["temp_table2", "test"]),
            {
                "temp_table": create_df_test_pickle_data(
                    "temp_table", SnowparkPandasObjectType.DATAFRAME
                ),
                "temp_table2": create_series_test_pickle_data("temp_table2"),
            },
            [DataFrame, int, [Series, str]],
        ),
        (
            {"arg1": True, "arg2": "temp_table", "arg3": "temp_table1"},
            {
                "temp_table": create_df_test_pickle_data(
                    "temp_table", SnowparkPandasObjectType.QUERY_COMPILER
                ),
                "temp_table2": create_series_test_pickle_data("temp_table2"),
                "temp_table3": create_df_test_pickle_data(
                    "temp_table3", SnowparkPandasObjectType.DATAFRAME
                ),
            },
            {"arg1": bool, "arg2": SnowflakeQueryCompiler, "arg3": str},
        ),
        (
            {
                "arg1": True,
                "arg2": "temp_table",
                "arg3": ["temp_table1", "temp_table3"],
            },
            {
                "temp_table": create_df_test_pickle_data(
                    "temp_table", SnowparkPandasObjectType.QUERY_COMPILER
                ),
                "temp_table2": create_series_test_pickle_data("temp_table2"),
                "temp_table3": create_df_test_pickle_data(
                    "temp_table3", SnowparkPandasObjectType.DATAFRAME
                ),
            },
            {
                "arg1": bool,
                "arg2": SnowflakeQueryCompiler,
                "arg3": [str, DataFrame],
            },
        ),
    ],
)
def test_try_recover_snowpark_pandas_objects(
    mock_session, obj, pickle_data_dict, expected_obj_type
) -> None:
    obj_result = StoredProcedureDefault._try_recover_snowpark_pandas_objects(
        mock_session, obj, pickle_data_dict
    )
    validate_obj_type_recursively(obj_result, expected_obj_type)


@pytest.mark.parametrize(
    "obj, expected_obj_type, pickle_data_size",
    [
        (mock_snowpark_pandas_object(SnowparkPandasObjectType.QUERY_COMPILER), str, 1),
        ("test", str, 0),
        (
            [
                5,
                mock_snowpark_pandas_object(SnowparkPandasObjectType.QUERY_COMPILER),
                "test",
            ],
            [int, str, str],
            1,
        ),
        (
            [
                5,
                mock_snowpark_pandas_object(SnowparkPandasObjectType.QUERY_COMPILER),
                (
                    "test",
                    mock_snowpark_pandas_object(SnowparkPandasObjectType.DATAFRAME),
                    [
                        mock_snowpark_pandas_object(SnowparkPandasObjectType.SERIES),
                        False,
                    ],
                ),
            ],
            [int, str, (str, str, [str, bool])],
            3,
        ),
        (
            {
                "arg1": True,
                "arg2": mock_snowpark_pandas_object(
                    SnowparkPandasObjectType.QUERY_COMPILER
                ),
                "arg3": mock_snowpark_pandas_object(SnowparkPandasObjectType.SERIES),
            },
            {"arg1": bool, "arg2": str, "arg3": str},
            2,
        ),
        (
            {
                "arg1": True,
                "arg2": [
                    mock_snowpark_pandas_object(
                        SnowparkPandasObjectType.QUERY_COMPILER
                    ),
                    10,
                ],
                "arg3": mock_snowpark_pandas_object(SnowparkPandasObjectType.SERIES),
            },
            {"arg1": bool, "arg2": [str, int], "arg3": str},
            2,
        ),
    ],
)
def test_try_pickle_snowpark_pandas_objects(
    obj, expected_obj_type, pickle_data_size
) -> None:
    (
        obj_result,
        pickle_data,
    ) = StoredProcedureDefault._try_pickle_snowpark_pandas_objects(obj)
    validate_obj_type_recursively(obj_result, expected_obj_type)
    assert len(pickle_data) == pickle_data_size
