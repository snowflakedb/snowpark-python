#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.snowpark.dataframe import DataFrame as SnowparkDataFrame
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import (
    DataFrameReference,
    OrderedDataFrame,
    OrderingColumn,
)
from snowflake.snowpark.types import (
    ColumnIdentifier,
    IntegerType,
    StructField,
    StructType,
)


@pytest.fixture(scope="function")
def mock_snowpark_dataframe() -> SnowparkDataFrame:
    fake_snowpark_dataframe = mock.create_autospec(SnowparkDataFrame)
    snowpark_df_schema = StructType(
        [
            StructField(
                column_identifier=ColumnIdentifier('"a"'), datatype=IntegerType
            ),
            StructField(
                column_identifier=ColumnIdentifier('"b"'), datatype=IntegerType
            ),
            StructField(
                column_identifier=ColumnIdentifier('"C"'), datatype=IntegerType
            ),
            StructField(
                column_identifier=ColumnIdentifier('"d"'), datatype=IntegerType
            ),
            StructField(
                column_identifier=ColumnIdentifier('"INDEX"'), datatype=IntegerType
            ),
        ]
    )
    fake_snowpark_dataframe.schema = snowpark_df_schema
    return fake_snowpark_dataframe


@pytest.mark.parametrize(
    "row_pos_quoted_identifier, ordering_columns",
    [
        ('"INDEX"', [OrderingColumn('"a"')]),
        (None, [OrderingColumn('"a"', '"C"')]),
        (None, [OrderingColumn('"a"')]),
    ],
)
def test_row_position_column(
    mock_snowpark_dataframe, row_pos_quoted_identifier, ordering_columns
) -> None:
    ordered_dataframe = OrderedDataFrame(
        DataFrameReference(mock_snowpark_dataframe),
        ordering_columns=ordering_columns,
        row_position_snowflake_quoted_identifier=row_pos_quoted_identifier,
    )

    row_position_column = ordered_dataframe._row_position_snowpark_column()
    if row_pos_quoted_identifier is not None:
        assert row_position_column.get_name() == row_pos_quoted_identifier
    else:
        assert row_position_column.get_name() is None
        assert str(row_position_column) == "Column[WINDOWEXPRESSION - LITERAL]"


def test_property_immutability(mock_snowpark_dataframe):
    ordering_columns = [OrderingColumn('"d"'), OrderingColumn('"INDEX"')]
    ordered_dataframe = OrderedDataFrame(
        DataFrameReference(mock_snowpark_dataframe),
        ordering_columns=ordering_columns,
        row_position_snowflake_quoted_identifier=None,
    )

    # verify immutability of projected_column_snowflake_quoted_identifiers
    assert ordered_dataframe.projected_column_snowflake_quoted_identifiers == [
        '"a"',
        '"b"',
        '"C"',
        '"d"',
        '"INDEX"',
    ]
    ordered_dataframe.projected_column_snowflake_quoted_identifiers.append('"abc"')
    assert ordered_dataframe.projected_column_snowflake_quoted_identifiers == [
        '"a"',
        '"b"',
        '"C"',
        '"d"',
        '"INDEX"',
    ]

    # verify immutability of ordering columns
    assert ordered_dataframe.ordering_columns == ordering_columns
    ordered_dataframe.ordering_columns.append(OrderingColumn('"abc"'))
    assert ordered_dataframe.ordering_columns == ordering_columns

    assert (
        type(ordered_dataframe._projected_column_snowflake_quoted_identifiers_tuple)
        == tuple
    )
    assert type(ordered_dataframe._ordering_columns_tuple) == tuple


def test_ordered_dataframe_no_ordering_columns_negative(
    mock_snowpark_dataframe,
) -> None:
    with pytest.raises(AssertionError, match="ordering_columns cannot be empty"):
        OrderedDataFrame(
            DataFrameReference(mock_snowpark_dataframe),
            ordering_columns=[],
            row_position_snowflake_quoted_identifier=None,
        )


def test_ordered_dataframe_missing_ordering_column_negative(mock_snowpark_dataframe):
    with pytest.raises(AssertionError, match='ordering column "E" not found'):
        OrderedDataFrame(
            DataFrameReference(mock_snowpark_dataframe),
            ordering_columns=[OrderingColumn('"E"')],
            row_position_snowflake_quoted_identifier='"a"',
        )


def test_ordered_dataframe_missing_row_position_column_negative(
    mock_snowpark_dataframe,
):
    with pytest.raises(AssertionError, match='row position column "E" not found'):
        OrderedDataFrame(
            DataFrameReference(mock_snowpark_dataframe),
            ordering_columns=[OrderingColumn('"INDEX"')],
            row_position_snowflake_quoted_identifier='"E"',
        )
