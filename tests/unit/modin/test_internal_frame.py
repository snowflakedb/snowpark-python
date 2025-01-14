#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re
from unittest.mock import patch

import pandas as pd
import pytest

from snowflake.snowpark._internal.analyzer.sort_expression import (
    Ascending,
    Descending,
    NullsFirst,
    NullsLast,
    SortOrder,
)
from snowflake.snowpark.functions import col
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
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
from tests.integ.modin.utils import VALID_PANDAS_LABELS, assert_index_equal


class TestDataFrames:
    def __init__(self, sp_df: OrderedDataFrame, internal: InternalFrame) -> None:
        self.ordered_dataframe: OrderedDataFrame = sp_df
        self.internal_frame: InternalFrame = internal


@pytest.mark.modin_sp_precommit
@pytest.fixture(scope="function")
@patch("snowflake.snowpark.dataframe.DataFrame")
def test_dataframes(mock_dataframe) -> TestDataFrames:
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
    mock_dataframe.schema = snowpark_df_schema
    mock_dataframe.select.return_value = mock_dataframe
    ordered_dataframe = OrderedDataFrame(
        DataFrameReference(mock_dataframe), ordering_columns=[OrderingColumn('"INDEX"')]
    )

    internal_frame = InternalFrame.create(
        ordered_dataframe=ordered_dataframe,
        data_column_pandas_labels=["a", "b", "C", "a"],
        data_column_pandas_index_names=[None],
        data_column_snowflake_quoted_identifiers=['"a"', '"b"', '"C"', '"d"'],
        index_column_pandas_labels=[None],
        index_column_snowflake_quoted_identifiers=['"INDEX"'],
        data_column_types=None,
        index_column_types=None,
    )

    return TestDataFrames(ordered_dataframe, internal_frame)


@pytest.mark.modin_sp_precommit
@pytest.fixture(scope="function")
@patch("snowflake.snowpark.dataframe.DataFrame")
def test_dataframes_with_multiindex_on_column(mock_dataframe) -> TestDataFrames:
    snowpark_df_schema = StructType(
        [
            StructField(
                column_identifier=ColumnIdentifier("\"('a', 'C')\""),
                datatype=IntegerType,
            ),
            StructField(
                column_identifier=ColumnIdentifier("\"('b', 'D')\""),
                datatype=IntegerType,
            ),
            StructField(
                column_identifier=ColumnIdentifier('"INDEX"'), datatype=IntegerType
            ),
        ]
    )
    mock_dataframe.schema = snowpark_df_schema
    mock_dataframe.select.return_value = mock_dataframe
    ordered_dataframe = OrderedDataFrame(
        DataFrameReference(mock_dataframe), ordering_columns=[OrderingColumn('"INDEX"')]
    )

    internal_frame = InternalFrame.create(
        ordered_dataframe=ordered_dataframe,
        data_column_pandas_labels=[("a", "C"), ("b", "D")],
        data_column_pandas_index_names=["x", "y"],
        data_column_snowflake_quoted_identifiers=["\"('a', 'C')\"", "\"('b', 'D')\""],
        index_column_pandas_labels=[None],
        index_column_snowflake_quoted_identifiers=['"INDEX"'],
        data_column_types=None,
        index_column_types=None,
    )

    return TestDataFrames(ordered_dataframe, internal_frame)


@pytest.mark.modin_sp_precommit
def test_snowflake_quoted_identifier_without_quote_negative(test_dataframes) -> None:
    with pytest.raises(AssertionError) as exc:
        InternalFrame.create(
            ordered_dataframe=test_dataframes.ordered_dataframe,
            data_column_pandas_labels=["a", "b", "C"],
            data_column_pandas_index_names=[None],
            data_column_snowflake_quoted_identifiers=["a", "b", "c"],
            index_column_pandas_labels=[None],
            index_column_snowflake_quoted_identifiers=['"INDEX"'],
            data_column_types=None,
            index_column_types=None,
        )

    assert "Found not-quoted identifier for 'dataframe column':'a'" in str(exc.value)


@pytest.mark.modin_sp_precommit
def test_column_labels_and_quoted_identifiers_have_same_length_negative(
    test_dataframes,
) -> None:
    # check data columns
    with pytest.raises(AssertionError):
        InternalFrame.create(
            ordered_dataframe=test_dataframes.ordered_dataframe,
            data_column_pandas_labels=["a", "b"],
            data_column_pandas_index_names=[None],
            data_column_snowflake_quoted_identifiers=['"a"'],
            index_column_pandas_labels=[None],
            index_column_snowflake_quoted_identifiers=['"INDEX"'],
            data_column_types=None,
            index_column_types=None,
        )

    # check index columns
    with pytest.raises(AssertionError):
        InternalFrame.create(
            ordered_dataframe=test_dataframes.ordered_dataframe,
            data_column_pandas_labels=["a", "b", "C"],
            data_column_pandas_index_names=[None],
            data_column_snowflake_quoted_identifiers=['"a"', '"b"', '"C"'],
            index_column_pandas_labels=[],
            index_column_snowflake_quoted_identifiers=['"INDEX"'],
            data_column_types=None,
            index_column_types=None,
        )


@pytest.mark.modin_sp_precommit
def test_internal_frame_missing_data_column_negative(test_dataframes):
    with pytest.raises(AssertionError) as exc:
        InternalFrame.create(
            ordered_dataframe=test_dataframes.ordered_dataframe,
            data_column_pandas_labels=["a", "b", "C"],
            data_column_pandas_index_names=[None],
            data_column_snowflake_quoted_identifiers=['"a"', '"b"', '"D"'],
            index_column_pandas_labels=[None],
            index_column_snowflake_quoted_identifiers=['"INDEX"'],
            data_column_types=None,
            index_column_types=None,
        )

    assert 'dataframe column="D" not found in snowpark dataframe schema' in str(
        exc.value
    )


@pytest.mark.modin_sp_precommit
def test_internal_frame_missing_index_column_negative(test_dataframes):
    with pytest.raises(AssertionError) as exc:
        InternalFrame.create(
            ordered_dataframe=test_dataframes.ordered_dataframe,
            data_column_pandas_labels=["a", "b", "C"],
            data_column_pandas_index_names=[None],
            data_column_snowflake_quoted_identifiers=['"a"', '"b"', '"C"'],
            index_column_pandas_labels=[None],
            index_column_snowflake_quoted_identifiers=['"E"'],
            data_column_types=None,
            index_column_types=None,
        )

    assert 'dataframe column="E" not found in snowpark dataframe schema' in str(
        exc.value
    )


@pytest.mark.modin_sp_precommit
def test_internal_frame_properties(test_dataframes) -> None:
    internal_frame = test_dataframes.internal_frame
    # check index_column_snowflake_quoted_identifiers
    index_column_snowflake_quoted_identifiers = (
        internal_frame.index_column_snowflake_quoted_identifiers
    )
    assert index_column_snowflake_quoted_identifiers == ['"INDEX"']

    # check data_column_snowflake_quoted_identifiers
    data_column_snowflake_quoted_identifiers = (
        internal_frame.data_column_snowflake_quoted_identifiers
    )
    assert data_column_snowflake_quoted_identifiers == ['"a"', '"b"', '"C"', '"d"']

    # check index_column_pandas_labels
    index_column_pandas_labels = internal_frame.index_column_pandas_labels
    assert index_column_pandas_labels == [None]

    # check data_column_pandas_labels
    data_column_pandas_labels = internal_frame.data_column_pandas_labels
    assert data_column_pandas_labels == ["a", "b", "C", "a"]

    # check ordering_column_snowflake_quoted_identifiers
    ordering_column_snowflake_quoted_identifiers = (
        internal_frame.ordering_column_snowflake_quoted_identifiers
    )
    assert ordering_column_snowflake_quoted_identifiers == ['"INDEX"']


@pytest.mark.modin_sp_precommit
def test_pandas_label_as_empty_and_none(test_dataframes) -> None:
    internal_frame = InternalFrame.create(
        ordered_dataframe=test_dataframes.ordered_dataframe,
        data_column_pandas_labels=["", "b", None],
        data_column_pandas_index_names=[None],
        data_column_snowflake_quoted_identifiers=['"a"', '"b"', '"C"'],
        index_column_pandas_labels=[None],
        index_column_snowflake_quoted_identifiers=['"INDEX"'],
        data_column_types=None,
        index_column_types=None,
    )

    assert internal_frame.data_column_pandas_labels == ["", "b", None]
    assert internal_frame.data_column_snowflake_quoted_identifiers == [
        '"a"',
        '"b"',
        '"C"',
    ]


@pytest.mark.modin_sp_precommit
def test_ordering_column_snowpark_column() -> None:
    ordering_column = OrderingColumn('"A"')
    snowpark_column = ordering_column.snowpark_column
    # default rule should be ascending and nulls last
    assert isinstance(snowpark_column._expression, SortOrder)
    assert isinstance(snowpark_column._expression.direction, Ascending)
    assert isinstance(snowpark_column._expression.null_ordering, NullsLast)

    # verify ascending false and nulls last
    ordering_column = OrderingColumn('"A"', ascending=False)
    snowpark_column = ordering_column.snowpark_column
    assert isinstance(snowpark_column._expression, SortOrder)
    assert isinstance(snowpark_column._expression.direction, Descending)
    assert isinstance(snowpark_column._expression.null_ordering, NullsLast)

    # verify ascending true, nulls first
    ordering_column = OrderingColumn('"A"', na_last=False)
    snowpark_column = ordering_column.snowpark_column
    assert isinstance(snowpark_column._expression, SortOrder)
    assert isinstance(snowpark_column._expression.direction, Ascending)
    assert isinstance(snowpark_column._expression.null_ordering, NullsFirst)

    # verify ascending false, nulls first
    ordering_column = OrderingColumn('"A"', ascending=False, na_last=False)
    snowpark_column = ordering_column.snowpark_column
    assert isinstance(snowpark_column._expression, SortOrder)
    assert isinstance(snowpark_column._expression.direction, Descending)
    assert isinstance(snowpark_column._expression.null_ordering, NullsFirst)


@pytest.mark.modin_sp_precommit
def test_internal_frame_ordering_columns(test_dataframes) -> None:
    # ordering column is part of the index + data column
    test_dataframes.ordered_dataframe._ordering_columns_tuple = (
        OrderingColumn('"INDEX"'),
        OrderingColumn('"a"'),
        OrderingColumn('"C"'),
    )
    internal_frame = InternalFrame.create(
        ordered_dataframe=test_dataframes.ordered_dataframe,
        data_column_pandas_labels=["a", "b", "C"],
        data_column_pandas_index_names=[None],
        data_column_snowflake_quoted_identifiers=['"a"', '"b"', '"C"'],
        index_column_pandas_labels=[None],
        index_column_snowflake_quoted_identifiers=['"INDEX"'],
        data_column_types=None,
        index_column_types=None,
    )

    assert internal_frame.ordering_column_snowflake_quoted_identifiers == [
        '"INDEX"',
        '"a"',
        '"C"',
    ]

    # ordering column is neither part of index nor data column
    test_dataframes.ordered_dataframe._ordering_columns_tuple = (
        OrderingColumn('"b"'),
        OrderingColumn('"C"'),
    )
    internal_frame = InternalFrame.create(
        ordered_dataframe=test_dataframes.ordered_dataframe,
        data_column_pandas_labels=["a"],
        data_column_pandas_index_names=[None],
        data_column_snowflake_quoted_identifiers=['"a"'],
        index_column_pandas_labels=[None],
        index_column_snowflake_quoted_identifiers=['"INDEX"'],
        data_column_types=None,
        index_column_types=None,
    )

    assert internal_frame.ordering_column_snowflake_quoted_identifiers == [
        '"b"',
        '"C"',
    ]

    # ordering columns has column from data column, and column from none index
    # or data column
    internal_frame = InternalFrame.create(
        ordered_dataframe=test_dataframes.ordered_dataframe,
        data_column_pandas_labels=["a", "C"],
        data_column_pandas_index_names=[None],
        data_column_snowflake_quoted_identifiers=['"a"', '"C"'],
        index_column_pandas_labels=[None],
        index_column_snowflake_quoted_identifiers=['"INDEX"'],
        data_column_types=None,
        index_column_types=None,
    )

    assert internal_frame.ordering_column_snowflake_quoted_identifiers == [
        '"b"',
        '"C"',
    ]


@pytest.mark.modin_sp_precommit
@pytest.mark.parametrize("pandas_label", VALID_PANDAS_LABELS)
def test_data_column_pandas_index_names(pandas_label, test_dataframes) -> None:
    test_dataframes.ordered_dataframe._ordering_columns_tuple_tuple = (
        OrderingColumn('"b"'),
        OrderingColumn('"C"'),
    )
    internal_frame = InternalFrame.create(
        ordered_dataframe=test_dataframes.ordered_dataframe,
        data_column_pandas_labels=["a", "C"],
        data_column_pandas_index_names=[pandas_label],
        data_column_snowflake_quoted_identifiers=['"a"', '"C"'],
        index_column_pandas_labels=[None],
        index_column_snowflake_quoted_identifiers=['"INDEX"'],
        data_column_types=None,
        index_column_types=None,
    )

    assert internal_frame.data_column_pandas_index_names == [pandas_label]


@pytest.mark.modin_sp_precommit
def test_data_column_pandas_multiindex(
    test_dataframes_with_multiindex_on_column,
) -> None:
    assert (
        test_dataframes_with_multiindex_on_column.internal_frame.data_column_pandas_index_names
        == ["x", "y"]
    )


@pytest.mark.modin_sp_precommit
@pytest.mark.parametrize(
    "data_column_pandas_labels, data_column_pandas_index_names, expected_error_message",
    [
        (
            [("a", "C"), ("b", "D")],
            [],
            "data_column_pandas_index_names cannot be empty",
        ),
        (
            [("a", "C"), "b"],
            ["x", "y"],
            "pandas label on MultiIndex column must be a tuple with length larger than 1, but got",
        ),
        (
            [("a", "C"), ("b",)],
            ["x", "y"],
            "pandas label on MultiIndex column must be a tuple with length larger than 1, but got",
        ),
        (
            [
                ("a", "C"),
                (
                    "b",
                    "b",
                    "b",
                ),
            ],
            ["x", "y"],
            "All tuples in data_column_pandas_labels must have the same length 2",
        ),
        (
            [("a", "C"), ("b", "D")],
            ["x", "y", "z"],
            "All tuples in data_column_pandas_labels must have the same length 3",
        ),
    ],
)
def test_data_column_pandas_multiindex_negative(
    test_dataframes_with_multiindex_on_column,
    data_column_pandas_labels,
    data_column_pandas_index_names,
    expected_error_message,
) -> None:
    with pytest.raises(
        AssertionError,
        match=expected_error_message,
    ):
        InternalFrame.create(
            ordered_dataframe=test_dataframes_with_multiindex_on_column.ordered_dataframe,
            data_column_pandas_labels=data_column_pandas_labels,
            data_column_pandas_index_names=data_column_pandas_index_names,
            data_column_snowflake_quoted_identifiers=[
                "\"('a', 'C')\"",
                "\"('b', 'D')\"",
            ],
            index_column_pandas_labels=[None],
            index_column_snowflake_quoted_identifiers=['"INDEX"'],
            data_column_types=None,
            index_column_types=None,
        )


@pytest.mark.modin_sp_precommit
def test_get_snowflake_quoted_identifiers_by_pandas_labels_empty(
    test_dataframes,
) -> None:
    frame = test_dataframes.internal_frame
    assert (
        frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(pandas_labels=[])
        == []
    )


@pytest.mark.modin_sp_precommit
def test_get_snowflake_quoted_identifiers_by_pandas_labels_empty_not_include_index(
    test_dataframes,
) -> None:
    test_dataframes.ordered_dataframe._ordering_columns_tuple = (OrderingColumn('"a"'),)
    internal_frame = InternalFrame.create(
        ordered_dataframe=test_dataframes.ordered_dataframe,
        data_column_pandas_labels=["a", "b", "C"],
        data_column_pandas_index_names=["index"],
        data_column_snowflake_quoted_identifiers=['"a"', '"b"', '"C"'],
        index_column_pandas_labels=["index"],
        index_column_snowflake_quoted_identifiers=['"INDEX"'],
        data_column_types=None,
        index_column_types=None,
    )

    assert internal_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
        pandas_labels=["index", "a", "b", "C"], include_index=True
    ) == [('"INDEX"',), ('"a"',), ('"b"',), ('"C"',)]

    assert internal_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
        pandas_labels=["index", "a", "b", "C"], include_index=False
    ) == [(), ('"a"',), ('"b"',), ('"C"',)]


@pytest.mark.modin_sp_precommit
@pytest.mark.parametrize(
    "labels, expected_identifiers",
    [
        (["a"], [('"a"', '"d"')]),
        (["b"], [('"b"',)]),
        (["C"], [('"C"',)]),
        ([None], [('"INDEX"',)]),
    ],
)
def test_get_snowflake_quoted_identifiers_by_pandas_labels(
    test_dataframes, labels, expected_identifiers
) -> None:
    frame = test_dataframes.internal_frame
    assert (
        frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
            pandas_labels=labels
        )
        == expected_identifiers
    )


@pytest.mark.modin_sp_precommit
@pytest.mark.parametrize(
    "labels, expected_identifiers",
    [(["a", "b"], [('"a"', '"d"'), ('"b"',)]), (["C", None], [('"C"',), ('"INDEX"',)])],
)
def test_get_snowflake_quoted_identifiers_by_pandas_labels_multiple(
    test_dataframes, labels, expected_identifiers
) -> None:
    frame = test_dataframes.internal_frame
    assert (
        frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
            pandas_labels=labels
        )
        == expected_identifiers
    )


@pytest.mark.modin_sp_precommit
@pytest.mark.parametrize(
    "labels, expected_identifiers",
    [(["A"], [()]), (["B"], [()]), (["c"], [()])],
)
def test_get_snowflake_quoted_identifiers_by_pandas_labels_case_sensitive(
    test_dataframes, labels, expected_identifiers
) -> None:
    frame = test_dataframes.internal_frame
    assert (
        frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
            pandas_labels=labels
        )
        == expected_identifiers
    )


@pytest.mark.modin_sp_precommit
@pytest.mark.parametrize(
    "labels, expected_identifiers", [(["ABC"], [()]), (["abc"], [()])]
)
def test_get_snowflake_quoted_identifiers_by_pandas_labels_missing(
    test_dataframes, labels, expected_identifiers
) -> None:
    frame = test_dataframes.internal_frame
    assert (
        frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
            pandas_labels=labels
        )
        == expected_identifiers
    )


@pytest.mark.modin_sp_precommit
def test_data_columns_index(test_dataframes, test_dataframes_with_multiindex_on_column):
    assert_index_equal(
        test_dataframes.internal_frame.data_columns_index,
        pd.Index(["a", "b", "C", "a"]),
    )
    assert_index_equal(
        test_dataframes_with_multiindex_on_column.internal_frame.data_columns_index,
        pd.MultiIndex.from_tuples([("a", "C"), ("b", "D")], names=["x", "y"]),
    )


@pytest.mark.modin_sp_precommit
def test_is_multiindex(test_dataframes, test_dataframes_with_multiindex_on_column):
    assert not test_dataframes.internal_frame.is_multiindex(axis=0)
    assert not test_dataframes.internal_frame.is_multiindex(axis=1)
    assert not test_dataframes_with_multiindex_on_column.internal_frame.is_multiindex(
        axis=0
    )
    assert test_dataframes_with_multiindex_on_column.internal_frame.is_multiindex(
        axis=1
    )

    with pytest.raises(ValueError, match="'axis' can only be 0 or 1"):
        test_dataframes.internal_frame.is_multiindex(axis=-1)


@pytest.mark.modin_sp_precommit
def test_immutability(test_dataframes):
    frame = test_dataframes.internal_frame

    def verify_immutability(attr_name: str) -> None:
        """
        Verify immutability of list attributes.
        """
        original_value = getattr(frame, attr_name).copy()
        # apply mutation
        getattr(frame, attr_name).append("abc")
        assert getattr(frame, attr_name) == original_value

    verify_immutability("data_column_pandas_labels")
    verify_immutability("data_column_snowflake_quoted_identifiers")
    verify_immutability("index_column_pandas_labels")
    verify_immutability("index_column_snowflake_quoted_identifiers")
    verify_immutability("data_column_pandas_index_names")
    verify_immutability("ordering_column_snowflake_quoted_identifiers")

    frame.data_columns_index.set_names(["abc"], inplace=True)
    assert frame.data_columns_index.names == [None]

    frame.index_columns_pandas_index().set_names(["abc"], inplace=True)
    assert frame.index_columns_pandas_index().names == [None]

    assert len(frame.ordering_columns) == 1
    frame.ordering_columns.append(OrderingColumn("abc"))
    assert len(frame.ordering_columns) == 1

    assert type(frame.label_to_snowflake_quoted_identifier) == tuple
    assert type(frame.data_column_index_names) == tuple


@pytest.mark.modin_sp_precommit
@pytest.mark.parametrize("level0, level1", [(1, 1), (2, 1), (1, 2), (2, 3)])
@patch("snowflake.snowpark.dataframe.DataFrame")
def test_num_levels(mock_dataframe, level0, level1):
    snowpark_df_schema = StructType(
        [
            StructField(
                column_identifier=ColumnIdentifier('"a"'), datatype=IntegerType
            ),
            StructField(
                column_identifier=ColumnIdentifier('"b"'), datatype=IntegerType
            ),
            StructField(
                column_identifier=ColumnIdentifier('"x"'), datatype=IntegerType
            ),
            StructField(
                column_identifier=ColumnIdentifier('"y"'), datatype=IntegerType
            ),
        ]
    )
    mock_dataframe.schema = snowpark_df_schema
    mock_dataframe.select.return_value = mock_dataframe

    if level1 == 1:
        data_column_pandas_levels = ["a", "b"]
    else:
        data_column_pandas_levels = [tuple(["a"] * level1), tuple(["a"] * level1)]

    ordered_dataframe = OrderedDataFrame(
        DataFrameReference(mock_dataframe), ordering_columns=[OrderingColumn('"a"')]
    )
    frame = InternalFrame.create(
        ordered_dataframe=ordered_dataframe,
        data_column_pandas_labels=data_column_pandas_levels,
        data_column_pandas_index_names=[None] * level1,
        data_column_snowflake_quoted_identifiers=['"x"', '"y"'],
        index_column_pandas_labels=[None] * level0,
        index_column_snowflake_quoted_identifiers=['"a"', '"b"'][:level0],
        data_column_types=None,
        index_column_types=None,
    )
    assert frame.num_index_levels(axis=0) == level0
    assert frame.num_index_levels(axis=1) == level1


@pytest.mark.modin_sp_precommit
@pytest.mark.parametrize(
    "pandas_labels, frame_identifier, expected_message",
    [
        (
            ["A", "B"],
            "condition",
            "Multiple columns are mapped to each label in ['B'] in DataFrame condition",
        ),
        (
            ["A", "B"],
            None,
            "Multiple columns are mapped to each label in ['B'] in DataFrame",
        ),
        (
            ["A", "B", "C"],
            "other",
            "Multiple columns are mapped to each label in ['B', 'C'] in DataFrame other",
        ),
        (["A", "F"], None, None),
        (["A"], None, None),
    ],
)
@patch("snowflake.snowpark.dataframe.DataFrame")
def test_validation_duplicated_data_columns_for_labels(
    mock_dataframe, pandas_labels, frame_identifier, expected_message
):
    snowpark_df_schema = StructType(
        [
            StructField(
                column_identifier=ColumnIdentifier('"A"'), datatype=IntegerType
            ),
            StructField(
                column_identifier=ColumnIdentifier('"B_1"'), datatype=IntegerType
            ),
            StructField(
                column_identifier=ColumnIdentifier('"C_1"'), datatype=IntegerType
            ),
            StructField(
                column_identifier=ColumnIdentifier('"B_2"'), datatype=IntegerType
            ),
            StructField(
                column_identifier=ColumnIdentifier('"C_2"'), datatype=IntegerType
            ),
            StructField(
                column_identifier=ColumnIdentifier('"F"'), datatype=IntegerType
            ),
            StructField(
                column_identifier=ColumnIdentifier('"F_INDEX"'), datatype=IntegerType
            ),
        ]
    )
    mock_dataframe.schema = snowpark_df_schema
    mock_dataframe.select.return_value = mock_dataframe
    mock_dataframe._ordering_columns = [OrderingColumn('"F_INDEX"')]

    ordered_dataframe = OrderedDataFrame(
        DataFrameReference(mock_dataframe),
        ordering_columns=[OrderingColumn('"F_INDEX"')],
    )
    internal_frame = InternalFrame.create(
        ordered_dataframe=ordered_dataframe,
        data_column_pandas_labels=["A", "B", "C", "B", "C", "F"],
        data_column_pandas_index_names=[None],
        data_column_snowflake_quoted_identifiers=[
            '"A"',
            '"B_1"',
            '"C_1"',
            '"B_2"',
            '"C_2"',
            '"F"',
        ],
        index_column_pandas_labels=["F"],
        index_column_snowflake_quoted_identifiers=['"F_INDEX"'],
        data_column_types=None,
        index_column_types=None,
    )

    if expected_message is not None:
        with pytest.raises(ValueError, match=re.escape(expected_message)):
            internal_frame.validate_no_duplicated_data_columns_mapped_for_labels(
                pandas_labels, frame_identifier
            )
    else:
        internal_frame.validate_no_duplicated_data_columns_mapped_for_labels(
            pandas_labels, frame_identifier
        )


@pytest.mark.modin_sp_precommit
def test_update_columns_quoted_identifier_with_expressions_negative(test_dataframes):
    with pytest.raises(ValueError, match="is not in"):
        test_dataframes.internal_frame.update_snowflake_quoted_identifiers_with_expressions(
            {'"x"': col('"a"') + 1}
        ).frame
