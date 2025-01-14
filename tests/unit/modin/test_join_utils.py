#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from collections.abc import Hashable
from unittest import mock

import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin._internal.frame import (
    OrderedDataFrame,
    OrderingColumn,
)
from snowflake.snowpark.modin.plugin._internal.join_utils import (
    InheritJoinIndex,
    JoinKeyCoalesceConfig,
    _create_internal_frame_with_join_or_align_result,
)
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    InternalFrame,
)


def mock_internal_frame(
    data_column_pandas_labels: list[Hashable],
    data_column_pandas_index_names: list[Hashable],
    data_column_snowflake_quoted_identifiers: list[str],
    index_column_pandas_labels: list[Hashable],
    index_column_snowflake_quoted_identifiers: list[str],
) -> InternalFrame:
    ordered_dataframe = mock.create_autospec(OrderedDataFrame)
    ordered_dataframe.projected_column_snowflake_quoted_identifiers = (
        data_column_snowflake_quoted_identifiers
        + index_column_snowflake_quoted_identifiers
    )
    ordered_dataframe.ordering_columns = [
        OrderingColumn(col)
        for col in ordered_dataframe.projected_column_snowflake_quoted_identifiers
    ]
    internal_frame = InternalFrame.create(
        ordered_dataframe=ordered_dataframe,
        data_column_pandas_labels=data_column_pandas_labels,
        data_column_pandas_index_names=data_column_pandas_index_names,
        data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
        index_column_pandas_labels=index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=index_column_snowflake_quoted_identifiers,
        data_column_types=[None] * len(data_column_pandas_labels),
        index_column_types=[None] * len(index_column_pandas_labels),
    )

    return internal_frame


def test_create_internal_frame_with_result_using_invalid_methods():
    left_frame = mock_internal_frame(
        data_column_pandas_labels=["a1", "b1"],
        data_column_pandas_index_names=[None],
        data_column_snowflake_quoted_identifiers=['"A1"', '"B1"'],
        index_column_pandas_labels=["i1"],
        index_column_snowflake_quoted_identifiers=['"I1"'],
    )

    right_frame = mock_internal_frame(
        data_column_pandas_labels=["a2", "b2"],
        data_column_pandas_index_names=[None],
        data_column_snowflake_quoted_identifiers=['"A2"', '"B2"'],
        index_column_pandas_labels=["i2"],
        index_column_snowflake_quoted_identifiers=['"I2"'],
    )

    result_ordered_frame = mock.create_autospec(OrderedDataFrame)
    result_ordered_frame.projected_column_snowflake_quoted_identifiers = [
        '"I1"',
        '"A1"',
        '"B1"',
        '"I2"',
        '"A2"',
        '"B2"',
    ]
    result_ordered_frame._ordering_columns_tuple = [
        OrderingColumn('"I1"'),
        OrderingColumn('"I2"'),
    ]

    with pytest.raises(AssertionError, match="Unsupported join/align type invalid"):
        _create_internal_frame_with_join_or_align_result(
            result_ordered_frame=result_ordered_frame,
            left=left_frame,
            right=right_frame,
            how="invalid",
            left_on=['"I1"'],
            right_on=['"I2"'],
            sort=False,
            key_coalesce_config=[JoinKeyCoalesceConfig.LEFT],
            inherit_index=InheritJoinIndex.FROM_LEFT,
        )
