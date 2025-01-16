#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# This test file contains tests that execute a single underlying snowpark/snowflake pivot query.

from unittest import mock

from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.pivot_utils import (
    generate_pivot_aggregation_value_label_snowflake_quoted_identifier_mappings,
)
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=0)
def test_generate_pivot_aggregation_value_label_pairs():
    fake_frame = mock.create_autospec(InternalFrame)
    fake_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels = mock.Mock(
        return_value=[
            ('"A"',),
            (
                '"B"',
                '"B_2"',
            ),
        ]
    )

    values_label_to_snowflake_quoted_identifiers = (
        generate_pivot_aggregation_value_label_snowflake_quoted_identifier_mappings(
            ["A", "B"], fake_frame
        )
    )

    assert len(values_label_to_snowflake_quoted_identifiers) == 3
    assert (
        values_label_to_snowflake_quoted_identifiers[0].pandas_label == "A"
        and values_label_to_snowflake_quoted_identifiers[0].snowflake_quoted_identifier
        == '"A"'
    )
    assert (
        values_label_to_snowflake_quoted_identifiers[1].pandas_label == "B"
        and values_label_to_snowflake_quoted_identifiers[1].snowflake_quoted_identifier
        == '"B"'
    )
    assert (
        values_label_to_snowflake_quoted_identifiers[2].pandas_label == "B"
        and values_label_to_snowflake_quoted_identifiers[2].snowflake_quoted_identifier
        == '"B_2"'
    )

    fake_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels = mock.Mock(
        return_value=[]
    )
    values_label_to_snowflake_quoted_identifiers = (
        generate_pivot_aggregation_value_label_snowflake_quoted_identifier_mappings(
            [], fake_frame
        )
    )

    assert len(values_label_to_snowflake_quoted_identifiers) == 1
    assert (
        values_label_to_snowflake_quoted_identifiers[0].pandas_label is None
        and values_label_to_snowflake_quoted_identifiers[0].snowflake_quoted_identifier
        is None
    )
