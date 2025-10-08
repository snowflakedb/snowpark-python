#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
Unit tests for auto-switching functionality based on unsupported arguments.
"""

import pytest

from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    UnsupportedArgsRule,
)


class TestMalformedConditions:
    def test_malformed_conditions_caught_at_init(self):
        # Test that malformed conditions are caught during rule creation.

        with pytest.raises(ValueError, match="Invalid condition at index 0"):
            UnsupportedArgsRule(
                unsupported_conditions=[
                    ("single_item",),
                ]
            )

        with pytest.raises(ValueError, match="Invalid condition at index 1"):
            UnsupportedArgsRule(
                unsupported_conditions=[
                    ("valid_param", "valid_value"),
                    "not_a_tuple",
                ]
            )

        with pytest.raises(ValueError, match="Invalid condition at index 0"):
            UnsupportedArgsRule(
                unsupported_conditions=[
                    (None, "reason_for_none"),
                ]
            )
