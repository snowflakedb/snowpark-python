#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from enum import Enum


class CompilationStageTelemetryField(Enum):
    TYPE_LARGE_QUERY_BREAKDOWN_ENABLED = "snowpark_large_query_breakdown_enabled"
    TYPE_LARGE_QUERY_BREAKDOWN_OPTIMIZATION_SKIPPED = (
        "snowpark_large_query_breakdown_optimization_skipped"
    )
    TYPE_LARGE_QUERY_BREAKDOWN_UPDATE_COMPLEXITY_BOUNDS = (
        "snowpark_large_query_breakdown_update_complexity_bounds"
    )
    KEY_REASON = "reason"
    KEY_LOWER_BOUND = "lower_bound"
    KEY_UPPER_BOUND = "upper_bound"


class SkipLargeQueryBreakdownCategory(Enum):
    ACTIVE_TRANSACTION = "active transaction"
    VIEW_DYNAMIC_TABLE = "view or dynamic table command"
