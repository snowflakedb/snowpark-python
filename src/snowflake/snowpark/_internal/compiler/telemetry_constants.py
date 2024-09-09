#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from enum import Enum


class CompilationStageTelemetryField(Enum):
    TYPE_LARGE_QUERY_BREAKDOWN_OPTIMIZATION_SKIPPED = (
        "snowpark_large_query_breakdown_optimization_skipped"
    )
    KEY_REASON = "reason"


class SkipLargeQueryBreakdownCategory(Enum):
    ACTIVE_TRANSACTION = "active transaction"
    VIEW_DYNAMIC_TABLE = "view or dynamic table command"
