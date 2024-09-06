#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from enum import Enum


class CompilationStageTelemetryField(Enum):
    # types
    TYPE_LARGE_QUERY_BREAKDOWN_OPTIMIZATION_SKIPPED = (
        "snowpark_large_query_breakdown_optimization_skipped"
    )
    TYPE_COMPILATION_STAGE_STATISTICS = "snowpark_compilation_stage_statistics"

    # keys
    KEY_REASON = "reason"
    PLAN_UUID = "plan_uuid"
    BEFORE_COMPLEXITY_SCORE = "before_complexity_score"
    AFTER_COMPLEXITY_SCORES = "after_complexity_scores"


class SkipLargeQueryBreakdownCategory(Enum):
    ACTIVE_TRANSACTION = "active transaction"
    VIEW_DYNAMIC_TABLE = "view or dynamic table command"
