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
    TIME_TAKEN_FOR_COMPILATION = "time_taken_for_compilation_sec"
    TIME_TAKEN_FOR_DEEP_COPY_PLAN = "time_taken_for_deep_copy_plan_sec"
    TIME_TAKEN_FOR_CTE_OPTIMIZATION = "time_taken_for_cte_optimization_sec"
    TIME_TAKEN_FOR_LARGE_QUERY_BREAKDOWN = "time_taken_for_large_query_breakdown_sec"
    COMPLEXITY_SCORE_BOUNDS = "complexity_score_bounds"
    COMPLEXITY_SCORE_BEFORE_COMPILATION = "complexity_score_before_compilation"
    COMPLEXITY_SCORE_AFTER_CTE_OPTIMIZATION = "complexity_score_after_cte_optimization"
    COMPLEXITY_SCORE_AFTER_LARGE_QUERY_BREAKDOWN = (
        "complexity_score_after_large_query_breakdown"
    )


class SkipLargeQueryBreakdownCategory(Enum):
    ACTIVE_TRANSACTION = "active transaction"
    VIEW_DYNAMIC_TABLE = "view or dynamic table command"
