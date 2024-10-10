#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from enum import Enum


class CompilationStageTelemetryField(Enum):
    # dataframe query stats that are used for the
    # new compilation stage optimizations
    QUERY_PLAN_HEIGHT = "query_plan_height"
    QUERY_PLAN_NUM_SELECTS_WITH_COMPLEXITY_MERGED = (
        "query_plan_num_selects_with_complexity_merged"
    )
    QUERY_PLAN_NUM_DUPLICATE_NODES = "query_plan_num_duplicate_nodes"
    QUERY_PLAN_COMPLEXITY = "query_plan_complexity"

    # types
    TYPE_LARGE_QUERY_BREAKDOWN_OPTIMIZATION_SKIPPED = (
        "snowpark_large_query_breakdown_optimization_skipped"
    )
    TYPE_COMPILATION_STAGE_STATISTICS = "snowpark_compilation_stage_statistics"
    TYPE_LARGE_QUERY_BREAKDOWN_UPDATE_COMPLEXITY_BOUNDS = (
        "snowpark_large_query_breakdown_update_complexity_bounds"
    )

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

    # keys for repeated subquery elimination
    CTE_NODE_CREATED = "cte_node_created"


class SkipLargeQueryBreakdownCategory(Enum):
    ACTIVE_TRANSACTION = "active transaction"
    VIEW_DYNAMIC_TABLE = "view or dynamic table command"
