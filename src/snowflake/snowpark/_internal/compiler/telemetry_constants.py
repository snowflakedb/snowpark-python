#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
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
    QUERY_PLAN_DUPLICATED_NODE_COMPLEXITY_DISTRIBUTION = (
        "query_plan_duplicated_node_complexity_distribution"
    )
    QUERY_PLAN_COMPLEXITY = "query_plan_complexity"

    # types
    TYPE_COMPILATION_STAGE_STATISTICS = "snowpark_compilation_stage_statistics"
    TYPE_LARGE_QUERY_BREAKDOWN_UPDATE_COMPLEXITY_BOUNDS = (
        "snowpark_large_query_breakdown_update_complexity_bounds"
    )

    # categories
    CAT_COMPILATION_STAGE_STATS = "query_compilation_stage_statistics"
    CAT_COMPILATION_STAGE_ERROR = "query_compilation_stage_error"
    CAT_SNOWFLAKE_PLAN_METRICS = "snowflake_plan_metrics"

    # keys
    KEY_REASON = "reason"
    PLAN_UUID = "plan_uuid"
    ERROR_TYPE = "error_type"
    ERROR_MESSAGE = "error_message"
    TIME_TAKEN_FOR_COMPILATION = "time_taken_for_compilation_sec"
    TIME_TAKEN_FOR_DEEP_COPY_PLAN = "time_taken_for_deep_copy_plan_sec"
    TIME_TAKEN_FOR_CTE_OPTIMIZATION = "time_taken_for_cte_optimization_sec"
    TIME_TAKEN_FOR_LARGE_QUERY_BREAKDOWN = "time_taken_for_large_query_breakdown_sec"
    LARGE_QUERY_BREAKDOWN_OPTIMIZATION_SKIPPED = (
        "query_breakdown_optimization_skipped_reason"
    )

    # keys for repeated subquery elimination
    CTE_NODE_CREATED = "cte_node_created"

    # keys for large query breakdown
    BREAKDOWN_SUMMARY = "breakdown_summary"
    COMPLEXITY_SCORE_AFTER_CTE_OPTIMIZATION = "complexity_score_after_cte_optimization"
    COMPLEXITY_SCORE_AFTER_LARGE_QUERY_BREAKDOWN = (
        "complexity_score_after_large_query_breakdown"
    )
    COMPLEXITY_SCORE_BEFORE_COMPILATION = "complexity_score_before_compilation"
    COMPLEXITY_SCORE_BOUNDS = "complexity_score_bounds"
    NUM_PARTITIONS_MADE = "num_partitions_made"
    NUM_PIPELINE_BREAKER_USED = "num_pipeline_breaker_used"
    NUM_RELAXED_BREAKER_USED = "num_relaxed_breaker_used"
    FAILED_PARTITION_SUMMARY = "failed_partition_summary"


class NodeBreakdownCategory(Enum):
    SCORE_BELOW_LOWER_BOUND = "num_nodes_below_lower_bound"
    SCORE_ABOVE_UPPER_BOUND = "num_nodes_above_upper_bound"
    NON_PIPELINE_BREAKER = "num_non_pipeline_breaker_nodes"
    EXTERNAL_CTE_REF = "num_external_cte_ref_nodes"
    VALID_NODE = "num_valid_nodes"
    VALID_NODE_RELAXED = "num_valid_nodes_relaxed"


class SkipLargeQueryBreakdownCategory(Enum):
    ACTIVE_TRANSACTION = "active transaction"
    VIEW_DYNAMIC_TABLE = "view or dynamic table command"
    NO_ACTIVE_DATABASE = "no active database"
    NO_ACTIVE_SCHEMA = "no active schema"
