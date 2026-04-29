#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Context module for Snowpark."""
import logging
import sys
from typing import Callable, Optional

import snowflake.snowpark
import threading

_logger = logging.getLogger(__name__)

_use_scoped_temp_objects = True

# This is an internal-only global flag, used to determine whether to execute code in a client's local sandbox or connect to a Snowflake account.
# If this is True, then the session instance is forcibly set to None to avoid any interaction with a Snowflake account.
_is_execution_environment_sandboxed_for_client: bool = False

# This callback, assigned by the caller environment outside Snowpark, can be used to share information about the extension function to be registered.
# It should also return a decision on whether to proceed with registring the extension function with the Snowflake account.
# If _should_continue_registration is None, i.e. a caller environment never assigned it an alternate callable, then we want to continue registration as part of the regular Snowpark workflow.
# If _should_continue_registration is not None, i.e. a caller environment has assigned it an alternate callable, then the callback is responsible for determining the rest of the Snowpark workflow.
_should_continue_registration: Optional[Callable[..., bool]] = None


# Internal-only global flag that determines if structured type semantics should be used
_use_structured_type_semantics = False
_use_structured_type_semantics_lock = threading.RLock()

# This is an internal-only global flag, used to determine whether the api code which will be executed is compatible with snowflake.snowpark_connect
_is_snowpark_connect_compatible_mode = False
# Internal-only global flag that enables improved SQL simplifier query flattening
# for filter, sort, select, and distinct. When True (default), the branch
# improvements are active regardless of _is_snowpark_connect_compatible_mode.
_snowpark_connect_flatten_select_after_sort = True
_aggregation_function_set = (
    set()
)  # lower cased names of aggregation functions, used in sql simplification
_aggregation_function_set_lock = threading.RLock()

# Hardcoded fallback for system built-in aggregation functions.
# Used when the dynamic query fails to retrieve the list from the database.
#
# Generated via:
#   show functions ->> select "name" from $1 where "is_aggregate" = 'Y'
#
# Entries with parentheses in the name (COUNT(*), COUNT_INTERNAL(*)) are excluded
# because FunctionExpression.name stores only the function name without parens,
# so they can never match at the lookup site.
_KNOWN_AGGREGATION_FUNCTIONS = frozenset(
    [
        "accumulate",
        "agg",
        "ai_agg",
        "ai_summarize_agg",
        "any_value",
        "approximate_count_distinct",
        "approximate_jaccard_index",
        "approximate_similarity",
        "approx_count_distinct",
        "approx_percentile",
        "approx_percentile_accumulate",
        "approx_percentile_combine",
        "approx_top_k",
        "approx_top_k_accumulate",
        "approx_top_k_combine",
        "arrayagg",
        "array_agg",
        "array_union_agg",
        "array_unique_agg",
        "avg",
        "bitandagg",
        "bitand_agg",
        "bitmap_construct_agg",
        "bitmap_or_agg",
        "bitoragg",
        "bitor_agg",
        "bitxoragg",
        "bitxor_agg",
        "bit_andagg",
        "bit_and_agg",
        "bit_oragg",
        "bit_or_agg",
        "bit_xoragg",
        "bit_xor_agg",
        "booland_agg",
        "boolor_agg",
        "boolxor_agg",
        "corr",
        "count",
        "count_if",
        "count_internal",
        "covar_pop",
        "covar_samp",
        "datasketches_hll",
        "datasketches_hll_accumulate",
        "datasketches_hll_combine",
        "first_value",
        "hash_agg",
        "hll",
        "hll_accumulate",
        "hll_combine",
        "kurtosis",
        "last_value",
        "listagg",
        "max",
        "max_by",
        "median",
        "min",
        "minhash",
        "minhash_combine",
        "min_by",
        "mode",
        "objectagg",
        "object_agg",
        "percentile_cont",
        "percentile_disc",
        "regr_avgx",
        "regr_avgy",
        "regr_count",
        "regr_intercept",
        "regr_r2",
        "regr_slope",
        "regr_sxx",
        "regr_sxy",
        "regr_syy",
        "skew",
        "stddev",
        "stddev_pop",
        "stddev_samp",
        "st_intersection_agg_geography_internal",
        "st_union_agg_geography_internal",
        "sum",
        "sum_internal",
        "sum_internal_real",
        "sum_real",
        "variance",
        "variance_pop",
        "variance_samp",
        "var_pop",
        "var_samp",
        "vector_avg",
        "vector_max",
        "vector_min",
        "vector_sum",
    ]
)

_cte_error_threshold = 3  # 0 to disable auto-cte-disable, otherwise the number of times CTE optimization can fail before it is automatically disabled for the remainder of the session.

# Following are internal-only global flags, used to enable development features.
_enable_dataframe_trace_on_error = False
_debug_eager_schema_validation = False

# This is an internal-only global flag, used to determine whether to enable query line tracking for tracing sql compilation errors.
_enable_trace_sql_errors_to_dataframe = False

# SNOW-2362050: Enable this fix by default.
# Global flag for fix 2360274. When enabled schema queries will use NULL as a place holder for any values inside structured objects
_enable_fix_2360274 = False

# internal only dictionary store the default precision of integral types, if the type does not appear in the
# dictionary, the default precision is None.
# example: _integral_type_default_precision = {IntegerType: 9}, IntegerType default _precision is 9 now
_integral_type_default_precision = {}

# The fully qualified name of the Anaconda shared repository (conda channel).
_ANACONDA_SHARED_REPOSITORY = "snowflake.snowpark.anaconda_shared_repository"
# The fully qualified name of the PyPI shared repository (pypi channel).
_PYPI_SHARED_REPOSITORY = "snowflake.snowpark.pypi_shared_repository"
# In case of failures and for routing to the right session package store, we use this
_DEFAULT_ARTIFACT_REPOSITORY = (
    _ANACONDA_SHARED_REPOSITORY
    if sys.version_info < (3, 14)
    else _PYPI_SHARED_REPOSITORY
)


def configure_development_features(
    *,
    enable_eager_schema_validation: bool = False,
    enable_dataframe_trace_on_error: bool = False,
    enable_trace_sql_errors_to_dataframe: bool = False,
) -> None:
    """
    Configure development features for the session.

    Args:
        enable_eager_schema_validation: If True, dataframe schemas are eagerly validated by querying
            for column metadata after every dataframe operation. This adds additional query overhead.
        enable_dataframe_trace_on_error: If True, upon failure, we will add most recent dataframe
            operations to the error trace. This enables the AST collection in the session.
        enable_trace_sql_errors_to_dataframe: If True, we will enable tracing sql compilation errors
            to the associated dataframe operations. This enables the AST collection in the session.
    Note:
        This feature is experimental since 1.33.0. Do not use it in production.
    """
    _logger.warning(
        "configure_development_features() is experimental since 1.33.0. Do not use it in production.",
    )
    global _debug_eager_schema_validation
    global _enable_dataframe_trace_on_error
    global _enable_trace_sql_errors_to_dataframe
    _debug_eager_schema_validation = enable_eager_schema_validation

    if enable_dataframe_trace_on_error or enable_trace_sql_errors_to_dataframe:
        _enable_dataframe_trace_on_error = enable_dataframe_trace_on_error
        _enable_trace_sql_errors_to_dataframe = enable_trace_sql_errors_to_dataframe
        with snowflake.snowpark.session._session_management_lock:
            sessions = snowflake.snowpark.session._get_active_sessions(
                require_at_least_one=False
            )
            try:
                for active_session in sessions:
                    active_session._set_ast_enabled_internal(True)
            except Exception as e:  # pragma: no cover
                _logger.warning(
                    f"Cannot enable AST collection in the session due to {str(e)}. Some development features may not work as expected.",
                )
    else:
        _enable_dataframe_trace_on_error = False
        _enable_trace_sql_errors_to_dataframe = False


def _should_use_structured_type_semantics():
    global _use_structured_type_semantics
    global _use_structured_type_semantics_lock
    with _use_structured_type_semantics_lock:
        return _use_structured_type_semantics


def get_active_session() -> "snowflake.snowpark.Session":
    """Returns the current active Snowpark session.

    Raises: SnowparkSessionException: If there is more than one active session or no active sessions.

    Returns:
        A :class:`Session` object for the current session.
    """
    return snowflake.snowpark.session._get_active_session()
