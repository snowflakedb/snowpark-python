#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Context module for Snowpark."""
import logging
import sys
import weakref
from typing import Any, Callable, Optional

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

# Default backend selector for the Snowpark Catalog when running in
# Snowpark-Connect / SCOS compatible mode. True -> SQL-based backend,
# False -> legacy snowflake.core REST backend. Read live by Catalog.__init__.
_use_sql_base_catalog = True
# Internal-only global flag that enables improved SQL simplifier query flattening
# for filter, sort, select, and distinct. When True (default), the branch
# improvements are active regardless of _is_snowpark_connect_compatible_mode.
_snowpark_connect_flatten_select_after_sort = True
_aggregation_function_set = (
    set()
)  # lower cased names of aggregation functions, used in sql simplification
_aggregation_function_set_lock = threading.RLock()
_aggregation_function_prefetch_state: dict[str, Any] = {
    "lock": threading.RLock(),
    "event": None,
    "job": None,
}

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
        "approx_count_distinct",
        "approx_percentile",
        "approx_percentile_accumulate",
        "approx_percentile_combine",
        "approx_top_k",
        "approx_top_k_accumulate",
        "approx_top_k_combine",
        "approximate_count_distinct",
        "approximate_jaccard_index",
        "approximate_similarity",
        "array_agg",
        "array_union_agg",
        "array_unique_agg",
        "arrayagg",
        "avg",
        "bit_and_agg",
        "bit_andagg",
        "bit_or_agg",
        "bit_oragg",
        "bit_xor_agg",
        "bit_xoragg",
        "bitand_agg",
        "bitandagg",
        "bitmap_and_agg",
        "bitmap_construct_agg",
        "bitmap_or_agg",
        "bitor_agg",
        "bitoragg",
        "bitxor_agg",
        "bitxoragg",
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
        "min_by",
        "minhash",
        "minhash_combine",
        "mode",
        "object_agg",
        "objectagg",
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
        "st_intersection_agg_geography_internal",
        "st_union_agg_geography_internal",
        "stddev",
        "stddev_pop",
        "stddev_samp",
        "sum",
        "sum_internal",
        "sum_internal_real",
        "sum_real",
        "summarize_agg",
        "var_pop",
        "var_samp",
        "variance",
        "variance_pop",
        "variance_samp",
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
_enable_metadata_validation = False

# This is an internal-only global flag, used to determine whether to enable query line tracking for tracing sql compilation errors.
_enable_trace_sql_errors_to_dataframe = False
# State from before the development features were first turned on (None while they are off),
# so that turning them off puts everything back as it was.
_development_features_snapshot: Optional[dict] = None

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
    enable_eager_schema_validation: Optional[bool] = None,
    enable_dataframe_trace_on_error: Optional[bool] = None,
    enable_trace_sql_errors_to_dataframe: Optional[bool] = None,
    enable_metadata_validation: Optional[bool] = None,
) -> None:
    """
    Configure development features for the session.

    Only the features passed explicitly are changed; features that are not passed keep their
    current setting. Calling this function without any argument disables every development
    feature. All features are disabled by default.

    Args:
        enable_eager_schema_validation: If True, dataframe schemas are eagerly validated by querying
            for column metadata after every dataframe operation. This adds additional query overhead.
        enable_dataframe_trace_on_error: If True, upon failure, we will add most recent dataframe
            operations to the error trace. This enables the AST collection in the session.
        enable_trace_sql_errors_to_dataframe: If True, we will enable tracing sql compilation errors
            to the associated dataframe operations. This enables the AST collection in the session.
        enable_metadata_validation: If True, select, with_column(s), filter and sort raise an error
            when they reference a column that is missing from the column names the client already
            holds. No query is sent for this check; it is skipped when those names are not available.
    Note:
        This feature is experimental since 1.33.0. Do not use it in production.

        Turning on a tracing feature enables the AST collection and, to avoid a known issue with
        the AST, disables the automatic clean-up of temporary tables in the sessions. When all the
        features are turned off again, every setting the features touched, including the feature
        settings, the AST collection and the automatic clean-up of temporary tables, goes back to
        what it was before the features were first turned on. A cleaner setting you changed yourself
        while the features were on, such as ``session.auto_clean_up_temp_table_enabled``, is kept.
    """
    _logger.warning(
        "configure_development_features() is experimental since 1.33.0. Do not use it in production.",
    )
    global _debug_eager_schema_validation
    global _enable_dataframe_trace_on_error
    global _enable_trace_sql_errors_to_dataframe
    global _enable_metadata_validation
    global _development_features_snapshot

    requested = (
        enable_eager_schema_validation,
        enable_dataframe_trace_on_error,
        enable_trace_sql_errors_to_dataframe,
        enable_metadata_validation,
    )
    if all(value is None for value in requested):
        requested = (False, False, False, False)
    (
        eager_schema_validation,
        dataframe_trace_on_error,
        trace_sql_errors_to_dataframe,
        metadata_validation,
    ) = requested

    from snowflake.snowpark._internal.utils import is_ast_enabled

    with snowflake.snowpark.session._session_management_lock:
        sessions = snowflake.snowpark.session._get_active_sessions(
            require_at_least_one=False
        )
        if _development_features_snapshot is None and any(requested):
            _development_features_snapshot = {
                "flags": (
                    _debug_eager_schema_validation,
                    _enable_dataframe_trace_on_error,
                    _enable_trace_sql_errors_to_dataframe,
                    _enable_metadata_validation,
                ),
                "ast_enabled": is_ast_enabled(),
                "cleaners": weakref.WeakKeyDictionary(
                    {
                        active_session: active_session._auto_clean_up_temp_table_enabled
                        for active_session in sessions
                    }
                ),
            }

        if eager_schema_validation is not None:
            _debug_eager_schema_validation = eager_schema_validation
        if dataframe_trace_on_error is not None:
            _enable_dataframe_trace_on_error = dataframe_trace_on_error
        if trace_sql_errors_to_dataframe is not None:
            _enable_trace_sql_errors_to_dataframe = trace_sql_errors_to_dataframe
        if metadata_validation is not None:
            _enable_metadata_validation = metadata_validation

        if _enable_dataframe_trace_on_error or _enable_trace_sql_errors_to_dataframe:
            try:
                for active_session in sessions:
                    active_session._set_ast_enabled_internal(True)
            except Exception as e:  # pragma: no cover
                _logger.warning(
                    f"Cannot enable AST collection in the session due to {str(e)}. Some development features may not work as expected.",
                )
        elif _development_features_snapshot is not None and not (
            _debug_eager_schema_validation or _enable_metadata_validation
        ):
            snapshot = _development_features_snapshot
            _development_features_snapshot = None
            (
                _debug_eager_schema_validation,
                _enable_dataframe_trace_on_error,
                _enable_trace_sql_errors_to_dataframe,
                _enable_metadata_validation,
            ) = snapshot["flags"]
            # only undo AST if the features turned it on; touching it otherwise would pin its state
            turn_ast_off = not snapshot["ast_enabled"] and is_ast_enabled()
            for active_session in sessions:
                if turn_ast_off:
                    # also gives the cleaner back to the sessions the AST had disabled it for
                    active_session._set_ast_enabled_internal(False)
                elif (
                    snapshot["cleaners"].get(active_session)
                    and active_session._auto_clean_up_temp_table_disabled_by_ast
                ):
                    active_session._auto_clean_up_temp_table_enabled = True
                    active_session._auto_clean_up_temp_table_disabled_by_ast = False


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
