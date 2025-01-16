#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import List, Optional

from snowflake.snowpark._internal.analyzer.expression import (
    Expression,
    FunctionExpression,
)
from snowflake.snowpark._internal.analyzer.table_function import TableFunctionExpression
from snowflake.snowpark._internal.analyzer.window_expression import WindowExpression

# Set of functions (defined in snowpark-python/src/snowflake/snowpark/functions.py) that do not
# block merge projections of nested select.
# Be careful when adding functions in this list, it must statisfy the following condition:
# 1) not aggregation
# 2) no data generation
# 3) not nondeterministic or nondeterministic with unique reference,
VALID_PROJECTION_MERGE_FUNCTIONS = (
    "abs",
    "acos",
    "acosh",
    "add_months",
    "array_cat",
    "array_compact",
    "array_construct",
    "array_construct_compact" "array_contains",
    "array_distinct",
    "array_except",
    "array_flatten",
    "array_generate_range",
    "array_insert",
    "array_intersection",
    "array_max",
    "array_min",
    "array_position",
    "array_prepend",
    "array_size",
    "array_slice",
    "array_sort",
    "array_to_string",
    "arrays_overlap",
    "arrays_to_object",
    "arrays_zip",
    "as_array",
    "as_binary",
    "as_char",
    "as_date",
    "as_decimal",
    "as_double",
    "as_integer",
    "as_object",
    "as_real",
    "as_time",
    "as_timestamp_ltz",
    "as_timestamp_ntz",
    "as_timestamp_tz",
    "as_varchar",
    "asin",
    "asinh",
    "atan",
    "atan2",
    "between",
    "bitand",
    "bitcount" "bitnot",
    "bitor",
    "bitshiftleft",
    "bitshiftright",
    "bitxor",
    "bround",
    "cast",
    "cbrt",
    "ceil",
    "char",
    "charindex",
    "check_json",
    "check_xml",
    "coalesce",
    "collate",
    "collation",
    "concat",
    "concat_ws",
    "contains",
    "convert_timezoneobject_construct_keep_null",
    "cos",
    "cosh",
    "date_from_parts",
    "date_part",
    "date_trunc",
    "dateadd",
    "datediff",
    "daydiff",
    "dayname",
    "dayofmonth",
    "dayofweek",
    "dayofyear",
    "degrees",
    "div0",
    "endswith",
    "equal_nan",
    "exp",
    "factorial",
    "floor",
    "from_utc_timestamp",
    "get",
    "get_ignore_case",
    "get_path",
    "greatest",
    "hour",
    "iff",
    "in_",
    "initcap",
    "insert",
    "is_array",
    "is_binary",
    "is_boolean",
    "is_char",
    "is_date",
    "is_decimalis_double",
    "is_integer",
    "is_null",
    "is_null_value",
    "is_object",
    "is_real",
    "is_time",
    "is_timestamp_ltz",
    "is_timestamp_ntz",
    "is_timestamp_tz",
    "json_extract_path_text",
    "last_day",
    "least",
    "left",
    "length",
    "log",
    "lower",
    "lpad",
    "ltrim",
    "minute",
    "month",
    "monthname",
    "months_between",
    "negate",
    "next_day",
    "nvl",
    "nvl2",
    "object_construct",
    "object_delete",
    "object_insert",
    "object_keys",
    "object_pick",
    "parse_json",
    "parse_xml",
    "pow",
    "previous_day",
    "quarter",
    "radians",
    "regexp_count",
    "regexp_extract",
    "regexp_replace",
    "repeat",
    "replace",
    "reverse",
    "right",
    "round",
    "rpad",
    "rtrim",
    "second",
    "sign",
    "sin",
    "sinh",
    "soundex",
    "split",
    "sqrt",
    "startswith",
    "strip_null_valuearray_append",
    "strtok_to_array",
    "substring",
    "sysdate",
    "tan",
    "time_from_parts",
    "timestamp_from_parts",
    "timestamp_ltz_from_parts",
    "timestamp_ntz_from_parts",
    "timestamp_tz_from_parts",
    "to_array",
    "to_binary",
    "to_boolean",
    "to_char",
    "to_date",
    "to_decimal",
    "to_double",
    "to_geography",
    "to_geometry",
    "to_json",
    "to_object",
    "to_time",
    "to_timestamp",
    "to_timestamp_ltz",
    "to_timestamp_ntz",
    "to_timestamp_tz",
    "to_utc_timestamp",
    "to_variant",
    "to_xml",
    "translate",
    "trim",
    "trunc",
    "typeof",
    "upper",
    "vector_cosine_distance",
    "vector_inner_product",
    "vector_l2_distance",
    "weekofyear",
    "year",
)


def has_invalid_projection_merge_functions(
    expressions: Optional[List[Expression]],
) -> bool:
    """
    Check if the given list of expressions contains any functions that blocks the merge
    of projections.

    For each expression, the check is applied recursively to its-self and the child expressions.
    A function blocks the merge or inlining of projection expression if it is
    1) a window function
    2) a table function expression
    3) a function expression that is data generator or not in the VALID_PROJECTION_MERGE_FUNCTIONS list
    """

    if expressions is None:
        return False
    for exp in expressions:
        if isinstance(exp, WindowExpression):
            return True
        if isinstance(expressions, TableFunctionExpression):
            # TODO: it seems that it is not possible to have TableFunctionExpression as projection,
            #   should double check that
            return True  # pragma: no cover
        if isinstance(exp, FunctionExpression) and (
            exp.is_data_generator
            or (not exp.name.lower() in VALID_PROJECTION_MERGE_FUNCTIONS)
        ):
            return True
        if exp is not None and has_invalid_projection_merge_functions(exp.children):
            return True

    return False
