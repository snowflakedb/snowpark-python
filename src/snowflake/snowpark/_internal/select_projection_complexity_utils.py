#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import List

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
# 3) not nondeterministic with unique reference,
VALID_PROJECTION_MERGE_FUNCTIONS = (
    "add_months",
    "bitnot",
    "bitshiftleft",
    "bitshiftright",
    "bround",
    "convert_timezone" "object_construct_keep_null",
    "object_construct",
    "coalesce",
    "equal_nan",
    "is_null",
    "negate",
    "to_boolean",
    "to_decimal",
    "to_double",
    "div0",
    "sqrt",
    "abs",
    "acos",
    "asin",
    "atan",
    "atan2",
    "ceil",
    "cos",
    "cosh",
    "exp",
    "factorial",
    "floor",
    "sin",
    "sinh",
    "tan",
    "degrees",
    "radians",
    "initcap",
    "length",
    "lower",
    "lpad",
    "ltrim",
    "rpad",
    "rtrim",
    "repeat",
    "reverse",
    "upper",
    "soundex",
    "trim",
    "strtok_to_array",
    "log",
    "pow",
    "round",
    "sign",
    "split",
    "substring",
    "array_to_string",
    "array_slice",
    "array_size",
    "regexp_count",
    "regexp_extract",
    "regexp_replace",
    "replace",
    "charindex",
    "collate",
    "collation",
    "concat",
    "concat_ws",
    "translate",
    "contains",
    "startswith",
    "endswith",
    "insert",
    "left",
    "right",
    "char",
    "to_char",
    "to_time",
    "to_timestamp",
    "to_timestamp_ntz",
    "to_timestamp_ltz",
    "to_timestamp_tz",
    "from_utc_timestamp",
    "to_utc_timestamp",
    "to_date",
    "hour",
    "last_day",
    "minute",
    "next_day",
    "previous_day",
    "second",
    "month",
    "monthname",
    "quarter",
    "year",
    "sysdate",
    "months_between",
    "to_geography",
    "to_geometry",
    "arrays_overlap",
    "array_distinct",
    "array_intersection",
    "array_except",
    "array_min",
    "array_max",
    "array_flatten",
    "array_sort",
    "arrays_to_object",
    "arrays_zip",
    "array_generate_range",
    "dateadd",
    "datediff",
    "daydiff",
    "trunc",
    "date_part",
    "date_from_parts",
    "date_trunc",
    "dayname",
    "dayofmonth",
    "dayofweek",
    "dayofyear",
    "is_array",
    "is_boolean",
    "is_binary",
    "is_char",
    "is_date",
    "is_decimal" "is_double",
    "is_real",
    "is_integer",
    "is_null_value",
    "is_object",
    "is_time",
    "is_timestamp_ltz",
    "is_timestamp_ntz",
    "is_timestamp_tz",
    "time_from_parts",
    "timestamp_from_parts",
    "timestamp_ltz_from_parts",
    "timestamp_ntz_from_parts",
    "timestamp_tz_from_parts",
    "weekofyear",
    "typeof",
    "check_json",
    "check_xml",
    "json_extract_path_text",
    "parse_json",
    "parse_xml",
    "strip_null_value" "array_append",
    "array_cat",
    "array_compact",
    "array_construct",
    "array_construct_compact" "array_contains",
    "array_insert",
    "array_position",
    "array_prepend",
    "object_delete",
    "object_insert",
    "object_pick",
    "vector_cosine_distance",
    "vector_l2_distance",
    "vector_inner_product",
    "as_array",
    "as_binary",
    "as_char",
    "as_varchar",
    "as_date",
    "as_decimal",
    "as_double",
    "as_real",
    "as_integer",
    "as_object",
    "as_time",
    "as_timestamp_ltz",
    "as_timestamp_ntz",
    "as_timestamp_tz",
    "to_binary",
    "to_array",
    "to_json",
    "to_object",
    "to_variant",
    "to_xml",
    "get_ignore_case",
    "object_keys",
    "get_path",
    "get",
    "iff",
    "in_",
    "greatest",
    "least",
)


def has_invalid_projection_merge_functions(expressions: List[Expression]) -> bool:
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
            return True
        if isinstance(exp, FunctionExpression) and (
            exp.is_data_generator
            or (not exp.name.lower() in VALID_PROJECTION_MERGE_FUNCTIONS)
        ):
            return True
        if exp is not None and has_invalid_projection_merge_functions(exp.children):
            return True

    return False
