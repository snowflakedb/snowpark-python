#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import json
import math
import os
import sys
import tempfile
from typing import Any, Dict, List, Optional, Tuple, Union, Literal, Sequence

from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.options import pyarrow
from snowflake.connector.pandas_tools import (
    _create_temp_stage,
    _create_temp_file_format,
    build_location_helper,
)
from snowflake.snowpark._internal.analyzer.binary_plan_node import (
    AsOf,
    Except,
    Intersect,
    JoinType,
    LeftAnti,
    LeftSemi,
    NaturalJoin,
    UsingJoin,
)
from snowflake.snowpark._internal.analyzer.datatype_mapper import (
    schema_expression,
    to_sql,
)
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.type_utils import convert_sp_to_sf_type
from snowflake.snowpark._internal.utils import (
    ALREADY_QUOTED,
    DOUBLE_QUOTE,
    EMPTY_STRING,
    TempObjectType,
    escape_quotes,
    escape_single_quotes,
    get_temp_type_for_object,
    is_single_quoted,
    is_sql_select_statement,
    quote_name,
    random_name_for_temp_object,
    unwrap_single_quote,
)
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import DataType

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

LEFT_PARENTHESIS = "("
RIGHT_PARENTHESIS = ")"
LEFT_BRACKET = "["
RIGHT_BRACKET = "]"
AS = " AS "
EXCLUDE = " EXCLUDE "
AND = " AND "
OR = " OR "
NOT = " NOT "
STAR = " * "
SPACE = " "
SINGLE_QUOTE = "'"
COMMA = ", "
MINUS = " - "
PLUS = " + "
DISTINCT = " DISTINCT "
LIKE = " LIKE "
CAST = " CAST "
TRY_CAST = " TRY_CAST "
IN = " IN "
GROUP_BY = " GROUP BY "
PARTITION_BY = " PARTITION BY "
ORDER_BY = " ORDER BY "
CLUSTER_BY = " CLUSTER BY "
REFRESH_MODE = " REFRESH_MODE "
INITIALIZE = " INITIALIZE "
DATA_RETENTION_TIME_IN_DAYS = " DATA_RETENTION_TIME_IN_DAYS "
MAX_DATA_EXTENSION_TIME_IN_DAYS = " MAX_DATA_EXTENSION_TIME_IN_DAYS "
OVER = " OVER "
SELECT = " SELECT "
FROM = " FROM "
WHERE = " WHERE "
LIMIT = " LIMIT "
OFFSET = " OFFSET "
PIVOT = " PIVOT "
UNPIVOT = " UNPIVOT "
FOR = " FOR "
ON = " ON "
USING = " USING "
JOIN = " JOIN "
NATURAL = " NATURAL "
ASOF = " ASOF "
MATCH_CONDITION = " MATCH_CONDITION "
EXISTS = " EXISTS "
CREATE = " CREATE "
TABLE = " TABLE "
REPLACE = " REPLACE "
VIEW = " VIEW "
DYNAMIC = " DYNAMIC "
LAG = " LAG "
WAREHOUSE = " WAREHOUSE "
TEMPORARY = " TEMPORARY "
TRANSIENT = " TRANSIENT "
IF = " If "
INSERT = " INSERT "
OVERWRITE = " OVERWRITE "
INTO = " INTO "
VALUES = " VALUES "
SEQ8 = " SEQ8() "
ROW_NUMBER = " ROW_NUMBER() "
ONE = " 1 "
GENERATOR = "GENERATOR"
ROW_COUNT = "ROWCOUNT"
RIGHT_ARROW = " => "
NUMBER = " NUMBER "
STRING = " STRING "
UNSAT_FILTER = " 1 = 0 "
BETWEEN = " BETWEEN "
FOLLOWING = " FOLLOWING "
PRECEDING = " PRECEDING "
DOLLAR = "$"
DOUBLE_COLON = "::"
DROP = " DROP "
FILE = " FILE "
FILES = " FILES "
FORMAT = " FORMAT "
TYPE = " TYPE "
EQUALS = " = "
LOCATION = " LOCATION "
FILE_FORMAT = " FILE_FORMAT "
FORMAT_NAME = " FORMAT_NAME "
COPY = " COPY "
COPY_GRANTS = " COPY GRANTS "
ENABLE_SCHEMA_EVOLUTION = " ENABLE_SCHEMA_EVOLUTION "
DATA_RETENTION_TIME_IN_DAYS = " DATA_RETENTION_TIME_IN_DAYS "
MAX_DATA_EXTENSION_TIME_IN_DAYS = " MAX_DATA_EXTENSION_TIME_IN_DAYS "
CHANGE_TRACKING = " CHANGE_TRACKING "
EXTERNAL_VOLUME = " EXTERNAL_VOLUME "
CATALOG = " CATALOG "
BASE_LOCATION = " BASE_LOCATION "
CATALOG_SYNC = " CATALOG_SYNC "
STORAGE_SERIALIZATION_POLICY = " STORAGE_SERIALIZATION_POLICY "
REG_EXP = " REGEXP "
COLLATE = " COLLATE "
RESULT_SCAN = " RESULT_SCAN"
INFER_SCHEMA = " INFER_SCHEMA "
SAMPLE = " SAMPLE "
ROWS = " ROWS "
CASE = " CASE "
WHEN = " WHEN "
THEN = " THEN "
ELSE = " ELSE "
END = " END "
FLATTEN = " FLATTEN "
INPUT = " INPUT "
PATH = " PATH "
OUTER = " OUTER "
RECURSIVE = " RECURSIVE "
MODE = " MODE "
LATERAL = " LATERAL "
PUT = " PUT "
GET = " GET "
GROUPING_SETS = " GROUPING SETS "
QUESTION_MARK = "?"
PERCENT_S = r"%s"
SINGLE_COLON = ":"
PATTERN = " PATTERN "
WITHIN_GROUP = " WITHIN GROUP "
VALIDATION_MODE = " VALIDATION_MODE "
UPDATE = " UPDATE "
DELETE = " DELETE "
SET = " SET "
MERGE = " MERGE "
MATCHED = " MATCHED "
LISTAGG = " LISTAGG "
HEADER = " HEADER "
COMMENT = " COMMENT "
IGNORE_NULLS = " IGNORE NULLS "
INCLUDE_NULLS = " INCLUDE NULLS "
UNION = " UNION "
UNION_ALL = " UNION ALL "
RENAME = " RENAME "
INTERSECT = f" {Intersect.sql} "
EXCEPT = f" {Except.sql} "
NOT_NULL = " NOT NULL "
WITH = "WITH "
DEFAULT_ON_NULL = " DEFAULT ON NULL "
ANY = " ANY "
ICEBERG = " ICEBERG "
ICEBERG_VERSION = "ICEBERG_VERSION"
RENAME_FIELDS = " RENAME FIELDS"
ADD_FIELDS = " ADD FIELDS"
NEW_LINE = "\n"
TAB = "    "
UUID_COMMENT = "-- {}"
MODEL = "MODEL"
EXCLAMATION_MARK = "!"
HAVING = " HAVING "

TEMPORARY_STRING_SET = frozenset(["temporary", "temp"])


def format_uuid(uuid: Optional[str], with_new_line: bool = True) -> str:
    """
    Format a uuid into a comment, if the uuid is not empty.
    """
    if not uuid:
        return EMPTY_STRING
    if with_new_line:
        return f"\n{UUID_COMMENT.format(uuid)}\n"
    return f"{UUID_COMMENT.format(uuid)}"


def validate_iceberg_config(iceberg_config: Optional[dict]) -> Dict[str, str]:
    if iceberg_config is None:
        return dict()

    iceberg_config = {k.lower(): v for k, v in iceberg_config.items()}

    return {
        EXTERNAL_VOLUME: iceberg_config.get("external_volume", None),
        CATALOG: iceberg_config.get("catalog", None),
        BASE_LOCATION: iceberg_config.get("base_location", None),
        CATALOG_SYNC: iceberg_config.get("catalog_sync", None),
        STORAGE_SERIALIZATION_POLICY: iceberg_config.get(
            "storage_serialization_policy", None
        ),
        ICEBERG_VERSION: iceberg_config.get("iceberg_version", None),
    }


def result_scan_statement(uuid_place_holder: str) -> str:
    return (
        SELECT
        + STAR
        + FROM
        + TABLE
        + LEFT_PARENTHESIS
        + RESULT_SCAN
        + LEFT_PARENTHESIS
        + SINGLE_QUOTE
        + uuid_place_holder
        + SINGLE_QUOTE
        + RIGHT_PARENTHESIS
        + RIGHT_PARENTHESIS
    )


def model_expression(
    model_name: str,
    version_or_alias_name: Optional[str],
    method_name: str,
    children: List[str],
) -> str:
    model_args_str = (
        f"{model_name}{COMMA}{version_or_alias_name}"
        if version_or_alias_name
        else model_name
    )
    return f"{MODEL}{LEFT_PARENTHESIS}{model_args_str}{RIGHT_PARENTHESIS}{EXCLAMATION_MARK}{method_name}{LEFT_PARENTHESIS}{COMMA.join(children)}{RIGHT_PARENTHESIS}"


def function_expression(name: str, children: List[str], is_distinct: bool) -> str:
    return (
        name
        + LEFT_PARENTHESIS
        + f"{DISTINCT if is_distinct else EMPTY_STRING}"
        + COMMA.join(children)
        + RIGHT_PARENTHESIS
    )


def named_arguments_function(name: str, args: Dict[str, str]) -> str:
    return (
        name
        + LEFT_PARENTHESIS
        + COMMA.join([key + RIGHT_ARROW + value for key, value in args.items()])
        + RIGHT_PARENTHESIS
    )


def partition_spec(col_exprs: List[str]) -> str:
    return f"PARTITION BY {COMMA.join(col_exprs)}" if col_exprs else EMPTY_STRING


def order_by_spec(col_exprs: List[str]) -> str:
    if not col_exprs:
        return EMPTY_STRING
    return ORDER_BY + NEW_LINE + TAB + (COMMA + NEW_LINE + TAB).join(col_exprs)


def table_function_partition_spec(
    over: bool, partition_exprs: List[str], order_exprs: List[str]
) -> str:
    return (
        f"{OVER}{LEFT_PARENTHESIS}{partition_spec(partition_exprs)}{SPACE}{order_by_spec(order_exprs)}{RIGHT_PARENTHESIS}"
        if over
        else EMPTY_STRING
    )


def subquery_expression(child: str) -> str:
    return LEFT_PARENTHESIS + child + RIGHT_PARENTHESIS


def binary_arithmetic_expression(op: str, left: str, right: str) -> str:
    return LEFT_PARENTHESIS + left + SPACE + op + SPACE + right + RIGHT_PARENTHESIS


def alias_expression(origin: str, alias: str) -> str:
    return origin + AS + alias


def within_group_expression(column: str, order_by_cols: List[str]) -> str:
    return (
        column
        + WITHIN_GROUP
        + LEFT_PARENTHESIS
        + ORDER_BY
        + NEW_LINE
        + TAB
        + (COMMA + NEW_LINE + TAB).join(order_by_cols)
        + RIGHT_PARENTHESIS
    )


def limit_expression(num: int) -> str:
    return LIMIT + str(num)


def grouping_set_expression(args: List[List[str]]) -> str:
    flat_args = [LEFT_PARENTHESIS + COMMA.join(arg) + RIGHT_PARENTHESIS for arg in args]
    return GROUPING_SETS + LEFT_PARENTHESIS + COMMA.join(flat_args) + RIGHT_PARENTHESIS


def like_expression(expr: str, pattern: str) -> str:
    return expr + LIKE + pattern


def block_expression(expressions: List[str]) -> str:
    return LEFT_PARENTHESIS + COMMA.join(expressions) + RIGHT_PARENTHESIS


def in_expression(column: str, values: List[str]) -> str:
    return column + IN + block_expression(values)


def regexp_expression(expr: str, pattern: str, parameters: Optional[str] = None) -> str:
    if parameters is not None:
        return function_expression("RLIKE", [expr, pattern, parameters], False)
    else:
        return expr + REG_EXP + pattern


def collate_expression(expr: str, collation_spec: str) -> str:
    return expr + COLLATE + single_quote(collation_spec)


def subfield_expression(expr: str, field: Union[str, int]) -> str:
    return (
        expr
        + LEFT_BRACKET
        + (
            SINGLE_QUOTE + field + SINGLE_QUOTE
            if isinstance(field, str)
            else str(field)
        )
        + RIGHT_BRACKET
    )


def flatten_expression(
    input_: str, path: Optional[str], outer: bool, recursive: bool, mode: str
) -> str:
    return (
        FLATTEN
        + LEFT_PARENTHESIS
        + INPUT
        + RIGHT_ARROW
        + input_
        + COMMA
        + PATH
        + RIGHT_ARROW
        + SINGLE_QUOTE
        + (path or EMPTY_STRING)
        + SINGLE_QUOTE
        + COMMA
        + OUTER
        + RIGHT_ARROW
        + str(outer).upper()
        + COMMA
        + RECURSIVE
        + RIGHT_ARROW
        + str(recursive).upper()
        + COMMA
        + MODE
        + RIGHT_ARROW
        + SINGLE_QUOTE
        + mode
        + SINGLE_QUOTE
        + RIGHT_PARENTHESIS
    )


def lateral_statement(
    lateral_expression: str, child: str, child_uuid: Optional[str] = None
) -> str:
    UUID = format_uuid(child_uuid)
    return (
        SELECT
        + STAR
        + NEW_LINE
        + FROM
        + LEFT_PARENTHESIS
        + NEW_LINE
        + UUID
        + child
        + NEW_LINE
        + UUID
        + RIGHT_PARENTHESIS
        + COMMA
        + NEW_LINE
        + LATERAL
        + lateral_expression
    )


def join_table_function_statement(
    func: str,
    child: str,
    left_cols: List[str],
    right_cols: List[str],
    use_constant_subquery_alias: bool,
    child_uuid: Optional[str] = None,
) -> str:
    LEFT_ALIAS = (
        "T_LEFT"
        if use_constant_subquery_alias
        else random_name_for_temp_object(TempObjectType.TABLE)
    )
    RIGHT_ALIAS = (
        "T_RIGHT"
        if use_constant_subquery_alias
        else random_name_for_temp_object(TempObjectType.TABLE)
    )

    left_cols = [f"{LEFT_ALIAS}.{col}" for col in left_cols]
    right_cols = [f"{RIGHT_ALIAS}.{col}" for col in right_cols]
    select_cols = (COMMA + NEW_LINE + TAB).join(left_cols + right_cols)
    UUID = format_uuid(child_uuid)

    return (
        SELECT
        + NEW_LINE
        + TAB
        + select_cols
        + NEW_LINE
        + FROM
        + LEFT_PARENTHESIS
        + NEW_LINE
        + UUID
        + child
        + NEW_LINE
        + UUID
        + RIGHT_PARENTHESIS
        + AS
        + LEFT_ALIAS
        + NEW_LINE
        + JOIN
        + NEW_LINE
        + table(func)
        + AS
        + RIGHT_ALIAS
    )


def table_function_statement(func: str, operators: Optional[List[str]] = None) -> str:
    if operators is None:
        return project_statement([], table(func))
    return project_statement(operators, table(func))


def case_when_expression(branches: List[Tuple[str, str]], else_value: str) -> str:
    return (
        CASE
        + EMPTY_STRING.join(
            [WHEN + condition + THEN + value for condition, value in branches]
        )
        + ELSE
        + else_value
        + END
    )


def project_statement(
    project: List[str],
    child: str,
    is_distinct: bool = False,
    child_uuid: Optional[str] = None,
) -> str:
    if not project:
        columns = STAR
    else:
        columns = NEW_LINE + TAB + (COMMA + NEW_LINE + TAB).join(project)
    UUID = format_uuid(child_uuid)
    return (
        SELECT
        + f"{DISTINCT if is_distinct else EMPTY_STRING}"
        + columns
        + NEW_LINE
        + FROM
        + LEFT_PARENTHESIS
        + NEW_LINE
        + UUID
        + child
        + NEW_LINE
        + UUID
        + RIGHT_PARENTHESIS
    )


def filter_statement(
    condition: str, is_having: bool, child: str, child_uuid: Optional[str] = None
) -> str:
    if is_having:
        return child + NEW_LINE + HAVING + condition
    else:
        return (
            project_statement([], child, child_uuid=child_uuid)
            + NEW_LINE
            + WHERE
            + condition
        )


def sample_statement(
    child: str,
    probability_fraction: Optional[float] = None,
    row_count: Optional[int] = None,
    child_uuid: Optional[str] = None,
):
    """Generates the sql text for the sample part of the plan being executed"""
    if probability_fraction is not None:
        return (
            project_statement([], child, child_uuid=child_uuid)
            + SAMPLE
            + LEFT_PARENTHESIS
            + str(probability_fraction * 100)
            + RIGHT_PARENTHESIS
        )
    elif row_count is not None:
        return (
            project_statement([], child, child_uuid=child_uuid)
            + SAMPLE
            + LEFT_PARENTHESIS
            + str(row_count)
            + ROWS
            + RIGHT_PARENTHESIS
        )
    # this shouldn't happen because upstream code will validate either probability_fraction or row_count will have a value.
    else:  # pragma: no cover
        raise ValueError(
            "Either 'probability_fraction' or 'row_count' must not be None."
        )


def sample_by_statement(
    child: str, col: str, fractions: Dict[Any, float], child_uuid: Optional[str] = None
) -> str:
    PERCENT_RANK_COL = random_name_for_temp_object(TempObjectType.COLUMN)
    LEFT_ALIAS = "SNOWPARK_LEFT"
    RIGHT_ALIAS = "SNOWPARK_RIGHT"
    UUID = format_uuid(child_uuid)
    child_with_percentage_rank_stmt = (
        SELECT
        + STAR
        + COMMA
        + f"PERCENT_RANK() OVER (PARTITION BY {col} ORDER BY RANDOM()) AS {PERCENT_RANK_COL}"
        + FROM
        + LEFT_PARENTHESIS
        + NEW_LINE
        + UUID
        + child
        + NEW_LINE
        + UUID
        + RIGHT_PARENTHESIS
    )

    # PERCENT_RANK assigns values between 0.0 - 1.0 both inclusive. In our, query we only
    # select values where percent_rank <= value. If value = 0, then we will select one sample
    # unless we update the fractions as done below. This update ensures that, if the original
    # stratified sample fraction = 0, we select 0 rows for the given key.
    updated_fractions = {k: v if v > 0 else -1 for k, v in fractions.items()}
    fraction_flatten_stmt = f"SELECT KEY, VALUE FROM TABLE(FLATTEN(input => parse_json('{json.dumps(updated_fractions)}')))"

    return (
        SELECT
        + f"{LEFT_ALIAS}.* EXCLUDE {PERCENT_RANK_COL}"
        + FROM
        + LEFT_PARENTHESIS
        + NEW_LINE
        + child_with_percentage_rank_stmt
        + NEW_LINE
        + RIGHT_PARENTHESIS
        + AS
        + LEFT_ALIAS
        + JOIN
        + LEFT_PARENTHESIS
        + NEW_LINE
        + fraction_flatten_stmt
        + NEW_LINE
        + RIGHT_PARENTHESIS
        + AS
        + RIGHT_ALIAS
        + ON
        + f"{LEFT_ALIAS}.{col} = {RIGHT_ALIAS}.KEY"
        + WHERE
        + f"{LEFT_ALIAS}.{PERCENT_RANK_COL} <= {RIGHT_ALIAS}.VALUE"
    )


def aggregate_statement(
    grouping_exprs: List[str],
    aggregate_exprs: List[str],
    child: str,
    child_uuid: Optional[str] = None,
) -> str:
    # add limit 1 because aggregate may be on non-aggregate function in a scalar aggregation
    # for example, df.agg(lit(1))
    return project_statement(aggregate_exprs, child, child_uuid=child_uuid) + (
        limit_expression(1)
        if not grouping_exprs
        else (
            NEW_LINE
            + GROUP_BY
            + NEW_LINE
            + TAB
            + (COMMA + NEW_LINE + TAB).join(grouping_exprs)
        )
    )


def sort_statement(
    order: List[str],
    is_order_by_append: bool,
    child: str,
    child_uuid: Optional[str] = None,
) -> str:
    return (
        (
            child
            if is_order_by_append
            else project_statement([], child, child_uuid=child_uuid)
        )
        + NEW_LINE
        + ORDER_BY
        + NEW_LINE
        + TAB
        + (COMMA + NEW_LINE + TAB).join(order)
    )


def range_statement(
    start: int, end: int, step: int, column_name: str, child_uuid: Optional[str] = None
) -> str:
    range = end - start

    if (range > 0 > step) or (range < 0 < step):
        count = 0
    else:
        count = math.ceil(range / step)

    return project_statement(
        [
            LEFT_PARENTHESIS
            + ROW_NUMBER
            + OVER
            + LEFT_PARENTHESIS
            + ORDER_BY
            + SEQ8
            + RIGHT_PARENTHESIS
            + MINUS
            + ONE
            + RIGHT_PARENTHESIS
            + STAR
            + LEFT_PARENTHESIS
            + str(step)
            + RIGHT_PARENTHESIS
            + PLUS
            + LEFT_PARENTHESIS
            + str(start)
            + RIGHT_PARENTHESIS
            + AS
            + column_name
        ],
        table(generator(0 if count < 0 else count)),
        child_uuid=child_uuid,
    )


def schema_query_for_values_statement(output: List[Attribute]) -> str:
    cells = [schema_expression(attr.datatype, attr.nullable) for attr in output]

    query = (
        SELECT
        + COMMA.join([f"{DOLLAR}{i+1}{AS}{attr.name}" for i, attr in enumerate(output)])
        + FROM
        + VALUES
        + LEFT_PARENTHESIS
        + COMMA.join(cells)
        + RIGHT_PARENTHESIS
    )
    return query


def values_statement(output: List[Attribute], data: List[Row]) -> str:
    data_types = [attr.datatype for attr in output]
    names = [quote_name(attr.name) for attr in output]
    rows = []
    for row in data:
        cells = [
            to_sql(value, data_type, from_values_statement=True)
            for value, data_type in zip(row, data_types)
        ]
        rows.append(LEFT_PARENTHESIS + COMMA.join(cells) + RIGHT_PARENTHESIS)

    query = (
        SELECT
        + COMMA.join([f"{DOLLAR}{i+1}{AS}{c}" for i, c in enumerate(names)])
        + FROM
        + VALUES
        + COMMA.join(rows)
    )
    return query


def empty_values_statement(output: List[Attribute]) -> str:
    data = [Row(*[None] * len(output))]
    return filter_statement(UNSAT_FILTER, False, values_statement(output, data))


def set_operator_statement(left: str, right: str, operator: str) -> str:
    return (
        LEFT_PARENTHESIS
        + left
        + RIGHT_PARENTHESIS
        + SPACE
        + operator
        + SPACE
        + LEFT_PARENTHESIS
        + right
        + RIGHT_PARENTHESIS
    )


def left_semi_or_anti_join_statement(
    left: str,
    right: str,
    join_type: JoinType,
    condition: str,
    use_constant_subquery_alias: bool,
) -> str:
    left_alias = (
        "SNOWPARK_LEFT"
        if use_constant_subquery_alias
        else random_name_for_temp_object(TempObjectType.TABLE)
    )
    right_alias = (
        "SNOWPARK_RIGHT"
        if use_constant_subquery_alias
        else random_name_for_temp_object(TempObjectType.TABLE)
    )

    if isinstance(join_type, LeftSemi):
        where_condition = WHERE + EXISTS
    else:
        where_condition = WHERE + NOT + EXISTS

    # this generates sql like "Where a = b"
    join_condition = WHERE + condition

    return (
        SELECT
        + STAR
        + FROM
        + LEFT_PARENTHESIS
        + left
        + RIGHT_PARENTHESIS
        + AS
        + left_alias
        + where_condition
        + LEFT_PARENTHESIS
        + SELECT
        + STAR
        + FROM
        + LEFT_PARENTHESIS
        + right
        + RIGHT_PARENTHESIS
        + AS
        + right_alias
        + f"{join_condition if join_condition else EMPTY_STRING}"
        + RIGHT_PARENTHESIS
    )


def asof_join_statement(
    left: str,
    right: str,
    join_condition: str,
    match_condition: str,
    use_constant_subquery_alias: bool,
):
    left_alias = (
        "SNOWPARK_LEFT"
        if use_constant_subquery_alias
        else random_name_for_temp_object(TempObjectType.TABLE)
    )
    right_alias = (
        "SNOWPARK_RIGHT"
        if use_constant_subquery_alias
        else random_name_for_temp_object(TempObjectType.TABLE)
    )

    on_sql = ON + join_condition if join_condition else EMPTY_STRING

    return (
        SELECT
        + STAR
        + FROM
        + LEFT_PARENTHESIS
        + left
        + RIGHT_PARENTHESIS
        + AS
        + left_alias
        + ASOF
        + JOIN
        + LEFT_PARENTHESIS
        + right
        + RIGHT_PARENTHESIS
        + AS
        + right_alias
        + MATCH_CONDITION
        + LEFT_PARENTHESIS
        + match_condition
        + RIGHT_PARENTHESIS
        + on_sql
    )


def snowflake_supported_join_statement(
    left: str,
    right: str,
    join_type: JoinType,
    condition: str,
    match_condition: str,
    use_constant_subquery_alias: bool,
    left_uuid: Optional[str] = None,
    right_uuid: Optional[str] = None,
) -> str:
    LEFT_UUID = format_uuid(left_uuid)
    RIGHT_UUID = format_uuid(right_uuid)
    left_alias = (
        "SNOWPARK_LEFT"
        if use_constant_subquery_alias
        else random_name_for_temp_object(TempObjectType.TABLE)
    )
    right_alias = (
        "SNOWPARK_RIGHT"
        if use_constant_subquery_alias
        else random_name_for_temp_object(TempObjectType.TABLE)
    )

    if isinstance(join_type, UsingJoin):
        join_sql = join_type.tpe.sql
    elif isinstance(join_type, NaturalJoin):
        join_sql = NATURAL + join_type.tpe.sql
    else:
        join_sql = join_type.sql

    # This generates sql like "USING(a, b)"
    using_condition = None
    if isinstance(join_type, UsingJoin):
        if len(join_type.using_columns) != 0:
            using_condition = (
                USING
                + LEFT_PARENTHESIS
                + COMMA.join(join_type.using_columns)
                + RIGHT_PARENTHESIS
            )

    # This generates sql like "ON a = b"
    join_condition = None
    if condition:
        join_condition = ON + condition

    if using_condition and join_condition:
        raise ValueError("A join should either have using clause or a join condition")

    match_condition = (
        (MATCH_CONDITION + match_condition) if match_condition else EMPTY_STRING
    )

    source = (
        LEFT_PARENTHESIS
        + NEW_LINE
        + LEFT_UUID
        + left
        + NEW_LINE
        + LEFT_UUID
        + RIGHT_PARENTHESIS
        + AS
        + left_alias
        + SPACE
        + NEW_LINE
        + join_sql
        + JOIN
        + NEW_LINE
        + LEFT_PARENTHESIS
        + NEW_LINE
        + RIGHT_UUID
        + right
        + NEW_LINE
        + RIGHT_UUID
        + RIGHT_PARENTHESIS
        + AS
        + right_alias
        + NEW_LINE
        + f"{match_condition if match_condition else EMPTY_STRING}"
        + f"{using_condition if using_condition else EMPTY_STRING}"
        + f"{join_condition if join_condition else EMPTY_STRING}"
    )

    return project_statement([], source)


def join_statement(
    left: str,
    right: str,
    join_type: JoinType,
    join_condition: str,
    match_condition: str,
    use_constant_subquery_alias: bool,
    left_uuid: Optional[str] = None,
    right_uuid: Optional[str] = None,
) -> str:
    if isinstance(join_type, (LeftSemi, LeftAnti)):
        return left_semi_or_anti_join_statement(
            left, right, join_type, join_condition, use_constant_subquery_alias
        )
    if isinstance(join_type, AsOf):
        return asof_join_statement(
            left, right, join_condition, match_condition, use_constant_subquery_alias
        )
    if isinstance(join_type, UsingJoin) and isinstance(
        join_type.tpe, (LeftSemi, LeftAnti)
    ):
        raise ValueError(f"Unexpected using clause in {join_type.tpe} join")
    return snowflake_supported_join_statement(
        left,
        right,
        join_type,
        join_condition,
        match_condition,
        use_constant_subquery_alias,
        left_uuid=left_uuid,
        right_uuid=right_uuid,
    )


def get_comment_sql(comment: Optional[str]) -> str:
    return (
        COMMENT + EQUALS + SINGLE_QUOTE + escape_single_quotes(comment) + SINGLE_QUOTE
        if comment
        else EMPTY_STRING
    )


def create_table_statement(
    table_name: str,
    schema: str,
    replace: bool = False,
    error: bool = True,
    table_type: str = EMPTY_STRING,
    clustering_key: Optional[Iterable[str]] = None,
    comment: Optional[str] = None,
    enable_schema_evolution: Optional[bool] = None,
    data_retention_time: Optional[int] = None,
    max_data_extension_time: Optional[int] = None,
    change_tracking: Optional[bool] = None,
    copy_grants: bool = False,
    *,
    use_scoped_temp_objects: bool = False,
    is_generated: bool = False,
    iceberg_config: Optional[dict] = None,
) -> str:
    cluster_by_clause = (
        (CLUSTER_BY + LEFT_PARENTHESIS + COMMA.join(clustering_key) + RIGHT_PARENTHESIS)
        if clustering_key
        else EMPTY_STRING
    )
    comment_sql = get_comment_sql(comment)
    options = {
        ENABLE_SCHEMA_EVOLUTION: enable_schema_evolution,
        DATA_RETENTION_TIME_IN_DAYS: data_retention_time,
        MAX_DATA_EXTENSION_TIME_IN_DAYS: max_data_extension_time,
        CHANGE_TRACKING: change_tracking,
    }

    iceberg_config = validate_iceberg_config(iceberg_config)
    options.update(iceberg_config)
    options_statement = get_options_statement(options)

    return (
        f"{CREATE}{(OR + REPLACE) if replace else EMPTY_STRING}"
        f" {(get_temp_type_for_object(use_scoped_temp_objects, is_generated) if table_type.lower() in TEMPORARY_STRING_SET else table_type).upper()} "
        f"{ICEBERG if iceberg_config else EMPTY_STRING}{TABLE}{table_name}{(IF + NOT + EXISTS) if not replace and not error else EMPTY_STRING}"
        f"{LEFT_PARENTHESIS}{schema}{RIGHT_PARENTHESIS}{cluster_by_clause}"
        f"{options_statement}{COPY_GRANTS if copy_grants else EMPTY_STRING}{comment_sql}"
    )


def insert_into_statement(
    table_name: str,
    child: str,
    column_names: Optional[Iterable[str]] = None,
    overwrite: bool = False,
) -> str:
    table_columns = f"({COMMA.join(column_names)})" if column_names else EMPTY_STRING
    if is_sql_select_statement(child):
        return f"{INSERT}{OVERWRITE if overwrite else EMPTY_STRING}{INTO}{table_name}{table_columns}{SPACE}{child}"
    return f"{INSERT}{INTO}{table_name}{table_columns}{project_statement([], child)}"


def batch_insert_into_statement(
    table_name: str, column_names: List[str], paramstyle: Optional[str]
) -> str:
    num_cols = len(column_names)
    # paramstyle is initialized as None is stored proc environment
    paramstyle = paramstyle.lower() if paramstyle else "qmark"
    supported_paramstyle = ["qmark", "numeric", "format", "pyformat"]

    if paramstyle == "qmark":
        placeholder_marks = [QUESTION_MARK] * num_cols
    elif paramstyle == "numeric":
        placeholder_marks = [f"{SINGLE_COLON}{i+1}" for i in range(num_cols)]
    elif paramstyle in ("format", "pyformat"):
        placeholder_marks = [PERCENT_S] * num_cols
    else:
        raise ValueError(
            f"'{paramstyle}' is not a recognized paramstyle. "
            f"Supported values are: {', '.join(supported_paramstyle)}"
        )

    return (
        f"{INSERT}{INTO}{table_name}"
        f"{LEFT_PARENTHESIS}{COMMA.join(column_names)}{RIGHT_PARENTHESIS}"
        f"{VALUES}{LEFT_PARENTHESIS}"
        f"{COMMA.join(placeholder_marks)}{RIGHT_PARENTHESIS}"
    )


def create_table_as_select_statement(
    table_name: str,
    child: str,
    column_definition: Optional[str],
    replace: bool = False,
    error: bool = True,
    table_type: str = EMPTY_STRING,
    clustering_key: Optional[Iterable[str]] = None,
    comment: Optional[str] = None,
    enable_schema_evolution: Optional[bool] = None,
    data_retention_time: Optional[int] = None,
    max_data_extension_time: Optional[int] = None,
    change_tracking: Optional[bool] = None,
    copy_grants: bool = False,
    iceberg_config: Optional[dict] = None,
    *,
    use_scoped_temp_objects: bool = False,
    is_generated: bool = False,
) -> str:
    column_definition_sql = (
        f"{LEFT_PARENTHESIS}{column_definition}{RIGHT_PARENTHESIS}"
        if column_definition
        else EMPTY_STRING
    )
    cluster_by_clause = (
        (CLUSTER_BY + LEFT_PARENTHESIS + COMMA.join(clustering_key) + RIGHT_PARENTHESIS)
        if clustering_key
        else EMPTY_STRING
    )
    comment_sql = get_comment_sql(comment)
    options = {
        ENABLE_SCHEMA_EVOLUTION: enable_schema_evolution,
        DATA_RETENTION_TIME_IN_DAYS: data_retention_time,
        MAX_DATA_EXTENSION_TIME_IN_DAYS: max_data_extension_time,
        CHANGE_TRACKING: change_tracking,
    }
    iceberg_config = validate_iceberg_config(iceberg_config)
    options.update(iceberg_config)
    options_statement = get_options_statement(options)
    return (
        f"{CREATE}{OR + REPLACE if replace else EMPTY_STRING}"
        f" {(get_temp_type_for_object(use_scoped_temp_objects, is_generated) if table_type.lower() in TEMPORARY_STRING_SET else table_type).upper()} "
        f"{ICEBERG if iceberg_config else EMPTY_STRING}{TABLE}"
        f"{IF + NOT + EXISTS if not replace and not error else EMPTY_STRING} "
        f"{table_name}{column_definition_sql}{cluster_by_clause}{options_statement}"
        f"{COPY_GRANTS if copy_grants else EMPTY_STRING}{comment_sql} {AS}{project_statement([], child)}"
    )


def limit_statement(
    row_count: str,
    offset: str,
    child: str,
    on_top_of_order_by: bool,
    child_uuid: Optional[str] = None,
) -> str:
    return (
        f"{child if on_top_of_order_by else project_statement([], child, child_uuid=child_uuid)}"
        + LIMIT
        + row_count
        + OFFSET
        + offset
    )


def schema_cast_seq(schema: List[Attribute]) -> List[str]:
    res = []
    for index, attr in enumerate(schema):
        name = (
            DOLLAR
            + str(index + 1)
            + DOUBLE_COLON
            + convert_sp_to_sf_type(attr.datatype)
        )
        res.append(name + AS + quote_name(attr.name))
    return res


def schema_cast_named(schema: List[Tuple[str, str]]) -> List[str]:
    return [s[0] + AS + quote_name_without_upper_casing(s[1]) for s in schema]


def create_file_format_statement(
    format_name: str,
    file_type: str,
    options: Dict,
    temp: bool,
    if_not_exist: bool,
    *,
    use_scoped_temp_objects: bool = False,
    is_generated: bool = False,
) -> str:
    type_str = TYPE + EQUALS + file_type + SPACE
    options_str = (
        type_str if "TYPE" not in options else EMPTY_STRING
    ) + get_options_statement(options)
    return (
        CREATE
        + (
            get_temp_type_for_object(use_scoped_temp_objects, is_generated)
            if temp
            else EMPTY_STRING
        )
        + FILE
        + FORMAT
        + (IF + NOT + EXISTS if if_not_exist else EMPTY_STRING)
        + format_name
        + options_str
    )


def infer_schema_statement(
    path: str, file_format_name: str, options: Optional[Dict[str, str]] = None
) -> str:
    return (
        SELECT
        + STAR
        + FROM
        + TABLE
        + LEFT_PARENTHESIS
        + INFER_SCHEMA
        + LEFT_PARENTHESIS
        + LOCATION
        + RIGHT_ARROW
        + (path if is_single_quoted(path) else SINGLE_QUOTE + path + SINGLE_QUOTE)
        + COMMA
        + FILE_FORMAT
        + RIGHT_ARROW
        + SINGLE_QUOTE
        + file_format_name
        + SINGLE_QUOTE
        + (
            ", "
            + ", ".join(
                f"{k} => {convert_value_to_sql_option(v)}" for k, v in options.items()
            )
            if options
            else ""
        )
        + RIGHT_PARENTHESIS
        + RIGHT_PARENTHESIS
    )


def file_operation_statement(
    command: str, file_name: str, stage_location: str, options: Dict[str, str]
) -> str:
    if command.lower() == "put":
        return f"{PUT}{file_name}{SPACE}{stage_location}{SPACE}{get_options_statement(options)}"
    if command.lower() == "get":
        return f"{GET}{stage_location}{SPACE}{file_name}{SPACE}{get_options_statement(options)}"
    raise ValueError(f"Unsupported file operation type {command}")


def convert_value_to_sql_option(
    value: Optional[Union[str, bool, int, float, list, tuple]]
) -> str:
    if isinstance(value, str):
        if len(value) > 1 and is_single_quoted(value):
            return value
        else:
            value = value.replace(
                "'", "''"
            )  # escape single quotes before adding a pair of quotes
            return f"'{value}'"
    else:
        if isinstance(value, (list, tuple)):
            # Snowflake sql uses round brackets for options that are lists
            return f"({', '.join(convert_value_to_sql_option(val) for val in value)})"
        return str(value)


def get_options_statement(options: Dict[str, Any]) -> str:
    return (
        SPACE
        + SPACE.join(
            f"{k}{EQUALS}{convert_value_to_sql_option(v)}"
            for k, v in options.items()
            if v is not None
        )
        + SPACE
    )


def drop_file_format_if_exists_statement(format_name: str) -> str:
    return DROP + FILE + FORMAT + IF + EXISTS + format_name


def select_from_path_with_format_statement(
    project: List[str], path: str, format_name: str, pattern: Optional[str]
) -> str:
    select_statement = (
        SELECT + (STAR if not project else COMMA.join(project)) + FROM + path
    )
    format_statement = (
        (FILE_FORMAT + RIGHT_ARROW + single_quote(format_name))
        if format_name
        else EMPTY_STRING
    )
    pattern_statement = (
        (PATTERN + RIGHT_ARROW + single_quote(pattern)) if pattern else EMPTY_STRING
    )

    return (
        select_statement
        + (
            LEFT_PARENTHESIS
            + (format_statement if format_statement else EMPTY_STRING)
            + (COMMA if format_statement and pattern_statement else EMPTY_STRING)
            + (pattern_statement if pattern_statement else EMPTY_STRING)
            + RIGHT_PARENTHESIS
        )
        if format_statement or pattern_statement
        else EMPTY_STRING
    )


def unary_expression(child: str, sql_operator: str, operator_first: bool) -> str:
    return (
        (sql_operator + SPACE + child)
        if operator_first
        else (child + SPACE + sql_operator)
    )


def window_expression(window_function: str, window_spec: str) -> str:
    return window_function + OVER + LEFT_PARENTHESIS + window_spec + RIGHT_PARENTHESIS


def window_spec_expression(
    partition_exprs: List[str], order_exprs: List[str], frame_spec: str
) -> str:
    return f"{partition_spec(partition_exprs)}{SPACE}{order_by_spec(order_exprs)}{SPACE}{frame_spec}"


def specified_window_frame_expression(frame_type: str, lower: str, upper: str) -> str:
    return SPACE + frame_type + BETWEEN + lower + AND + upper + SPACE


def window_frame_boundary_expression(offset: str, is_following: bool) -> str:
    return offset + (FOLLOWING if is_following else PRECEDING)


def rank_related_function_expression(
    func_name: str, expr: str, offset: int, default: Optional[str], ignore_nulls: bool
) -> str:
    return (
        func_name
        + LEFT_PARENTHESIS
        + expr
        + (COMMA + str(offset) if offset is not None else EMPTY_STRING)
        + (COMMA + default if default else EMPTY_STRING)
        + RIGHT_PARENTHESIS
        + (IGNORE_NULLS if ignore_nulls else EMPTY_STRING)
    )


def cast_expression(
    child: str,
    datatype: DataType,
    try_: bool = False,
    is_rename: bool = False,
    is_add: bool = False,
) -> str:
    return (
        (TRY_CAST if try_ else CAST)
        + LEFT_PARENTHESIS
        + child
        + AS
        + convert_sp_to_sf_type(datatype)
        + (RENAME_FIELDS if is_rename else "")
        + (ADD_FIELDS if is_add else "")
        + RIGHT_PARENTHESIS
    )


def order_expression(name: str, direction: str, null_ordering: str) -> str:
    return name + SPACE + direction + SPACE + null_ordering


def create_or_replace_view_statement(
    name: str, child: str, is_temp: bool, comment: Optional[str], replace: bool
) -> str:
    comment_sql = get_comment_sql(comment)
    return (
        CREATE
        + f"{OR + REPLACE if replace else EMPTY_STRING}"
        + f"{TEMPORARY if is_temp else EMPTY_STRING}"
        + VIEW
        + name
        + comment_sql
        + AS
        + project_statement([], child)
    )


def create_or_replace_dynamic_table_statement(
    name: str,
    warehouse: str,
    lag: str,
    comment: Optional[str],
    replace: bool,
    if_not_exists: bool,
    refresh_mode: Optional[str],
    initialize: Optional[str],
    clustering_keys: Iterable[str],
    is_transient: bool,
    data_retention_time: Optional[int],
    max_data_extension_time: Optional[int],
    child: str,
    iceberg_config: Optional[dict] = None,
) -> str:
    cluster_by_sql = (
        f"{CLUSTER_BY}{LEFT_PARENTHESIS}{COMMA.join(clustering_keys)}{RIGHT_PARENTHESIS}"
        if clustering_keys
        else EMPTY_STRING
    )
    comment_sql = get_comment_sql(comment)
    refresh_and_initialize_options = get_options_statement(
        {
            REFRESH_MODE: refresh_mode,
            INITIALIZE: initialize,
        }
    )
    data_retention_options = get_options_statement(
        {
            DATA_RETENTION_TIME_IN_DAYS: data_retention_time,
            MAX_DATA_EXTENSION_TIME_IN_DAYS: max_data_extension_time,
        }
    )

    iceberg_options = get_options_statement(
        validate_iceberg_config(iceberg_config)
    ).strip()

    return (
        f"{CREATE}{OR + REPLACE if replace else EMPTY_STRING}{TRANSIENT if is_transient else EMPTY_STRING}"
        f"{DYNAMIC}{ICEBERG if iceberg_config else EMPTY_STRING}{TABLE}"
        f"{IF + NOT + EXISTS if if_not_exists else EMPTY_STRING}{name}{LAG}{EQUALS}"
        f"{convert_value_to_sql_option(lag)}{WAREHOUSE}{EQUALS}{warehouse}"
        f"{refresh_and_initialize_options}{cluster_by_sql}{data_retention_options}{iceberg_options}"
        f"{comment_sql}{AS}{project_statement([], child)}"
    )


def pivot_statement(
    pivot_column: str,
    pivot_values: Optional[Union[str, List[str]]],
    aggregate: str,
    default_on_null: Optional[str],
    child: str,
    should_alias_column_with_agg: bool,
    child_uuid: Optional[str] = None,
) -> str:
    select_str = STAR
    UUID = format_uuid(child_uuid)
    if isinstance(pivot_values, str):
        # The subexpression in this case already includes parenthesis.
        values_str = pivot_values
    else:
        values_str = (
            LEFT_PARENTHESIS
            + (ANY if pivot_values is None else COMMA.join(pivot_values))
            + RIGHT_PARENTHESIS
        )
        if pivot_values is not None and should_alias_column_with_agg:
            quoted_names = [quote_name(value) for value in pivot_values]
            # unwrap_single_quote on the value to match the output closer to what spark generates
            aliased_names = [
                quote_name(f"{unwrap_single_quote(value)}_{aggregate}")
                for value in pivot_values
            ]
            aliased_string = [
                f"{quoted_name}{AS}{aliased_name}"
                for aliased_name, quoted_name in zip(aliased_names, quoted_names)
            ]
            exclude_str = COMMA.join(quoted_names)
            aliased_str = COMMA.join(aliased_string)
            select_str = f"{STAR}{EXCLUDE}{LEFT_PARENTHESIS}{exclude_str}{RIGHT_PARENTHESIS}, {aliased_str}"

    return (
        SELECT
        + select_str
        + FROM
        + LEFT_PARENTHESIS
        + NEW_LINE
        + UUID
        + child
        + NEW_LINE
        + UUID
        + RIGHT_PARENTHESIS
        + NEW_LINE
        + PIVOT
        + LEFT_PARENTHESIS
        + NEW_LINE
        + TAB
        + aggregate
        + FOR
        + pivot_column
        + IN
        + values_str
        + (
            (DEFAULT_ON_NULL + LEFT_PARENTHESIS + default_on_null + RIGHT_PARENTHESIS)
            if default_on_null
            else EMPTY_STRING
        )
        + NEW_LINE
        + RIGHT_PARENTHESIS
    )


def unpivot_statement(
    value_column: str,
    name_column: str,
    column_list: List[str],
    include_nulls: bool,
    child: str,
    child_uuid: Optional[str] = None,
) -> str:
    UUID = format_uuid(child_uuid)
    return (
        SELECT
        + STAR
        + FROM
        + LEFT_PARENTHESIS
        + NEW_LINE
        + UUID
        + child
        + NEW_LINE
        + UUID
        + RIGHT_PARENTHESIS
        + NEW_LINE
        + UNPIVOT
        + (INCLUDE_NULLS if include_nulls else EMPTY_STRING)
        + LEFT_PARENTHESIS
        + NEW_LINE
        + TAB
        + value_column
        + FOR
        + name_column
        + IN
        + LEFT_PARENTHESIS
        + COMMA.join(column_list)
        + RIGHT_PARENTHESIS
        + NEW_LINE
        + RIGHT_PARENTHESIS
    )


def rename_statement(column_map: Dict[str, str], child: str) -> str:
    return (
        SELECT
        + STAR
        + RENAME
        + LEFT_PARENTHESIS
        + NEW_LINE
        + TAB
        + COMMA.join([f"{before}{AS}{after}" for before, after in column_map.items()])
        + NEW_LINE
        + RIGHT_PARENTHESIS
        + FROM
        + LEFT_PARENTHESIS
        + NEW_LINE
        + child
        + NEW_LINE
        + RIGHT_PARENTHESIS
    )


def copy_into_table(
    table_name: str,
    file_path: str,
    file_format_type: str,
    format_type_options: Dict[str, Any],
    copy_options: Dict[str, Any],
    pattern: Optional[str],
    *,
    files: Optional[str] = None,
    validation_mode: Optional[str] = None,
    column_names: Optional[List[str]] = None,
    transformations: Optional[List[str]] = None,
) -> str:
    """
    /* Standard data load */
    COPY INTO [<namespace>.]<table_name>
         FROM { internalStage | externalStage | externalLocation }
    [ FILES = ( '<file_name>' [ , '<file_name>' ] [ , ... ] ) ]
    [ PATTERN = '<regex_pattern>' ]
    [ FILE_FORMAT = ( { FORMAT_NAME = '[<namespace>.]<file_format_name>' |
                        TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ]
    [ copyOptions ]
    [ VALIDATION_MODE = RETURN_<n>_ROWS | RETURN_ERRORS | RETURN_ALL_ERRORS ]

    /* Data load with transformation */
    COPY INTO [<namespace>.]<table_name> [ ( <col_name> [ , <col_name> ... ] ) ]
         FROM ( SELECT [<alias>.]$<file_col_num>[.<element>] [ , [<alias>.]$<file_col_num>[.<element>] ... ]
                FROM { internalStage | externalStage } )
    [ FILES = ( '<file_name>' [ , '<file_name>' ] [ , ... ] ) ]
    [ PATTERN = '<regex_pattern>' ]
    [ FILE_FORMAT = ( { FORMAT_NAME = '[<namespace>.]<file_format_name>' |
                        TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ]
    [ copyOptions ]
    """
    column_str = (
        LEFT_PARENTHESIS + COMMA.join(column_names) + RIGHT_PARENTHESIS
        if column_names
        else EMPTY_STRING
    )
    from_str = (
        LEFT_PARENTHESIS
        + SELECT
        + COMMA.join(transformations)
        + FROM
        + file_path
        + RIGHT_PARENTHESIS
        if transformations
        else file_path
    )
    files_str = (
        FILES
        + EQUALS
        + LEFT_PARENTHESIS
        + COMMA.join([SINGLE_QUOTE + f + SINGLE_QUOTE for f in files])
        + RIGHT_PARENTHESIS
        if files
        else EMPTY_STRING
    )
    validation_str = (
        f"{VALIDATION_MODE}{EQUALS}{validation_mode}"
        if validation_mode
        else EMPTY_STRING
    )
    ftostr = get_file_format_spec(file_format_type, format_type_options)

    if copy_options:
        costr = SPACE + get_options_statement(copy_options) + SPACE
    else:
        costr = EMPTY_STRING

    return (
        COPY
        + INTO
        + table_name
        + column_str
        + FROM
        + from_str
        + (
            NEW_LINE + PATTERN + EQUALS + single_quote(pattern)
            if pattern
            else EMPTY_STRING
        )
        + files_str
        + ftostr
        + costr
        + validation_str
    )


def copy_into_location(
    query: str,
    stage_location: str,
    partition_by: Optional[str] = None,
    file_format_name: Optional[str] = None,
    file_format_type: Optional[str] = None,
    format_type_options: Optional[Dict[str, Any]] = None,
    header: bool = False,
    **copy_options: Any,
) -> str:
    """
    COPY INTO { internalStage | externalStage | externalLocation }
         FROM { [<namespace>.]<table_name> | ( <query> ) }
    [ PARTITION BY <expr> ]
    [ FILE_FORMAT = ( { FORMAT_NAME = '[<namespace>.]<file_format_name>' |
                        TYPE = { CSV | JSON | PARQUET } [ formatTypeOptions ] } ) ]
    [ copyOptions ]
    [ HEADER ]
    """
    partition_by_clause = (
        (PARTITION_BY + partition_by) if partition_by else EMPTY_STRING
    )
    format_name_clause = (
        (FORMAT_NAME + EQUALS + file_format_name) if file_format_name else EMPTY_STRING
    )
    file_type_clause = (
        (TYPE + EQUALS + file_format_type) if file_format_type else EMPTY_STRING
    )
    format_type_options_clause = (
        get_options_statement(format_type_options)
        if format_type_options
        else EMPTY_STRING
    )
    file_format_clause = (
        (
            FILE_FORMAT
            + EQUALS
            + LEFT_PARENTHESIS
            + (
                format_name_clause
                + SPACE
                + file_type_clause
                + SPACE
                + format_type_options_clause
            )
            + RIGHT_PARENTHESIS
        )
        if format_name_clause or file_type_clause or format_type_options_clause
        else EMPTY_STRING
    )
    copy_options_clause = (
        get_options_statement(copy_options) if copy_options else EMPTY_STRING
    )
    header_clause = f"{HEADER}{EQUALS}{header}" if header is not None else EMPTY_STRING
    return (
        COPY
        + INTO
        + stage_location
        + FROM
        + LEFT_PARENTHESIS
        + query
        + RIGHT_PARENTHESIS
        + partition_by_clause
        + file_format_clause
        + SPACE
        + copy_options_clause
        + SPACE
        + header_clause
    )


def update_statement(
    table_name: str,
    assignments: Dict[str, str],
    condition: Optional[str],
    source_data: Optional[str],
) -> str:
    return (
        UPDATE
        + table_name
        + SET
        + COMMA.join([k + EQUALS + v for k, v in assignments.items()])
        + (
            (FROM + LEFT_PARENTHESIS + source_data + RIGHT_PARENTHESIS)
            if source_data
            else EMPTY_STRING
        )
        + ((WHERE + condition) if condition else EMPTY_STRING)
    )


def delete_statement(
    table_name: str, condition: Optional[str], source_data: Optional[str]
) -> str:
    return (
        DELETE
        + FROM
        + table_name
        + (
            (USING + LEFT_PARENTHESIS + source_data + RIGHT_PARENTHESIS)
            if source_data
            else EMPTY_STRING
        )
        + ((WHERE + condition) if condition else EMPTY_STRING)
    )


def insert_merge_statement(
    condition: Optional[str], keys: List[str], values: List[str]
) -> str:
    return (
        WHEN
        + NOT
        + MATCHED
        + ((AND + condition) if condition else EMPTY_STRING)
        + THEN
        + INSERT
        + (
            (LEFT_PARENTHESIS + COMMA.join(keys) + RIGHT_PARENTHESIS)
            if keys
            else EMPTY_STRING
        )
        + VALUES
        + LEFT_PARENTHESIS
        + COMMA.join(values)
        + RIGHT_PARENTHESIS
    )


def update_merge_statement(condition: Optional[str], assignment: Dict[str, str]) -> str:
    return (
        WHEN
        + MATCHED
        + ((AND + condition) if condition else EMPTY_STRING)
        + THEN
        + UPDATE
        + SET
        + COMMA.join([k + EQUALS + v for k, v in assignment.items()])
    )


def delete_merge_statement(condition: Optional[str]) -> str:
    return (
        WHEN
        + MATCHED
        + ((AND + condition) if condition else EMPTY_STRING)
        + THEN
        + DELETE
    )


def merge_statement(
    table_name: str, source: str, join_expr: str, clauses: List[str]
) -> str:
    return (
        MERGE
        + INTO
        + table_name
        + USING
        + LEFT_PARENTHESIS
        + NEW_LINE
        + TAB
        + source
        + NEW_LINE
        + RIGHT_PARENTHESIS
        + NEW_LINE
        + ON
        + join_expr
        + NEW_LINE
        + EMPTY_STRING.join(clauses)
    )


def drop_table_if_exists_statement(table_name: str) -> str:
    return DROP + TABLE + IF + EXISTS + table_name


def attribute_to_schema_string(attributes: List[Attribute]) -> str:
    return COMMA.join(
        attr.name
        + SPACE
        + convert_sp_to_sf_type(attr.datatype, attr.nullable)
        + (NOT_NULL if not attr.nullable else EMPTY_STRING)
        for attr in attributes
    )


def schema_value_statement(output: List[Attribute]) -> str:
    return SELECT + COMMA.join(
        [
            schema_expression(attr.datatype, attr.nullable) + AS + quote_name(attr.name)
            for attr in output
        ]
    )


def list_agg(col: str, delimiter: str, is_distinct: bool) -> str:
    return (
        LISTAGG
        + LEFT_PARENTHESIS
        + f"{DISTINCT if is_distinct else EMPTY_STRING}"
        + col
        + COMMA
        + delimiter
        + RIGHT_PARENTHESIS
    )


def column_sum(cols: List[str]) -> str:
    return LEFT_PARENTHESIS + PLUS.join(cols) + RIGHT_PARENTHESIS


def generator(row_count: int) -> str:
    return (
        GENERATOR
        + LEFT_PARENTHESIS
        + ROW_COUNT
        + RIGHT_ARROW
        + str(row_count)
        + RIGHT_PARENTHESIS
    )


def table(content: str) -> str:
    return TABLE + LEFT_PARENTHESIS + content + RIGHT_PARENTHESIS


def single_quote(value: str) -> str:
    if value.startswith(SINGLE_QUOTE) and value.endswith(SINGLE_QUOTE):
        return value
    else:
        return SINGLE_QUOTE + value + SINGLE_QUOTE


def quote_name_without_upper_casing(name: str) -> str:
    return DOUBLE_QUOTE + escape_quotes(name) + DOUBLE_QUOTE


def unquote_if_quoted(string):
    return string[1:-1].replace('""', '"') if ALREADY_QUOTED.match(string) else string


# Most integer types map to number(38,0)
# https://docs.snowflake.com/en/sql-reference/
# data-types-numeric.html#int-integer-bigint-smallint-tinyint-byteint
def number(precision: int = 38, scale: int = 0) -> str:
    return (
        NUMBER
        + LEFT_PARENTHESIS
        + str(precision)
        + COMMA
        + str(scale)
        + RIGHT_PARENTHESIS
    )


def string(length: Optional[int] = None) -> str:
    if length:
        return STRING + LEFT_PARENTHESIS + str(length) + RIGHT_PARENTHESIS
    return STRING.strip()


def get_file_format_spec(
    file_format_type: str, format_type_options: Dict[str, Any]
) -> str:
    file_format_name = format_type_options.get("FORMAT_NAME")
    file_format_str = FILE_FORMAT + EQUALS + LEFT_PARENTHESIS
    if file_format_name is None:
        file_format_str += TYPE + EQUALS + file_format_type
        if format_type_options:
            file_format_str += (
                SPACE + get_options_statement(format_type_options) + SPACE
            )
    else:
        file_format_str += FORMAT_NAME + EQUALS + file_format_name
    file_format_str += RIGHT_PARENTHESIS
    return file_format_str


def cte_statement(queries: List[str], table_names: List[str]) -> str:
    result = COMMA.join(
        f"{table_name}{AS}{LEFT_PARENTHESIS}{query}{RIGHT_PARENTHESIS}"
        for query, table_name in zip(queries, table_names)
    )
    return f"{WITH}{result}"


def write_arrow(
    cursor: SnowflakeCursor,
    table: "pyarrow.Table",
    table_name: str,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    chunk_size: Optional[int] = None,
    compression: str = "gzip",
    on_error: str = "abort_statement",
    parallel: int = 4,
    quote_identifiers: bool = True,
    auto_create_table: bool = False,
    overwrite: bool = False,
    table_type: Literal["", "temp", "temporary", "transient"] = "",
    use_logical_type: Optional[bool] = None,
    use_scoped_temp_object: bool = False,
    **kwargs: Any,
) -> Tuple[
    bool,
    int,
    int,
    Sequence[
        Tuple[
            str,
            str,
            int,
            int,
            int,
            int,
            Optional[str],
            Optional[int],
            Optional[int],
            Optional[str],
        ]
    ],
]:
    """Writes a pyarrow.Table to a Snowflake table.

    The pyarrow Table is written out to temporary files, uploaded to a temporary stage, and then copied into the final location.

    Returns whether all files were ingested correctly, number of chunks uploaded, and number of rows ingested
    with all of the COPY INTO command's output for debugging purposes.

    Args:
        cursor: Snowflake connector cursor used to execute queries.
        table: The pyarrow Table that is written.
        table_name: Table name where we want to insert into.
        database: Database schema and table is in, if not provided the default one will be used (Default value = None).
        schema: Schema table is in, if not provided the default one will be used (Default value = None).
        chunk_size: Number of elements to be inserted in each batch, if not provided all elements will be dumped
            (Default value = None).
        compression: The compression used on the Parquet files, can only be gzip, or snappy. Gzip gives a
            better compression, while snappy is faster. Use whichever is more appropriate (Default value = 'gzip').
        on_error: Action to take when COPY INTO statements fail, default follows documentation at:
            https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions
            (Default value = 'abort_statement').
        parallel: Number of threads to be used when uploading chunks, default follows documentation at:
            https://docs.snowflake.com/en/sql-reference/sql/put.html#optional-parameters (Default value = 4).
        quote_identifiers: By default, identifiers, specifically database, schema, table and column names
            (from df.columns) will be quoted. If set to False, identifiers are passed on to Snowflake without quoting.
            I.e. identifiers will be coerced to uppercase by Snowflake.  (Default value = True)
        auto_create_table: When true, will automatically create a table with corresponding columns for each column in
            the passed in DataFrame. The table will not be created if it already exists
        table_type: The table type of to-be-created table. The supported table types include ``temp``/``temporary``
            and ``transient``. Empty means permanent table as per SQL convention.
        use_logical_type: Boolean that specifies whether to use Parquet logical types. With this file format option,
            Snowflake can interpret Parquet logical types during data loading. To enable Parquet logical types,
            set use_logical_type as True. Set to None to use Snowflakes default. For more information, see:
            https://docs.snowflake.com/en/sql-reference/sql/create-file-format
    """
    # SNOW-1904593: This function mostly copies the functionality of snowflake.connector.pandas_utils.write_pandas.
    # It should be pushed down into the connector, but would require a minimum required version bump.
    import pyarrow.parquet  # type: ignore

    if database is not None and schema is None:
        raise ProgrammingError(
            "Schema has to be provided to write_arrow when a database is provided"
        )
    compression_map = {"gzip": "auto", "snappy": "snappy", "none": "none"}
    if compression not in compression_map.keys():
        raise ProgrammingError(
            f"Invalid compression '{compression}', only acceptable values are: {compression_map.keys()}"
        )

    if table_type and table_type.lower() not in ["temp", "temporary", "transient"]:
        raise ProgrammingError(
            "Unsupported table type. Expected table types: temp/temporary, transient"
        )

    if chunk_size is None:
        chunk_size = len(table)

    if use_logical_type is None:
        sql_use_logical_type = ""
    elif use_logical_type:
        sql_use_logical_type = " USE_LOGICAL_TYPE = TRUE"
    else:
        sql_use_logical_type = " USE_LOGICAL_TYPE = FALSE"

    stage_location = _create_temp_stage(
        cursor,
        database,
        schema,
        quote_identifiers,
        compression,
        auto_create_table,
        overwrite,
        use_scoped_temp_object,
    )
    with tempfile.TemporaryDirectory() as tmp_folder:
        for file_number, offset in enumerate(range(0, len(table), chunk_size)):
            # write chunk to disk
            chunk_path = os.path.join(tmp_folder, f"{table_name}_{file_number}.parquet")
            pyarrow.parquet.write_table(
                table.slice(offset=offset, length=chunk_size),
                chunk_path,
                **kwargs,
            )
            # upload chunk
            upload_sql = (
                "PUT /* Python:snowflake.snowpark._internal.analyzer.analyzer_utils.write_arrow() */ "
                "'file://{path}' @{stage_location} PARALLEL={parallel}"
            ).format(
                path=chunk_path.replace("\\", "\\\\").replace("'", "\\'"),
                stage_location=stage_location,
                parallel=parallel,
            )
            cursor.execute(upload_sql, _is_internal=True)
            # Remove chunk file
            os.remove(chunk_path)

    if quote_identifiers:
        quote = '"'
        snowflake_column_names = [str(c).replace('"', '""') for c in table.schema.names]
    else:
        quote = ""
        snowflake_column_names = list(table.schema.names)
    columns = quote + f"{quote},{quote}".join(snowflake_column_names) + quote

    def drop_object(name: str, object_type: str) -> None:
        drop_sql = f"DROP {object_type.upper()} IF EXISTS {name} /* Python:snowflake.snowpark._internal.analyzer.analyzer_utils.write_arrow() */"
        cursor.execute(drop_sql, _is_internal=True)

    if auto_create_table or overwrite:
        file_format_location = _create_temp_file_format(
            cursor,
            database,
            schema,
            quote_identifiers,
            compression_map[compression],
            sql_use_logical_type,
            use_scoped_temp_object,
        )
        infer_schema_sql = f"SELECT COLUMN_NAME, TYPE FROM table(infer_schema(location=>'@{stage_location}', file_format=>'{file_format_location}'))"
        infer_result = cursor.execute(infer_schema_sql, _is_internal=True)
        assert infer_result is not None
        column_type_mapping = dict(infer_result.fetchall())  # pyright: ignore

        target_table_location = build_location_helper(
            database,
            schema,
            (
                random_name_for_temp_object(TempObjectType.TABLE)
                if (overwrite and auto_create_table)
                else table_name
            ),
            quote_identifiers,
        )

        parquet_columns = "$1:" + ",$1:".join(
            f"{quote}{snowflake_col}{quote}::{column_type_mapping[col]}"
            for snowflake_col, col in zip(snowflake_column_names, table.schema.names)
        )

        if auto_create_table:
            create_table_columns = ", ".join(
                [
                    f"{quote}{snowflake_col}{quote} {column_type_mapping[col]}"
                    for snowflake_col, col in zip(
                        snowflake_column_names, table.schema.names
                    )
                ]
            )
            create_table_sql = (
                f"CREATE {table_type.upper()} TABLE IF NOT EXISTS {target_table_location} "
                f"({create_table_columns})"
                f" /* Python:snowflake.snowpark._internal.analyzer.analyzer_utils.write_arrow() */ "
            )
            cursor.execute(create_table_sql, _is_internal=True)
    else:
        target_table_location = build_location_helper(
            database=database,
            schema=schema,
            name=table_name,
            quote_identifiers=quote_identifiers,
        )
        parquet_columns = "$1:" + ",$1:".join(
            f"{quote}{snowflake_col}{quote}" for snowflake_col in snowflake_column_names
        )

    try:
        if overwrite and (not auto_create_table):
            truncate_sql = f"TRUNCATE TABLE {target_table_location} /* Python:snowflake.snowpark._internal.analyzer.analyzer_utils.write_arrow() */"
            cursor.execute(truncate_sql, _is_internal=True)

        copy_into_sql = (
            f"COPY INTO {target_table_location} /* Python:snowflake.snowpark._internal.analyzer.analyzer_utils.write_arrow() */ "
            f"({columns}) "
            f"FROM (SELECT {parquet_columns} FROM @{stage_location}) "
            f"FILE_FORMAT=("
            f"TYPE=PARQUET "
            f"COMPRESSION={compression_map[compression]}"
            f"{' BINARY_AS_TEXT=FALSE' if auto_create_table or overwrite else ''}"
            f"{sql_use_logical_type}"
            f") "
            f"PURGE=TRUE ON_ERROR={on_error}"
        )
        copy_result = cursor.execute(copy_into_sql, _is_internal=True)
        assert copy_result is not None
        copy_results = copy_result.fetchall()

        if overwrite and auto_create_table:
            original_table_location = build_location_helper(
                database=database,
                schema=schema,
                name=table_name,
                quote_identifiers=quote_identifiers,
            )
            drop_object(original_table_location, "table")
            rename_table_sql = f"ALTER TABLE {target_table_location} RENAME TO {original_table_location} /* Python:snowflake.snowpark._internal.analyzer.analyzer_utils.write_arrow() */"
            cursor.execute(rename_table_sql, _is_internal=True)
    except ProgrammingError:
        if overwrite and auto_create_table:
            # drop table only if we created a new one with a random name
            drop_object(target_table_location, "table")
        raise
    finally:
        cursor.close()

    return (
        all(e[1] == "LOADED" for e in copy_results),
        len(copy_results),
        sum(int(e[3]) for e in copy_results),
        copy_results,  # pyright: ignore
    )
