#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import re
import typing
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from snowflake.snowpark._internal.analyzer.binary_plan_node import (
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
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.type_utils import convert_sp_to_sf_type
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    get_temp_type_for_object,
    is_single_quoted,
    random_name_for_temp_object,
)
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import DataType

LEFT_PARENTHESIS = "("
RIGHT_PARENTHESIS = ")"
LEFT_BRACKET = "["
RIGHT_BRACKET = "]"
AS = " AS "
AND = " AND "
OR = " OR "
NOT = " NOT "
STAR = " * "
EMPTY_STRING = ""
SPACE = " "
DOUBLE_QUOTE = '"'
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
EXISTS = " EXISTS "
CREATE = " CREATE "
TABLE = " TABLE "
REPLACE = " REPLACE "
VIEW = " VIEW "
TEMPORARY = " TEMPORARY "
IF = " If "
INSERT = " INSERT "
INTO = " INTO "
VALUES = " VALUES "
SEQ8 = " SEQ8() "
ROW_NUMBER = " ROW_NUMBER() "
ONE = " 1 "
GENERATOR = "GENERATOR"
ROW_COUNT = "ROWCOUNT"
RIGHT_ARROW = " => "
NUMBER = " NUMBER "
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
IGNORE_NULLS = " IGNORE NULLS "
UNION = " UNION "
UNION_ALL = " UNION ALL "
INTERSECT = f" {Intersect.sql} "
EXCEPT = f" {Except.sql} "

TEMPORARY_STRING_SET = frozenset(["temporary", "temp"])


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
    return f" ORDER BY {COMMA.join(col_exprs)}" if col_exprs else EMPTY_STRING


def table_function_partition_spec(
    over: bool, partition_exprs: List[str], order_exprs: List[str]
) -> str:
    return (
        f"{OVER}{LEFT_PARENTHESIS}{partition_spec(partition_exprs)}{SPACE}{order_by_spec(order_exprs)}{RIGHT_PARENTHESIS}"
        if over
        else EMPTY_STRING
    )


def binary_comparison(left: str, right: str, symbol: str) -> str:
    return left + SPACE + symbol + SPACE + right


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
        + COMMA.join(order_by_cols)
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


def regexp_expression(expr: str, pattern: str) -> str:
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


def lateral_statement(lateral_expression: str, child: str) -> str:
    return (
        SELECT
        + STAR
        + FROM
        + LEFT_PARENTHESIS
        + child
        + RIGHT_PARENTHESIS
        + COMMA
        + LATERAL
        + lateral_expression
    )


def join_table_function_statement(func: str, child: str) -> str:
    return (
        SELECT
        + STAR
        + FROM
        + LEFT_PARENTHESIS
        + child
        + RIGHT_PARENTHESIS
        + JOIN
        + table(func)
    )


def table_function_statement(func: str) -> str:
    return project_statement([], table(func))


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


def project_statement(project: List[str], child: str, is_distinct: bool = False) -> str:
    return (
        SELECT
        + f"{DISTINCT if is_distinct else EMPTY_STRING}"
        + f"{STAR if not project else COMMA.join(project)}"
        + FROM
        + LEFT_PARENTHESIS
        + child
        + RIGHT_PARENTHESIS
    )


def filter_statement(condition: str, child: str) -> str:
    return project_statement([], child) + WHERE + condition


def sample_statement(
    child: str,
    probability_fraction: Optional[float] = None,
    row_count: Optional[int] = None,
):
    """Generates the sql text for the sample part of the plan being executed"""
    if probability_fraction is not None:
        return (
            project_statement([], child)
            + SAMPLE
            + LEFT_PARENTHESIS
            + str(probability_fraction * 100)
            + RIGHT_PARENTHESIS
        )
    elif row_count is not None:
        return (
            project_statement([], child)
            + SAMPLE
            + LEFT_PARENTHESIS
            + str(row_count)
            + ROWS
            + RIGHT_PARENTHESIS
        )
    else:  # this shouldn't happen because upstream code will validate either probability_fraction or row_count will have a value.
        raise ValueError(
            "Either 'probability_fraction' or 'row_count' must not be None."
        )


def aggregate_statement(
    grouping_exprs: List[str], aggregate_exprs: List[str], child: str
) -> str:
    # add limit 1 because aggregate may be on non-aggregate function in a scalar aggregation
    # for example, df.agg(lit(1))
    return project_statement(aggregate_exprs, child) + (
        limit_expression(1)
        if not grouping_exprs
        else (GROUP_BY + COMMA.join(grouping_exprs))
    )


def sort_statement(order: List[str], child: str) -> str:
    return project_statement([], child) + ORDER_BY + COMMA.join(order)


def range_statement(start: int, end: int, step: int, column_name: str) -> str:
    range = end - start

    if range * step < 0:
        count = 0
    else:
        count = range / step + (1 if range % step != 0 and range * step > 0 else 0)

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
    )


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
    return filter_statement(UNSAT_FILTER, values_statement(output, data))


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
    left: str, right: str, join_type: JoinType, condition: str
) -> str:
    left_alias = random_name_for_temp_object(TempObjectType.TABLE)
    right_alias = random_name_for_temp_object(TempObjectType.TABLE)

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


def snowflake_supported_join_statement(
    left: str, right: str, join_type: JoinType, condition: str
) -> str:
    left_alias = random_name_for_temp_object(TempObjectType.TABLE)
    right_alias = random_name_for_temp_object(TempObjectType.TABLE)

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
        raise Exception("A join should either have using clause or a join condition")

    source = (
        LEFT_PARENTHESIS
        + left
        + RIGHT_PARENTHESIS
        + AS
        + left_alias
        + SPACE
        + join_sql
        + JOIN
        + LEFT_PARENTHESIS
        + right
        + RIGHT_PARENTHESIS
        + AS
        + right_alias
        + f"{using_condition if using_condition else EMPTY_STRING}"
        + f"{join_condition if join_condition else EMPTY_STRING}"
    )

    return project_statement([], source)


def join_statement(left: str, right: str, join_type: JoinType, condition: str) -> str:
    if isinstance(join_type, (LeftSemi, LeftAnti)):
        return left_semi_or_anti_join_statement(left, right, join_type, condition)
    if isinstance(join_type, UsingJoin):
        if isinstance(join_type.tpe, LeftSemi):
            raise Exception("Internal error: Unexpected Using clause in left semi join")
        if isinstance(join_type.tpe, LeftAnti):
            raise Exception("Internal error: Unexpected Using clause in left anti join")
    return snowflake_supported_join_statement(left, right, join_type, condition)


def create_table_statement(
    table_name: str,
    schema: str,
    replace: bool = False,
    error: bool = True,
    table_type: str = EMPTY_STRING,
    *,
    use_scoped_temp_objects: bool = False,
    is_generated: bool = False,
) -> str:
    return (
        f"{CREATE}{(OR + REPLACE) if replace else EMPTY_STRING}"
        f" {(get_temp_type_for_object(use_scoped_temp_objects, is_generated) if table_type.lower() in TEMPORARY_STRING_SET else table_type).upper()} "
        f"{TABLE}{table_name}{(IF + NOT + EXISTS) if not replace and not error else EMPTY_STRING}"
        f"{LEFT_PARENTHESIS}{schema}{RIGHT_PARENTHESIS}"
    )


def insert_into_statement(
    table_name: str, child: str, column_names: Optional[Iterable[str]] = None
) -> str:
    table_columns = f"({COMMA.join(column_names)})" if column_names else EMPTY_STRING
    return f"{INSERT}{INTO}{table_name}{table_columns}{project_statement([], child)}"


def batch_insert_into_statement(table_name: str, column_names: List[str]) -> str:
    return (
        f"{INSERT}{INTO}{table_name}"
        f"{LEFT_PARENTHESIS}{COMMA.join(column_names)}{RIGHT_PARENTHESIS}"
        f"{VALUES}{LEFT_PARENTHESIS}"
        f"{COMMA.join([QUESTION_MARK] * len(column_names))}{RIGHT_PARENTHESIS}"
    )


def create_table_as_select_statement(
    table_name: str,
    child: str,
    replace: bool = False,
    error: bool = True,
    table_type: str = EMPTY_STRING,
) -> str:
    return (
        f"{CREATE}{OR + REPLACE if replace else EMPTY_STRING} {table_type.upper()} {TABLE}"
        f"{IF + NOT + EXISTS if not replace and not error else EMPTY_STRING}"
        f" {table_name}{AS}{project_statement([], child)}"
    )


def limit_statement(
    row_count: str, offset: str, child: str, on_top_of_order_by: bool
) -> str:
    return (
        f"{child if on_top_of_order_by else project_statement([], child)}"
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
    options_str = TYPE + EQUALS + file_type + SPACE + get_options_statement(options)
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


def infer_schema_statement(path: str, file_format_name: str) -> str:
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
        + SINGLE_QUOTE
        + path
        + SINGLE_QUOTE
        + COMMA
        + FILE_FORMAT
        + RIGHT_ARROW
        + SINGLE_QUOTE
        + file_format_name
        + SINGLE_QUOTE
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


def convert_value_to_sql_option(value: Optional[Union[str, bool, int, float]]) -> str:
    if isinstance(value, str):
        if len(value) > 1 and is_single_quoted(value):
            return value
        else:
            value = value.replace(
                "'", "''"
            )  # escape single quotes before adding a pair of quotes
            return f"'{value}'"
    else:
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
    project: List[str], path: str, format_name: str, pattern: str
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
    func_name: str, expr: str, offset: int, default: str, ignore_nulls: bool
) -> str:
    return (
        func_name
        + LEFT_PARENTHESIS
        + expr
        + (COMMA + str(offset) if offset else EMPTY_STRING)
        + (COMMA + default if default else EMPTY_STRING)
        + RIGHT_PARENTHESIS
        + (IGNORE_NULLS if ignore_nulls else EMPTY_STRING)
    )


def cast_expression(child: str, datatype: DataType, try_: bool = False) -> str:
    return (
        (TRY_CAST if try_ else CAST)
        + LEFT_PARENTHESIS
        + child
        + AS
        + convert_sp_to_sf_type(datatype)
        + RIGHT_PARENTHESIS
    )


def order_expression(name: str, direction: str, null_ordering: str) -> str:
    return name + SPACE + direction + SPACE + null_ordering


def create_or_replace_view_statement(name: str, child: str, is_temp: bool) -> str:
    return (
        CREATE
        + OR
        + REPLACE
        + f"{TEMPORARY if is_temp else EMPTY_STRING}"
        + VIEW
        + name
        + AS
        + project_statement([], child)
    )


def pivot_statement(
    pivot_column: str, pivot_values: List[str], aggregate: str, child: str
) -> str:
    return (
        SELECT
        + STAR
        + FROM
        + LEFT_PARENTHESIS
        + child
        + RIGHT_PARENTHESIS
        + PIVOT
        + LEFT_PARENTHESIS
        + aggregate
        + FOR
        + pivot_column
        + IN
        + LEFT_PARENTHESIS
        + COMMA.join(pivot_values)
        + RIGHT_PARENTHESIS
        + RIGHT_PARENTHESIS
    )


def unpivot_statement(
    value_column: str, name_column: str, column_list: List[str], child: str
) -> str:
    return (
        SELECT
        + STAR
        + FROM
        + LEFT_PARENTHESIS
        + child
        + RIGHT_PARENTHESIS
        + UNPIVOT
        + LEFT_PARENTHESIS
        + value_column
        + FOR
        + name_column
        + IN
        + LEFT_PARENTHESIS
        + COMMA.join(column_list)
        + RIGHT_PARENTHESIS
        + RIGHT_PARENTHESIS
    )


def copy_into_table(
    table_name: str,
    file_path: str,
    file_format_type: str,
    format_type_options: Dict[str, Any],
    copy_options: Dict[str, Any],
    pattern: str,
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
        + (PATTERN + EQUALS + single_quote(pattern) if pattern else EMPTY_STRING)
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
        + source
        + RIGHT_PARENTHESIS
        + ON
        + join_expr
        + EMPTY_STRING.join(clauses)
    )


def drop_table_if_exists_statement(table_name: str) -> str:
    return DROP + TABLE + IF + EXISTS + table_name


def attribute_to_schema_string(attributes: List[Attribute]) -> str:
    return COMMA.join(
        attr.name + SPACE + convert_sp_to_sf_type(attr.datatype) for attr in attributes
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


ALREADY_QUOTED = re.compile('^(".+")$')
UNQUOTED_CASE_INSENSITIVE = re.compile("^([_A-Za-z]+[_A-Za-z0-9$]*)$")


def quote_name(name: str) -> str:
    if ALREADY_QUOTED.match(name):
        return validate_quoted_name(name)
    elif UNQUOTED_CASE_INSENSITIVE.match(name):
        return DOUBLE_QUOTE + escape_quotes(name.upper()) + DOUBLE_QUOTE
    else:
        return DOUBLE_QUOTE + escape_quotes(name) + DOUBLE_QUOTE


def validate_quoted_name(name: str) -> str:
    if DOUBLE_QUOTE in name[1:-1].replace(DOUBLE_QUOTE + DOUBLE_QUOTE, EMPTY_STRING):
        raise SnowparkClientExceptionMessages.PLAN_ANALYZER_INVALID_IDENTIFIER(name)
    else:
        return name


def quote_name_without_upper_casing(name: str) -> str:
    return DOUBLE_QUOTE + escape_quotes(name) + DOUBLE_QUOTE


def escape_quotes(unescaped: str) -> str:
    return unescaped.replace(DOUBLE_QUOTE, DOUBLE_QUOTE + DOUBLE_QUOTE)


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


def get_file_format_spec(
    file_format_type: str, format_type_options: typing.Dict[str, Any]
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
