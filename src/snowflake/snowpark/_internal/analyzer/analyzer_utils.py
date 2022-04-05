#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import re
from typing import Any, Dict, List, Optional, Tuple, Union

from snowflake.snowpark._internal.analyzer.binary_plan_node import (
    JoinType,
    LeftAnti,
    LeftSemi,
    NaturalJoin,
    UsingJoin,
)
from snowflake.snowpark._internal.analyzer.datatype_mapper import DataTypeMapper
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.type_utils import convert_to_sf_type
from snowflake.snowpark._internal.utils import TempObjectType, Utils
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import DataType


class AnalyzerUtils:
    _LeftParenthesis = "("
    _RightParenthesis = ")"
    _LeftBracket = "["
    _RightBracket = "]"
    _As = " AS "
    _And = " AND "
    _Or = " OR "
    _Not = " NOT "
    _Dot = "."
    _Star = " * "
    _EmptyString = ""
    _Space = " "
    _DoubleQuote = '"'
    _SingleQuote = "'"
    _Comma = ", "
    _Minus = " - "
    _Plus = " + "
    _Semicolon = ";"
    _ColumnNamePrefix = " COL_"
    _Distinct = " DISTINCT "
    _BitAnd = " BITAND "
    _BitOr = " BITOR "
    _BitXor = " BITXOR "
    _BitNot = " BITNOT "
    _BitShiftLeft = " BITSHIFTLEFT "
    _BitShiftRight = " BITSHIFTRIGHT "
    _Like = " LIKE "
    _Cast = " CAST "
    _TryCast = " TRY_CAST "
    _Iff = " IFF "
    _In = " IN "
    _ToDecimal = " TO_DECIMAL "
    _Asc = " ASC "
    _Desc = " DESC "
    _Pow = " POW "
    _GroupBy = " GROUP BY "
    _PartitionBy = " PARTITION BY "
    _OrderBy = " ORDER BY "
    _Over = " OVER "
    _Power = " POWER "
    _Round = " ROUND "
    _Concat = " CONCAT "
    _Select = " SELECT "
    _From = " FROM "
    _Where = " WHERE "
    _Limit = " LIMIT "
    _Pivot = " PIVOT "
    _Unpivot = " UNPIVOT "
    _For = " FOR "
    _On = " ON "
    _Using = " USING "
    _Join = " JOIN "
    _Natural = " NATURAL "
    _InnerJoin = " INNER JOIN "
    _LeftOuterJoin = " LEFT OUTER JOIN "
    _RightOuterJoin = " RIGHT OUTER JOIN "
    _FullOuterJoin = " FULL OUTER JOIN "
    _Exists = " EXISTS "
    _UnionAll = " UNION ALL "
    _Create = " CREATE "
    _Table = " TABLE "
    _Replace = " REPLACE "
    _View = " VIEW "
    _Temporary = " TEMPORARY "
    _If = " If "
    _Insert = " INSERT "
    _Into = " INTO "
    _Values = " VALUES "
    _InlineTable = " INLINE_TABLE "
    _Seq8 = " SEQ8() "
    _RowNumber = " ROW_NUMBER() "
    _One = " 1 "
    _Generator = "GENERATOR"
    _RowCount = "ROWCOUNT"
    _RightArrow = " => "
    _LessThanOrEqual = " <= "
    _Number = " NUMBER "
    _UnsatFilter = " 1 = 0 "
    _Is = " IS "
    _Null = " NULL "
    _Between = " BETWEEN "
    _Following = " FOLLOWING "
    _Preceding = " PRECEDING "
    _Dollar = "$"
    _DoubleColon = "::"
    _Drop = " DROP "
    _EqualNull = " EQUAL_NULL "
    _IsNaN = " = 'NaN'"
    _File = " FILE "
    _Files = " FILES "
    _Format = " FORMAT "
    _Type = " TYPE "
    _Equals = " = "
    _FileFormat = " FILE_FORMAT "
    _FormatName = " FORMAT_NAME "
    _Copy = " COPY "
    _RegExp = " REGEXP "
    _Collate = " COLLATE "
    _ResultScan = " RESULT_SCAN"
    _Sample = " SAMPLE "
    _Rows = " ROWS "
    _Case = " CASE "
    _When = " WHEN "
    _Then = " THEN "
    _Else = " ELSE "
    _End = " END "
    _Flatten = " FLATTEN "
    _Input = " INPUT "
    _Path = " PATH "
    _Outer = " OUTER "
    _Recursive = " RECURSIVE "
    _Mode = " MODE "
    _Lateral = " LATERAL "
    _Put = " PUT "
    _Get = " GET "
    _GroupingSets = " GROUPING SETS "
    _QuestionMark = "?"
    _Pattern = " PATTERN "
    _WithinGroup = " WITHIN GROUP "
    _ValidationMode = " VALIDATION_MODE "
    _Update = " UPDATE "
    _Delete = " DELETE "
    _Set = " SET "
    _Merge = " MERGE "
    _Matched = " MATCHED "
    _ListAgg = " LISTAGG "
    _Header = " HEADER "
    _IgnoreNulls = " IGNORE NULLS "

    @classmethod
    def result_scan_statement(cls, uuid_place_holder: str) -> str:
        return (
            cls._Select
            + cls._Star
            + cls._From
            + cls._Table
            + cls._LeftParenthesis
            + cls._ResultScan
            + cls._LeftParenthesis
            + cls._SingleQuote
            + uuid_place_holder
            + cls._SingleQuote
            + cls._RightParenthesis
            + cls._RightParenthesis
        )

    @classmethod
    def function_expression(
        cls, name: str, children: List[str], is_distinct: bool
    ) -> str:
        return (
            name
            + cls._LeftParenthesis
            + f"{cls._Distinct if is_distinct else cls._EmptyString}"
            + cls._Comma.join(children)
            + cls._RightParenthesis
        )

    @classmethod
    def named_arguments_function(cls, name: str, args: Dict[str, str]) -> str:
        return (
            name
            + cls._LeftParenthesis
            + cls._Comma.join(
                [key + cls._RightArrow + value for key, value in args.items()]
            )
            + cls._RightParenthesis
        )

    @classmethod
    def binary_comparison(cls, left: str, right: str, symbol: str) -> str:
        return left + cls._Space + symbol + cls._Space + right

    @classmethod
    def subquery_expression(cls, child: str) -> str:
        return cls._LeftParenthesis + child + cls._RightParenthesis

    @classmethod
    def binary_arithmetic_expression(cls, op: str, left: str, right: str) -> str:
        return (
            cls._LeftParenthesis
            + left
            + cls._Space
            + op
            + cls._Space
            + right
            + cls._RightParenthesis
        )

    @classmethod
    def alias_expression(cls, origin: str, alias: str) -> str:
        return origin + cls._As + alias

    @classmethod
    def within_group_expression(cls, column: str, order_by_cols: List[str]) -> str:
        return (
            column
            + cls._WithinGroup
            + cls._LeftParenthesis
            + cls._OrderBy
            + cls._Comma.join(order_by_cols)
            + cls._RightParenthesis
        )

    @classmethod
    def limit_expression(cls, num: int) -> str:
        return cls._Limit + str(num)

    @classmethod
    def grouping_set_expression(cls, args: List[List[str]]) -> str:
        flat_args = [
            cls._LeftParenthesis + cls._Comma.join(arg) + cls._RightParenthesis
            for arg in args
        ]
        return (
            cls._GroupingSets
            + cls._LeftParenthesis
            + cls._Comma.join(flat_args)
            + cls._RightParenthesis
        )

    @classmethod
    def like_expression(cls, expr: str, pattern: str) -> str:
        return expr + cls._Like + pattern

    @classmethod
    def block_expression(cls, expressions: List[str]) -> str:
        return (
            cls._LeftParenthesis + cls._Comma.join(expressions) + cls._RightParenthesis
        )

    @classmethod
    def in_expression(cls, column: str, values: List[str]) -> str:
        return column + cls._In + cls.block_expression(values)

    @classmethod
    def regexp_expression(cls, expr: str, pattern: str) -> str:
        return expr + cls._RegExp + pattern

    @classmethod
    def collate_expression(cls, expr: str, collation_spec: str) -> str:
        return expr + cls._Collate + cls.single_quote(collation_spec)

    @classmethod
    def subfield_expression(cls, expr: str, field: Union[str, int]) -> str:
        return (
            expr
            + cls._LeftBracket
            + (
                cls._SingleQuote + field + cls._SingleQuote
                if isinstance(field, str)
                else str(field)
            )
            + cls._RightBracket
        )

    @classmethod
    def flatten_expression(
        cls, input_: str, path: Optional[str], outer: bool, recursive: bool, mode: str
    ) -> str:
        return (
            cls._Flatten
            + cls._LeftParenthesis
            + cls._Input
            + cls._RightArrow
            + input_
            + cls._Comma
            + cls._Path
            + cls._RightArrow
            + cls._SingleQuote
            + (path or cls._EmptyString)
            + cls._SingleQuote
            + cls._Comma
            + cls._Outer
            + cls._RightArrow
            + str(outer).upper()
            + cls._Comma
            + cls._Recursive
            + cls._RightArrow
            + str(recursive).upper()
            + cls._Comma
            + cls._Mode
            + cls._RightArrow
            + cls._SingleQuote
            + mode
            + cls._SingleQuote
            + cls._RightParenthesis
        )

    @classmethod
    def lateral_statement(cls, lateral_expression: str, child: str) -> str:
        return (
            cls._Select
            + cls._Star
            + cls._From
            + cls._LeftParenthesis
            + child
            + cls._RightParenthesis
            + cls._Comma
            + cls._Lateral
            + lateral_expression
        )

    @classmethod
    def join_table_function_statement(cls, func: str, child: str) -> str:
        return (
            cls._Select
            + cls._Star
            + cls._From
            + cls._LeftParenthesis
            + child
            + cls._RightParenthesis
            + cls._Join
            + cls.table(func)
        )

    @classmethod
    def table_function_statement(cls, func: str) -> str:
        return cls.project_statement([], cls.table(func))

    @classmethod
    def case_when_expression(
        cls, branches: List[Tuple[str, str]], else_value: str
    ) -> str:
        return (
            cls._Case
            + cls._EmptyString.join(
                [
                    cls._When + condition + cls._Then + value
                    for condition, value in branches
                ]
            )
            + cls._Else
            + else_value
            + cls._End
        )

    @classmethod
    def project_statement(
        cls, project: List[str], child: str, is_distinct: bool = False
    ) -> str:
        return (
            cls._Select
            + f"{cls._Distinct if is_distinct else ''}"
            + f"{cls._Star if not project else cls._Comma.join(project)}"
            + cls._From
            + cls._LeftParenthesis
            + child
            + cls._RightParenthesis
        )

    @classmethod
    def filter_statement(cls, condition: str, child: str) -> str:
        return cls.project_statement([], child) + cls._Where + condition

    @classmethod
    def sample_statement(
        cls,
        child: str,
        probability_fraction: Optional[float] = None,
        row_count: Optional[int] = None,
    ):
        """Generates the sql text for the sample part of the plan being executed"""
        if probability_fraction is not None:
            return (
                cls.project_statement([], child)
                + cls._Sample
                + cls._LeftParenthesis
                + str(probability_fraction * 100)
                + cls._RightParenthesis
            )
        elif row_count is not None:
            return (
                cls.project_statement([], child)
                + cls._Sample
                + cls._LeftParenthesis
                + str(row_count)
                + cls._Rows
                + cls._RightParenthesis
            )
        else:  # this shouldn't happen because upstream code will validate either probability_fraction or row_count will have a value.
            raise ValueError(
                "Either 'probability_fraction' or 'row_count' must not be None."
            )

    @classmethod
    def aggregate_statement(
        cls, grouping_exprs: List[str], aggregate_exprs: List[str], child: str
    ) -> str:
        # add limit 1 because Spark may aggregate on non-aggregate function in a scalar aggregation
        # for example, df.agg(lit(1))
        return cls.project_statement(aggregate_exprs, child) + (
            cls.limit_expression(1)
            if not grouping_exprs
            else (cls._GroupBy + cls._Comma.join(grouping_exprs))
        )

    @classmethod
    def sort_statement(cls, order: List[str], child: str) -> str:
        return cls.project_statement([], child) + cls._OrderBy + ",".join(order)

    @classmethod
    def range_statement(cls, start: int, end: int, step: int, column_name: str) -> str:
        range = end - start

        if range * step < 0:
            count = 0
        else:
            count = range / step + (1 if range % step != 0 and range * step > 0 else 0)

        return cls.project_statement(
            [
                cls._LeftParenthesis
                + cls._RowNumber
                + cls._Over
                + cls._LeftParenthesis
                + cls._OrderBy
                + cls._Seq8
                + cls._RightParenthesis
                + cls._Minus
                + cls._One
                + cls._RightParenthesis
                + cls._Star
                + cls._LeftParenthesis
                + str(step)
                + cls._RightParenthesis
                + cls._Plus
                + cls._LeftParenthesis
                + str(start)
                + cls._RightParenthesis
                + cls._As
                + column_name
            ],
            cls.table(cls.generator(0 if count < 0 else count)),
        )

    @classmethod
    def values_statement(cls, output: List[Attribute], data: List[Row]) -> str:
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        data_types = [attr.datatype for attr in output]
        names = [AnalyzerUtils.quote_name(attr.name) for attr in output]
        rows = []
        for row in data:
            cells = [
                DataTypeMapper.to_sql(value, data_type)
                for value, data_type in zip(row, data_types)
            ]
            rows.append(
                cls._LeftParenthesis + cls._Comma.join(cells) + cls._RightParenthesis
            )
        query_source = (
            cls._Values
            + cls._Comma.join(rows)
            + cls._As
            + table_name
            + cls._LeftParenthesis
            + cls._Comma.join(names)
            + cls._RightParenthesis
        )
        return cls.project_statement([], query_source)

    @classmethod
    def empty_values_statement(cls, output: List[Attribute]) -> str:
        data = [Row(*[None] * len(output))]
        return cls.filter_statement(
            cls._UnsatFilter, cls.values_statement(output, data)
        )

    @classmethod
    def set_operator_statement(cls, left: str, right: str, operator: str) -> str:
        return (
            cls._LeftParenthesis
            + left
            + cls._RightParenthesis
            + cls._Space
            + operator
            + cls._Space
            + cls._LeftParenthesis
            + right
            + cls._RightParenthesis
        )

    @classmethod
    def left_semi_or_anti_join_statement(
        cls, left: str, right: str, join_type: JoinType, condition: str
    ) -> str:
        left_alias = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        right_alias = Utils.random_name_for_temp_object(TempObjectType.TABLE)

        if isinstance(join_type, LeftSemi):
            where_condition = cls._Where + cls._Exists
        else:
            where_condition = cls._Where + cls._Not + cls._Exists

        # this generates sql like "Where a = b"
        join_condition = cls._Where + condition

        return (
            cls._Select
            + cls._Star
            + cls._From
            + cls._LeftParenthesis
            + left
            + cls._RightParenthesis
            + cls._As
            + left_alias
            + where_condition
            + cls._LeftParenthesis
            + cls._Select
            + cls._Star
            + cls._From
            + cls._LeftParenthesis
            + right
            + cls._RightParenthesis
            + cls._As
            + right_alias
            + f"{join_condition if join_condition else cls._EmptyString}"
            + cls._RightParenthesis
        )

    @classmethod
    def snowflake_supported_join_statement(
        cls, left: str, right: str, join_type: JoinType, condition: str
    ) -> str:
        left_alias = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        right_alias = Utils.random_name_for_temp_object(TempObjectType.TABLE)

        if isinstance(join_type, UsingJoin):
            join_sql = join_type.tpe.sql
        elif isinstance(join_type, NaturalJoin):
            join_sql = cls._Natural + join_type.tpe.sql
        else:
            join_sql = join_type.sql

        # This generates sql like "USING(a, b)"
        using_condition = None
        if isinstance(join_type, UsingJoin):
            if len(join_type.using_columns) != 0:
                using_condition = (
                    cls._Using
                    + cls._LeftParenthesis
                    + cls._Comma.join(join_type.using_columns)
                    + cls._RightParenthesis
                )

        # This generates sql like "ON a = b"
        join_condition = None
        if condition:
            join_condition = cls._On + condition

        if using_condition and join_condition:
            raise Exception(
                "A join should either have using clause or a join condition"
            )

        source = (
            cls._LeftParenthesis
            + left
            + cls._RightParenthesis
            + cls._As
            + left_alias
            + cls._Space
            + join_sql
            + cls._Join
            + cls._LeftParenthesis
            + right
            + cls._RightParenthesis
            + cls._As
            + right_alias
            + f"{using_condition if using_condition else cls._EmptyString}"
            + f"{join_condition if join_condition else cls._EmptyString}"
        )

        return cls.project_statement([], source)

    @classmethod
    def join_statement(
        cls, left: str, right: str, join_type: JoinType, condition: str
    ) -> str:
        if isinstance(join_type, (LeftSemi, LeftAnti)):
            return cls.left_semi_or_anti_join_statement(
                left, right, join_type, condition
            )
        if isinstance(join_type, UsingJoin):
            if isinstance(join_type.tpe, LeftSemi):
                raise Exception(
                    "Internal error: Unexpected Using clause in left semi join"
                )
            if isinstance(join_type.tpe, LeftAnti):
                raise Exception(
                    "Internal error: Unexpected Using clause in left anti join"
                )
        return cls.snowflake_supported_join_statement(left, right, join_type, condition)

    @classmethod
    def create_table_statement(
        cls,
        table_name: str,
        schema: str,
        replace: bool = False,
        error: bool = True,
        temp: bool = False,
    ) -> str:
        return (
            f"{cls._Create}{(cls._Or + cls._Replace) if replace else cls._EmptyString} "
            f"{cls._Temporary if temp else cls._EmptyString}"
            f"{cls._Table}{table_name}{(cls._If + cls._Not + cls._Exists) if not replace and not error else ''}"
            f"{cls._LeftParenthesis}{schema}{cls._RightParenthesis}"
        )

    @classmethod
    def insert_into_statement(cls, table_name: str, child: str) -> str:
        return (
            f"{cls._Insert}{cls._Into}{table_name} {cls.project_statement([], child)}"
        )

    @classmethod
    def batch_insert_into_statement(
        cls, table_name: str, column_names: List[str]
    ) -> str:
        return (
            f"{cls._Insert}{cls._Into}{table_name}"
            f"{cls._LeftParenthesis}{cls._Comma.join(column_names)}{cls._RightParenthesis}"
            f"{cls._Values}{cls._LeftParenthesis}"
            f"{cls._Comma.join([cls._QuestionMark] * len(column_names))}{cls._RightParenthesis}"
        )

    @classmethod
    def create_table_as_select_statement(
        cls,
        table_name: str,
        child: str,
        replace: bool = False,
        error: bool = True,
        temp: bool = False,
    ) -> str:
        return (
            f"{cls._Create}{cls._Or + cls._Replace if replace else cls._EmptyString}{cls._Temporary if temp else cls._EmptyString}{cls._Table}"
            f"{cls._If + cls._Not + cls._Exists if not replace and not error else cls._EmptyString}"
            f" {table_name}{cls._As}{cls.project_statement([], child)}"
        )

    @classmethod
    def limit_statement(
        cls, row_count: str, child: str, on_top_of_order_by: bool
    ) -> str:
        return (
            f"{child if on_top_of_order_by else cls.project_statement([], child)}"
            + cls._Limit
            + row_count
        )

    @classmethod
    def schema_cast_seq(cls, schema: List[Attribute]) -> List[str]:
        res = []
        for index, attr in enumerate(schema):
            name = (
                cls._Dollar
                + str(index + 1)
                + cls._DoubleColon
                + convert_to_sf_type(attr.datatype)
            )
            res.append(name + cls._As + cls.quote_name(attr.name))
        return res

    @classmethod
    def create_file_format_statement(
        cls,
        format_name: str,
        file_type: str,
        options: Dict,
        temp: bool,
        if_not_exist: bool,
    ) -> str:
        options_str = (
            cls._Type
            + cls._Equals
            + file_type
            + cls._Space
            + cls.get_options_statement(options)
        )
        return (
            cls._Create
            + (cls._Temporary if temp else cls._EmptyString)
            + cls._File
            + cls._Format
            + (cls._If + cls._Not + cls._Exists if if_not_exist else cls._EmptyString)
            + format_name
            + options_str
        )

    @classmethod
    def file_operation_statement(
        cls, command: str, file_name: str, stage_location: str, options: Dict[str, str]
    ) -> str:
        if command.lower() == "put":
            return f"{cls._Put}{file_name}{cls._Space}{stage_location}{cls._Space}{cls.get_options_statement(options)}"
        if command.lower() == "get":
            return f"{cls._Get}{stage_location}{cls._Space}{file_name}{cls._Space}{cls.get_options_statement(options)}"
        raise ValueError(f"Unsupported file operation type {command}")

    @classmethod
    def get_options_statement(cls, options: Dict[str, Any]) -> str:
        return (
            cls._Space
            + cls._Space.join(
                # repr("a") return "'a'" instead of "a". This is what we need for str values. For bool, int, float, repr(v) and str(v) return the same.
                f"{k}={v if (isinstance(v, str) and Utils.is_single_quoted(v)) else repr(v)}"
                for k, v in options.items()
                if v is not None
            )
            + cls._Space
        )

    @classmethod
    def drop_file_format_if_exists_statement(cls, format_name: str) -> str:
        return cls._Drop + cls._File + cls._Format + cls._If + cls._Exists + format_name

    @classmethod
    def select_from_path_with_format_statement(
        cls, project: List[str], path: str, format_name: str, pattern: str
    ) -> str:
        select_statement = (
            cls._Select
            + (cls._Star if not project else cls._Comma.join(project))
            + cls._From
            + path
        )
        format_statement = (
            (cls._FileFormat + cls._RightArrow + cls.single_quote(format_name))
            if format_name
            else cls._EmptyString
        )
        pattern_statement = (
            (cls._Pattern + cls._RightArrow + cls.single_quote(pattern))
            if pattern
            else cls._EmptyString
        )

        return (
            select_statement
            + (
                cls._LeftParenthesis
                + (format_statement if format_statement else cls._EmptyString)
                + (
                    cls._Comma
                    if format_statement and pattern_statement
                    else cls._EmptyString
                )
                + (pattern_statement if pattern_statement else cls._EmptyString)
                + cls._RightParenthesis
            )
            if format_statement or pattern_statement
            else cls._EmptyString
        )

    @classmethod
    def unary_expression(
        cls, child: str, sql_operator: str, operator_first: bool
    ) -> str:
        return (
            (sql_operator + cls._Space + child)
            if operator_first
            else (child + cls._Space + sql_operator)
        )

    @classmethod
    def window_expression(cls, window_function: str, window_spec: str) -> str:
        return (
            window_function
            + cls._Over
            + cls._LeftParenthesis
            + window_spec
            + cls._RightParenthesis
        )

    @classmethod
    def window_spec_expression(
        cls, partition_spec: List[str], order_spec: List[str], frame_spec: str
    ) -> str:
        return (
            (
                cls._PartitionBy + cls._Comma.join(partition_spec)
                if partition_spec
                else cls._EmptyString
            )
            + (
                cls._OrderBy + cls._Comma.join(order_spec)
                if order_spec
                else cls._EmptyString
            )
            + frame_spec
        )

    @classmethod
    def specified_window_frame_expression(
        cls, frame_type: str, lower: str, upper: str
    ) -> str:
        return (
            cls._Space
            + frame_type
            + cls._Between
            + lower
            + cls._And
            + upper
            + cls._Space
        )

    @classmethod
    def window_frame_boundary_expression(cls, offset: str, is_following: bool) -> str:
        return offset + (cls._Following if is_following else cls._Preceding)

    @classmethod
    def rank_related_function_expression(
        cls, func_name: str, expr: str, offset: int, default: str, ignore_nulls: bool
    ) -> str:
        return (
            func_name
            + cls._LeftParenthesis
            + expr
            + cls._Comma
            + str(offset)
            + cls._Comma
            + default
            + cls._RightParenthesis
            + (cls._IgnoreNulls if ignore_nulls else cls._EmptyString)
        )

    @classmethod
    def cast_expression(cls, child: str, datatype: DataType, try_: bool = False) -> str:
        return (
            (cls._TryCast if try_ else cls._Cast)
            + cls._LeftParenthesis
            + child
            + cls._As
            + convert_to_sf_type(datatype)
            + cls._RightParenthesis
        )

    @classmethod
    def order_expression(cls, name: str, direction: str, null_ordering: str) -> str:
        return name + cls._Space + direction + cls._Space + null_ordering

    @classmethod
    def create_or_replace_view_statement(
        cls, name: str, child: str, is_temp: bool
    ) -> str:
        return (
            cls._Create
            + cls._Or
            + cls._Replace
            + f"{cls._Temporary if is_temp else cls._EmptyString}"
            + cls._View
            + name
            + cls._As
            + cls.project_statement([], child)
        )

    @classmethod
    def pivot_statement(
        cls, pivot_column: str, pivot_values: List[str], aggregate: str, child: str
    ) -> str:
        return (
            cls._Select
            + cls._Star
            + cls._From
            + cls._LeftParenthesis
            + child
            + cls._RightParenthesis
            + cls._Pivot
            + cls._LeftParenthesis
            + aggregate
            + cls._For
            + pivot_column
            + cls._In
            + cls._LeftParenthesis
            + cls._Comma.join(pivot_values)
            + cls._RightParenthesis
            + cls._RightParenthesis
        )

    @classmethod
    def unpivot_statement(
        cls, value_column: str, name_column: str, column_list: List[str], child: str
    ) -> str:
        return (
            cls._Select
            + cls._Star
            + cls._From
            + cls._LeftParenthesis
            + child
            + cls._RightParenthesis
            + cls._Unpivot
            + cls._LeftParenthesis
            + value_column
            + cls._For
            + name_column
            + cls._In
            + cls._LeftParenthesis
            + cls._Comma.join(column_list)
            + cls._RightParenthesis
            + cls._RightParenthesis
        )

    @classmethod
    def copy_into_table(
        cls,
        table_name: str,
        file_path: str,
        file_format: str,
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
            cls._LeftParenthesis + cls._Comma.join(column_names) + cls._RightParenthesis
            if column_names
            else cls._EmptyString
        )
        from_str = (
            cls._LeftParenthesis
            + cls._Select
            + cls._Comma.join(transformations)
            + cls._From
            + file_path
            + cls._RightParenthesis
            if transformations
            else file_path
        )
        files_str = (
            cls._Files
            + cls._Equals
            + cls._LeftParenthesis
            + cls._Comma.join([cls._SingleQuote + f + cls._SingleQuote for f in files])
            + cls._RightParenthesis
            if files
            else cls._EmptyString
        )
        validation_str = (
            f"{cls._ValidationMode} = {validation_mode}"
            if validation_mode
            else cls._EmptyString
        )
        ftostr = (
            cls._FileFormat
            + cls._Equals
            + cls._LeftParenthesis
            + cls._Type
            + cls._Equals
            + file_format
        )
        if format_type_options:
            ftostr += (
                cls._Space + cls.get_options_statement(format_type_options) + cls._Space
            )
        ftostr += cls._RightParenthesis

        if copy_options:
            costr = cls._Space + cls.get_options_statement(copy_options) + cls._Space
        else:
            costr = cls._EmptyString

        return (
            cls._Copy
            + cls._Into
            + table_name
            + column_str
            + cls._From
            + from_str
            + (
                cls._Pattern + cls._Equals + cls.single_quote(pattern)
                if pattern
                else cls._EmptyString
            )
            + files_str
            + ftostr
            + costr
            + validation_str
        )

    @classmethod
    def copy_into_location(
        cls,
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
            (cls._PartitionBy + partition_by) if partition_by else cls._EmptyString
        )
        format_name_clause = (
            (cls._FormatName + cls._Equals + file_format_name)
            if file_format_name
            else cls._EmptyString
        )
        file_type_clause = (
            (cls._Type + cls._Equals + file_format_type)
            if file_format_type
            else cls._EmptyString
        )
        format_type_options_clause = (
            cls.get_options_statement(format_type_options)
            if format_type_options
            else cls._EmptyString
        )
        file_format_clause = (
            (
                cls._FileFormat
                + cls._Equals
                + cls._LeftParenthesis
                + (
                    format_name_clause
                    + cls._Space
                    + file_type_clause
                    + cls._Space
                    + format_type_options_clause
                )
                + cls._RightParenthesis
            )
            if format_name_clause or file_type_clause or format_type_options_clause
            else cls._EmptyString
        )
        copy_options_clause = (
            cls.get_options_statement(copy_options)
            if copy_options
            else cls._EmptyString
        )
        header_clause = (
            f"{cls._Header}={header}" if header is not None else cls._EmptyString
        )
        return (
            cls._Copy
            + cls._Into
            + stage_location
            + cls._From
            + cls._LeftParenthesis
            + query
            + cls._RightParenthesis
            + partition_by_clause
            + file_format_clause
            + cls._Space
            + copy_options_clause
            + cls._Space
            + header_clause
        )

    @classmethod
    def update_statement(
        cls,
        table_name: str,
        assignments: Dict[str, str],
        condition: Optional[str],
        source_data: Optional[str],
    ) -> str:
        return (
            cls._Update
            + table_name
            + cls._Set
            + cls._Comma.join([k + cls._Equals + v for k, v in assignments.items()])
            + (
                (cls._From + cls._LeftParenthesis + source_data + cls._RightParenthesis)
                if source_data
                else cls._EmptyString
            )
            + ((cls._Where + condition) if condition else cls._EmptyString)
        )

    @classmethod
    def delete_statement(
        cls, table_name: str, condition: Optional[str], source_data: Optional[str]
    ) -> str:
        return (
            cls._Delete
            + cls._From
            + table_name
            + (
                (
                    cls._Using
                    + cls._LeftParenthesis
                    + source_data
                    + cls._RightParenthesis
                )
                if source_data
                else cls._EmptyString
            )
            + ((cls._Where + condition) if condition else cls._EmptyString)
        )

    @classmethod
    def insert_merge_statement(
        cls, condition: Optional[str], keys: List[str], values: List[str]
    ) -> str:
        return (
            cls._When
            + cls._Not
            + cls._Matched
            + ((cls._And + condition) if condition else cls._EmptyString)
            + cls._Then
            + cls._Insert
            + (
                (cls._LeftParenthesis + cls._Comma.join(keys) + cls._RightParenthesis)
                if keys
                else cls._EmptyString
            )
            + cls._Values
            + cls._LeftParenthesis
            + cls._Comma.join(values)
            + cls._RightParenthesis
        )

    @classmethod
    def update_merge_statement(
        cls, condition: Optional[str], assignment: Dict[str, str]
    ) -> str:
        return (
            cls._When
            + cls._Matched
            + ((cls._And + condition) if condition else cls._EmptyString)
            + cls._Then
            + cls._Update
            + cls._Set
            + cls._Comma.join([k + cls._Equals + v for k, v in assignment.items()])
        )

    @classmethod
    def delete_merge_statement(cls, condition: Optional[str]) -> str:
        return (
            cls._When
            + cls._Matched
            + ((cls._And + condition) if condition else cls._EmptyString)
            + cls._Then
            + cls._Delete
        )

    @classmethod
    def merge_statement(
        cls, table_name: str, source: str, join_expr: str, clauses: List[str]
    ) -> str:
        return (
            cls._Merge
            + cls._Into
            + table_name
            + cls._Using
            + cls._LeftParenthesis
            + source
            + cls._RightParenthesis
            + cls._On
            + join_expr
            + cls._EmptyString.join(clauses)
        )

    @classmethod
    def create_temp_table_statement(cls, table_name: str, schema: str) -> str:
        return (
            cls._Create
            + cls._Temporary
            + cls._Table
            + table_name
            + cls._LeftParenthesis
            + schema
            + cls._RightParenthesis
        )

    @classmethod
    def drop_table_if_exists_statement(cls, table_name: str) -> str:
        return cls._Drop + cls._Table + cls._If + cls._Exists + table_name

    @classmethod
    def attribute_to_schema_string(cls, attributes: List[Attribute]) -> str:
        return cls._Comma.join(
            attr.name + cls._Space + convert_to_sf_type(attr.datatype)
            for attr in attributes
        )

    @classmethod
    def schema_value_statement(cls, output: List[Attribute]) -> str:
        return cls._Select + cls._Comma.join(
            [
                DataTypeMapper.schema_expression(attr.datatype, attr.nullable)
                + cls._As
                + cls.quote_name(attr.name)
                for attr in output
            ]
        )

    @classmethod
    def list_agg(cls, col: str, delimiter: str, is_distinct: bool) -> str:
        return (
            cls._ListAgg
            + cls._LeftParenthesis
            + f"{cls._Distinct if is_distinct else cls._EmptyString}"
            + col
            + cls._Comma
            + delimiter
            + cls._RightParenthesis
        )

    @classmethod
    def generator(cls, row_count: int) -> str:
        return (
            cls._Generator
            + cls._LeftParenthesis
            + cls._RowCount
            + cls._RightArrow
            + str(row_count)
            + cls._RightParenthesis
        )

    @classmethod
    def table(cls, content: str) -> str:
        return cls._Table + cls._LeftParenthesis + content + cls._RightParenthesis

    @classmethod
    def single_quote(cls, value: str) -> str:
        if value.startswith(cls._SingleQuote) and value.endswith(cls._SingleQuote):
            return value
        else:
            return cls._SingleQuote + value + cls._SingleQuote

    @classmethod
    def quote_name(cls, name: str) -> str:
        already_quoted = re.compile('^(".+")$')
        unquoted_case_insensitive = re.compile("^([_A-Za-z]+[_A-Za-z0-9$]*)$")
        if already_quoted.match(name):
            return cls.validate_quoted_name(name)
        elif unquoted_case_insensitive.match(name):
            return cls._DoubleQuote + cls.escape_quotes(name.upper()) + cls._DoubleQuote
        else:
            return cls._DoubleQuote + cls.escape_quotes(name) + cls._DoubleQuote

    @classmethod
    def validate_quoted_name(cls, name: str) -> str:
        if cls._DoubleQuote in name[1:-1].replace(
            cls._DoubleQuote + cls._DoubleQuote, cls._EmptyString
        ):
            raise SnowparkClientExceptionMessages.PLAN_ANALYZER_INVALID_IDENTIFIER(name)
        else:
            return name

    @classmethod
    def quote_name_without_upper_casing(cls, name: str) -> str:
        return cls._DoubleQuote + cls.escape_quotes(name) + cls._DoubleQuote

    @classmethod
    def escape_quotes(cls, unescaped: str) -> str:
        return unescaped.replace(cls._DoubleQuote, cls._DoubleQuote + cls._DoubleQuote)

    # Most integer types map to number(38,0)
    # https://docs.snowflake.com/en/sql-reference/
    # data-types-numeric.html#int-integer-bigint-smallint-tinyint-byteint
    @classmethod
    def number(cls, precision: int = 38, scale: int = 0) -> str:
        return (
            cls._Number
            + cls._LeftParenthesis
            + str(precision)
            + cls._Comma
            + str(scale)
            + cls._RightParenthesis
        )
