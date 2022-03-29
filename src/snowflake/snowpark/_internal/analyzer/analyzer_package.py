#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import re
from typing import Any, Dict, List, Optional, Tuple, Union

from snowflake.snowpark._internal.analyzer.binary_plan_nodes import (
    JoinType as SPJoinType,
    LeftAnti as SPLeftAnti,
    LeftSemi as SPLeftSemi,
    NaturalJoin as SPNaturalJoin,
    UsingJoin as SPUsingJoin,
)
from snowflake.snowpark._internal.analyzer.datatype_mapper import DataTypeMapper
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.sp_expressions import Attribute
from snowflake.snowpark._internal.type_utils import convert_to_sf_type
from snowflake.snowpark._internal.utils import TempObjectType, Utils
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import DataType


class AnalyzerPackage:
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

    def result_scan_statement(self, uuid_place_holder: str) -> str:
        return (
            self._Select
            + self._Star
            + self._From
            + self._Table
            + self._LeftParenthesis
            + self._ResultScan
            + self._LeftParenthesis
            + self._SingleQuote
            + uuid_place_holder
            + self._SingleQuote
            + self._RightParenthesis
            + self._RightParenthesis
        )

    def function_expression(
        self, name: str, children: List[str], is_distinct: bool
    ) -> str:
        return (
            name
            + self._LeftParenthesis
            + f"{self._Distinct if is_distinct else self._EmptyString}"
            + self._Comma.join(children)
            + self._RightParenthesis
        )

    def named_arguments_function(self, name: str, args: Dict[str, str]) -> str:
        return (
            name
            + self._LeftParenthesis
            + self._Comma.join(
                [key + self._RightArrow + value for key, value in args.items()]
            )
            + self._RightParenthesis
        )

    def binary_comparison(self, left: str, right: str, symbol: str) -> str:
        return left + self._Space + symbol + self._Space + right

    def subquery_expression(self, child: str) -> str:
        return self._LeftParenthesis + child + self._RightParenthesis

    def binary_arithmetic_expression(self, op: str, left: str, right: str) -> str:
        return (
            self._LeftParenthesis
            + left
            + self._Space
            + op
            + self._Space
            + right
            + self._RightParenthesis
        )

    def alias_expression(self, origin: str, alias: str) -> str:
        return origin + self._As + alias

    def within_group_expression(self, column: str, order_by_cols: List[str]) -> str:
        return (
            column
            + self._WithinGroup
            + self._LeftParenthesis
            + self._OrderBy
            + self._Comma.join(order_by_cols)
            + self._RightParenthesis
        )

    def limit_expression(self, num: int) -> str:
        return self._Limit + str(num)

    def grouping_set_expression(self, args: List[List[str]]) -> str:
        flat_args = [
            self._LeftParenthesis + self._Comma.join(arg) + self._RightParenthesis
            for arg in args
        ]
        return (
            self._GroupingSets
            + self._LeftParenthesis
            + self._Comma.join(flat_args)
            + self._RightParenthesis
        )

    def like_expression(self, expr: str, pattern: str) -> str:
        return expr + self._Like + pattern

    def block_expression(self, expressions: List[str]) -> str:
        return (
            self._LeftParenthesis
            + self._Comma.join(expressions)
            + self._RightParenthesis
        )

    def in_expression(self, column: str, values: List[str]) -> str:
        return column + self._In + self.block_expression(values)

    def regexp_expression(self, expr: str, pattern: str) -> str:
        return expr + self._RegExp + pattern

    def collate_expression(self, expr: str, collation_spec: str) -> str:
        return expr + self._Collate + self.single_quote(collation_spec)

    def subfield_expression(self, expr: str, field: Union[str, int]) -> str:
        return (
            expr
            + self._LeftBracket
            + (
                self._SingleQuote + field + self._SingleQuote
                if isinstance(field, str)
                else str(field)
            )
            + self._RightBracket
        )

    def flatten_expression(
        self, input_: str, path: Optional[str], outer: bool, recursive: bool, mode: str
    ) -> str:
        return (
            self._Flatten
            + self._LeftParenthesis
            + self._Input
            + self._RightArrow
            + input_
            + self._Comma
            + self._Path
            + self._RightArrow
            + self._SingleQuote
            + (path or self._EmptyString)
            + self._SingleQuote
            + self._Comma
            + self._Outer
            + self._RightArrow
            + str(outer).upper()
            + self._Comma
            + self._Recursive
            + self._RightArrow
            + str(recursive).upper()
            + self._Comma
            + self._Mode
            + self._RightArrow
            + self._SingleQuote
            + mode
            + self._SingleQuote
            + self._RightParenthesis
        )

    def lateral_statement(self, lateral_expression: str, child: str) -> str:
        return (
            self._Select
            + self._Star
            + self._From
            + self._LeftParenthesis
            + child
            + self._RightParenthesis
            + self._Comma
            + self._Lateral
            + lateral_expression
        )

    def join_table_function_statement(self, func: str, child: str) -> str:
        return (
            self._Select
            + self._Star
            + self._From
            + self._LeftParenthesis
            + child
            + self._RightParenthesis
            + self._Join
            + self.table(func)
        )

    def table_function_statement(self, func: str) -> str:
        return self.project_statement([], self.table(func))

    def case_when_expression(
        self, branches: List[Tuple[str, str]], else_value: str
    ) -> str:
        return (
            self._Case
            + self._EmptyString.join(
                [
                    self._When + condition + self._Then + value
                    for condition, value in branches
                ]
            )
            + self._Else
            + else_value
            + self._End
        )

    def project_statement(
        self, project: List[str], child: str, is_distinct=False
    ) -> str:
        return (
            self._Select
            + f"{self._Distinct if is_distinct else ''}"
            + f"{self._Star if not project else self._Comma.join(project)}"
            + self._From
            + self._LeftParenthesis
            + child
            + self._RightParenthesis
        )

    def filter_statement(self, condition, child) -> str:
        return self.project_statement([], child) + self._Where + condition

    def sample_statement(
        self,
        child: str,
        probability_fraction: Optional[float] = None,
        row_count: Optional[int] = None,
    ):
        """Generates the sql text for the sample part of the plan being executed"""
        if probability_fraction is not None:
            return (
                self.project_statement([], child)
                + self._Sample
                + self._LeftParenthesis
                + str(probability_fraction * 100)
                + self._RightParenthesis
            )
        elif row_count is not None:
            return (
                self.project_statement([], child)
                + self._Sample
                + self._LeftParenthesis
                + str(row_count)
                + self._Rows
                + self._RightParenthesis
            )
        else:  # this shouldn't happen because upstream code will validate either probability_fraction or row_count will have a value.
            raise ValueError(
                "Either 'probability_fraction' or 'row_count' must not be None."
            )

    def aggregate_statement(
        self, grouping_exprs: List[str], aggregate_exprs: List[str], child: str
    ) -> str:
        # add limit 1 because Spark may aggregate on non-aggregate function in a scalar aggregation
        # for example, df.agg(lit(1))
        return self.project_statement(aggregate_exprs, child) + (
            self.limit_expression(1)
            if not grouping_exprs
            else (self._GroupBy + self._Comma.join(grouping_exprs))
        )

    def sort_statement(self, order: List[str], child: str) -> str:
        return self.project_statement([], child) + self._OrderBy + ",".join(order)

    def range_statement(self, start, end, step, column_name) -> str:
        range = end - start

        if range * step < 0:
            count = 0
        else:
            # TODO revisit
            count = range / step + (1 if range % step != 0 and range * step > 0 else 0)

        return self.project_statement(
            [
                self._LeftParenthesis
                + self._RowNumber
                + self._Over
                + self._LeftParenthesis
                + self._OrderBy
                + self._Seq8
                + self._RightParenthesis
                + self._Minus
                + self._One
                + self._RightParenthesis
                + self._Star
                + self._LeftParenthesis
                + str(step)
                + self._RightParenthesis
                + self._Plus
                + self._LeftParenthesis
                + str(start)
                + self._RightParenthesis
                + self._As
                + column_name
            ],
            self.table(self.generator(0 if count < 0 else count)),
        )

    def values_statement(self, output: List[Attribute], data: List[Row]) -> str:
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        data_types = [attr.datatype for attr in output]
        names = [AnalyzerPackage.quote_name(attr.name) for attr in output]
        rows = []
        for row in data:
            cells = [
                DataTypeMapper.to_sql(value, data_type)
                for value, data_type in zip(row, data_types)
            ]
            rows.append(
                self._LeftParenthesis + self._Comma.join(cells) + self._RightParenthesis
            )
        query_source = (
            self._Values
            + self._Comma.join(rows)
            + self._As
            + table_name
            + self._LeftParenthesis
            + self._Comma.join(names)
            + self._RightParenthesis
        )
        return self.project_statement([], query_source)

    def empty_values_statement(self, output: List[Attribute]):
        data = [Row(*[None] * len(output))]
        self.filter_statement(self._UnsatFilter, self.values_statement(output, data))

    def set_operator_statement(self, left: str, right: str, operator: str):
        return (
            self._LeftParenthesis
            + left
            + self._RightParenthesis
            + self._Space
            + operator
            + self._Space
            + self._LeftParenthesis
            + right
            + self._RightParenthesis
        )

    def left_semi_or_anti_join_statement(
        self, left: str, right: str, join_type: type, condition: str
    ) -> str:
        left_alias = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        right_alias = Utils.random_name_for_temp_object(TempObjectType.TABLE)

        if join_type == SPLeftSemi:
            where_condition = self._Where + self._Exists
        else:  # join_type == SPLeftAnti:
            where_condition = self._Where + self._Not + self._Exists

        # this generates sql like "Where a = b"
        join_condition = self._Where + condition

        return (
            self._Select
            + self._Star
            + self._From
            + self._LeftParenthesis
            + left
            + self._RightParenthesis
            + self._As
            + left_alias
            + where_condition
            + self._LeftParenthesis
            + self._Select
            + self._Star
            + self._From
            + self._LeftParenthesis
            + right
            + self._RightParenthesis
            + self._As
            + right_alias
            + f"{join_condition if join_condition else self._EmptyString}"
            + self._RightParenthesis
        )

    def snowflake_supported_join_statement(
        self, left: str, right: str, join_type: SPJoinType, condition: str
    ) -> str:
        left_alias = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        right_alias = Utils.random_name_for_temp_object(TempObjectType.TABLE)

        if isinstance(join_type, SPUsingJoin):
            join_sql = join_type.tpe.sql
        elif isinstance(join_type, SPNaturalJoin):
            join_sql = self._Natural + join_type.tpe.sql
        else:
            join_sql = join_type.sql

        # This generates sql like "USING(a, b)"
        using_condition = None
        if isinstance(join_type, SPUsingJoin):
            if len(join_type.using_columns) != 0:
                using_condition = (
                    self._Using
                    + self._LeftParenthesis
                    + self._Comma.join(join_type.using_columns)
                    + self._RightParenthesis
                )

        # This generates sql like "ON a = b"
        join_condition = None
        if condition:
            join_condition = self._On + condition

        if using_condition and join_condition:
            raise Exception(
                "A join should either have using clause or a join condition"
            )

        source = (
            self._LeftParenthesis
            + left
            + self._RightParenthesis
            + self._As
            + left_alias
            + self._Space
            + join_sql
            + self._Join
            + self._LeftParenthesis
            + right
            + self._RightParenthesis
            + self._As
            + right_alias
            + f"{using_condition if using_condition else self._EmptyString}"
            + f"{join_condition if join_condition else self._EmptyString}"
        )

        return self.project_statement([], source)

    def join_statement(
        self, left: str, right: str, join_type: SPJoinType, condition: str
    ) -> str:
        if isinstance(join_type, SPLeftSemi):
            return self.left_semi_or_anti_join_statement(
                left, right, SPLeftSemi, condition
            )
        if isinstance(join_type, SPLeftAnti):
            return self.left_semi_or_anti_join_statement(
                left, right, SPLeftAnti, condition
            )
        if isinstance(join_type, SPUsingJoin):
            if isinstance(join_type.tpe, SPLeftSemi):
                raise Exception(
                    "Internal error: Unexpected Using clause in left semi join"
                )
            if isinstance(join_type.tpe, SPLeftAnti):
                raise Exception(
                    "Internal error: Unexpected Using clause in left anti join"
                )
        return self.snowflake_supported_join_statement(
            left, right, join_type, condition
        )

    def create_table_statement(
        self,
        table_name: str,
        schema: str,
        replace: bool = False,
        error: bool = True,
        temp: bool = False,
    ) -> str:
        return (
            f"{self._Create}{(self._Or + self._Replace) if replace else self._EmptyString} "
            f"{self._Temporary if temp else self._EmptyString}"
            f"{self._Table}{table_name}{(self._If + self._Not + self._Exists) if not replace and not error else ''}"
            f"{self._LeftParenthesis}{schema}{self._RightParenthesis}"
        )

    def insert_into_statement(self, table_name: str, child: str) -> str:
        return f"{self._Insert}{self._Into}{table_name} {self.project_statement([], child)}"

    def batch_insert_into_statement(
        self, table_name: str, column_names: List[str]
    ) -> str:
        return (
            f"{self._Insert}{self._Into}{table_name}"
            f"{self._LeftParenthesis}{self._Comma.join(column_names)}{self._RightParenthesis}"
            f"{self._Values}{self._LeftParenthesis}"
            f"{self._Comma.join([self._QuestionMark] * len(column_names))}{self._RightParenthesis}"
        )

    def create_table_as_select_statement(
        self,
        table_name: str,
        child: str,
        replace: bool = False,
        error: bool = True,
        temp: bool = False,
    ) -> str:
        return (
            f"{self._Create}{self._Or + self._Replace if replace else self._EmptyString}{self._Temporary if temp else self._EmptyString}{self._Table}"
            f"{self._If + self._Not + self._Exists if not replace and not error else self._EmptyString}"
            f" {table_name}{self._As}{self.project_statement([], child)}"
        )

    def limit_statement(
        self, row_count: str, child: str, on_top_of_order_by: bool
    ) -> str:
        return (
            f"{child if on_top_of_order_by else self.project_statement([], child)}"
            + self._Limit
            + row_count
        )

    def schema_cast_seq(self, schema: List[Attribute]) -> List[str]:
        res = []
        for index, attr in enumerate(schema):
            name = (
                self._Dollar
                + str(index + 1)
                + self._DoubleColon
                + convert_to_sf_type(attr.datatype)
            )
            res.append(name + self._As + self.quote_name(attr.name))
        return res

    def create_file_format_statement(
        self,
        format_name: str,
        file_type: str,
        options: Dict,
        temp: bool,
        if_not_exist: bool,
    ) -> str:
        options_str = (
            self._Type
            + self._Equals
            + file_type
            + self._Space
            + self.get_options_statement(options)
        )
        return (
            self._Create
            + (self._Temporary if temp else self._EmptyString)
            + self._File
            + self._Format
            + (
                self._If + self._Not + self._Exists
                if if_not_exist
                else self._EmptyString
            )
            + format_name
            + options_str
        )

    def file_operation_statement(
        self, command: str, file_name: str, stage_location: str, options: Dict[str, str]
    ) -> str:
        if command.lower() == "put":
            return f"{self._Put}{file_name}{self._Space}{stage_location}{self._Space}{self.get_options_statement(options)}"
        if command.lower() == "get":
            return f"{self._Get}{stage_location}{self._Space}{file_name}{self._Space}{self.get_options_statement(options)}"
        raise ValueError(f"Unsupported file operation type {command}")

    def get_options_statement(self, options: Dict[str, Any]) -> str:
        return (
            self._Space
            + self._Space.join(
                # repr("a") return "'a'" instead of "a". This is what we need for str values. For bool, int, float, repr(v) and str(v) return the same.
                f"{k}={v if (isinstance(v, str) and Utils.is_single_quoted(v)) else repr(v)}"
                for k, v in options.items()
                if v is not None
            )
            + self._Space
        )

    def select_from_path_with_format_statement(
        self, project: List[str], path: str, format_name: str, pattern: str
    ) -> str:
        select_statement = (
            self._Select
            + (self._Star if not project else self._Comma.join(project))
            + self._From
            + path
        )
        format_statement = (
            (self._FileFormat + self._RightArrow + self.single_quote(format_name))
            if format_name
            else self._EmptyString
        )
        pattern_statement = (
            (self._Pattern + self._RightArrow + self.single_quote(pattern))
            if pattern
            else self._EmptyString
        )

        return (
            select_statement
            + (
                self._LeftParenthesis
                + (format_statement if format_statement else self._EmptyString)
                + (
                    self._Comma
                    if format_statement and pattern_statement
                    else self._EmptyString
                )
                + (pattern_statement if pattern_statement else self._EmptyString)
                + self._RightParenthesis
            )
            if format_statement or pattern_statement
            else self._EmptyString
        )

    def unary_minus_expression(self, child: str) -> str:
        return self._Minus + child

    def not_expression(self, child: str) -> str:
        return self._Not + child

    def is_nan_expression(self, child: str) -> str:
        return child + self._IsNaN

    def is_null_expression(self, child: str) -> str:
        return child + self._Is + self._Null

    def is_not_null_expression(self, child: str) -> str:
        return child + self._Is + self._Not + self._Null

    def window_expression(self, window_function: str, window_spec: str) -> str:
        return (
            window_function
            + self._Over
            + self._LeftParenthesis
            + window_spec
            + self._RightParenthesis
        )

    def window_spec_expression(
        self, partition_spec: List[str], order_spec: List[str], frame_spec: str
    ) -> str:
        return (
            (
                self._PartitionBy + self._Comma.join(partition_spec)
                if partition_spec
                else self._EmptyString
            )
            + (
                self._OrderBy + self._Comma.join(order_spec)
                if order_spec
                else self._EmptyString
            )
            + frame_spec
        )

    def specified_window_frame_expression(
        self, frame_type: str, lower: str, upper: str
    ) -> str:
        return (
            self._Space
            + frame_type
            + self._Between
            + lower
            + self._And
            + upper
            + self._Space
        )

    def window_frame_boundary_expression(self, offset: str, is_following: bool) -> str:
        return offset + (self._Following if is_following else self._Preceding)

    def rank_related_function_expression(
        self, func_name: str, expr: str, offset: int, default: str, ignore_nulls: bool
    ) -> str:
        return (
            func_name
            + self._LeftParenthesis
            + expr
            + self._Comma
            + str(offset)
            + self._Comma
            + default
            + self._RightParenthesis
            + (self._IgnoreNulls if ignore_nulls else self._EmptyString)
        )

    def cast_expression(
        self, child: str, datatype: DataType, try_: bool = False
    ) -> str:
        return (
            (self._TryCast if try_ else self._Cast)
            + self._LeftParenthesis
            + child
            + self._As
            + convert_to_sf_type(datatype)
            + self._RightParenthesis
        )

    def order_expression(self, name: str, direction: str, null_ordering: str) -> str:
        return name + self._Space + direction + self._Space + null_ordering

    def create_or_replace_view_statement(
        self, name: str, child: str, is_temp: bool
    ) -> str:
        return (
            self._Create
            + self._Or
            + self._Replace
            + f"{self._Temporary if is_temp else self._EmptyString}"
            + self._View
            + name
            + self._As
            + self.project_statement([], child)
        )

    def pivot_statement(
        self, pivot_column: str, pivot_values: List[str], aggregate: str, child: str
    ) -> str:
        return (
            self._Select
            + self._Star
            + self._From
            + self._LeftParenthesis
            + child
            + self._RightParenthesis
            + self._Pivot
            + self._LeftParenthesis
            + aggregate
            + self._For
            + pivot_column
            + self._In
            + self._LeftParenthesis
            + self._Comma.join(pivot_values)
            + self._RightParenthesis
            + self._RightParenthesis
        )

    def unpivot_statement(
        self, value_column: str, name_column: str, column_list: List[str], child: str
    ) -> str:
        return (
            self._Select
            + self._Star
            + self._From
            + self._LeftParenthesis
            + child
            + self._RightParenthesis
            + self._Unpivot
            + self._LeftParenthesis
            + value_column
            + self._For
            + name_column
            + self._In
            + self._LeftParenthesis
            + self._Comma.join(column_list)
            + self._RightParenthesis
            + self._RightParenthesis
        )

    def copy_into_table(
        self,
        table_name: str,
        file_path: str,
        file_format: str,
        format_type_options: Dict[str, Any],
        copy_options: Dict[str, str],
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
            self._LeftParenthesis
            + self._Comma.join(column_names)
            + self._RightParenthesis
            if column_names
            else self._EmptyString
        )
        from_str = (
            self._LeftParenthesis
            + self._Select
            + self._Comma.join(transformations)
            + self._From
            + file_path
            + self._RightParenthesis
            if transformations
            else file_path
        )
        files_str = (
            self._Files
            + self._Equals
            + self._LeftParenthesis
            + self._Comma.join(
                [self._SingleQuote + f + self._SingleQuote for f in files]
            )
            + self._RightParenthesis
            if files
            else self._EmptyString
        )
        validation_str = (
            f"{self._ValidationMode} = {validation_mode}"
            if validation_mode
            else self._EmptyString
        )
        ftostr = (
            self._FileFormat
            + self._Equals
            + self._LeftParenthesis
            + self._Type
            + self._Equals
            + file_format
        )
        if format_type_options:
            ftostr += (
                self._Space
                + self.get_options_statement(format_type_options)
                + self._Space
            )
        ftostr += self._RightParenthesis

        if copy_options:
            costr = self._Space + self.get_options_statement(copy_options) + self._Space
        else:
            costr = self._EmptyString

        return (
            self._Copy
            + self._Into
            + table_name
            + column_str
            + self._From
            + from_str
            + (
                self._Pattern + self._Equals + self.single_quote(pattern)
                if pattern
                else self._EmptyString
            )
            + files_str
            + ftostr
            + costr
            + validation_str
        )

    def copy_into_location(
        self,
        query: str,
        stage_location: str,
        partition_by: Optional[str] = None,
        file_format_name: Optional[str] = None,
        file_format_type: Optional[str] = None,
        format_type_options: Optional[Dict[str, Any]] = None,
        header: bool = False,
        **copy_options: Any,
    ):
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
            (self._PartitionBy + partition_by) if partition_by else self._EmptyString
        )
        format_name_clause = (
            (self._FormatName + self._Equals + file_format_name)
            if file_format_name
            else self._EmptyString
        )
        file_type_clause = (
            (self._Type + self._Equals + file_format_type)
            if file_format_type
            else self._EmptyString
        )
        format_type_options_clause = (
            self.get_options_statement(format_type_options)
            if format_type_options
            else self._EmptyString
        )
        file_format_clause = (
            (
                self._FileFormat
                + self._Equals
                + self._LeftParenthesis
                + (
                    format_name_clause
                    + self._Space
                    + file_type_clause
                    + self._Space
                    + format_type_options_clause
                )
                + self._RightParenthesis
            )
            if format_name_clause or file_type_clause or format_type_options_clause
            else self._EmptyString
        )
        copy_options_clause = (
            self.get_options_statement(copy_options)
            if copy_options
            else self._EmptyString
        )
        header_clause = (
            f"{self._Header}={header}" if header is not None else self._EmptyString
        )
        return (
            self._Copy
            + self._Into
            + stage_location
            + self._From
            + self._LeftParenthesis
            + query
            + self._RightParenthesis
            + partition_by_clause
            + file_format_clause
            + self._Space
            + copy_options_clause
            + self._Space
            + header_clause
        )

    def update_statement(
        self,
        table_name: str,
        assignments: Dict[str, str],
        condition: Optional[str],
        source_data: Optional[str],
    ) -> str:
        return (
            self._Update
            + table_name
            + self._Set
            + self._Comma.join([k + self._Equals + v for k, v in assignments.items()])
            + (
                (
                    self._From
                    + self._LeftParenthesis
                    + source_data
                    + self._RightParenthesis
                )
                if source_data
                else self._EmptyString
            )
            + ((self._Where + condition) if condition else self._EmptyString)
        )

    def delete_statement(
        self, table_name: str, condition: Optional[str], source_data: Optional[str]
    ) -> str:
        return (
            self._Delete
            + self._From
            + table_name
            + (
                (
                    self._Using
                    + self._LeftParenthesis
                    + source_data
                    + self._RightParenthesis
                )
                if source_data
                else self._EmptyString
            )
            + ((self._Where + condition) if condition else self._EmptyString)
        )

    def insert_merge_statement(
        self, condition: Optional[str], keys: List[str], values: List[str]
    ) -> str:
        return (
            self._When
            + self._Not
            + self._Matched
            + ((self._And + condition) if condition else self._EmptyString)
            + self._Then
            + self._Insert
            + (
                (
                    self._LeftParenthesis
                    + self._Comma.join(keys)
                    + self._RightParenthesis
                )
                if keys
                else self._EmptyString
            )
            + self._Values
            + self._LeftParenthesis
            + self._Comma.join(values)
            + self._RightParenthesis
        )

    def update_merge_statement(
        self, condition: Optional[str], assignment: Dict[str, str]
    ) -> str:
        return (
            self._When
            + self._Matched
            + ((self._And + condition) if condition else self._EmptyString)
            + self._Then
            + self._Update
            + self._Set
            + self._Comma.join([k + self._Equals + v for k, v in assignment.items()])
        )

    def delete_merge_statement(self, condition: Optional[str]) -> str:
        return (
            self._When
            + self._Matched
            + ((self._And + condition) if condition else self._EmptyString)
            + self._Then
            + self._Delete
        )

    def merge_statement(
        self, table_name: str, source: str, join_expr: str, clauses: List[str]
    ) -> str:
        return (
            self._Merge
            + self._Into
            + table_name
            + self._Using
            + self._LeftParenthesis
            + source
            + self._RightParenthesis
            + self._On
            + join_expr
            + self._EmptyString.join(clauses)
        )

    def create_temp_table_statement(self, table_name: str, schema: str) -> str:
        return (
            self._Create
            + self._Temporary
            + self._Table
            + table_name
            + self._LeftParenthesis
            + schema
            + self._RightParenthesis
        )

    def drop_table_if_exists_statement(self, table_name: str) -> str:
        return self._Drop + self._Table + self._If + self._Exists + table_name

    def drop_file_format_if_exists_statement(self, format_name: str) -> str:
        return (
            self._Drop
            + self._File
            + self._Format
            + self._If
            + self._Exists
            + format_name
        )

    def attribute_to_schema_string(self, attributes: List[Attribute]) -> str:
        return self._Comma.join(
            attr.name + self._Space + convert_to_sf_type(attr.datatype)
            for attr in attributes
        )

    def schema_value_statement(self, output: List[Attribute]) -> str:
        return self._Select + self._Comma.join(
            [
                DataTypeMapper.schema_expression(attr.datatype, attr.nullable)
                + self._As
                + self.quote_name(attr.name)
                for attr in output
            ]
        )

    def list_agg(self, col: str, delimiter: str, is_distinct: bool) -> str:
        return (
            self._ListAgg
            + self._LeftParenthesis
            + f"{self._Distinct if is_distinct else self._EmptyString}"
            + col
            + self._Comma
            + delimiter
            + self._RightParenthesis
        )

    def generator(self, row_count: int) -> str:
        return (
            self._Generator
            + self._LeftParenthesis
            + self._RowCount
            + self._RightArrow
            + str(row_count)
            + self._RightParenthesis
        )

    def table(self, content: str) -> str:
        return self._Table + self._LeftParenthesis + content + self._RightParenthesis

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
            return (
                cls._DoubleQuote + cls._escape_quotes(name.upper()) + cls._DoubleQuote
            )
        else:
            return cls._DoubleQuote + cls._escape_quotes(name) + cls._DoubleQuote

    @classmethod
    def validate_quoted_name(cls, name: str) -> str:
        if '"' in name[1:-1].replace('""', ""):
            raise SnowparkClientExceptionMessages.PLAN_ANALYZER_INVALID_IDENTIFIER(name)
        else:
            return name

    @classmethod
    def quote_name_without_upper_casing(cls, name: str) -> str:
        return cls._DoubleQuote + cls._escape_quotes(name) + cls._DoubleQuote

    @staticmethod
    def _escape_quotes(unescaped: str) -> str:
        return unescaped.replace('"', '""')

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
