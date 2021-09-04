#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import random
import re
from typing import Dict, List, Optional, Tuple, Union

from snowflake.snowpark.internal.analyzer.datatype_mapper import DataTypeMapper
from snowflake.snowpark.internal.analyzer.sf_attribute import Attribute
from snowflake.snowpark.internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark.internal.sp_expressions import Attribute as SPAttribute
from snowflake.snowpark.internal.utils import Utils
from snowflake.snowpark.row import Row
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from snowflake.snowpark.types.sf_types import DataType
from snowflake.snowpark.types.sp_join_types import (
    JoinType as SPJoinType,
    LeftAnti as SPLeftAnti,
    LeftSemi as SPLeftSemi,
    NaturalJoin as SPNaturalJoin,
    UsingJoin as SPUsingJoin,
)
from snowflake.snowpark.types.types_package import convert_to_sf_type


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
    _Format = " FORMAT "
    _Type = " TYPE "
    _Equals = " = "
    _FileFormat = " FILE_FORMAT "
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

    def binary_comparison(self, left: str, right: str, symbol: str) -> str:
        return left + self._Space + symbol + self._Space + right

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

    def limit_expression(self, num: int) -> str:
        return self._Limit + str(num)

    def like_expression(self, expr: str, pattern: str) -> str:
        return expr + self._Like + pattern

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
                if type(field) == str
                else str(field)
            )
            + self._RightBracket
        )

    def case_when_expression(
        self, branches: List[Tuple[str, str]], else_value: str
    ) -> str:
        return (
            self._Case
            + "".join(
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
        else:
            raise SnowparkClientExceptionMessages.PLAN_SAMPLING_NEED_ONE_PARAMETER()

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

    def values_statement(self, output: List["SPAttribute"], data: List["Row"]) -> str:
        table_name = AnalyzerPackage.random_name_for_temp_object()
        data_types = [attr.datatype for attr in output]
        names = [AnalyzerPackage.quote_name(attr.name) for attr in output]
        rows = []
        for row in data:
            cells = [
                DataTypeMapper.to_sql(value, data_type)
                for value, data_type in zip(row.to_list(), data_types)
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

    def empty_values_statement(self, output: List["SPAttribute"]):
        data = [Row.from_list([None] * len(output))]
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
        left_alias = self.random_name_for_temp_object()
        right_alias = self.random_name_for_temp_object()

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
        left_alias = self.random_name_for_temp_object()
        right_alias = self.random_name_for_temp_object()

        if type(join_type) == SPUsingJoin:
            join_sql = join_type.tpe.sql
        elif type(join_type) == SPNaturalJoin:
            join_sql = self._Natural + join_type.tpe.sql
        else:
            join_sql = join_type.sql

        # This generates sql like "USING(a, b)"
        using_condition = None
        if type(join_type) == SPUsingJoin:
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
        if type(join_type) == SPLeftSemi:
            return self.left_semi_or_anti_join_statement(
                left, right, SPLeftSemi, condition
            )
        if type(join_type) == SPLeftAnti:
            return self.left_semi_or_anti_join_statement(
                left, right, SPLeftAnti, condition
            )
        if type(join_type) == SPUsingJoin:
            if type(join_type.tpe) == SPLeftSemi:
                raise Exception(
                    "Internal error: Unexpected Using clause in left semi join"
                )
            if type(join_type.tpe) == SPLeftAnti:
                raise Exception(
                    "Internal error: Unexpected Using clause in left anti join"
                )
        return self.snowflake_supported_join_statement(
            left, right, join_type, condition
        )

    def create_table_statement(self, table_name: str, schema: str, replace: bool = False, error: bool = True) -> str:
        return self._Create + (self._Or + self._Replace if replace else self._EmptyString) + self._Table + table_name + ( self._If + self._Not + self._Exists if (not replace and not error) else self._EmptyString) + self._LeftParenthesis + schema + self._RightParenthesis

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
            self._Type + self._Equals + file_type + self.get_options_statement(options)
        )
        return (
            self._Create
            + (self._Temporary if temp else "")
            + self._File
            + self._Format
            + (self._If + self._Not + self._Exists if if_not_exist else "")
            + format_name
            + options_str
        )

    def get_options_statement(self, options: Dict[str, str]) -> str:
        return (
            self._Space
            + self._Space.join(
                k + self._Space + self._Equals + self._Space + v
                for k, v in options.items()
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
            else ""
        )
        pattern_statement = (
            (self._Pattern + self._RightArrow + self.single_quote(pattern))
            if pattern
            else ""
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

    def cast_expression(self, child: str, datatype: DataType) -> str:
        return (
            self._Cast
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
            + child
        )

    def copy_into_table(
        self,
        table_name: str,
        filepath: str,
        format: str,
        format_type_options: Dict[str, str],
        copy_options: Dict[str, str],
        pattern: str,
    ) -> str:
        """copy into <table_name> from <file_path> file_format = (type =
        <format> <format_type_options>) <copy_options>"""

        if format_type_options:
            ftostr = (
                self._Space
                + self._Space.join(f"{k}={v}" for k, v in format_type_options.items())
                + self._Space
            )
        else:
            ftostr = ""

        if copy_options:
            costr = (
                self._Space
                + self._Space.join(f"{k}={v}" for k, v in copy_options.items())
                + self._Space
            )
        else:
            costr = ""

        return (
            self._Copy
            + self._Into
            + table_name
            + self._From
            + filepath
            + (
                self._Pattern + self._Equals + self.single_quote(pattern)
                if pattern
                else self._EmptyString
            )
            + self._FileFormat
            + self._Equals
            + self._LeftParenthesis
            + self._Type
            + self._Equals
            + format
            + ftostr
            + self._RightParenthesis
            + costr
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
            raise SnowparkClientException(f"invalid identifier '{name}'")
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
    # TODO static
    def number(self, precision: int = 38, scale: int = 0) -> str:
        return (
            self._Number
            + self._LeftParenthesis
            + str(precision)
            + self._Comma
            + str(scale)
            + self._RightParenthesis
        )

    @staticmethod
    def random_name_for_temp_object() -> str:
        return f"SN_TEMP_OBJECT_{Utils.random_number()}"
