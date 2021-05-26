#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from .datatype_mapper import DataTypeMapper

import re
import random


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
    _DoubleQuote = "\""
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

    def result_scan_statement(self, uuid_place_holder):
        return self._Select + self._Star + self._From + self._Table + self._LeftParenthesis + \
               self._ResultScan + self._LeftParenthesis + self._SingleQuote + uuid_place_holder + \
               self._SingleQuote + self._RightParenthesis + self._RightParenthesis

    def function_expression(self, name: str, children: list, is_distinct: bool):
        return name + self._LeftParenthesis + f"{self._Distinct if is_distinct else self._EmptyString}" + self._Comma.join(children) + self._RightParenthesis

    def project_statement(self, project=None, child=None, is_distinct=False):
        return self._Select + \
               f"{self._Distinct if is_distinct else ''}" + \
               f"{self._Star if not project else self._Comma.join(project)}" + \
               self._From + self._LeftParenthesis + child + self._RightParenthesis

    def filter_statement(self, condition, child):
        return self.project_statement([], child) + self._Where + condition

    def range_statement(self, start, end, step, column_name):
        range = end - start

        if range * step < 0:
            count = 0
        else:
            # TODO revisit
            count = range / step + (1 if range % step != 0 and range * step > 0 else 0)

        return self.project_statement([
            self._LeftParenthesis + self._RowNumber + self._Over + self._LeftParenthesis + self._OrderBy + self._Seq8 +
            self._RightParenthesis + self._Minus + self._One + self._RightParenthesis + self._Star + self._LeftParenthesis +
            str(step) + self._RightParenthesis + self._Plus + self._LeftParenthesis + str(
                start) + self._RightParenthesis +
            self._As + column_name
        ],
            self.table(self.generator(0 if (count < 0) else count)))

    def schema_value_statement(self, output):
        return self._Select +\
               self._Comma.join([DataTypeMapper.schema_expression(attr.data_type,attr.nullable) +
                                 self._As + self.quote_name(attr.name) for attr in output])

    def generator(self, row_count):
        return self._Generator + self._LeftParenthesis + self._RowCount + self._RightArrow + \
               str(row_count) + self._RightParenthesis

    def table(self, content):
        return self._Table + self._LeftParenthesis + content + self._RightParenthesis

    def single_quote(self, value: str):
        if value.startswith(self._SingleQuote) and value.endswith(self._SingleQuote):
            return value
        else:
            return self._SingleQuote + value + self._SingleQuote

    def quote_name(self, name: str):
        already_quoted = re.compile("^(\".+\")$")
        unquoted_case_insensitive = re.compile("^([_A-Za-z]+[_A-Za-z0-9$]*)$")
        if already_quoted.match(name):
            return name
        elif unquoted_case_insensitive.match(name):
            return self._DoubleQuote + self._escape_quotes(name.upper()) + self._DoubleQuote
        else:
            return self._DoubleQuote + self._escape_quotes(name) + self._DoubleQuote

    def quote_name_without_upper_casing(self, name):
        return self._DoubleQuote + self._escape_quotes(name) + self._DoubleQuote

    def _escape_quotes(self, unescaped):
        return unescaped.replace('\"', '\"\"')

    # Most integer types map to number(38,0)
    # https://docs.snowflake.com/en/sql-reference/
    # data-types-numeric.html#int-integer-bigint-smallint-tinyint-byteint
    # TODO static
    def number(self, precision: int = 38, scale: int = 0):
        return self._Number + self._LeftParenthesis + str(precision) + self._Comma + str(
            scale) + self._RightParenthesis

    @staticmethod
    def randomNameForTempObject():
        return f"SN_TEMP_OBJECT_${random.randint(0, pow(2, 31))}"
