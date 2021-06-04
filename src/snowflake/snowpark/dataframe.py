#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from .column import Column
from .internal.sp_expressions import NamedExpression as SPNamedExpression, \
    ResolvedStar as SPResolvedStar, Literal as SPLiteral, Attribute as SPAttribute
from .internal.analyzer.analyzer_package import AnalyzerPackage
from .plans.logical.logical_plan import Project as SPProject, Filter as SPFilter
from .plans.logical.basic_logical_operators import Join as SPJoin
from .plans.logical.hints import JoinHint as SPJoinHint
from .types.sp_join_types import JoinType as SPJoinType, LeftSemi as SPLeftSemi, \
    LeftAnti as SPLeftAnti, UsingJoin as SPUsingJoin

from typing import List
from random import choice
import string


class DataFrame:
    NUM_PREFIX_DIGITS = 4

    def __init__(self, session=None, plan=None):
        self.session = session
        self.__plan = session.analyzer.resolve(plan)
        self.analyzer_package = AnalyzerPackage()

        # Use this to simulate scala's lazy val
        self.__placeholder_output = None

    def generate_prefix(self, prefix: str) -> str:
        alphanumeric = string.ascii_lowercase + string.digits
        return f"{prefix}_{''.join(choice(alphanumeric) for _ in range(self.NUM_PREFIX_DIGITS))}_"

    def collect(self):
        return self.session.conn.execute(self.__plan)

    def cache_result(self):
        raise Exception("Not implemented. df.cache_result()")

    # TODO - for JMV, it prints to stdout
    def explain(self):
        raise Exception("Not implemented. df.explain()")

    def to_df(self, names):
        raise Exception("Not implemented. df.to_df()")

    # TODO
    def sort(self, exprs):
        pass

    # TODO
    # apply() equivalent. subscriptable

    # TODO wrap column to python object?
    def col(self, col_name: str) -> 'Column':
        if col_name == '*':
            return Column(SPResolvedStar(self.__plan.output()))
        else:
            return Column(self.__resolve(col_name))

    def select(self, expr) -> 'DataFrame':
        if type(expr) == str:
            cols = [Column(expr)]
        elif type(expr) == list:
            cols = [e if type(e) == Column else Column(e) for e in expr]
        elif type(expr) == Column:
            cols = [expr]
        else:
            raise Exception("Select input must be str or list")

        return self.__with_plan(SPProject([c.named() for c in cols], self.__plan))

    # TODO complete. requires plan.output
    def drop(self, cols) -> 'DataFrame':
        names = []
        if type(cols) is str:
            names.append(cols)
        elif type(cols) is list:
            for c in cols:
                if type(c) is str:
                    names.append(c)
                elif type(c) is Column and isinstance(c.expression, SPNamedExpression):
                    names.append(c.expression.name)
                else:
                    raise Exception(
                        f"Could not drop column {str(c)}. Can only drop columns by name.")

        normalized = set(self.analyzer_package.quote_name(n) for n in names)
        existing = set(attr.name for attr in self.__output())
        keep_col_names = existing - normalized
        if not keep_col_names:
            raise Exception("Cannot drop all columns")
        else:
            return self.select(list(keep_col_names))

    # TODO
    def filter(self, expr) -> 'DataFrame':
        if type(expr) == str:
            column = Column(expr)
            return self.__with_plan(SPFilter(column.expression, self.__plan))
        if type(expr) == Column:
            return self.__with_plan(SPFilter(expr.expression, self.__plan))

    def where(self, expr) -> 'DataFrame':
        return self.filter(expr)

    def join(self, other, using_columns=None, join_type=None) -> 'DataFrame':
        if isinstance(other, DataFrame):
            if self is other or self.__plan is other.__plan:
                raise Exception(
                    "Joining a DataFrame to itself can lead to incorrect results due to ambiguity of column references. Instead, join this DataFrame to a clone() of itself.")

            if using_columns is not None and not isinstance(using_columns, list):
                using_columns = [using_columns]

            sp_join_type = SPJoinType.from_string('inner') if not join_type \
                else SPJoinType.from_string(join_type)

            return self.__join_dataframes(other, using_columns, sp_join_type)

        # TODO handle case where other is a TableFunction
        # if isinstance(other, TableFunction):
        #    return self.__join_dataframe_table_function(other, using_columns)
        raise Exception("Invalid type for join. Must be Dataframe")

    def __join_dataframes(self, other: 'DataFrame', using_columns: List[str],
                          join_type: SPJoinType) -> 'DataFrame':
        if type(join_type) in [SPLeftSemi, SPLeftAnti]:
            # Create a Column with expression 'true AND <expr> AND <expr> .."
            join_cond = Column(SPLiteral.create(True))
            for c in using_columns:
                quoted = self.analyzer_package.quote_name(c)
                join_cond = join_cond & (self.col(quoted) == other.col(quoted))
            return self.__join_dataframes_internal(other, join_type, join_cond)
        else:
            lhs, rhs = self.__disambiguate(self, other, join_type, using_columns)
            return self.__with_plan(
                SPJoin(lhs.__plan, rhs.__plan,
                       SPUsingJoin(join_type, using_columns), None, SPJoinHint.none()))

    def __join_dataframes_internal(self, right: 'DataFrame', join_type: SPJoinType,
                                   join_exprs: Column) -> 'DataFrame':
        (lhs, rhs) = self.__disambiguate(self, right, join_type, [])
        return self.__with_plan(
            SPJoin(lhs.__plan, rhs.__plan, join_type, join_exprs.expression, SPJoinHint.none()))

    # TODO complete function. Requires TableFunction
    def __join_dataframe_table_function(self, table_function, columns) -> 'DataFrame':
        pass

    # Utils
    def __resolve(self, col_name: str) -> SPNamedExpression:
        normalized_col_name = self.analyzer_package.quote_name(col_name)
        col = list(filter(lambda attr: attr.name == normalized_col_name, self.__output()))
        if len(col) == 1:
            return col[0].with_name(normalized_col_name)
        else:
            raise Exception(f"Cannot resolve column name {col_name}")

    @staticmethod
    def __alias_if_needed(df: 'DataFrame', c: str, prefix: str, common_col_names: List[str]):
        col = df.col(c)
        if c in common_col_names:
            unquoted = c
            while unquoted.startswith('\"') and unquoted.endswith('\"'):
                unquoted = unquoted[1:-1]
            return col.alias(f"{prefix}{unquoted}")
        else:
            return col

    def __disambiguate(self, lhs: 'DataFrame', rhs: 'DataFrame', join_type: SPJoinType,
                       using_columns: List[str]):
        # Normalize the using columns.
        normalized_using_columns = set(self.analyzer_package.quote_name(c) for c in using_columns)
        #  Check if the LHS and RHS have columns in common. If they don't just return them as-is. If
        #  they do have columns in common, alias the common columns with randomly generated l_
        #  and r_ prefixes for the left and right sides respectively.
        #  We assume the column names from the schema are normalized and quoted.
        lhs_names = set(attr.name for attr in lhs.__output())
        rhs_names = set(attr.name for attr in rhs.__output())
        common_col_names = list(lhs_names.intersection(rhs_names) - normalized_using_columns)

        if not common_col_names:
            return lhs, rhs

        if type(join_type) in [SPLeftSemi, SPLeftAnti]:
            lhs_remapped = lhs
        else:
            lhs_prefix = self.generate_prefix('l')
            lhs_remapped = lhs.select(
                [self.__alias_if_needed(lhs, name, lhs_prefix, common_col_names) for name in
                 lhs_names])

        rhs_prefix = self.generate_prefix('r')
        rhs_remapped = rhs.select(
            [self.__alias_if_needed(rhs, name, rhs_prefix, common_col_names) for name in rhs_names])
        return lhs_remapped, rhs_remapped

    def __output(self) -> List[SPAttribute]:
        if not self.__placeholder_output:
            self.__placeholder_output = self.__plan.output()
        return self.__placeholder_output

    def __with_plan(self, plan):
        return DataFrame(self.session, plan)
