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
    LeftAnti as SPLeftAnti, UsingJoin as SPUsingJoin, Cross as SPCrossJoin

from typing import List
from random import choice
import string

from .snowpark_client_exception import SnowparkClientException


class DataFrame:
    __NUM_PREFIX_DIGITS = 4

    def __init__(self, session=None, plan=None):
        self.session = session
        self.__plan = session.analyzer.resolve(plan)

        # Use this to simulate scala's lazy val
        self.__placeholder_output = None

    @staticmethod
    def __generate_prefix(prefix: str) -> str:
        alphanumeric = string.ascii_lowercase + string.digits
        return f"{prefix}_{''.join(choice(alphanumeric) for _ in range(DataFrame.__NUM_PREFIX_DIGITS))}_"

    def collect(self):
        return self.session.conn.execute(self.__plan)

    # TODO
    def cache_result(self):
        raise Exception("Not implemented. df.cache_result()")

    # TODO
    def explain(self):
        raise Exception("Not implemented. df.explain()")

    def toDF(self, col_names: List[str]) -> 'DataFrame':
        """
        Creates a new DataFrame containing columns with the specified names.

        The number of column names that you pass in must match the number of columns in the existing
        DataFrame.
        :param col_names: list of new column names
        :return: a Dataframe
        """
        assert len(self.__output()) == len(col_names), \
            (f"The number of columns doesn't match. "
             f"Old column names ({len(self.__output())}): "
             f"{','.join(attr.name for attr in self.__output())}. "
             f"New column names ({len(col_names)}): {','.join(col_names)}.")

        new_cols = []
        for attr, name in zip(self.__output(), col_names):
            new_cols.append(Column(attr).alias(name))
        return self.select(new_cols)

    # TODO
    def sort(self, exprs):
        pass

    def __getitem__(self, item):
        if type(item) == str:
            return self.col(item)
        elif isinstance(item, Column):
            return self.filter(item)
        elif type(item) in [list, tuple]:
            return self.select(item)
        elif type(item) == int:
            return self.__getitem__(self.columns[item])
        else:
            raise TypeError(f"unexpected item type: {type(item)}")

    def __getattr__(self, name):
        # TODO revisit, do we want to uppercase the name, or should the user do that?
        if AnalyzerPackage.quote_name(name) not in self.columns:
            raise AttributeError(f"{self.__class__.__name__} object has no attribute {name}")
        return self.col(name)

    @property
    def columns(self) -> List[str]:
        """ Returns all column names as a list"""
        # Does not exist in scala snowpark.
        return [attr.name for attr in self.__output()]

    def col(self, col_name: str) -> 'Column':
        if col_name == '*':
            return Column(SPResolvedStar(self.__plan.output()))
        else:
            return Column(self.__resolve(col_name))

    def select(self, expr) -> 'DataFrame':
        if type(expr) == str:
            cols = [Column(expr)]
        elif type(expr) in [list, tuple]:
            cols = [e if type(e) == Column else Column(e) for e in expr]
        elif type(expr) == Column:
            cols = [expr]
        else:
            raise SnowparkClientException("Select input must be Column, str, or list")

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
                    raise SnowparkClientException(
                        f"Could not drop column {str(c)}. Can only drop columns by name.")

        normalized = set(AnalyzerPackage.quote_name(n) for n in names)
        existing = set(attr.name for attr in self.__output())
        keep_col_names = existing - normalized
        if not keep_col_names:
            raise SnowparkClientException("Cannot drop all columns")
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
        """Filters rows based on given condition. This is equivalent to calling [[filter]]. """
        return self.filter(expr)

    def join(self, other, using_columns=None, join_type=None) -> 'DataFrame':
        """ Performs a join of the specified type (`join_type`) with the current DataFrame and
        another DataFrame (`other`) on a list of columns (`using_columns`).

        The method assumes that the columns in `usingColumns` have the same meaning in the left and
        right DataFrames.

        For example:
            dfLeftJoin = df1.join(df2, "a", "left")
            dfOuterJoin = df.join(df2, ["a","b"], "outer")

        :param other: The other Dataframe to join.
        :param using_columns: A list of names of the columns, or the column objects, to use for the
        join.
        :param join_type: The type of join (e.g. "right", "outer", etc.).
        :return: a DataFrame
        """
        if isinstance(other, DataFrame):
            if self is other or self.__plan is other.__plan:
                raise Exception(
                    "Joining a DataFrame to itself can lead to incorrect results due to ambiguity of column references. Instead, join this DataFrame to a clone() of itself.")

            if type(join_type) == SPCrossJoin or \
                    (type(join_type) == str and join_type.strip().lower().replace('_', '').startswith('cross')):
                if using_columns:
                    raise Exception("Cross joins cannot take columns as input.")

            if using_columns is not None and not isinstance(using_columns, list):
                using_columns = [using_columns]

            sp_join_type = SPJoinType.from_string('inner') if not join_type \
                else SPJoinType.from_string(join_type)

            return self.__join_dataframes(other, using_columns, sp_join_type)

        # TODO handle case where other is a TableFunction
        # if isinstance(other, TableFunction):
        #    return self.__join_dataframe_table_function(other, using_columns)
        raise Exception("Invalid type for join. Must be Dataframe")

    def crossJoin(self, other: 'DataFrame'):
        """ Performs a cross join, which returns the cartesian product of the current DataFrame and
        another DataFrame (`other`).

        If the current and `right` DataFrames have columns with the same name, and you need to refer
        to one of these columns in the returned DataFrame, use the [[coll]] function
        on the current or `other` DataFrame to disambiguate references to these columns.

        For example:
        df_cross = this.crossJoin(other)
        project = df.df_cross.select([this("common_col"), other("common_col")])

        :param other The other Dataframe to join.
        :return a Dataframe
        """
        return self.__join_dataframes_internal(other, SPJoinType.from_string('cross'), None)

    def __join_dataframes(self, other: 'DataFrame', using_columns: List[str],
                          join_type: SPJoinType) -> 'DataFrame':
        if type(join_type) in [SPLeftSemi, SPLeftAnti]:
            # Create a Column with expression 'true AND <expr> AND <expr> .."
            join_cond = Column(SPLiteral.create(True))
            for c in using_columns:
                quoted = AnalyzerPackage.quote_name(c)
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
        expression = join_exprs.expression if join_exprs else None
        return self.__with_plan(
            SPJoin(lhs.__plan, rhs.__plan, join_type, expression, SPJoinHint.none()))

    # TODO complete function. Requires TableFunction
    def __join_dataframe_table_function(self, table_function, columns) -> 'DataFrame':
        pass

    # Utils
    def __resolve(self, col_name: str) -> SPNamedExpression:
        normalized_col_name = AnalyzerPackage.quote_name(col_name)
        cols = list(filter(lambda attr: attr.name == normalized_col_name, self.__output()))
        if len(cols) == 1:
            return cols[0].with_name(normalized_col_name)
        else:
            raise Exception(f"Cannot resolve column name {col_name}")

    @staticmethod
    def __alias_if_needed(df: 'DataFrame', c: str, prefix: str, common_col_names: List[str]):
        col = df.col(c)
        if c in common_col_names:
            unquoted = c.strip('"')
            return col.alias(f"{prefix}{unquoted}")
        else:
            return col

    def __disambiguate(self, lhs: 'DataFrame', rhs: 'DataFrame', join_type: SPJoinType,
                       using_columns: List[str]):
        # Normalize the using columns.
        normalized_using_columns = set(AnalyzerPackage.quote_name(c) for c in using_columns)
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
            lhs_prefix = self.__generate_prefix('l')
            lhs_remapped = lhs.select(
                [self.__alias_if_needed(lhs, name, lhs_prefix, common_col_names) for name in
                 lhs_names])

        rhs_prefix = self.__generate_prefix('r')
        rhs_remapped = rhs.select(
            [self.__alias_if_needed(rhs, name, rhs_prefix, common_col_names) for name in rhs_names])
        return lhs_remapped, rhs_remapped

    def __output(self) -> List[SPAttribute]:
        if not self.__placeholder_output:
            self.__placeholder_output = self.__plan.output()
        return self.__placeholder_output

    def __with_plan(self, plan):
        return DataFrame(self.session, plan)
