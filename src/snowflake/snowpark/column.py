#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from src.snowflake.snowpark.internal.analyzer.analyzer_package import AnalyzerPackage
from src.snowflake.snowpark.internal.sp_expressions import Expression as SPExpression, \
    UnresolvedAttribute as SPUnresolvedAttribute, UnresolvedStar as SPUnresolvedStar, \
    UnresolvedAlias as SPUnresolvedAlias, NamedExpression as SPNamedExpression, Alias as SPAlias, \
    Literal as SPLiteral, EqualTo as SPEqualTo, GreaterThan as SPGreaterThan, GreaterThanOrEqual \
    as SPGreaterThanOrEqual


class Column:

    def __init__(self, expr):
        if type(expr) == str:
            self.expression = self.__get_expr(expr)
        elif isinstance(expr, SPExpression):
            self.expression = expr
        else:
            raise Exception("Column ctor takes only str or expression.")

        self.analyzer_package = AnalyzerPackage()

    # TODO make subscriptable

    # Overload operators.
    def __eq__(self, other) -> 'Column':
        right = self.__to_expr(other)
        return self.with_expr(SPEqualTo(self.expression, right))

    def __gt__(self, other) -> 'Column':
        return self.with_expr(SPGreaterThan(self.expression, self.__to_expr(other)))

    def __ge__(self, other) -> 'Column':
        return self.with_expr(SPGreaterThanOrEqual(self.expression, self.__to_expr(other)))

    def named(self) -> SPExpression:
        if isinstance(self.expression, SPNamedExpression):
            return self.expression
        else:
            return SPUnresolvedAlias(self.expression, None)

    def getName(self) -> str:
        """Returns the column name if it has one."""
        if isinstance(self.expression, SPNamedExpression):
            return self.expression.name
        else:
            return None

    # TODO revisit toString() functionality
    def __repr__(self):
        return f"Column[{self.expression.toString()}]"

    def alias(self, alias: str) -> 'Column':
        """Returns a new renamed Column. Alias of [[name]]."""
        return self.name(alias)

    def name(self, alias) -> 'Column':
        """Returns a new renamed Column."""
        return self.with_expr(SPAlias(self.expression, self.analyzer_package.quote_name(alias)))

    @staticmethod
    def __to_expr(expr) -> SPExpression:
        if type(expr) is Column:
            return expr.expression
        # TODO revisit: instead of doing SPLit(exp).expr we check if SPExpression and return that
        # or crate an SPLiteral expression
        elif isinstance(expr, SPExpression):
            return expr
        else:
            return SPLiteral.create(expr)

    @classmethod
    def expr(cls, e):
        return cls(SPUnresolvedAttribute.quoted(e))

    @staticmethod
    def __get_expr(name):
        if name == "*":
            return SPUnresolvedStar(None)
        elif name.endswith('.*'):
            parts = SPUnresolvedAttribute.parse_attribute_name(name[0:-2])
            return SPUnresolvedStar(parts)
        else:
            return SPUnresolvedAttribute.quoted(name)

    @classmethod
    def with_expr(cls, new_expr):
        return cls(new_expr)

# TODO
# class CaseExp(Column):
#
