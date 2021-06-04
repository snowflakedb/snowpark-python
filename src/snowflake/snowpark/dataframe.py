#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from .column import Column
from .row import Row
from .internal.sp_expressions import NamedExpression
from .internal.analyzer.analyzer_package import AnalyzerPackage
from .plans.logical.logical_plan import Project, Filter
from .snowpark_client_exception import SnowparkClientException


class DataFrame:

    def __init__(self, session=None, plan=None):
        self.session = session
        self.__plan = session.analyzer.resolve(plan)

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
    def col(self, col_name):
        pass

    def select(self, expr) -> 'DataFrame':
        if type(expr) == str:
            cols = [Column(expr)]
        elif type(expr) == list:
            cols = [e if type(e) == Column else Column(e) for e in expr]
        elif type(expr) == Column:
            cols = [expr]
        else:
            raise SnowparkClientException("Select input must be str or list")

        return self.__with_plan(Project([c.named() for c in cols], self.__plan))

    # TODO complete. requires plan.output
    def drop(self, cols) -> 'DataFrame':
        names = []
        if type(cols) is str:
            names.append(cols)
        elif type(cols) is list:
            for c in cols:
                if type(c) is str:
                    names.append(c)
                elif type(c) is Column and isinstance(c.expression, NamedExpression):
                    names.append(c.expression.name)
                else:
                    raise SnowparkClientException(f"Could not drop column {str(c)}. Can only drop columns by name.")

        analyzer_package = AnalyzerPackage()
        normalized = set(analyzer_package.quote_name(n) for n in names)
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
            return self.__with_plan(Filter(column.expression, self.__plan))
        if type(expr) == Column:
            return self.__with_plan(Filter(expr.expression, self.__plan))

    def where(self, expr) -> 'DataFrame':
        return self.filter(expr)

    def __output(self):
        return self.__plan.output()

    def __with_plan(self, plan):
        return DataFrame(self.session, plan)
