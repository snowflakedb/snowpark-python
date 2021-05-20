#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from .PColumn import PColumn
from .Row import Row
from .internal.Expression import NamedExpression
from .plans.logical.LogicalPlan import Project, Filter


class DataFrame:

    def __init__(self, session=None, plan=None, jvm_df=None):
        self.session = session
        self.__plan = None
        if not self.session.use_jvm_for_plans:
            self.__plan = session.analyzer.resolve(plan)
        self.__jvm_df = jvm_df

    def collect(self):
        if self.session.use_jvm_for_network:
            return [Row([row.get(i) for i in range(row.size())])
                    for row in self.__jvm_df.collect()]
        else:
            if not self.session.use_jvm_for_plans:
                return self.session.conn.execute(self.__plan)
            else:
                return self.session.conn.execute(self)

    def cache_result(self):
        if self.session.use_jvm_for_plans:
            self.__jvm_df.cacheResult()
        else:
            raise Exception("Not implemented. df.cache_result()")

    # TODO - for JMV, it prints to stdout
    def explain(self):
        if self.session.use_jvm_for_plans:
            self.__jvm_df.explainString()
        else:
            raise Exception("Not implemented. df.explain()")

    def to_df(self, names):
        if self.session.use_jvm_for_plans:
            return DataFrame(session=self.session, jvm_df=self.__jvm_df.toDF(names))
        else:
            raise Exception("Not implemented. df.to_df()")

    # TODO
    def sort(self, exprs):
        pass

    # TODO
    # apply() equivalent. subscriptable

    # TODO wrap column to python object?
    def col(self, col_name):
        if self.session.use_jvm_for_plans:
            return self.__jvm_df.col()

    def select(self, cols):
        if self.session.use_jvm_for_plans:
            return self.__select_with_jvm_dfs(cols)
        else:
            return self.__select_with_py_dfs(cols)

    def drop(self, names):
        if self.session.use_jvm_for_plans:
            return self.__drop_with_jvm_dfs(names)
        else:
            return self.__drop_with_py_dfs(names)

    # TODO
    def filter(self, expr):
        if self.session.use_jvm_for_plans:
            return self.__filter_with_jvm_dfs(expr)
        else:
            return self.__filter_with_py_dfs(expr)

    def where(self, expr):
        self.filter(expr)

    def _get_sql_queries_for_df(self):
        queries = self.session._get_queries_for_df(self.__jvm_df)
        return queries

    def __ref_scala_object(self, jvm, object_name):
        clazz = jvm.java.lang.Class.forName(object_name + "$")
        ff = clazz.getDeclaredField("MODULE$")
        o = ff.get(None)
        return o

    def __filter_with_jvm_dfs(self, expr):
        if type(expr) == str:
            j_column = self.__ref_scala_object(self.session._PSession__jvm,
                                               'com.snowflake.snowpark.Column').expr(expr)
            j_df = self.__jvm_df.filter(j_column)
            return DataFrame(session=self.session,
                             plan=self.__plan,
                             jvm_df=j_df)

    def __filter_with_py_dfs(self, condition):
        if type(condition) == str:
            column = PColumn(condition)
            return self.__with_plan(Filter(column.expression, self.__plan))

    def __select_with_jvm_dfs(self, expr):
        if type(expr) == str:
            j_column = self.__ref_scala_object(self.session._PSession__jvm,
                                               'com.snowflake.snowpark.Column').expr(expr)
            j_df = self.__jvm_df.select(j_column)
            return DataFrame(session=self.session,
                             plan=self.__plan,
                             jvm_df=j_df)
        if type(expr) == list:
            j_df = self.__jvm_df.select(expr)
            return DataFrame(session=self.session, plan=self.__plan, jvm_df=j_df)

    def __select_with_py_dfs(self, expr):
        if type(expr) == str:
            cols = [PColumn(expr)]
        elif type(expr) == list:
            cols = [e if type(e) == PColumn else PColumn(e) for e in expr]
        elif type(expr) == PColumn:
            cols = [expr]
        else:
            raise Exception("Select input must be str or list")

        return self.__with_plan(Project([c.named() for c in cols], self.__plan))

    def __drop_with_jvm_dfs(self, names):
        if type(names) == str:
            names = [names]
        j_df = self.__jvm_df.drop(names)
        return DataFrame(session=self.session, jvm_df=j_df)

    # TODO complete. requires plan.output
    def __drop_with_py_dfs(self, cols):
        names = []
        for c in cols:
            if type(c) is str:
                names.append(c)
            elif type(c) is PColumn and isinstance(c.expression, NamedExpression):
                names.append(c.expression.name)
            else:
                raise Exception(f"Could not drop column {str(c)}. Can only drop columns by name.")

        #normalized = set([quote_name(n) for n in names])
        #keep_col_names = set([ output]

    def __output(self):
        return self.__plan.output()

    def __with_plan(self, plan):
        return DataFrame(self.session, plan)
