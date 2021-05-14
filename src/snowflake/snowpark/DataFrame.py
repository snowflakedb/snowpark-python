#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from .Row import Row


class DataFrame:

    def __init__(self, session=None, plan=None, jvm_df=None):
        self.session = session
        self.__plan = plan
        self.__jvm_df = jvm_df

    def collect(self):
        if self.session.use_jvm_for_network:
            return [Row([row.get(i) for i in range(row.size())])
                    for row in self.__jvm_df.collect()]
        else:
            # TODO change to plan when we have separate python ASTs etc
            if self.__plan:
                return self.session.conn.execute(self.__plan)
            return self.session.conn.execute(self)

    # TODO
    def filter(self, expr):
        if self.session.use_jvm_for_plans:
            return self.__filter_with_jvm_dfs(expr)

        raise Exception("Not implemented. Filter()")

    def select(self, expr):
        if self.session.use_jvm_for_plans:
            return self.__select_with_jvm_dfs(expr)

        raise Exception("Not implemented. Select()")

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

