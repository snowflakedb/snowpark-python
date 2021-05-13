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
            return [Row([row.get(i) for i in range(row.size())]) for row in self.__jvm_df.collect()]
        else:
            # TODO change to plan when we have separate python ASTs etc
            if self.__plan:
                return self.session.conn.execute(self.__plan)
            return self.session.conn.execute(self)

    def _get_sql_queries_for_df(self):
        queries = self.session._get_queries_for_df(self.__jvm_df)
        return queries
