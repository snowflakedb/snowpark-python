#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from src.snowflake.snowpark.internal.analyzer.snowflake_plan import SnowflakePlan
from .dataframe import DataFrame
from .row import Row

from snowflake.connector import SnowflakeConnection


class ServerConnection:

    def __init__(self, conn: SnowflakeConnection):
        self.__conn = conn
        self._cursor = None

    def close(self):
        self.__conn.close()

    def get_session_id(self):
        return self.__conn.session_id

    def run_query(self, query):
        self._cursor = self.__conn.cursor()
        results_cursor = self._cursor.execute(query)
        data = results_cursor.fetchall()
        return data

    # TODO revisit
    def result_set_to_rows(self, result_set):
        rows = [Row(row) for row in result_set]
        return rows

    def execute(self, input):
        # Handle scenarios for whether we pass queries or a DF
        if type(input) == DataFrame:
            return self.result_set_to_rows(self.get_result_set_from_DF(input))
        if type(input) == SnowflakePlan:
            return self.result_set_to_rows(self.get_result_set_from_plan(input))
        # TODO cleanup
        raise Exception("Should not have reached here. Serverconnection.execute")

    def get_result_set_from_DF(self, df):
        queries = df._get_sql_queries_for_df()
        action_id = df.session._generate_new_action_id()

        result = None
        try:
            placeholders = {}
            for query in queries:
                final_query = query
                for holder, Id in placeholders.items():
                    # TODO revisit
                    final_query = final_query.replace(holder, Id)
                if action_id < df.session.get_last_canceled_id():
                    raise Exception("Query was canceled by user")
                result = self.run_query(final_query)
                # TODO revisit
                # last_id = result.get_query_id()
                # placeholders[query.query_id_placeholder] = last_id
        finally:
            # delete create tmp object
            # TODO get plan.postActions
            pass

        return result

    def get_result_set_from_plan(self, plan):
        action_id = plan.session._generate_new_action_id()

        result = None
        try:
            placeholders = {}
            for query in plan.queries:
                final_query = query.sql
                for holder, Id in placeholders.items():
                    # TODO revisit
                    final_query = final_query.replace(holder, Id)
                if action_id < plan.session.get_last_canceled_id():
                    raise Exception("Query was canceled by user")
                result = self.run_query(final_query)
                # TODO revisit
                # last_id = result.get_query_id()
                # placeholders[query.query_id_placeholder] = last_id
        finally:
            # delete create tmp object
            # TODO get plan.postActions
            pass

        return result

    # TODO
    def get_parameter_value(self, parameter_name):
        pass

    # TODO
    def wrap_exception(self):
        pass
