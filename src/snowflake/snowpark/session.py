#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from .dataframe import DataFrame
from .server_connection import ServerConnection
from src.snowflake.snowpark.internal.analyzer.snowflake_plan import SnowflakePlanBuilder
from src.snowflake.snowpark.internal.analyzer.sf_attribute import Attribute
from typing import (
    List,
)
# from py4j.java_gateway import JavaGateway, GatewayParameters

import pathlib

import snowflake.connector

from .plans.logical.basic_logical_operators import Range
from .internal.analyzer_obj import Analyzer
from .plans.logical.logical_plan import UnresolvedRelation


class Session:
    __STAGE_PREFIX = "@"

    def __init__(self, config, use_jvm_for_network, use_jvm_for_plans):
        self._config = config
        self.query_tag = None
        self.__claspathURIs = {}
        self.__stage_created = False
        self._sessionId = -1  # TODO
        self._snowpark_jar_in_deps = False
        self.__session_stage = "snowSession_" + str(self._sessionId)

        self.__plan_builder = SnowflakePlanBuilder(self)

        self.__last_action_id = 0
        self.__last_canceled_id = 0

        self.analyzer = Analyzer(self)

        # TODO
        # self.factory

        # Setup SF-connector connections
        self.__init_conn(config)

        # JVM Stuff
        self.use_jvm_for_network = use_jvm_for_network
        self.use_jvm_for_plans = True if use_jvm_for_network else use_jvm_for_plans

        if self.use_jvm_for_network or self.use_jvm_for_plans:
            self.__start_jvm(config)

    def __init_conn(self, config):
        print("Connecting python-connector to SF...")
        connector_conn = snowflake.connector.connect(**config)
        self.conn = ServerConnection(connector_conn)
        self.__session_id = self.conn.get_session_id()

    # TODO should be removed eventually, same for __set_jvm_session
    def __start_jvm(self, config):
        self.__gateway = JavaGateway(
            gateway_parameters=GatewayParameters(port=25335, auto_convert=True))
        self.__jvm = self.__gateway.jvm
        global jvm
        jvm = self.__jvm
        self.__set_jvm_session(config)

    # TODO need to rewrite for SSO, keys etc.
    def __set_jvm_session(self, config):
        print("Connecting JVM to SF...")
        jvm_config = self.__jvm.java.util.HashMap()
        jvm_config["URL"] = config['url'] + ':' + str(config['port'])
        jvm_config["USER"] = config['user']
        jvm_config["PASSWORD"] = config['password']
        jvm_config["PROTOCOL"] = config['protocol']
        if 'ssl' in config:
            jvm_config['ssl'] = 'off'
        self.__jvm_session = self.__jvm.com.snowflake.snowpark.Session.builder().configs(
            jvm_config).create()

    def _generate_new_action_id(self):
        self.__last_action_id += 1
        return self.__last_action_id

    def close(self):
        # TODO revisit
        self.connection.close()

    def get_last_canceled_id(self):
        return self.__last_canceled_id

    # TODO fix conn and session_id
    def cancel_all(self):
        """
        Cancel all running action functions, and no effect on the future action request.
        :return: None
        """
        self.__last_canceled_id = self.__last_action_id
        self.conn.run_query(f"select system$$cancel_all_queries({self.conn.get_session_id()})")

    def get_dependencies(self):
        """
        Returns all the dependencies added for user defined functions. Includes any automatically
        added jars. :return: set
        """
        return set(self.__claspathURIs.keys())

    def _get_local_file_dependencies(self):
        result = set()
        for dep in self.get_dependencies():
            if not dep.startswith(self.__STAGE_PREFIX):
                result.add(dep)
        return result

    @property
    def connection(self):
        return self.conn

    # TODO
    def add_dependency(self, path):
        trimmed_path = path.strip()
        pass

    # TODO - determine what to use for each case
    def remove_dependency(self, path):
        trimmed_path = path.strip()
        if trimmed_path.startswith(self.__STAGE_PREFIX):
            self.__claspathURIs.pop(pathlib.Path(trimmed_path).as_uri())
        else:
            # Should be the equivalent of scala.File() here
            self.__claspathURIs.pop(pathlib.Path(trimmed_path).as_uri())

    def set_query_tag(self, query_tag):
        self.query_tag = query_tag

    # TODO
    def _resolve_jav_dependencies(self, stage_location):
        pass

    # TODO
    def _do_upload(self, uri, stage_location):
        pass

    # TODO
    def _list_files_in_stage(self, stage_location):
        pass

    def table(self, name) -> DataFrame:
        """ Returns a DataFrame representing the contents of the specified table. 'name' can be a
        fully qualified identifier and must conform to the rules for a Snowflake identifier.
        """
        fqdn = None
        if type(name) is str:
            fqdn = [name]
        elif type(name) is list:
            fqdn = name
        else:
            raise Exception("Table name should be str or list of strings.")
        if self.use_jvm_for_plans:
            return self.__table_with_jvm_dfs(fqdn)
        else:
            return self.__table_with_py_dfs(fqdn)

    def __table_with_jvm_dfs(self, name) -> DataFrame:
        jvm_df = self.__jvm_session.table(name)
        return DataFrame(session=self, jvm_df=jvm_df)

    def __table_with_py_dfs(self, fqdn) -> DataFrame:
        return DataFrame(self, UnresolvedRelation(fqdn))

    def sql(self, query) -> DataFrame:
        if self.use_jvm_for_plans:
            jdf = self.__jvm_session.sql(query)
            return DataFrame(session=self, jvm_df=jdf)
        else:
            # TODO entry point for non-jvm logical plans
            return DataFrame(session=self, jvm_df=None, plan=self.__plan_builder.query(query, None))

    def _run_query(self, query):
        return self.conn.run_query(query)

    def get_result_attributes(self, query: str) -> List['Attribute']:
        return self.conn.get_result_attributes(query)

    def range(self, *args) -> DataFrame:
        start, step = 0, 1

        if len(args) == 3:
            start, end, step = args[0], args[1], args[2]
        elif len(args) == 2:
            start, end = args[0], args[1]
        elif len(args) == 1:
            end = args[0]
        else:
            raise Exception(f"Range requires one to three arguments. {len(args)} provided.")

        if self.use_jvm_for_plans:
            return DataFrame(session=self, jvm_df=self.__jvm_session.range(start, end, step))
        else:
            return DataFrame(session=self, plan=Range(start, end, step))

    def _get_queries_for_df(self, jdf):
        """ Takes as input a pointer to a Scala DF in the JVM and returns the SQL queries to be
        sent to the SF backend. """
        scala_list = self.__jvm_session.queries(jdf)
        java_list = self.__jvm.scala.collection.JavaConverters.seqAsJavaList(scala_list)
        return [el for el in java_list]

    # TODO complete
    def __disable_stderr(self):
        # Look into https://docs.python.org/3/library/contextlib.html#contextlib.redirect_stderr
        # and take into account that "Note that the global side effect on sys.stdout means that
        # this context manager is not suitable for use in library code and most threaded
        # applications. It also has no effect on the output of subprocesses. However, it is still
        # a useful approach for many utility scripts."
        pass

    class SessionBuilder:
        """The SessionBuilder holds all the configuration properties
        and is used to create a Session. """

        __options = {}

        def create(self):
            self.__create_internal(None)

        # TODO complete, requires: creating Session only from Connector-connection
        def __create_internal(self, conn):
            if conn:
                return
            if not conn:
                # return setActiveSession(Session(ServerConnection(conn)))
                pass
