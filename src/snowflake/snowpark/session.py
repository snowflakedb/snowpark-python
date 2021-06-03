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
    Dict,
    List,
    Optional,
    Union,
)

import pathlib

from snowflake.connector import SnowflakeConnection
from .plans.logical.basic_logical_operators import Range
from .internal.analyzer_obj import Analyzer
from .plans.logical.logical_plan import UnresolvedRelation


class Session:
    __STAGE_PREFIX = "@"

    def __init__(self, conn: ServerConnection):
        self.conn = conn
        self.__query_tag = None
        self.__classpath_URIs = {}
        self.__stage_created = False
        self._snowpark_jar_in_deps = False
        self._session_id = self.conn.get_session_id()
        self.__session_stage = "snowSession_" + str(self._session_id)

        self.__plan_builder = SnowflakePlanBuilder(self)

        self.__last_action_id = 0
        self.__last_canceled_id = 0

        self.analyzer = Analyzer(self)

        # TODO
        # self.factory

    def _generate_new_action_id(self):
        self.__last_action_id += 1
        return self.__last_action_id

    def close(self):
        # TODO revisit
        self.conn.close()

    def get_last_canceled_id(self):
        return self.__last_canceled_id

    # TODO fix conn and session_id
    def cancel_all(self):
        """
        Cancel all running action functions, and no effect on the future action request.
        :return: None
        """
        self.__last_canceled_id = self.__last_action_id
        self.conn.run_query(f"select system$$cancel_all_queries({self._session_id})")

    def get_dependencies(self):
        """
        Returns all the dependencies added for user defined functions. Includes any automatically
        added jars. :return: set
        """
        return set(self.__classpath_URIs.keys())

    def _get_local_file_dependencies(self):
        result = set()
        for dep in self.get_dependencies():
            if not dep.startswith(self.__STAGE_PREFIX):
                result.add(dep)
        return result

    @property
    def get_python_connector_connection(self):
        return self.conn.connection

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
        self.__query_tag = query_tag

    @property
    def query_tag(self):
        return self.__query_tag

    # TODO
    def _resolve_jar_dependencies(self, stage_location):
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
        return DataFrame(self, UnresolvedRelation(fqdn))

    def sql(self, query) -> DataFrame:
        return DataFrame(session=self, plan=self.__plan_builder.query(query, None))

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

        return DataFrame(session=self, plan=Range(start, end, step))

    def get_default_database(self) -> Optional[str]:
        return self.conn.get_default_database()

    def get_default_schema(self) -> Optional[str]:
        return self.conn.get_default_schema()

    def get_current_database(self) -> Optional[str]:
        return self.conn.get_current_database()

    def get_current_schema(self) -> Optional[str]:
        return self.conn.get_current_schema()

    # TODO complete
    def __disable_stderr(self):
        # Look into https://docs.python.org/3/library/contextlib.html#contextlib.redirect_stderr
        # and take into account that "Note that the global side effect on sys.stdout means that
        # this context manager is not suitable for use in library code and most threaded
        # applications. It also has no effect on the output of subprocesses. However, it is still
        # a useful approach for many utility scripts."
        pass

    @staticmethod
    def builder():
        return Session.SessionBuilder()

    class SessionBuilder:
        """The SessionBuilder holds all the configuration properties
        and is used to create a Session. """

        def __init__(self):
            self.__options = {}

        def _remove_config(self, key: str):
            self.__options.pop(key, None)
            return self

        def config(self, key: str, value: Union[int, str]):
            self.__options[key] = value
            return self

        def configs(self, options: Dict[str, Union[int, str]]):
            self.__options = {**self.__options, **options}
            return self

        def create(self):
            return self.__create_internal(conn=None)

        def __create_internal(self, conn: Optional[SnowflakeConnection] = None):
            # TODO: log and setActiveSession
            return Session(ServerConnection({}, conn) if conn else ServerConnection(self.__options))
