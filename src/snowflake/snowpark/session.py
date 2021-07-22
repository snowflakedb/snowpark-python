#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

import datetime
import decimal
import logging
import pathlib
from array import array
from functools import reduce
from logging import getLogger
from typing import Dict, List, NamedTuple, Optional, Tuple, Union

from snowflake.connector import SnowflakeConnection

from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.dataframe_reader import DataFrameReader
from snowflake.snowpark.functions import (
    column,
    parse_json,
    to_array,
    to_date,
    to_decimal,
    to_object,
    to_time,
    to_timestamp,
    to_variant,
)
from snowflake.snowpark.internal.analyzer.analyzer_package import AnalyzerPackage
from snowflake.snowpark.internal.analyzer.sf_attribute import Attribute
from snowflake.snowpark.internal.analyzer.snowflake_plan import (
    SnowflakePlanBuilder,
    SnowflakeValues,
)
from snowflake.snowpark.internal.analyzer_obj import Analyzer
from snowflake.snowpark.internal.server_connection import ServerConnection
from snowflake.snowpark.internal.sp_expressions import (
    AttributeReference as SPAttributeReference,
)
from snowflake.snowpark.internal.utils import Utils
from snowflake.snowpark.plans.logical.basic_logical_operators import Range
from snowflake.snowpark.plans.logical.logical_plan import UnresolvedRelation
from snowflake.snowpark.row import Row
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from snowflake.snowpark.types.sf_types import (
    ArrayType,
    AtomicType,
    DateType,
    DecimalType,
    Geography,
    GeographyType,
    MapType,
    StructType,
    TimestampType,
    TimeType,
    Variant,
    VariantType,
)
from snowflake.snowpark.types.sp_data_types import StringType as SPStringType
from snowflake.snowpark.types.types_package import (
    _infer_schema_from_list,
    _merge_type,
    snow_type_to_sp_type,
)

logger = getLogger(__name__)
_active_session = None


class _SessionMeta(type):
    """The metaclass of Session is defined with builder property, such that
    we can call [[Session.builder]] to create a session instance, and disallow
    creating a builder instance from a a session instance.
    """

    @property
    def builder(cls):
        return cls._SessionBuilder()


class Session(metaclass=_SessionMeta):
    __STAGE_PREFIX = "@"

    def __init__(self, conn: ServerConnection):
        self.conn = conn
        self.__query_tag = None
        self.__classpath_URIs = {}
        self.__stage_created = False
        self._snowpark_jar_in_deps = False
        self._session_id = self.conn.get_session_id()
        self.__session_stage = "snowSession_" + str(self._session_id)
        self._session_info = f"""
"version" : {Utils.get_version()},
"python.version" : {Utils.get_python_version()},
"python.connector.version" : {Utils.get_connector_version()},
"os.name" : {Utils.get_os_name()}
"""
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
        global _active_session
        if _active_session == self:
            _active_session = None
        self.conn.close()

    def get_last_canceled_id(self):
        return self.__last_canceled_id

    # TODO fix conn and session_id
    def cancel_all(self):
        """
        Cancel all running action functions, and no effect on the future action request.
        :return: None
        """
        logger.info("Canceling all running queries")
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
            self.__classpath_URIs.pop(pathlib.Path(trimmed_path).as_uri())
        else:
            # Should be the equivalent of scala.File() here
            self.__classpath_URIs.pop(pathlib.Path(trimmed_path).as_uri())

    def set_query_tag(self, query_tag):
        self.__query_tag = query_tag

    @property
    def query_tag(self):
        return self.__query_tag

    # TODO
    def _resolve_jar_dependencies(self, stage_location):
        pass

    def _do_upload(
        self,
        uri: str,
        stage_location: str,
        dest_prefix: str = None,
        parallel=4,
        compress_data=True,
        source_compression: str = "AUTO_DETECT",
        overwrite: bool = False,
    ):
        return self.conn.upload_file(
            uri,
            stage_location,
            dest_prefix,
            parallel,
            compress_data,
            source_compression,
            overwrite,
        )

    # TODO
    def _list_files_in_stage(self, stage_location):
        pass

    def table(self, name) -> DataFrame:
        """Returns a DataFrame representing the contents of the specified table. 'name' can be a
        fully qualified identifier and must conform to the rules for a Snowflake identifier.
        """
        if type(name) == str:
            fqdn = [name]
        elif type(name) == list:
            fqdn = name
        else:
            raise TypeError("Table name should be string or list of strings.")
        for n in fqdn:
            Utils.validate_object_name(n)
        return DataFrame(self, UnresolvedRelation(fqdn))

    def sql(self, query) -> DataFrame:
        return DataFrame(session=self, plan=self.__plan_builder.query(query, None))

    @property
    def read(self) -> "DataFrameReader":
        """Returns a [[DataFrameReader]] that you can use to read data from various
        supported sources (e.g. a file in a stage) as a DataFrame."""
        return DataFrameReader(self)

    def _run_query(self, query):
        return self.conn.run_query(query)["data"]

    def get_result_attributes(self, query: str) -> List["Attribute"]:
        return self.conn.get_result_attributes(query)

    def createDataFrame(
        self,
        data: Union[List, Tuple, NamedTuple, Dict],
        schema: Optional[StructType] = None,
    ) -> DataFrame:
        """
        Creates a new DataFrame containing the specified values from the local data.
        When schema is None, the schema will be inferred from the data across all rows.
        Any type and length inconsistency across rows will be reported.
        Valid inputs:
        1. `data` can only be a list, tuple, nametuple or dict.
        2. If `data` is a 1D list and tuple, nametuple or dict, every element will constitute
           a row in the dataframe.
        3. Otherwise, `data` can only be a list or tuple of lists, tuples, nametuples or dicts,
           where every iterable in this list will constitute a row in the dataframe.
        """
        if data is None:
            raise ValueError("Data cannot be None.")

        # check the type of data
        if not isinstance(data, (list, tuple, dict)):
            raise TypeError(
                "createDataFrame() function only accepts data in List, NamedTuple,"
                " Tuple or Dict type."
            )

        # check whether data is empty
        if len(data) == 0:
            return DataFrame(self)

        # convert data to be a list of Rows
        # also checks the type of every row, which should be same across data
        rows = []
        names = None
        for row in data:
            if not row:
                rows.append(Row(None))
            elif isinstance(row, Row):
                rows.append(row)
            elif isinstance(row, dict):
                if not names:
                    names = list(row.keys())
                rows.append(Row(list(row.values())))
            elif isinstance(row, (tuple, list)):
                if hasattr(row, "_fields") and not names:  # namedtuple
                    names = list(row._fields)
                rows.append(Row(row))
            else:
                rows.append(Row([row]))

        # check the length of every row, which should be same across data
        if len({row.size() for row in rows}) != 1:
            raise ValueError("Data consists of rows with different lengths.")

        # infer the schema based on the data
        if not schema:
            schema = reduce(
                _merge_type,
                (_infer_schema_from_list(row.to_list(), names) for row in rows),
            )

        # get spark attributes and data types
        sp_attrs, data_types = [], []
        for field in schema.fields:
            sp_type = (
                SPStringType()
                if type(field.datatype)
                in [
                    VariantType,
                    ArrayType,
                    MapType,
                    GeographyType,
                    TimeType,
                    DateType,
                    TimestampType,
                ]
                else snow_type_to_sp_type(field.datatype)
            )
            sp_attrs.append(
                SPAttributeReference(
                    AnalyzerPackage.quote_name(field.name), sp_type, field.nullable
                )
            )
            data_types.append(field.datatype)

        # convert all variant/time/geography/array/map data to string
        converted = []
        for row in rows:
            converted_row = []
            for value, data_type in zip(row.to_list(), data_types):
                if value is None:
                    converted_row.append(None)
                elif type(value) == decimal.Decimal and type(data_type) == DecimalType:
                    converted_row.append(value)
                elif (
                    type(value) == datetime.datetime
                    and type(data_type) == TimestampType
                ):
                    converted_row.append(str(value))
                elif type(value) == datetime.time and type(data_type) == TimeType:
                    converted_row.append(str(value))
                elif type(value) == datetime.date and type(data_type) == DateType:
                    converted_row.append(str(value))
                elif isinstance(data_type, AtomicType):  # consider inheritance
                    converted_row.append(value)
                elif type(value) == Variant and type(data_type) == VariantType:
                    converted_row.append(value.as_json_string())
                elif type(value) == Geography and type(data_type) == GeographyType:
                    converted_row.append(value.as_geo_json())
                elif (
                    type(value) in [list, tuple, array] and type(data_type) == ArrayType
                ):
                    converted_row.append(Variant(value).as_json_string())
                elif type(value) == dict and type(data_type) == MapType:
                    converted_row.append(Variant(value).as_json_string())
                else:
                    raise SnowparkClientException(
                        "{} {} can't be converted to {}".format(
                            type(value), value, data_type.to_string()
                        )
                    )
            converted.append(Row.from_list(converted_row))

        # construct a project statement to convert string value back to variant
        project_columns = []
        for field in schema.fields:
            if type(field.datatype) == DecimalType:
                project_columns.append(
                    to_decimal(
                        column(field.name),
                        field.datatype.precision,
                        field.datatype.scale,
                    ).as_(field.name)
                )
            elif type(field.datatype) == TimestampType:
                project_columns.append(to_timestamp(column(field.name)).as_(field.name))
            elif type(field.datatype) == TimeType:
                project_columns.append(to_time(column(field.name)).as_(field.name))
            elif type(field.datatype) == DateType:
                project_columns.append(to_date(column(field.name)).as_(field.name))
            elif type(field.datatype) == VariantType:
                project_columns.append(
                    to_variant(parse_json(column(field.name))).as_(field.name)
                )
            elif type(field.datatype) == ArrayType:
                project_columns.append(
                    to_array(parse_json(column(field.name))).as_(field.name)
                )
            elif type(field.datatype) == MapType:
                project_columns.append(
                    to_object(parse_json(column(field.name))).as_(field.name)
                )
            # TODO: support geo type
            # elif type(field.data_type) == Geography:
            else:
                project_columns.append(column(field.name))

        return DataFrame(self, SnowflakeValues(sp_attrs, converted)).select(
            project_columns
        )

    def range(self, *args) -> DataFrame:
        start, step = 0, 1

        if len(args) == 3:
            start, end, step = args[0], args[1], args[2]
        elif len(args) == 2:
            start, end = args[0], args[1]
        elif len(args) == 1:
            end = args[0]
        else:
            raise ValueError(
                f"range() requires one to three arguments. {len(args)} provided."
            )

        return DataFrame(session=self, plan=Range(start, end, step))

    def getDefaultDatabase(self) -> Optional[str]:
        return self.conn.get_default_database()

    def getDefaultSchema(self) -> Optional[str]:
        return self.conn.get_default_schema()

    def getCurrentDatabase(self) -> Optional[str]:
        return self.conn.get_current_database()

    def getCurrentSchema(self) -> Optional[str]:
        return self.conn.get_current_schema()

    def getFullyQualifiedCurrentSchema(self) -> str:
        database = self.getCurrentDatabase()
        schema = self.getCurrentSchema()
        if database is None or schema is None:
            missing_item = "DATABASE" if not database else "SCHEMA"
            # TODO: SNOW-372569 Use ErrorMessage
            raise SnowparkClientException(
                "The {} is not set for the current session.".format(missing_item)
            )
        return database + "." + schema

    @staticmethod
    def _get_active_session() -> Optional["Session"]:
        return _active_session

    @staticmethod
    def _set_active_session(session: "Session") -> "Session":
        logger.info(
            "Python Snowpark Session information: {}".format(session._session_info)
        )
        global _active_session
        if _active_session:
            logger.info("Overwriting an already active session")
        _active_session = session
        return session

    # TODO complete
    def __disable_stderr(self):
        # Look into https://docs.python.org/3/library/contextlib.html#contextlib.redirect_stderr
        # and take into account that "Note that the global side effect on sys.stdout means that
        # this context manager is not suitable for use in library code and most threaded
        # applications. It also has no effect on the output of subprocesses. However, it is still
        # a useful approach for many utility scripts."
        pass

    class _SessionBuilder:
        """The SessionBuilder holds all the configuration properties
        and is used to create a Session."""

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
            # set the log level of the conncector logger to ERROR to avoid massive logging
            logging.getLogger("snowflake.connector").setLevel(logging.ERROR)
            return Session._set_active_session(
                Session(
                    ServerConnection({}, conn)
                    if conn
                    else ServerConnection(self.__options)
                )
            )
