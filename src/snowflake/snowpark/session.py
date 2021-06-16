#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from .dataframe import DataFrame
from .server_connection import ServerConnection
from .row import Row
from .snowpark_client_exception import SnowparkClientException
from src.snowflake.snowpark.internal.analyzer.snowflake_plan import SnowflakePlanBuilder, SnowflakeValues
from src.snowflake.snowpark.internal.analyzer.sf_attribute import Attribute
from typing import (
    Dict,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)
import decimal
import datetime
from array import array
import pathlib

from snowflake.connector import SnowflakeConnection
from .plans.logical.basic_logical_operators import Range
from .plans.logical.logical_plan import UnresolvedRelation
from .internal.analyzer.analyzer_package import AnalyzerPackage
from .internal.analyzer_obj import Analyzer
from .internal.sp_expressions import AttributeReference as SPAttributeReference
from .types.sf_types import StructType, VariantType, ArrayType, MapType, GeographyType, TimeType, DateType, \
    TimestampType, DecimalType, AtomicType, Variant, Geography
from .types.types_package import snow_type_to_sp_type, _infer_schema_from_list
from .types.sp_data_types import StringType as SPStringType
from .functions import column, parse_json, to_decimal, to_timestamp, to_date, to_time, to_array, to_variant, to_object


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

    def createDataFrame(self, data: Union[List, Tuple, NamedTuple, Dict],
                        schema: Optional[StructType] = None) -> DataFrame:
        """
        Creates a new DataFrame containing the specified values.
        Currently this function only accepts data from a List, Tuple, NameTuple or Dict, and performs type and length
        check across rows.
        When schema is None, the schema will be inferred from the first row of the data.
        """
        if data is None:
            raise SnowparkClientException("Data can not be None. ")

        # check the type of data
        if not isinstance(data, (list, tuple, dict)):
            raise SnowparkClientException("createDataFrame() function only accepts data in List, NamedTuple,"
                                          " Tuple or Dict type.")

        # check whether data is empty
        if len(data) == 0:
            return DataFrame(self)

        # convert data to be a list of Rows
        # also checks the type of every row, which should be same across data
        rows = []
        names = None
        tpe = None
        for row in data:
            if not tpe:
                tpe = type(row)
            elif tpe != type(row):
                raise SnowparkClientException("Data consists of rows with different types {} and {}."
                                              .format(tpe, type(row)))
            if not row:
                rows.append(Row(None))
            elif isinstance(row, Row):
                rows.append(row)
            elif isinstance(row, dict):
                if not names:
                    names = list(row.keys())
                rows.append(Row(row.values()))
            elif isinstance(row, (tuple, list)):
                if hasattr(row, "_fields") and not names:  # namedtuple
                    names = list(row._fields)
                rows.append(Row(row))
            else:
                rows.append(Row([row]))

        # check the length of every row, which should be same across data
        if len(set(row.size() for row in rows)) != 1:
            raise SnowparkClientException("Data consists of rows with different lengths.")

        # infer the schema based the first row
        if not schema:
            schema = _infer_schema_from_list(rows[0].to_list(), names)

        # get spark attributes and data types
        sp_attrs, data_types = [], []
        for field in schema.fields:
            sp_type = SPStringType() if type(field.data_type) in \
                                        [VariantType, ArrayType, MapType, GeographyType, TimeType, DateType,
                                         TimestampType] else snow_type_to_sp_type(field.data_type)
            sp_attrs.append(SPAttributeReference(AnalyzerPackage.quote_name(field.name), sp_type, field.nullable))
            data_types.append(field.data_type)

        # convert all variant/time/geography/array/map data to string
        converted = []
        for row in rows:
            converted_row = []
            for value, data_type in zip(row.to_list(), data_types):
                if value is None:
                    converted_row.append(None)
                elif type(value) == decimal.Decimal and type(data_type) == DecimalType:
                    converted_row.append(value)
                elif type(value) == datetime.datetime and type(data_type) == TimestampType:
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
                elif type(value) == array and type(data_type) == ArrayType:
                    converted_row.append(Variant(value).as_json_string())
                elif type(value) == dict and type(data_type) == MapType:
                    converted_row.append(Variant(value).as_json_string())
                else:
                    raise SnowparkClientException("{} {} can't be converted to {}".format(type(value), value,
                                                                                          data_type.to_string()))
            converted.append(Row.from_list(converted_row))

        # construct a project statement to convert string value back to variant
        project_columns = []
        for field in schema.fields:
            if type(field.data_type) == DecimalType:
                project_columns.append(to_decimal(column(field.name), field.data_type.precision,
                                                  field.data_type.scale).as_(field.name))
            elif type(field.data_type) == TimestampType:
                project_columns.append(to_timestamp(column(field.name)).as_(field.name))
            elif type(field.data_type) == TimeType:
                project_columns.append(to_time(column(field.name)).as_(field.name))
            elif type(field.data_type) == DateType:
                project_columns.append(to_date(column(field.name)).as_(field.name))
            elif type(field.data_type) == VariantType:
                project_columns.append(to_variant(parse_json(column(field.name))).as_(field.name))
            elif type(field.data_type) == ArrayType:
                project_columns.append(to_array(parse_json(column(field.name))).as_(field.name))
            elif type(field.data_type) == MapType:
                project_columns.append(to_object(parse_json(column(field.name))).as_(field.name))
            # TODO: support geo type
            # elif type(field.data_type) == Geography:
            else:
                project_columns.append(column(field.name))

        return DataFrame(self, SnowflakeValues(sp_attrs, converted)).select(project_columns)

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
