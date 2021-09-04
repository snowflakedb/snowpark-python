#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import datetime
import decimal
import logging
import os
from array import array
from functools import reduce
from logging import getLogger
from typing import Dict, List, NamedTuple, Optional, Tuple, Union

import cloudpickle

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
from snowflake.snowpark.udf import UDFRegistration

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
        self.__import_paths: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
        self.__cloudpickle_path = {
            os.path.dirname(cloudpickle.__file__): ("cloudpickle", None)
        }
        self.__session_id = self.conn.get_session_id()
        self._session_info = f"""
"version" : {Utils.get_version()},
"python.version" : {Utils.get_python_version()},
"python.connector.version" : {Utils.get_connector_version()},
"python.connector.session.id" : {self.__session_id},
"os.name" : {Utils.get_os_name()}
"""
        self.__session_stage = f"snowSession_{self.__session_id}"
        self.__stage_created = False
        self.__udf_registration = None
        self.__plan_builder = SnowflakePlanBuilder(self)

        self.__last_action_id = 0
        self.__last_canceled_id = 0

        self.__temp_objects_map = {}

        self.analyzer = Analyzer(self)


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

    def cancel_all(self):
        """
        Cancel all running action functions, and no effect on the future action request.
        :return: None
        """
        logger.info("Canceling all running queries")
        self.__last_canceled_id = self.__last_action_id
        self.conn.run_query(f"select system$$cancel_all_queries({self.__session_id})")

    def getImports(self) -> List[str]:
        """
        Returns all imports added for user defined functions.
        """
        return list(self.__import_paths.keys())

    def _get_local_imports(self) -> List[str]:
        return [
            dep for dep in self.getImports() if not dep.startswith(self.__STAGE_PREFIX)
        ]

    def getPythonConnectorConnection(self) -> SnowflakeConnection:
        return self.conn.connection

    def addImports(
        self,
        *paths: Union[str, List[str]],
        import_as: Optional[Union[str, List[str]]] = None,
    ):
        """
        Registers file(s) in stage or local file(s) as imports of a user-defined function
        (UDF). The local file can be a compressed file (e.g., zip), a Python file (.py),
        a directory, or any other file resource.

        Args:
            paths: The paths of local files or remote files in the stage. In each case,

                1. if the path points to a local file, this file will be uploaded to the
                stage where the UDF is registered and Snowflake will import the file when
                executing that UDF.

                2. if the path points to a local directory, the directory will be compressed
                as a zip file and will be uploaded to the stage where the UDF is registered
                and Snowflake will import the file when executing that UDF.

                3. if the path points to a file in a stage, the file will be included in the
                imports when executing a UDF.
            import_as: The relative Python import paths in a UDF, as a :class:`str` or a list
                of :class:`str`. If it is not provided or it is None, the UDF will import it
                directly without any leading package/module. This argument will become a no-op
                if the path points to a stage file or a non-Python (.py) local file.

        Examples::

            # import a local file
            session.addImports(“/tmp/my_dir/my_module.py”)
            @udf
            def f():
                from my_module import g
                return g()

            # import a local file with `import_as`
            session.addImports(“/tmp/my_dir/my_module.py”, import_as="my_dir.my_module")
            @udf
            def f():
                from my_dir.my_module import g
                return g()

            # import a stage file
            session.addImports(“@stage/test.py”)

        Note:
            1. In favor of the lazy execution, the file will not be uploaded to the stage
            immediately, and it will be uploaded when a UDF is created.

            2. Snowpark library calculates a checksum for every file/directory.
            If there is a file or directory existing in the stage, Snowpark library will
            compare their checksums to determine whether it should be overwritten.
            Therefore, after uploading a local file to the stage, if the user makes
            some changes on this file and intends to upload it again, just call this
            function with the file path again, the existing file in the stage will be
            overwritten.
        """
        # parse arguments to the lists and do some simple sanity checks
        trimmed_paths = [p.strip() for p in Utils.parse_positional_args_to_list(*paths)]
        if import_as:
            if type(import_as) == str:
                import_as_list = [import_as.strip()]
            elif type(import_as) in (list, tuple):
                import_as_list = [i.strip() for i in import_as]
            else:
                raise TypeError(
                    f"import_as can only be str or list, but got {type(import_as)}"
                )
            if len(trimmed_paths) != len(import_as_list):
                raise ValueError(
                    f"The length of paths ({len(trimmed_paths)}) should be same with "
                    f"the length of import_as ({len(import_as_list)})."
                )
        else:
            import_as_list = [None] * len(trimmed_paths)

        for i in range(len(trimmed_paths)):
            path = trimmed_paths[i]
            if not path.startswith(self.__STAGE_PREFIX):
                if not os.path.exists(path):
                    raise FileNotFoundError(f"{path} is not found")
                if not os.path.isfile(path) and not os.path.isdir(path):
                    raise ValueError(
                        f"addImports() only accepts a local file or directory,"
                        f" or a file in a stage, but got {path}"
                    )
                abs_path = os.path.abspath(path)

                # convert the Python import path to the file path
                # and extract the leading path, where
                # absolute path = [leading path]/[parsed file path of Python import path]
                if import_as_list[i] is not None:
                    # the import path only works for the directory and the Python file
                    if os.path.isdir(abs_path):
                        import_as_path = import_as_list[i].replace(".", "/")
                    elif os.path.isfile(abs_path) and abs_path.endswith(".py"):
                        import_as_path = f"{import_as_list[i].replace('.', '/')}.py"
                    else:
                        import_as_path = None
                    if import_as_path:
                        if abs_path.endswith(import_as_path):
                            leading_path = abs_path[: -len(import_as_path)]
                        else:
                            raise ValueError(
                                f"import_as {import_as_list[i]} is invalid "
                                f"because it's not a part of path {abs_path}"
                            )
                    else:
                        leading_path = None
                else:
                    leading_path = None

                self.__import_paths[abs_path] = (
                    # Include the information about import path to the checksum
                    # calculation, so if the import path changes, the checksum
                    # will change and the file in the stage will be overwritten.
                    Utils.calculate_md5(abs_path, additional_info=leading_path),
                    leading_path,
                )
            else:
                self.__import_paths[path] = (None, None)

    def removeImports(self, *paths: Union[str, List[str]]):
        """
        Removes file(s) in stage or local file(s) from imports of a user-defined function (UDF).

        Args:
            paths: a list paths pointing to local files or remote files in the stage

        Examples::

            session.removeImports(“/tmp/dir1/test.py”)
            session.removeImports(“/tmp/dir1”)
            session.removeImports(“@stage/test.py”)
        """
        trimmed_paths = [p.strip() for p in Utils.parse_positional_args_to_list(*paths)]
        for path in trimmed_paths:
            abs_path = (
                os.path.abspath(path)
                if not path.startswith(self.__STAGE_PREFIX)
                else path
            )
            if abs_path not in self.__import_paths:
                raise KeyError(f"{abs_path} is not found in the existing imports")
            else:
                self.__import_paths.pop(abs_path)

    def clearImports(self):
        """
        Clears file(s) in stage or local file(s) from imports of a user-defined function (UDF).

        Example::

            session.clearImports()
        """
        self.__import_paths.clear()

    def _resolve_imports(self, stage_location: str) -> List[str]:
        """Resolve the imports and upload local files (if any) to the stage."""
        resolved_stage_files = []
        stage_file_list = self._list_files_in_stage(stage_location)
        normalized_stage_location = Utils.normalize_stage_location(stage_location)

        # always import cloudpickle
        import_paths = {**self.__import_paths, **self.__cloudpickle_path}
        for path, (prefix, leading_path) in import_paths.items():
            # stage file
            if path.startswith(self.__STAGE_PREFIX):
                resolved_stage_files.append(path)
            else:
                filename = (
                    f"{os.path.basename(path)}.zip"
                    if os.path.isdir(path) or path.endswith(".py")
                    else os.path.basename(path)
                )
                filename_with_prefix = f"{prefix}/{filename}"
                if any(
                    filename_with_prefix in stage_file for stage_file in stage_file_list
                ):
                    logger.info(
                        f"{filename} exists on {normalized_stage_location}, skipped"
                    )
                else:
                    # local directory or .py file
                    if os.path.isdir(path) or path.endswith(".py"):
                        input_stream = Utils.zip_file_or_directory_to_stream(
                            path, leading_path, add_init_py=True
                        )
                        self.conn.upload_stream(
                            input_stream=input_stream,
                            stage_location=normalized_stage_location,
                            dest_filename=filename,
                            dest_prefix=prefix,
                            compress_data=False,
                            overwrite=True,
                        )
                    # local file
                    else:
                        self.conn.upload_file(
                            path=path,
                            stage_location=normalized_stage_location,
                            dest_prefix=prefix,
                            compress_data=False,
                            overwrite=True,
                        )
                resolved_stage_files.append(
                    f"{normalized_stage_location}/{filename_with_prefix}"
                )

        return resolved_stage_files

    def _list_files_in_stage(self, stage_location: Optional[str] = None) -> List[str]:
        normalized = Utils.normalize_stage_location(
            stage_location if stage_location else self.__session_stage
        )

        # TODO: get the prefix length of normalized stage string
        return [
            row.get_string(0)
            for row in self.sql(f"ls {normalized}").select('"name"').collect()
        ]

    def set_query_tag(self, query_tag):
        self.__query_tag = query_tag

    @property
    def query_tag(self):
        return self.__query_tag

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

    def _get_result_attributes(self, query: str) -> List["Attribute"]:
        return self.conn.get_result_attributes(query)

    def getSessionStage(self) -> str:
        """
        Returns the name of the temporary stage created by the Snowpark library for uploading and
        store temporary artifacts for this session. These artifacts include libraries and packages
        for UDFs that you define in this session via [[addImports]] or [[addRequirements]].
        """
        qualified_stage_name = (
            f"{self.getFullyQualifiedCurrentSchema()}.{self.__session_stage}"
        )
        if not self.__stage_created:
            self._run_query(
                f"create temporary stage if not exists {qualified_stage_name}"
            )
            self.__stage_created = True
        return f"@{qualified_stage_name}"

    def createDataFrame(
        self,
        data: Union[List, Tuple, NamedTuple, Dict],
        schema: Optional[StructType] = None,
    ) -> DataFrame:
        """Creates a new DataFrame containing the specified values from the local
        data. When `schema` is None, the schema will be inferred from the data across
        all rows. Any type and length inconsistency across rows will be reported.


        Valid inputs:

        1. `data` can only be a list, tuple, nametuple or dict.
        2. If `data` is a 1D list and tuple, nametuple or dict, every element will constitute a row in the dataframe.
        3. Otherwise, `data` can only be a list or tuple of lists, tuples, nametuples or dicts, where every iterable in this list will constitute a row in the dataframe.
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

    @property
    def udf(self) -> UDFRegistration:
        """Returns a [[UDFRegistration]] object that you can use to register UDFs."""
        if not self.__udf_registration:
            self.__udf_registration = UDFRegistration(self)
        return self.__udf_registration

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

    def __record_temp_object(self, temp_object_type, name:str):
        # Make the name fully qualified by prepending database and schema to the name.
        multipart_identifier = name.split(".")
        if len(multipart_identifier) == 1:
            multipart_identifier = [self.getCurrentSchema()] + multipart_identifier
        if len(multipart_identifier) == 2:
            multipart_identifier = [self.getCurrentDatabase()] + multipart_identifier

        fully_qualified_name = ".".join(multipart_identifier)
        self.__temp_objects_map[fully_qualified_name] = temp_object_type

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
