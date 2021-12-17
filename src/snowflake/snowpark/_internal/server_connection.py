#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
import functools
import time
from logging import getLogger
from typing import IO, Any, Dict, List, Optional, Union

import snowflake.connector
from snowflake.connector import SnowflakeConnection, connect
from snowflake.connector.constants import FIELD_ID_TO_NAME
from snowflake.connector.cursor import ResultMetadata
from snowflake.connector.description import PLATFORM
from snowflake.connector.errors import NotSupportedError
from snowflake.connector.network import ReauthenticationRequest
from snowflake.connector.options import pandas
from snowflake.snowpark._internal.analyzer.analyzer_package import AnalyzerPackage
from snowflake.snowpark._internal.analyzer.sf_attribute import Attribute
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    BatchInsertQuery,
    SnowflakePlan,
)
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.utils import Utils
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    TimestampType,
    TimeType,
    VariantType,
    _GeographyType,
)

logger = getLogger(__name__)

# set `paramstyle` to qmark for batch insertion
snowflake.connector.paramstyle = "qmark"

# parameters needed for usage tracking
PARAM_APPLICATION = "application"
PARAM_INTERNAL_APPLICATION_NAME = "internal_application_name"
PARAM_INTERNAL_APPLICATION_VERSION = "internal_application_version"


class ServerConnection:
    class _Decorator:
        @classmethod
        def wrap_exception(cls, func):
            def wrap(*args, **kwargs):
                # self._conn.is_closed()
                if args[0]._conn.is_closed():
                    raise SnowparkClientExceptionMessages.SERVER_SESSION_HAS_BEEN_CLOSED()
                try:
                    return func(*args, **kwargs)
                except ReauthenticationRequest as ex:
                    raise SnowparkClientExceptionMessages.SERVER_SESSION_EXPIRED(
                        ex.cause
                    )
                except Exception as ex:
                    # TODO: SNOW-363951 handle telemetry
                    raise ex

            return wrap

        @classmethod
        def log_msg_and_telemetry(cls, msg):
            def log_and_telemetry(func):
                @functools.wraps(func)
                def wrap(*args, **kwargs):
                    # TODO: SNOW-363951 handle telemetry
                    logger.info(msg)
                    start_time = time.perf_counter()
                    func(*args, **kwargs)
                    end_time = time.perf_counter()
                    duration = end_time - start_time
                    logger.info(f"Finished in {duration:.4f} secs")

                return wrap

            return log_and_telemetry

    def __init__(
        self,
        options: Dict[str, Union[int, str]],
        conn: Optional[SnowflakeConnection] = None,
    ):
        self._lower_case_parameters = {k.lower(): v for k, v in options.items()}
        self.__add_application_name()
        self._conn = conn if conn else connect(**self._lower_case_parameters)
        self._is_stored_proc = PLATFORM == "XP"
        self._cursor = self._conn.cursor()

    def __add_application_name(self):
        if PARAM_APPLICATION not in self._lower_case_parameters:
            self._lower_case_parameters[
                PARAM_APPLICATION
            ] = Utils.get_application_name()
        if PARAM_INTERNAL_APPLICATION_NAME not in self._lower_case_parameters:
            self._lower_case_parameters[
                PARAM_INTERNAL_APPLICATION_NAME
            ] = Utils.get_application_name()
        if PARAM_INTERNAL_APPLICATION_VERSION not in self._lower_case_parameters:
            self._lower_case_parameters[
                PARAM_INTERNAL_APPLICATION_VERSION
            ] = Utils.get_version()

    def close(self) -> None:
        if self._conn:
            self._conn.close()

    def is_closed(self) -> bool:
        return self._conn.is_closed()

    @_Decorator.wrap_exception
    def get_session_id(self) -> int:
        return self._conn.session_id

    def get_default_database(self) -> Optional[str]:
        return (
            AnalyzerPackage.quote_name(self._lower_case_parameters["database"])
            if "database" in self._lower_case_parameters
            else None
        )

    def get_default_schema(self) -> Optional[str]:
        return (
            AnalyzerPackage.quote_name(self._lower_case_parameters["schema"])
            if "schema" in self._lower_case_parameters
            else None
        )

    @_Decorator.wrap_exception
    def get_current_database(self) -> Optional[str]:
        database_name = self._conn.database or self._get_string_datum(
            "SELECT CURRENT_DATABASE()"
        )
        return (
            AnalyzerPackage.quote_name_without_upper_casing(database_name)
            if database_name
            else None
        )

    @_Decorator.wrap_exception
    def get_current_schema(self) -> Optional[str]:
        schema_name = self._conn.schema or self._get_string_datum(
            "SELECT CURRENT_SCHEMA()"
        )
        return (
            AnalyzerPackage.quote_name_without_upper_casing(schema_name)
            if schema_name
            else None
        )

    @_Decorator.wrap_exception
    def get_parameter_value(self, parameter_name: str) -> Optional[str]:
        # TODO: logging and running show command to get the parameter value if it's not present in connector
        return self._conn._session_parameters.get(parameter_name.upper(), None)

    def _get_string_datum(self, query: str) -> Optional[str]:
        rows = self.result_set_to_rows(self.run_query(query)["data"])
        return rows[0][0] if len(rows) > 0 else None

    @staticmethod
    def _get_data_type(column_type_name: str, precision: int, scale: int) -> DataType:
        """Convert the Snowflake logical type to the Snowpark type."""
        if column_type_name == "ARRAY":
            return ArrayType(StringType())
        if column_type_name == "VARIANT":
            return VariantType()
        if column_type_name == "OBJECT":
            return MapType(StringType(), StringType())
        if column_type_name == "GEOGRAPHY":  # not supported by python connector
            return _GeographyType()
        if column_type_name == "BOOLEAN":
            return BooleanType()
        if column_type_name == "BINARY":
            return BinaryType()
        if column_type_name == "TEXT":
            return StringType()
        if column_type_name == "TIME":
            return TimeType()
        if (
            column_type_name == "TIMESTAMP"
            or column_type_name == "TIMESTAMP_LTZ"
            or column_type_name == "TIMESTAMP_TZ"
            or column_type_name == "TIMESTAMP_NTZ"
        ):
            return TimestampType()
        if column_type_name == "DATE":
            return DateType()
        if column_type_name == "DECIMAL" or (
            column_type_name == "FIXED" and scale != 0
        ):
            if precision != 0 or scale != 0:
                if precision > DecimalType._MAX_PRECISION:
                    return DecimalType(
                        DecimalType._MAX_PRECISION,
                        scale + precision - DecimalType._MAX_SCALE,
                    )
                else:
                    return DecimalType(precision, scale)
            else:
                return DecimalType(38, 18)
        if column_type_name == "REAL":
            return DoubleType()
        if column_type_name == "FIXED" and scale == 0:
            return LongType()
        raise NotImplementedError(
            "Unsupported type: {}, precision: {}, scale: {}".format(
                column_type_name, precision, scale
            )
        )

    @staticmethod
    def convert_result_meta_to_attribute(meta: List[ResultMetadata]) -> List[Attribute]:
        attributes = []
        for column_name, type_value, _, _, precision, scale, nullable in meta:
            quoted_name = AnalyzerPackage.quote_name_without_upper_casing(column_name)
            attributes.append(
                Attribute(
                    quoted_name,
                    ServerConnection._get_data_type(
                        FIELD_ID_TO_NAME[type_value], precision, scale
                    ),
                    nullable,
                )
            )
        return attributes

    @_Decorator.wrap_exception
    def get_result_attributes(self, query: str) -> List[Attribute]:
        lowercase = query.strip().lower()
        if lowercase.startswith("put") or lowercase.startswith("get"):
            return []
        else:
            return ServerConnection.convert_result_meta_to_attribute(
                self._cursor.describe(query)
            )

    @_Decorator.log_msg_and_telemetry("Uploading file to stage")
    def upload_file(
        self,
        path: str,
        stage_location: str,
        dest_prefix: str = "",
        parallel: int = 4,
        compress_data: bool = True,
        source_compression: str = "AUTO_DETECT",
        overwrite: bool = False,
    ) -> None:
        uri = f"file://{path}"
        self.run_query(
            self.__build_put_statement(
                uri,
                stage_location,
                dest_prefix,
                parallel,
                compress_data,
                source_compression,
                overwrite,
            )
        )

    @_Decorator.log_msg_and_telemetry("Uploading stream to stage")
    def upload_stream(
        self,
        input_stream: IO[bytes],
        stage_location: str,
        dest_filename: str,
        dest_prefix: str = "",
        parallel: int = 4,
        compress_data: bool = True,
        source_compression: str = "AUTO_DETECT",
        overwrite: bool = False,
    ) -> None:
        if self._is_stored_proc:
            qualified_stage_name = Utils.normalize_stage_location(stage_location)
            dest_prefix_name = (
                dest_prefix
                if not dest_prefix or dest_prefix.startswith("/")
                else f"/{dest_prefix}"
            )
            dest_path = f"{qualified_stage_name}{dest_prefix_name if dest_prefix_name else ''}/{dest_filename}"
            self._conn.upload_stream(input_stream=input_stream, dest_path=dest_path)
            return

        uri = f"file:///tmp/placeholder/{dest_filename}"
        try:
            self.run_query(
                self.__build_put_statement(
                    uri,
                    stage_location,
                    dest_prefix,
                    parallel,
                    compress_data,
                    source_compression,
                    overwrite,
                ),
                file_stream=input_stream,
            )
        # If ValueError is raised and the stream is closed, we throw the error.
        # https://docs.python.org/3/library/io.html#io.IOBase.close
        except ValueError as ex:
            if input_stream.closed:
                raise SnowparkClientExceptionMessages.SERVER_UDF_UPLOAD_FILE_STREAM_CLOSED(
                    dest_filename
                )
            else:
                raise ex

    def __build_put_statement(
        self,
        local_path: str,
        stage_location: str,
        dest_prefix: str = "",
        parallel: int = 4,
        compress_data: bool = True,
        source_compression: str = "AUTO_DETECT",
        overwrite: bool = False,
    ) -> str:
        qualified_stage_name = Utils.normalize_stage_location(stage_location)
        dest_prefix_name = (
            dest_prefix
            if not dest_prefix or dest_prefix.startswith("/")
            else f"/{dest_prefix}"
        )
        target_path = (
            f"{qualified_stage_name}{dest_prefix_name if dest_prefix_name else ''}"
        )
        parallel_str = f"PARALLEL = {parallel}"
        compress_str = f"AUTO_COMPRESS = {str(compress_data).upper()}"
        source_compression_str = f"SOURCE_COMPRESSION = {source_compression.upper()}"
        overwrite_str = f"OVERWRITE = {str(overwrite).upper()}"
        final_statement = f"PUT {local_path} {target_path} {parallel_str} {compress_str} {source_compression_str} {overwrite_str}"
        return final_statement

    @_Decorator.wrap_exception
    def run_query(
        self, query: str, to_pandas: bool = False, **kwargs
    ) -> Dict[str, Any]:
        try:
            results_cursor = self._cursor.execute(query, **kwargs)
            logger.info(
                "Execute query [queryID: {}] {}".format(results_cursor.sfqid, query)
            )
        except Exception as ex:
            logger.error("Failed to execute query {}\n{}".format(query, ex))
            raise ex

        # fetch_pandas_all() only works for SELECT statements
        # We call fetchall() if fetch_pandas_all() fails, because
        # when the query plan has multiple queries, it will have
        # non-select statements, and it shouldn't fail if the user
        # calls toPandas() to execute the query.
        if to_pandas:
            try:
                data = results_cursor.fetch_pandas_all()
            except NotSupportedError:
                data = results_cursor.fetchall()
            except BaseException as ex:
                raise SnowparkClientExceptionMessages.SERVER_FAILED_FETCH_PANDAS(
                    str(ex)
                )
        else:
            data = results_cursor.fetchall()
        return {"data": data, "sfqid": results_cursor.sfqid}

    def result_set_to_rows(
        self, result_set: List[Any], result_meta: Optional[List[ResultMetadata]] = None
    ) -> List[Row]:
        if result_meta:
            col_names = [col.name for col in result_meta]
            rows = []
            for data in result_set:
                row = Row(*data)
                # row might have duplicated column names
                row._fields = col_names
                rows.append(row)
        else:
            rows = [Row(*row) for row in result_set]
        return rows

    def execute(
        self, plan: SnowflakePlan, to_pandas: bool = False, **kwargs
    ) -> Union[List[Row], "pandas.DataFrame"]:
        result_set, result_meta = self.get_result_set(plan, to_pandas, **kwargs)
        return (
            result_set
            if to_pandas
            else self.result_set_to_rows(result_set, result_meta)
        )

    @SnowflakePlan.Decorator.wrap_exception
    def get_result_set(
        self,
        plan: SnowflakePlan,
        to_pandas: bool = False,
        **kwargs,
    ) -> (Union[List[Any], "pandas.DataFrame"], List[ResultMetadata]):
        action_id = plan.session._generate_new_action_id()

        result, result_meta = None, None
        try:
            placeholders = {}
            for query in plan.queries:
                if isinstance(query, BatchInsertQuery):
                    self.run_batch_insert(query.sql, query.rows, **kwargs)
                else:
                    final_query = query.sql
                    for holder, id_ in placeholders.items():
                        final_query = final_query.replace(holder, id_)
                    result = self.run_query(final_query, to_pandas, **kwargs)
                    placeholders[query.query_id_place_holder] = result["sfqid"]
                    result_meta = self._cursor.description
                if action_id < plan.session._get_last_canceled_id():
                    raise SnowparkClientExceptionMessages.SERVER_QUERY_IS_CANCELLED()
        finally:
            # delete created tmp object
            for action in plan.post_actions:
                self.run_query(action, **kwargs)

        if result is None:
            raise SnowparkClientExceptionMessages.SQL_LAST_QUERY_RETURN_RESULTSET()

        return result["data"], result_meta

    def get_result_and_metadata(
        self, plan: SnowflakePlan, **kwargs
    ) -> (List[Row], List[Attribute]):
        result_set, result_meta = self.get_result_set(plan, **kwargs)
        result = self.result_set_to_rows(result_set)
        meta = ServerConnection.convert_result_meta_to_attribute(result_meta)
        return result, meta

    @_Decorator.wrap_exception
    def run_batch_insert(self, query: str, rows: List[Row], **kwargs) -> None:
        # with qmark, Python data type will be dynamically mapped to Snowflake data type
        # https://docs.snowflake.com/en/user-guide/python-connector-api.html#data-type-mappings-for-qmark-and-numeric-bindings
        params = [list(row) for row in rows]
        query_tag = (
            kwargs["_statement_params"]["QUERY_TAG"]
            if "_statement_params" in kwargs
            and "QUERY_TAG" in kwargs["_statement_params"]
            and not self._is_stored_proc
            else None
        )
        if query_tag:
            self._cursor.execute(f"alter session set query_tag='{query_tag}'")
        self._cursor.executemany(query, params)
        if query_tag:
            self._cursor.execute("alter session unset query_tag")
        logger.info(f"Execute batch insertion query %s", query)
