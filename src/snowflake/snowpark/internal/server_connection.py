#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import functools
import time
from logging import getLogger
from typing import IO, Any, Dict, List, Optional, Tuple, Union

from snowflake.connector import SnowflakeConnection, connect
from snowflake.connector.constants import FIELD_ID_TO_NAME
from snowflake.connector.network import ReauthenticationRequest
from snowflake.snowpark.internal.analyzer.analyzer_package import AnalyzerPackage
from snowflake.snowpark.internal.analyzer.sf_attribute import Attribute
from snowflake.snowpark.internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark.internal.utils import Utils
from snowflake.snowpark.row import Row
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from snowflake.snowpark.types.sf_types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    GeographyType,
    LongType,
    MapType,
    StringType,
    TimestampType,
    TimeType,
    VariantType,
)

logger = getLogger(__name__)


class ServerConnection:
    class _Decorator:
        @classmethod
        def wrap_exception(cls, func):
            def wrap(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except ReauthenticationRequest as ex:
                    raise SnowparkClientException(
                        "Snowpark session expired, please recreate your session\n"
                        + ex.cause
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
        if conn:
            self.__conn = conn
        else:
            # TODO: SNOW-372520 Manage connection url and parameters
            if "host" not in self._lower_case_parameters:
                raise ValueError("missing required parameter host.")
            self.__conn = connect(**self._lower_case_parameters)
        self._cursor = self.__conn.cursor()

    def close(self):
        self.__conn.close()

    @property
    def connection(self):
        return self.__conn

    @_Decorator.wrap_exception
    def get_session_id(self) -> str:
        return self.__conn.session_id

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
        database_name = self.__conn.database or self._get_string_datum(
            "SELECT CURRENT_DATABASE()"
        )
        return (
            AnalyzerPackage.quote_name_without_upper_casing(database_name)
            if database_name
            else None
        )

    @_Decorator.wrap_exception
    def get_current_schema(self) -> Optional[str]:
        schema_name = self.__conn.schema or self._get_string_datum(
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
        return self.__conn._session_parameters.get(parameter_name.upper(), None)

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
            return GeographyType()
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
                if precision > DecimalType.MAX_PRECISION:
                    return DecimalType(
                        DecimalType.MAX_PRECISION,
                        scale + precision - DecimalType.MAX_SCALE,
                    )
                else:
                    return DecimalType(precision, scale)
            else:
                return DecimalType(38, 15)  # Spark 1.5.0 default
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
    def convert_result_meta_to_attribute(
        meta: List[Tuple[Any, ...]]
    ) -> List["Attribute"]:
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
    def get_result_attributes(self, query: str) -> List["Attribute"]:
        lowercase = query.strip().lower()
        if lowercase.startswith("put") or lowercase.startswith("get"):
            return []
        else:
            return ServerConnection.convert_result_meta_to_attribute(
                self._cursor.describe(query)
            )

    @_Decorator.wrap_exception
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
    ):
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

    @_Decorator.wrap_exception
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
    ):
        uri = f"file:///tmp/placeholder/{dest_filename}"
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
    def run_query(self, query, to_pandas=False, **kwargs):
        try:
            results_cursor = self._cursor.execute(query, **kwargs)
            logger.info(
                "Execute query [queryID: {}] {}".format(results_cursor.sfqid, query)
            )
        except Exception as ex:
            logger.error("Failed to execute query {}\n{}".format(query, ex))
            raise ex
        if to_pandas:
            data = results_cursor.fetch_pandas_all()
        else:
            data = results_cursor.fetchall()
        return {"data": data, "sfqid": results_cursor.sfqid}

    # TODO revisit
    def result_set_to_rows(self, result_set):
        rows = [Row(*row) for row in result_set]
        return rows

    def execute(self, plan: "SnowflakePlan", to_pandas=False, **kwargs):
        if to_pandas:
            return self.get_result_set(plan, to_pandas=True, **kwargs)
        else:
            return self.result_set_to_rows(self.get_result_set(plan))

    @SnowflakePlan.Decorator.wrap_exception
    def get_result_set(
        self,
        plan,
        to_pandas=False,
        **kwargs,
    ):
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
                    raise SnowparkClientException("Query was canceled by user")
                result = self.run_query(final_query, to_pandas, **kwargs)
                # TODO revisit
                last_id = result["sfqid"]
                placeholders[query.query_id_place_holder] = last_id
        finally:
            # delete create tmp object
            for action in plan.post_actions:
                self.run_query(action)

        return result["data"]

    def get_result_and_metadata(
        self, plan: SnowflakePlan
    ) -> (List["Row"], List["Attribute"]):
        result_set = self.get_result_set(plan)
        result = self.result_set_to_rows(result_set)
        meta = ServerConnection.convert_result_meta_to_attribute(
            self._cursor.description
        )
        return result, meta
