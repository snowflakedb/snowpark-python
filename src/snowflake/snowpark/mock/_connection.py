#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import functools
import json
import logging
import uuid
from copy import copy
from decimal import Decimal
from logging import getLogger
from typing import IO, Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union
from unittest.mock import Mock

import snowflake.snowpark.mock._constants
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.cursor import ResultMetadata, SnowflakeCursor
from snowflake.connector.errors import NotSupportedError
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    escape_quotes,
    quote_name,
    quote_name_without_upper_casing,
    unquote_if_quoted,
)
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import SaveMode
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.utils import (
    is_in_stored_procedure,
    result_set_to_rows,
)
from snowflake.snowpark.async_job import AsyncJob, _AsyncResultType
from snowflake.snowpark.exceptions import SnowparkSessionException
from snowflake.snowpark.mock._options import pandas
from snowflake.snowpark.mock._plan import MockExecutionPlan, execute_mock_plan
from snowflake.snowpark.mock._snowflake_data_type import TableEmulator
from snowflake.snowpark.mock._stage_registry import StageEntityRegistry
from snowflake.snowpark.mock._telemetry import LocalTestOOBTelemetryService
from snowflake.snowpark.mock._util import get_fully_qualified_name
from snowflake.snowpark.mock.exceptions import SnowparkLocalTestingException
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import (
    ArrayType,
    DecimalType,
    MapType,
    VariantType,
    _IntegralType,
)

logger = getLogger(__name__)

# parameters needed for usage tracking
PARAM_APPLICATION = "application"
PARAM_INTERNAL_APPLICATION_NAME = "internal_application_name"
PARAM_INTERNAL_APPLICATION_VERSION = "internal_application_version"


class MockedSnowflakeConnection(SnowflakeConnection):
    def __init__(self, *args, **kwargs) -> None:
        # pass "application" is a trick to bypass the logic in the constructor to check input params to
        # avoid rewrite the whole logic -- "application" is not used in any place.
        super().__init__(*args, **kwargs, application="localtesting")
        self._password = None

    def connect(self, **kwargs) -> None:
        self._rest = Mock()

    def close(self, retry: bool = True) -> None:
        self._rest = None

    def is_closed(self) -> bool:
        """Checks whether the connection has been closed."""
        return self.rest is None

    @property
    def telemetry_enabled(self) -> bool:
        return False

    @telemetry_enabled.setter
    def telemetry_enabled(self, _) -> None:
        self._telemetry_enabled = False


class MockServerConnection:
    class TabularEntityRegistry:
        # Registry to store tables and views.
        def __init__(self, conn: "MockServerConnection") -> None:
            self.table_registry = {}
            self.view_registry = {}
            self.conn = conn

        def is_existing_table(self, name: Union[str, Iterable[str]]) -> bool:
            current_schema = self.conn._get_current_parameter("schema")
            current_database = self.conn._get_current_parameter("database")
            qualified_name = get_fully_qualified_name(
                name, current_schema, current_database
            )
            return qualified_name in self.table_registry

        def is_existing_view(self, name: Union[str, Iterable[str]]) -> bool:
            current_schema = self.conn._get_current_parameter("schema")
            current_database = self.conn._get_current_parameter("database")
            qualified_name = get_fully_qualified_name(
                name, current_schema, current_database
            )
            return qualified_name in self.view_registry

        def read_table(self, name: Union[str, Iterable[str]]) -> TableEmulator:
            current_schema = self.conn._get_current_parameter("schema")
            current_database = self.conn._get_current_parameter("database")
            qualified_name = get_fully_qualified_name(
                name, current_schema, current_database
            )
            if qualified_name in self.table_registry:
                return copy(self.table_registry[qualified_name])
            else:
                raise SnowparkLocalTestingException(
                    f"Object '{name}' does not exist or not authorized."
                )

        def write_table(
            self, name: Union[str, Iterable[str]], table: TableEmulator, mode: SaveMode
        ) -> Row:
            for column in table.columns:
                if not table[column].sf_type.nullable and table[column].isnull().any():
                    raise SnowparkLocalTestingException(
                        "NULL result in a non-nullable column"
                    )
            current_schema = self.conn._get_current_parameter("schema")
            current_database = self.conn._get_current_parameter("database")
            name = get_fully_qualified_name(name, current_schema, current_database)
            table = copy(table)
            if mode == SaveMode.APPEND:
                # Fix append by index
                if name in self.table_registry:
                    target_table = self.table_registry[name]

                    if len(table.columns.to_list()) != len(
                        target_table.columns.to_list()
                    ):
                        raise SnowparkLocalTestingException(
                            f"Cannot append because incoming data has different schema {table.columns.to_list()} than existing table { target_table.columns.to_list()}"
                        )

                    table.columns = target_table.columns
                    self.table_registry[name] = pandas.concat(
                        [target_table, table], ignore_index=True
                    )
                    self.table_registry[name].sf_types = target_table.sf_types
                else:
                    self.table_registry[name] = table
            elif mode == SaveMode.IGNORE:
                if name not in self.table_registry:
                    self.table_registry[name] = table
            elif mode == SaveMode.OVERWRITE:
                self.table_registry[name] = table
            elif mode == SaveMode.ERROR_IF_EXISTS:
                if name in self.table_registry:
                    raise SnowparkLocalTestingException(f"Table {name} already exists")
                else:
                    self.table_registry[name] = table
            elif mode == SaveMode.TRUNCATE:
                if name in self.table_registry:
                    target_table = self.table_registry[name]
                    input_schema = table.columns.to_list()
                    existing_schema = target_table.columns.to_list()
                    if len(input_schema) <= len(existing_schema) and (
                        all(
                            target_table[col].sf_type.nullable
                            for col in (existing_schema[len(input_schema) :])
                        )
                    ):
                        for col in existing_schema[len(input_schema) :]:
                            table[col] = None
                            table.sf_types[col] = target_table[col].sf_type
                    else:
                        raise SnowparkLocalTestingException(
                            f"Cannot truncate because incoming data has different schema {table.columns.to_list()} than existing table { target_table.columns.to_list()}"
                        )

                self.table_registry[name] = table
            else:
                raise SnowparkLocalTestingException(f"Unrecognized mode: {mode}")
            return [
                Row(status=f"Table {name} successfully created.")
            ]  # TODO: match message

        def drop_table(self, name: Union[str, Iterable[str]]) -> None:
            current_schema = self.conn._get_current_parameter("schema")
            current_database = self.conn._get_current_parameter("database")
            name = get_fully_qualified_name(name, current_schema, current_database)
            if name in self.table_registry:
                self.table_registry.pop(name)

        def create_or_replace_view(
            self, execution_plan: MockExecutionPlan, name: Union[str, Iterable[str]]
        ):
            current_schema = self.conn._get_current_parameter("schema")
            current_database = self.conn._get_current_parameter("database")
            name = get_fully_qualified_name(name, current_schema, current_database)
            self.view_registry[name] = execution_plan

        def get_review(self, name: Union[str, Iterable[str]]) -> MockExecutionPlan:
            current_schema = self.conn._get_current_parameter("schema")
            current_database = self.conn._get_current_parameter("database")
            name = get_fully_qualified_name(name, current_schema, current_database)
            if name in self.view_registry:
                return self.view_registry[name]
            raise SnowparkLocalTestingException(f"View {name} does not exist")

    def __init__(self, options: Optional[Dict[str, Any]] = None) -> None:
        self._conn = MockedSnowflakeConnection()
        self._cursor = Mock()
        self._lower_case_parameters = {}
        self.remove_query_listener = Mock()
        self.add_query_listener = Mock()
        self._telemetry_client = Mock()
        self.entity_registry = MockServerConnection.TabularEntityRegistry(self)
        self.stage_registry = StageEntityRegistry(self)
        self._conn._session_parameters = {
            "ENABLE_ASYNC_QUERY_IN_PYTHON_STORED_PROCS": False,
            "_PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS_STRING": True,
            "_PYTHON_SNOWPARK_USE_SQL_SIMPLIFIER_STRING": True,
        }
        self._options = options or {}
        self._active_account = self._options.get(
            "account", snowflake.snowpark.mock._constants.CURRENT_ACCOUNT
        )
        self._active_warehouse = self._options.get(
            "warehouse", snowflake.snowpark.mock._constants.CURRENT_WAREHOUSE
        )
        self._active_user = self._options.get(
            "user", snowflake.snowpark.mock._constants.CURRENT_USER
        )
        self._active_database = self._options.get(
            "database", snowflake.snowpark.mock._constants.CURRENT_DATABASE
        )
        self._active_role = self._options.get(
            "role", snowflake.snowpark.mock._constants.CURRENT_ROLE
        )
        self._active_schema = self._options.get(
            "schema", snowflake.snowpark.mock._constants.CURRENT_SCHEMA
        )
        self._connection_uuid = str(uuid.uuid4())
        # by default, usage telemetry is collected
        self._disable_local_testing_telemetry = self._options.get(
            "disable_local_testing_telemetry", False
        )
        self._oob_telemetry = LocalTestOOBTelemetryService.get_instance()
        if self._disable_local_testing_telemetry or is_in_stored_procedure():
            # after disabling, the log will basically be a no-op, not sending any telemetry
            self._oob_telemetry.disable()
        else:
            self._oob_telemetry.log_session_creation(self._connection_uuid)

    def log_not_supported_error(
        self,
        external_feature_name: Optional[str] = None,
        internal_feature_name: Optional[str] = None,
        error_message: Optional[str] = None,
        parameters_info: Optional[dict] = None,
        raise_error: Optional[type] = None,
        warning_logger: Optional[logging.Logger] = None,
    ):
        """
        send telemetry to oob servie, can raise error or logging a warning based upon the input

        Args:
            external_feature_name: customer facing feature name, this information is used to raise error
            internal_feature_name: optional internal api/feature name, this information is used to track internal api
            error_message: optional error message overwrite the default message
            parameters_info: optionals parameters information related to the feature
            raise_error: Set to an exception to raise exception
            warning_logger: Set logger to log a warning message
        """
        self._oob_telemetry.log_not_supported_error(
            external_feature_name=external_feature_name,
            internal_feature_name=internal_feature_name,
            parameters_info=parameters_info,
            error_message=error_message,
            connection_uuid=self._connection_uuid,
            raise_error=raise_error,
            warning_logger=warning_logger,
        )

    def _get_client_side_session_parameter(self, name: str, default_value: Any) -> Any:
        # mock implementation
        return (
            self._conn._session_parameters.get(name, default_value)
            if self._conn._session_parameters
            else default_value
        )

    def get_session_id(self) -> int:
        return 1

    def close(self) -> None:
        if self._conn:
            self._conn.close()

    def is_closed(self) -> bool:
        return self._conn.is_closed()

    def _get_current_parameter(self, param: str, quoted: bool = True) -> Optional[str]:
        try:
            name = getattr(self, f"_active_{param}", None)
            if name and len(name) >= 2 and name[0] == name[-1] == '"':
                # it is a quoted identifier, return the original value
                return name
            name = name.upper() if name is not None else name
            return (
                (
                    quote_name_without_upper_casing(name)
                    if quoted
                    else escape_quotes(name)
                )
                if name
                else None
            )
        except AttributeError:
            return None

    def _get_string_datum(self, query: str) -> Optional[str]:
        rows = result_set_to_rows(self.run_query(query)["data"])
        return rows[0][0] if len(rows) > 0 else None

    # @SnowflakePlan.Decorator.wrap_exception
    # def get_result_attributes(self, query: str) -> List[Attribute]:
    #     return convert_result_meta_to_attribute(self._cursor.describe(query))

    def upload_file(
        self,
        path: str,
        stage_location: str,
        dest_prefix: str = "",
        parallel: int = 4,
        compress_data: bool = True,
        source_compression: str = "AUTO_DETECT",
        overwrite: bool = False,
    ) -> Optional[Dict[str, Any]]:
        self.log_not_supported_error(
            external_feature_name="MockServerConnection.upload_file",
            raise_error=NotImplementedError,
        )

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
        is_in_udf: bool = False,
    ) -> Optional[Dict[str, Any]]:
        if compress_data:
            self.log_not_supported_error(
                external_feature_name="upload_stream with auto_compress=True",
                internal_feature_name="MockServerConnection.upload_stream",
                parameters_info={"compress_data": str(compress_data)},
                raise_error=NotImplementedError,
            )
        self._cursor.description = [
            ResultMetadata(
                name="source",
                type_code=2,
                display_size=None,
                internal_size=16777216,
                precision=None,
                scale=None,
                is_nullable=False,
            ),
            ResultMetadata(
                name="target",
                type_code=2,
                display_size=None,
                internal_size=16777216,
                precision=None,
                scale=None,
                is_nullable=False,
            ),
            ResultMetadata(
                name="source_size",
                type_code=0,
                display_size=None,
                internal_size=16777216,
                precision=0,
                scale=0,
                is_nullable=False,
            ),
            ResultMetadata(
                name="target_size",
                type_code=0,
                display_size=None,
                internal_size=16777216,
                precision=0,
                scale=0,
                is_nullable=False,
            ),
            ResultMetadata(
                name="source_compression",
                type_code=2,
                display_size=None,
                internal_size=16777216,
                precision=None,
                scale=None,
                is_nullable=False,
            ),
            ResultMetadata(
                name="target_compression",
                type_code=2,
                display_size=None,
                internal_size=16777216,
                precision=None,
                scale=None,
                is_nullable=False,
            ),
            ResultMetadata(
                name="status",
                type_code=2,
                display_size=None,
                internal_size=16777216,
                precision=None,
                scale=None,
                is_nullable=False,
            ),
            ResultMetadata(
                name="message",
                type_code=2,
                display_size=None,
                internal_size=16777216,
                precision=None,
                scale=None,
                is_nullable=False,
            ),
        ]
        return self.stage_registry.upload_stream(
            input_stream, stage_location, dest_filename, overwrite=overwrite
        )

    def run_query(
        self,
        query: str,
        to_pandas: bool = False,
        to_iter: bool = False,
        is_ddl_on_temp_object: bool = False,
        block: bool = True,
        data_type: _AsyncResultType = _AsyncResultType.ROW,
        async_job_plan: Optional[
            SnowflakePlan
        ] = None,  # this argument is currently only used by AsyncJob
        **kwargs,
    ) -> Union[Dict[str, Any], AsyncJob]:
        self.log_not_supported_error(
            external_feature_name="Running SQL queries",
            internal_feature_name="MockServerConnection.run_query",
            raise_error=NotImplementedError,
        )

    def _to_data_or_iter(
        self,
        results_cursor: SnowflakeCursor,
        to_pandas: bool = False,
        to_iter: bool = False,
    ) -> Dict[str, Any]:
        if to_pandas:
            try:
                data_or_iter = (
                    map(
                        functools.partial(
                            _fix_pandas_df_fixed_type, results_cursor=results_cursor
                        ),
                        results_cursor.fetch_pandas_batches(split_blocks=True),
                    )
                    if to_iter
                    else _fix_pandas_df_fixed_type(
                        results_cursor.fetch_pandas_all(split_blocks=True),
                        results_cursor,
                    )
                )
            except NotSupportedError:
                data_or_iter = (
                    iter(results_cursor) if to_iter else results_cursor.fetchall()
                )
            except KeyboardInterrupt:
                raise
            except BaseException as ex:
                raise SnowparkClientExceptionMessages.SERVER_FAILED_FETCH_PANDAS(
                    str(ex)
                )
        else:
            data_or_iter = (
                iter(results_cursor) if to_iter else results_cursor.fetchall()
            )

        return {"data": data_or_iter, "sfqid": results_cursor.sfqid}

    def execute(
        self,
        plan: MockExecutionPlan,
        to_pandas: bool = False,
        to_iter: bool = False,
        block: bool = True,
        data_type: _AsyncResultType = _AsyncResultType.ROW,
        case_sensitive: bool = True,
        **kwargs,
    ) -> Union[
        List[Row], "pandas.DataFrame", Iterator[Row], Iterator["pandas.DataFrame"]
    ]:
        if self._conn.is_closed():
            raise SnowparkSessionException(
                "Cannot perform this operation because the session has been closed.",
                error_code="1404",
            )
        if not block:
            self.log_not_supported_error(
                external_feature_name="Async job",
                internal_feature_name="MockServerConnection.execute",
                parameters_info={"block": str(block)},
                raise_error=NotImplementedError,
            )

        res = execute_mock_plan(plan, plan.expr_to_alias)
        if isinstance(res, TableEmulator):
            # stringfy the variant type in the result df
            for col in res.columns:
                if isinstance(
                    res.sf_types[col].datatype, (ArrayType, MapType, VariantType)
                ):
                    from snowflake.snowpark.mock import CUSTOM_JSON_ENCODER

                    for idx, row in res.iterrows():
                        if row[col] is not None:
                            # Snowflake sorts maps by key before serializing
                            if isinstance(row[col], dict):
                                row[col] = dict(sorted(row[col].items()))

                            res.loc[idx, col] = json.dumps(
                                row[col],
                                cls=CUSTOM_JSON_ENCODER,
                                indent=2,
                                sort_keys=True,
                            )
                        else:
                            # snowflake returns Python None instead of the str 'null' for DataType data
                            res.loc[idx, col] = (
                                "null" if idx in res._null_rows_idxs_map[col] else None
                            )

            # when setting output rows, snowpark python running against snowflake don't escape double quotes
            # in column names. while in the local testing calculation, double quotes are preserved.
            # to align with snowflake behavior, we unquote name here
            columns = [unquote_if_quoted(col_name) for col_name in res.columns]
            rows = []
            # TODO: SNOW-976145, move to index based approach to store col type mapping
            #  for now we only use the index based approach in aggregation functions
            if res.sf_types_by_col_index:
                keys = sorted(res.sf_types_by_col_index.keys())
                sf_types = [res.sf_types_by_col_index[key] for key in keys]
            else:
                sf_types = [res.sf_types[col] for col in res.columns]
            for pdr in res.itertuples(index=False, name=None):
                row_struct = (
                    Row._builder.build(*columns)
                    .set_case_sensitive(case_sensitive)
                    .to_row()
                )
                row = row_struct(
                    *[
                        Decimal("{0:.{1}f}".format(v, sf_types[i].datatype.scale))
                        if isinstance(sf_types[i].datatype, DecimalType)
                        and v is not None
                        else v
                        for i, v in enumerate(pdr)
                    ]
                )
                row._fields = columns
                rows.append(row)
        elif isinstance(res, list):
            rows = [r for r in res]

        if to_pandas:
            pandas_df = pandas.DataFrame()
            for col_name in res.columns:
                pandas_df[unquote_if_quoted(col_name)] = res[col_name].tolist()
            rows = _fix_pandas_df_fixed_type(res)

            # the following implementation is just to make DataFrame.to_pandas_batches API workable
            # in snowflake, large data result are split into multiple data chunks
            # and sent back to the client, thus it makes sense to have the generator
            # however, local testing is designed for local testing
            # we do not mock the splitting into data chunks behavior
            rows = [rows] if to_iter else rows

        if to_iter:
            return iter(rows)

        return rows

    @SnowflakePlan.Decorator.wrap_exception
    def get_result_set(
        self,
        plan: SnowflakePlan,
        to_pandas: bool = False,
        to_iter: bool = False,
        block: bool = True,
        data_type: _AsyncResultType = _AsyncResultType.ROW,
        **kwargs,
    ) -> Tuple[
        Dict[
            str,
            Union[
                List[Any],
                "pandas.DataFrame",
                SnowflakeCursor,
                Iterator["pandas.DataFrame"],
                str,
            ],
        ],
        List[ResultMetadata],
    ]:
        self.log_not_supported_error(
            external_feature_name="Running SQL queries",
            internal_feature_name="MockServerConnection.get_result_set",
            raise_error=NotImplementedError,
        )

    def get_result_and_metadata(
        self, plan: SnowflakePlan, **kwargs
    ) -> Tuple[List[Row], List[Attribute]]:
        res = execute_mock_plan(plan, plan.expr_to_alias)
        attrs = [
            Attribute(
                name=quote_name(column_name.strip()),
                datatype=column_data.sf_type
                if column_data.sf_type
                else res.sf_types[column_name],
            )
            for column_name, column_data in res.items()
        ]

        rows = []
        for i in range(len(res)):
            values = []
            for j, attr in enumerate(attrs):
                value = res.iloc[i, j]
                if (
                    isinstance(attr.datatype.datatype, DecimalType)
                    and value is not None
                ):
                    value = Decimal(
                        "{0:.{1}f}".format(value, attr.datatype.datatype.scale)
                    )
                values.append(value)
            rows.append(Row(*values))

        return rows, attrs

    def get_result_query_id(self, plan: SnowflakePlan, **kwargs) -> str:
        self.log_not_supported_error(
            external_feature_name="Running SQL queries",
            internal_feature_name="MockServerConnection.get_result_query_id",
            raise_error=NotImplementedError,
        )


def _fix_pandas_df_fixed_type(table_res: TableEmulator) -> "pandas.DataFrame":
    pd_df = pandas.DataFrame()
    for col_name in table_res.columns:
        col_sf_type = table_res.sf_types[col_name]
        pd_df_col_name = unquote_if_quoted(col_name)
        if (
            isinstance(col_sf_type.datatype, DecimalType)
            and col_sf_type.datatype.precision is not None
            and col_sf_type.datatype.scale == 0
            and not str(table_res[col_name].dtype).startswith("int")
        ):
            # this is to mock the behavior that precision is explicitly set to non-default value 38
            # optimize pd.DataFrame dtype of integer to align the behavior with live connection
            if col_sf_type.datatype.precision <= 2:
                pd_df[pd_df_col_name] = table_res[col_name].astype("int8")
            elif col_sf_type.datatype.precision <= 4:
                pd_df[pd_df_col_name] = table_res[col_name].astype("int16")
            elif col_sf_type.datatype.precision <= 8:
                pd_df[pd_df_col_name] = table_res[col_name].astype("int32")
            else:
                pd_df[pd_df_col_name] = table_res[col_name].astype("int64")
        elif isinstance(col_sf_type.datatype, _IntegralType):
            try:
                if table_res[col_name].hasnans:
                    pd_df[pd_df_col_name] = pandas.to_numeric(
                        table_res[col_name].tolist(), downcast="integer"
                    )
                else:
                    pd_df[pd_df_col_name] = table_res[col_name].astype("int64")
            except OverflowError:
                pd_df[pd_df_col_name] = pandas.to_numeric(
                    table_res[col_name].tolist(), downcast="integer"
                )
        else:
            pd_df[pd_df_col_name] = table_res[col_name].tolist()

    return pd_df
