#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from copy import copy
from logging import getLogger
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

from snowflake.snowpark.mock._options import pandas

from snowflake.connector.cursor import ResultMetadata
from snowflake.snowpark._internal.analyzer.analyzer_utils import unquote_if_quoted
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    SnowflakePlan,
    PlanQueryType,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    SaveMode,
    SnowflakeCreateTable,
)
from snowflake.snowpark._internal.analyzer.table_merge_expression import TableUpdate
from snowflake.snowpark._internal.analyzer.unary_plan_node import CreateViewCommand
from snowflake.snowpark._internal.server_connection import DEFAULT_STRING_SIZE
from snowflake.snowpark._internal.utils import result_set_to_rows
from snowflake.snowpark.async_job import _AsyncResultType
from snowflake.snowpark.mock._connection import MockServerConnection
from snowflake.snowpark.mock._nop_plan import NopExecutionPlan
from snowflake.snowpark.mock._telemetry import LocalTestOOBTelemetryService
from snowflake.snowpark.mock._util import get_fully_qualified_name
from snowflake.snowpark.mock.exceptions import SnowparkLocalTestingException
from snowflake.snowpark.row import Row, canonicalize_field
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    GeographyType,
    GeometryType,
    MapType,
    NullType,
    StringType,
    StructType,
    TimestampType,
    TimeType,
    VariantType,
    _NumericType,
)

if TYPE_CHECKING:
    try:
        from snowflake.connector.cursor import ResultMetadataV2
    except ImportError:
        ResultMetadataV2 = ResultMetadata

logger = getLogger(__name__)


class NopConnection(MockServerConnection):
    class NopEntityRegistry(MockServerConnection.TabularEntityRegistry):
        def read_table(self, name: Union[str, Iterable[str]]) -> LogicalPlan:
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
            self,
            name: Union[str, Iterable[str]],
            table: LogicalPlan,
            mode: SaveMode,
            column_names: Optional[List[str]] = None,
        ) -> List[Row]:
            current_schema = self.conn._get_current_parameter("schema")
            current_database = self.conn._get_current_parameter("database")
            name = get_fully_qualified_name(name, current_schema, current_database)
            self.table_registry[name] = table
            return [Row(status=f"Table {name} successfully created.")]

    def __init__(self, options: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(options)
        self._conn._session_parameters = {
            "ENABLE_ASYNC_QUERY_IN_PYTHON_STORED_PROCS": False,
            "_PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS_STRING": False,
            "_PYTHON_SNOWPARK_USE_SQL_SIMPLIFIER_STRING": False,
            "PYTHON_SNOWPARK_GENERATE_MULTILINE_QUERIES": True,
        }
        self._disable_local_testing_telemetry = True
        self._oob_telemetry = LocalTestOOBTelemetryService.get_instance()
        self._oob_telemetry.disable()
        self._suppress_not_implemented_error = True
        self.entity_registry = NopConnection.NopEntityRegistry(self)

    def execute(
        self,
        plan: NopExecutionPlan,
        to_pandas: bool = False,
        to_iter: bool = False,
        block: bool = True,
        data_type: _AsyncResultType = _AsyncResultType.ROW,
        case_sensitive: bool = True,
        **kwargs,
    ) -> Union[
        List[Row], "pandas.DataFrame", Iterator[Row], Iterator["pandas.DataFrame"]
    ]:
        source_plan = plan.source_plan

        if hasattr(source_plan, "execution_queries"):
            # If temp read-only table, explicitly create it.
            # This occurs when code such as to_snowpark_pandas is run where the Snowpark version of the table is
            # cloned and then read.
            from snowflake.snowpark.mock import TableEmulator

            for plan_query_type, query in source_plan.execution_queries.items():
                if query:
                    query_sql = query[0].sql
                    if (
                        plan_query_type == PlanQueryType.QUERIES
                        and "TEMPORARY READ ONLY TABLE" in query_sql
                    ):
                        temp_table_name = query_sql.split("TEMPORARY READ ONLY TABLE ")[
                            1
                        ].split(" ")[0]
                        self.entity_registry.write_table(
                            temp_table_name,
                            TableEmulator({"A": [1], "B": [1], "C": [1]}),
                            SaveMode.IGNORE,
                        )

        if isinstance(source_plan, SnowflakeCreateTable):
            result = self.entity_registry.write_table(
                source_plan.table_name,
                source_plan,
                source_plan.mode,
                column_names=source_plan.column_names,
            )
        elif isinstance(source_plan, CreateViewCommand):
            result = self.entity_registry.create_or_replace_view(
                source_plan, source_plan.name, source_plan.replace
            )
        else:
            result_meta = []
            result_values = []
            value = 0
            for col in plan.attributes:
                data_type = (
                    _NumericType()
                    if isinstance(source_plan, TableUpdate)
                    else col.datatype
                )
                is_nullable = col.nullable
                if is_nullable:
                    if isinstance(
                        data_type,
                        (
                            NullType,
                            ArrayType,
                            MapType,
                            StructType,
                            GeographyType,
                            GeometryType,
                        ),
                    ):
                        value = None

                if isinstance(data_type, _NumericType):
                    value = 0
                elif isinstance(data_type, StringType):
                    value = "a"
                elif isinstance(data_type, BinaryType):
                    value = ""
                elif isinstance(data_type, DateType):
                    value = "date('2020-9-16')"
                elif isinstance(data_type, BooleanType):
                    value = True
                elif isinstance(data_type, TimeType):
                    value = "to_time('04:15:29.999')"
                elif isinstance(data_type, TimestampType):
                    value = "to_timestamp('2020-09-16 06:30:00')"
                elif isinstance(data_type, ArrayType):
                    value = []
                elif isinstance(data_type, MapType):
                    value = {}
                elif isinstance(data_type, VariantType):
                    value = {}
                elif isinstance(data_type, GeographyType):
                    value = "to_geography('POINT(-122.35 37.55)')"
                elif isinstance(data_type, GeometryType):
                    value = "to_geometry('POINT(-122.35 37.55)')"
                else:
                    pass

                result_meta.append(
                    ResultMetadata(
                        name=col.name
                        if case_sensitive
                        else unquote_if_quoted(canonicalize_field(col.name)),
                        type_code=2,
                        display_size=None,
                        internal_size=DEFAULT_STRING_SIZE,
                        precision=None,
                        scale=None,
                        is_nullable=col.nullable,
                    )
                )
                result_values.append(value)

            result_row = result_set_to_rows(
                [tuple(result_values)], result_meta, case_sensitive
            )

            # Create a dummy single row DataFrame with the expected schema.  Note that the schema
            # attributes is a based on best effort based on most common operators but won't work for
            # things like dynamic pivot and possibly other operators.
            if to_pandas:
                result = pandas.DataFrame(
                    [[v for v in result_row[0]]],
                    columns=[rm.name for rm in result_meta],
                )
            else:
                result = result_row

        self.notify_mock_query_record_listener(**kwargs)
        return result

    def get_result_and_metadata(
        self, plan: SnowflakePlan, **kwargs
    ) -> Tuple[List[Row], List[Attribute]]:
        self.notify_mock_query_record_listener(**kwargs)
        return ([], [])
