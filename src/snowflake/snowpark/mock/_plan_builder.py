#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from typing import Dict, List, Optional, Tuple

from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlanBuilder
from snowflake.snowpark._internal.utils import is_single_quoted
from snowflake.snowpark.mock._plan import MockExecutionPlan, MockFileOperation
from snowflake.snowpark.mock._stage_registry import SUPPORT_READ_OPTIONS
from snowflake.snowpark.mock._telemetry import LocalTestOOBTelemetryService


class MockSnowflakePlanBuilder(SnowflakePlanBuilder):
    def create_temp_table(self, *args, **kwargs):
        LocalTestOOBTelemetryService.get_instance().log_not_supported_error(
            external_feature_name="DataFrame.cache_result",
            internal_feature_name="MockSnowflakePlanBuilder.create_temp_table",
            raise_error=NotImplementedError,
        )

    def read_file(
        self,
        path: str,
        format: str,
        options: Dict[str, str],
        schema: List[Attribute],
        schema_to_cast: Optional[List[Tuple[str, str]]] = None,
        transformations: Optional[List[str]] = None,
        metadata_project: Optional[List[str]] = None,
        metadata_schema: Optional[List[Attribute]] = None,
    ) -> MockExecutionPlan:
        if format.lower() not in SUPPORT_READ_OPTIONS.keys():
            LocalTestOOBTelemetryService.get_instance().log_not_supported_error(
                external_feature_name=f"Reading {format} data into dataframe",
                internal_feature_name="MockSnowflakePlanBuilder.read_file",
                parameters_info={"format": str(format)},
                raise_error=NotImplementedError,
            )
        return MockExecutionPlan(
            source_plan=MockFileOperation(
                session=self.session,
                operator=MockFileOperation.Operator.READ_FILE,
                stage_location=path,
                format=format,
                schema=schema,
                options=options,
            ),
            session=self.session,
        )

    def file_operation_plan(
        self, command: str, file_name: str, stage_location: str, options: Dict[str, str]
    ) -> MockExecutionPlan:
        if options.get("auto_compress", False):
            LocalTestOOBTelemetryService.get_instance().log_not_supported_error(
                external_feature_name="File operation PUT with auto_compress=True",
                internal_feature_name="MockSnowflakePlanBuilder.file_operation_plan",
                parameters_info={"auto_compress": "True", "command": str(command)},
                raise_error=NotImplementedError,
            )
        return MockExecutionPlan(
            source_plan=MockFileOperation(
                session=self.session,
                operator=MockFileOperation.Operator(command),
                local_file_name=file_name,
                stage_location=stage_location[1:-1]
                if is_single_quoted(stage_location)
                else stage_location,
                options=options,
            ),
            session=self.session,
        )
