#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from typing import Dict, List, Optional, Tuple

from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlanBuilder
from snowflake.snowpark._internal.utils import is_single_quoted
from snowflake.snowpark.mock.plan import MockExecutionPlan, MockFileOperation
from snowflake.snowpark.mock.telemetry import local_test_not_implemented_error


class MockSnowflakePlanBuilder(SnowflakePlanBuilder):
    def create_temp_table(self, *args, **kwargs):
        local_test_not_implemented_error(
            not_implemented_method_name="DataFrame.cache_result"
        )
        raise NotImplementedError(
            "[Local Testing] DataFrame.cache_result is currently not implemented."
        )

    def read_file(
        self,
        path: str,
        format: str,
        options: Dict[str, str],
        fully_qualified_schema: str,
        schema: List[Attribute],
        schema_to_cast: Optional[List[Tuple[str, str]]] = None,
        transformations: Optional[List[str]] = None,
    ) -> MockExecutionPlan:
        if format.upper() != "CSV":
            local_test_not_implemented_error(
                not_implemented_method_name=f"DataFrameReader.{format.lower()}"
            )
            raise NotImplementedError(
                "[Local Testing] Reading non CSV data into dataframe is not currently supported."
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
            local_test_not_implemented_error(
                not_implemented_method_name="FileOperation.put",
                extra_info="auto_compress=True",
            )
            raise NotImplementedError(
                "[Local Testing] PUT with auto_compress=True is currently not supported."
            )
        if command == "get":
            local_test_not_implemented_error(
                not_implemented_method_name="FileOperation.get"
            )
            raise NotImplementedError("[Local Testing] GET is currently not supported.")
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
