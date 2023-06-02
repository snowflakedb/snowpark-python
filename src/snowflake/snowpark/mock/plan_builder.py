#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from typing import Dict, List, Optional, Tuple

from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlanBuilder
from snowflake.snowpark._internal.utils import is_single_quoted
from snowflake.snowpark.mock.mock_plan import MockFileOperation


class MockSnowflakePlanBuilder(SnowflakePlanBuilder):
    def read_file(
        self,
        path: str,
        format: str,
        options: Dict[str, str],
        fully_qualified_schema: str,
        schema: List[Attribute],
        schema_to_cast: Optional[List[Tuple[str, str]]] = None,
        transformations: Optional[List[str]] = None,
    ):
        return MockFileOperation(
            session=self.session,
            operator=MockFileOperation.Operator.READ_FILE,
            stage_location=path,
            format=format,
            schema=schema,
        )

    def file_operation_plan(
        self, command: str, file_name: str, stage_location: str, options: Dict[str, str]
    ) -> MockFileOperation:
        return MockFileOperation(
            session=self.session,
            operator=MockFileOperation.Operator(command),
            local_file_name=file_name,
            stage_location=stage_location[1:-1]
            if is_single_quoted(stage_location)
            else stage_location,
        )
