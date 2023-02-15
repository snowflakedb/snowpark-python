#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from typing import List, Optional

import pandas as pd

from snowflake.snowpark._internal.analyzer.expression import Expression
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import SnowflakeValues
from snowflake.snowpark.mock.mock_plan import MockExecutionPlan
from snowflake.snowpark.mock.mock_select_statement import (
    MockSelectable,
    MockSelectExecutionPlan,
    MockSelectStatement,
)


def execute_mock_plan(plan: MockExecutionPlan) -> pd.DataFrame:
    source_plan = plan.source_plan if isinstance(plan, MockExecutionPlan) else plan
    if isinstance(source_plan, SnowflakeValues):
        return pd.DataFrame(
            source_plan.data, columns=[x.name for x in source_plan.output]
        )

    if isinstance(source_plan, MockSelectExecutionPlan):
        return execute_mock_plan(source_plan.execution_plan)
    if isinstance(source_plan, MockSelectStatement):
        projection: Optional[List[Expression]] = source_plan.projection
        from_: Optional[MockSelectable] = source_plan.from_
        where: Optional[Expression] = source_plan.where
        order_by: Optional[List[Expression]] = source_plan.order_by
        limit_: Optional[int] = source_plan.limit_
        offset: Optional[int] = source_plan.offset

        from_df = execute_mock_plan(from_)
        column_names = [x.name for x in projection]
        return from_df[column_names]


def describe(plan: MockExecutionPlan):
    result = execute_mock_plan(plan)
    return result.columns
