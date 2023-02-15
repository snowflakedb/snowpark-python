#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from functools import cached_property
from typing import List, Optional, Union

from snowflake.snowpark._internal.analyzer.binary_expression import (
    BinaryArithmeticExpression,
)
from snowflake.snowpark._internal.analyzer.expression import Attribute, Expression
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    SnowflakeValues,
)
from snowflake.snowpark._internal.analyzer.unary_expression import (
    Alias,
    UnresolvedAlias,
)
from snowflake.snowpark.mock.mock_select_statement import (
    MockSelectable,
    MockSelectExecutionPlan,
    MockSelectStatement,
)


class MockExecutionPlan(LogicalPlan):
    def __init__(
        self,
        session,
        *,
        child: Optional["MockExecutionPlan"] = None,
        source_plan: Optional[LogicalPlan] = None
    ):
        super().__init__()
        self.session = session
        self.source_plan = source_plan
        self.child = child
        self.expr_to_alias = {}

    @cached_property
    def attributes(self) -> List[Attribute]:
        # output = analyze_attributes(self.schema_query, self.session)
        # self.schema_query = schema_value_statement(output)
        output = describe(self)
        return output

    @cached_property
    def output(self) -> List[Attribute]:
        return [Attribute(a.name, a.datatype, a.nullable) for a in self.attributes]


import pandas as pd


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
        # where: Optional[Expression] = source_plan.where
        # order_by: Optional[List[Expression]] = source_plan.order_by
        # limit_: Optional[int] = source_plan.limit_
        # offset: Optional[int] = source_plan.offset

        from_df = execute_mock_plan(from_)
        result_df = pd.DataFrame()
        for exp in projection:
            column_name = plan.session._analyzer.analyze(exp)
            column_series = calculate_expression(exp, from_df)
            result_df[column_name] = column_series
        return result_df


def describe(plan: MockExecutionPlan):
    result = execute_mock_plan(plan)
    return [Attribute(c) for c in result.columns]


def calculate_expression(
    exp: Expression, input_data: Union[pd.DataFrame, pd.Series]
) -> pd.Series:
    if isinstance(exp, (UnresolvedAlias, Alias)):
        return calculate_expression(exp.child, input_data)
    if isinstance(exp, Attribute):
        return input_data[exp.name]
    if isinstance(exp, BinaryArithmeticExpression):
        left = input_data[exp.left.name]  # left is an UnresolvedAttribute
        right = input_data[exp.right.name]  # right is an UnresolvedAttribute
        op = exp.sql_operator
        if op == "+":
            return left + right
        elif op == "-":
            return left - right
        elif op == "*":
            return left * right
