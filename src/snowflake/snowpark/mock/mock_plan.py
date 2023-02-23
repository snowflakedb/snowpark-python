#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from functools import cached_property
from typing import List, NoReturn, Optional, Union

from snowflake.snowpark._internal.analyzer.binary_expression import (
    BinaryArithmeticExpression,
)
from snowflake.snowpark._internal.analyzer.expression import Attribute, Expression
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    SnowflakeValues,
)
from snowflake.snowpark._internal.analyzer.sort_expression import Ascending, NullsFirst
from snowflake.snowpark._internal.analyzer.unary_expression import (
    Alias,
    UnresolvedAlias,
)
from snowflake.snowpark.mock.mock_select_statement import (
    MockSelectable,
    MockSelectExecutionPlan,
    MockSelectStatement,
)
from snowflake.snowpark.mock.snowflake_data_type import ColumnEmulator, TableEmulator


class MockExecutionPlan(LogicalPlan):
    def __init__(
        self,
        session,
        *,
        child: Optional["MockExecutionPlan"] = None,
        source_plan: Optional[LogicalPlan] = None
    ) -> NoReturn:
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


def execute_mock_plan(plan: MockExecutionPlan) -> TableEmulator:
    source_plan = plan.source_plan if isinstance(plan, MockExecutionPlan) else plan
    if isinstance(source_plan, SnowflakeValues):
        return TableEmulator(
            source_plan.data,
            columns=[x.name for x in source_plan.output],
            sf_types={x.name: x.datatype for x in source_plan.output},
        )

    if isinstance(source_plan, MockSelectExecutionPlan):
        return execute_mock_plan(source_plan.execution_plan)
    if isinstance(source_plan, MockSelectStatement):
        projection: Optional[List[Expression]] = source_plan.projection
        from_: Optional[MockSelectable] = source_plan.from_
        where: Optional[Expression] = source_plan.where
        order_by: Optional[List[Expression]] = source_plan.order_by
        # limit_: Optional[int] = source_plan.limit_
        # offset: Optional[int] = source_plan.offset

        from_df = execute_mock_plan(from_)
        result_df = TableEmulator()
        for exp in projection:
            column_name = plan.session._analyzer.analyze(exp)
            column_series = calculate_expression(exp, from_df)
            result_df[column_name] = column_series

        if where:
            # TODO: more tests, various operators
            # TODO: column name contains quotes
            where_query = plan.session._analyzer.analyze(where, escape_column_name=True)
            result_df = result_df.query(where_query)

        sort_columns_array = []
        sort_orders_array = []
        null_first_last_array = []
        for exp in order_by:
            sort_columns_array.append(plan.session._analyzer.analyze(exp.child))
            sort_orders_array.append(isinstance(exp.direction, Ascending))
            null_first_last_array.append(exp.null_ordering == NullsFirst)

        if sort_columns_array:
            # TODO: pandas DataFrame doesn't support the input of a list of na_position
            result_df = result_df.sort_values(
                by=sort_columns_array, ascending=sort_orders_array
            )

        return result_df


def describe(plan: MockExecutionPlan):
    result = execute_mock_plan(plan)
    return [Attribute(result[c].name, result[c].sf_type) for c in result.columns]


def calculate_expression(
    exp: Expression, input_data: Union[TableEmulator, ColumnEmulator]
) -> ColumnEmulator:
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
        elif op == "/":
            return left / right
