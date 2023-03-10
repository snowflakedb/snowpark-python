#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from functools import cached_property, partial, cmp_to_key
from typing import List, NoReturn, Optional, Union

from snowflake.snowpark._internal.analyzer.binary_expression import (
    BinaryArithmeticExpression,
)
from snowflake.snowpark._internal.analyzer.expression import *
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    SnowflakeValues,
)
from snowflake.snowpark._internal.analyzer.sort_expression import Ascending, NullsFirst
from snowflake.snowpark._internal.analyzer.unary_expression import (
    Alias,
    UnresolvedAlias,
    Not,
    IsNull,
    IsNaN,
    IsNotNull
)
from snowflake.snowpark._internal.analyzer.binary_expression import *
from snowflake.snowpark.mock.mock_select_statement import (
    MockSelectable,
    MockSelectExecutionPlan,
    MockSelectStatement,
)
from snowflake.snowpark.mock.snowflake_data_type import ColumnEmulator, TableEmulator

from .util import convert_wildcard_to_regex, custom_comparator


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
            dtype=object,
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
            condition = calculate_condition(where, result_df, plan.session._analyzer)
            result_df = result_df[condition]

        sort_columns_array = []
        sort_orders_array = []
        null_first_last_array = []
        if order_by:
            for exp in order_by:
                sort_columns_array.append(plan.session._analyzer.analyze(exp.child))
                sort_orders_array.append(isinstance(exp.direction, Ascending))
                null_first_last_array.append(isinstance(exp.null_ordering, NullsFirst) or exp.null_ordering == NullsFirst)

        if sort_columns_array:
            kk = reversed(list(zip(sort_columns_array, sort_orders_array, null_first_last_array)))
            for column, ascending, null_first in kk:
                comparator = partial(custom_comparator, ascending, null_first)
                result_df = result_df.sort_values(
                    by=column,
                    key=comparator
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


def calculate_condition(
    exp: Expression,
    dataframe,
    analyzer,
    condition=None
):
    if isinstance(exp, Attribute):
        return analyzer.analyze(exp)
    if isinstance(exp, IsNull):
        child_condition = calculate_condition(exp.child, dataframe, analyzer, condition)
        return dataframe[child_condition].isnull()
    if isinstance(exp, IsNotNull):
        child_condition = calculate_condition(exp.child, dataframe, analyzer, condition)
        return ~dataframe[child_condition].isnull()
    if isinstance(exp, IsNaN):
        child_condition = calculate_condition(exp.child, dataframe, analyzer, condition)
        return dataframe[child_condition].isna()
    if isinstance(exp, Not):
        child_condition = calculate_condition(exp.child, dataframe, analyzer, condition)
        return ~child_condition
    if isinstance(exp, UnresolvedAttribute):
        return analyzer.analyze(exp)
    if isinstance(exp, Literal):
        return exp.value
    if isinstance(exp, BinaryExpression):
        new_condition = None
        left = calculate_condition(exp.left, dataframe, analyzer, condition)
        right = calculate_condition(exp.right, dataframe, analyzer, condition)

        if isinstance(exp.left, (UnresolvedAttribute, Attribute)):
            left = dataframe[left]
        if isinstance(exp.right, (UnresolvedAttribute, Attribute)):
            right = dataframe[right]
        if isinstance(exp, Multiply):
            new_condition = left * right
        if isinstance(exp, Divide):
            new_condition = left / right
        if isinstance(exp, Add):
            new_condition = left + right
        if isinstance(exp, Subtract):
            new_condition = left - right
        if isinstance(exp, EqualTo):
            new_condition = left == right
        if isinstance(exp, NotEqualTo):
            new_condition = left != right
        if isinstance(exp, GreaterThanOrEqual):
            new_condition = left >= right
        if isinstance(exp, GreaterThan):
            new_condition = left > right
        if isinstance(exp, LessThanOrEqual):
            new_condition = left <= right
        if isinstance(exp, LessThan):
            new_condition = left < right
        if isinstance(exp, And):
            new_condition = (left & right) if not condition else (left & right) & condition
        if isinstance(exp, Or):
            new_condition = (left | right) if not condition else (left | right) & condition
        if isinstance(exp, EqualNullSafe):
            new_condition = (left == right) | (left.isna() & right.isna()) | (left.isnull() & right.isnull())
        return new_condition
    if isinstance(exp, RegExp):
        column = calculate_condition(exp.expr, dataframe, analyzer, condition)
        pattern = str(analyzer.analyze(exp.pattern))
        pattern = f'^{pattern}' if not pattern.startswith('^') else pattern
        pattern = f'{pattern}$' if not pattern.endswith('$') else pattern
        return dataframe[column].str.match(pattern)
    if isinstance(exp, Like):
        column = calculate_condition(exp.expr, dataframe, analyzer, condition)
        pattern = convert_wildcard_to_regex(str(analyzer.analyze(exp.pattern)))
        return dataframe[column].str.match(pattern)
    if isinstance(exp, InExpression):
        column = analyzer.analyze(exp.columns)
        values = [calculate_condition(expression, dataframe, analyzer) for expression in exp.values]
        return dataframe[column].isin(values)
