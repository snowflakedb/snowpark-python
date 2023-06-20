#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from functools import cached_property, partial
from typing import List, NoReturn, Optional, Union

import numpy as np
import pandas as pd

from snowflake.snowpark._internal.analyzer.analyzer_utils import UNION, UNION_ALL
from snowflake.snowpark._internal.analyzer.binary_expression import (
    Add,
    And,
    BinaryExpression,
    Divide,
    EqualNullSafe,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Multiply,
    NotEqualTo,
    Or,
    Subtract,
)
from snowflake.snowpark._internal.analyzer.expression import (
    Attribute,
    Expression,
    FunctionExpression,
    InExpression,
    Like,
    ListAgg,
    Literal,
    MultipleExpression,
    RegExp,
    UnresolvedAttribute,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    SnowflakeValues,
)
from snowflake.snowpark._internal.analyzer.sort_expression import Ascending, NullsFirst
from snowflake.snowpark._internal.analyzer.unary_expression import (
    Alias,
    IsNaN,
    IsNotNull,
    IsNull,
    Not,
    UnresolvedAlias,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import Aggregate
from snowflake.snowpark.mock.mock_functions import MOCK_FUNCTION_IMPLEMENTATION_MAP
from snowflake.snowpark.mock.mock_select_statement import (
    MockSelectable,
    MockSelectExecutionPlan,
    MockSelectSnowflakePlan,
    MockSelectStatement,
    MockSetStatement,
)
from snowflake.snowpark.mock.snowflake_data_type import ColumnEmulator, TableEmulator
from snowflake.snowpark.mock.util import convert_wildcard_to_regex, custom_comparator
from snowflake.snowpark.types import ArrayType, BooleanType, _NumericType


class MockExecutionPlan(LogicalPlan):
    def __init__(
        self,
        session,
        *,
        child: Optional["MockExecutionPlan"] = None,
        source_plan: Optional[LogicalPlan] = None,
    ) -> NoReturn:
        super().__init__()
        self.session = session
        self.source_plan = source_plan
        self.child = child
        self.expr_to_alias = {}
        self.queries = []
        self.post_actions = []
        self.api_calls = None

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
        table = TableEmulator(
            source_plan.data,
            columns=[x.name for x in source_plan.output],
            sf_types={x.name: x.datatype for x in source_plan.output},
            dtype=object,
        )
        for column_name in table.columns:
            sf_type = table.sf_types[column_name]
            table[column_name].set_sf_type(table.sf_types[column_name])
            if not isinstance(sf_type, _NumericType):
                table[column_name].replace(np.nan, None, inplace=True)
        return table
    if isinstance(source_plan, MockSelectExecutionPlan):
        return execute_mock_plan(source_plan.execution_plan)
    if isinstance(source_plan, MockSelectSnowflakePlan):
        return execute_mock_plan(source_plan.snowflake_plan)
    if isinstance(source_plan, MockSelectStatement):
        projection: Optional[List[Expression]] = source_plan.projection
        from_: Optional[MockSelectable] = source_plan.from_
        where: Optional[Expression] = source_plan.where
        order_by: Optional[List[Expression]] = source_plan.order_by
        # limit_: Optional[int] = source_plan.limit_
        # offset: Optional[int] = source_plan.offset

        from_df = execute_mock_plan(from_)

        if not projection and isinstance(from_, MockSetStatement):
            projection = from_.set_operands[0].selectable.projection

        result_df = TableEmulator()
        if projection:
            for exp in projection:
                column_name = source_plan.analyzer.analyze(exp)
                column_series = calculate_expression(exp, from_df, source_plan.analyzer)
                result_df[column_name] = column_series
        else:
            result_df = from_df

        if where:
            condition = calculate_expression(where, result_df, source_plan.analyzer)
            result_df = result_df[condition]

        sort_columns_array = []
        sort_orders_array = []
        null_first_last_array = []
        if order_by:
            for exp in order_by:
                sort_columns_array.append(source_plan.analyzer.analyze(exp.child))
                sort_orders_array.append(isinstance(exp.direction, Ascending))
                null_first_last_array.append(
                    isinstance(exp.null_ordering, NullsFirst)
                    or exp.null_ordering == NullsFirst
                )

        if sort_columns_array:
            kk = reversed(
                list(zip(sort_columns_array, sort_orders_array, null_first_last_array))
            )
            for column, ascending, null_first in kk:
                comparator = partial(custom_comparator, ascending, null_first)
                result_df = result_df.sort_values(by=column, key=comparator)
        return result_df
    if isinstance(source_plan, MockSetStatement):
        first_operand = source_plan.set_operands[0]
        res_df = execute_mock_plan(
            MockExecutionPlan(
                source_plan.analyzer.session, source_plan=first_operand.selectable
            )
        )
        for operand in source_plan.set_operands[1:]:
            operator = operand.operator
            if operator in (UNION, UNION_ALL):
                cur_df = execute_mock_plan(
                    MockExecutionPlan(
                        source_plan.analyzer.session, source_plan=operand.selectable
                    )
                )
                res_df = pd.concat([res_df, cur_df], ignore_index=True)
                res_df = (
                    res_df.drop_duplicates().reset_index(drop=True)
                    if operator == UNION
                    else res_df
                )
            else:
                raise NotImplementedError("Set statement not implemented")
        return res_df
    if isinstance(source_plan, Aggregate):
        child_rf = execute_mock_plan(source_plan.child)
        column_exps = [
            plan.session._analyzer.analyze(exp)
            for exp in source_plan.grouping_expressions
        ]

        # Aggregate may not have column_exps, which is allowed in the case of `Dataframe.agg`, in this case we pass
        # lambda x: True as the `by` parameter
        children_dfs = child_rf.groupby(by=column_exps or (lambda x: True), sort=False)
        # we first define the returning DataFrame with its column names
        result_df = TableEmulator(
            columns=[
                plan.session._analyzer.analyze(exp)
                for exp in source_plan.aggregate_expressions
            ]
        )
        for group_keys, indices in children_dfs.indices.items():
            # we construct row by row
            cur_group = child_rf.iloc[indices]
            # each row starts with group keys/column expressions, if there is no group keys/column expressions
            # it means aggregation without group (Datagrame.agg)
            values = (
                (list(group_keys) if isinstance(group_keys, tuple) else [group_keys])
                if column_exps
                else []
            )
            # the first len(column_exps) items of calculate_expression are the group_by column expressions,
            # the remaining are the aggregation function expressions
            for exp in source_plan.aggregate_expressions[len(column_exps) :]:
                cal_exp_res = calculate_expression(
                    exp, cur_group, plan.session._analyzer
                )
                # and then append the calculated value
                values.append(cal_exp_res.iat[0])
            result_df.loc[len(result_df.index)] = values
        return result_df


def describe(plan: MockExecutionPlan):
    result = execute_mock_plan(plan)
    return [Attribute(result[c].name, result[c].sf_type) for c in result.columns]


def calculate_expression(
    exp: Expression,
    input_data: Union[TableEmulator, ColumnEmulator],
    analyzer,
) -> ColumnEmulator:
    if isinstance(exp, (UnresolvedAttribute, Attribute)):
        return input_data[exp.name]
    if isinstance(exp, (UnresolvedAlias, Alias)):
        return calculate_expression(exp.child, input_data, analyzer)
    if isinstance(exp, Attribute):
        return input_data[exp.name]
    if isinstance(exp, FunctionExpression):
        kw = {}
        # evaluated_children maps to parameters passed to the function call
        evaluated_children = [
            calculate_expression(c, input_data, analyzer) for c in exp.children
        ]
        if exp.name not in MOCK_FUNCTION_IMPLEMENTATION_MAP:
            raise NotImplementedError(
                f"Function {exp.name} has not been implemented yet."
            )
        column_count = 1
        # functions that requires special care of the arguments
        if exp.name in ("approx_percentile", "approx_percentile_estimate"):
            # approx_percentile expects the second child to be a float
            kw["percentile"] = float(evaluated_children[1])
        if exp.name in ("covar_pop", "covar_samp", "object_agg"):
            # covar_pop expects the second child to be another ColumnEmulator
            column_count = 2
        if exp.name == "array_agg":
            kw["is_distinct"] = exp.is_distinct
        if exp.name == "grouping":
            column_count = 0  # 0 indicate all columns
        if exp.name == "percentile_cont":
            # This one's syntax is different from other aggregation functions
            raise NotImplementedError("percentile_cont is not implemented yet")

        # ==== for functions =====
        # TODO: this needs a re-design
        if exp.name == "to_date":
            kw["fmt"] = (
                str(evaluated_children[1]) if len(evaluated_children) > 1 else None
            )
        if exp.name == "contains":
            return MOCK_FUNCTION_IMPLEMENTATION_MAP[exp.name](
                evaluated_children[0], evaluated_children[1]
            )
        if exp.name == "abs":
            return MOCK_FUNCTION_IMPLEMENTATION_MAP[exp.name](evaluated_children[0])

        output_columns = (
            evaluated_children[:column_count]
            if column_count != 0
            else evaluated_children
        )
        return MOCK_FUNCTION_IMPLEMENTATION_MAP[exp.name](output_columns, **kw)
    if isinstance(exp, ListAgg):
        column = calculate_expression(exp.col, input_data, analyzer)
        column.sf_type = ArrayType()
        return MOCK_FUNCTION_IMPLEMENTATION_MAP["listagg"](
            [column], is_distinct=exp.is_distinct, delimiter=exp.delimiter
        )
    if isinstance(exp, IsNull):
        child_column = calculate_expression(exp.child, input_data, analyzer)
        return ColumnEmulator(
            data=[bool(data is None) for data in child_column], sf_type=BooleanType()
        )
    if isinstance(exp, IsNotNull):
        child_column = calculate_expression(exp.child, input_data, analyzer)
        return ColumnEmulator(
            data=[bool(data is not None) for data in child_column],
            sf_type=BooleanType(),
        )
    if isinstance(exp, IsNaN):
        child_column = calculate_expression(exp.child, input_data, analyzer)
        result = child_column.isna()
        result.sf_type = BooleanType()
        return result
    if isinstance(exp, Not):
        child_column = calculate_expression(exp.child, input_data, analyzer)
        return ~child_column
    if isinstance(exp, UnresolvedAttribute):
        return analyzer.analyze(exp)
    if isinstance(exp, Literal):
        column = ColumnEmulator(
            data=[exp.value] * len(input_data), sf_type=exp.datatype
        )
        return column
    if isinstance(exp, BinaryExpression):
        new_column = None
        left = calculate_expression(exp.left, input_data, analyzer)
        right = calculate_expression(exp.right, input_data, analyzer)
        if isinstance(exp, Multiply):
            new_column = left * right
        if isinstance(exp, Divide):
            new_column = left / right
        if isinstance(exp, Add):
            new_column = left + right
        if isinstance(exp, Subtract):
            new_column = left - right
        if isinstance(exp, EqualTo):
            new_column = left == right
        if isinstance(exp, NotEqualTo):
            new_column = left != right
        if isinstance(exp, GreaterThanOrEqual):
            new_column = left >= right
        if isinstance(exp, GreaterThan):
            new_column = left > right
        if isinstance(exp, LessThanOrEqual):
            new_column = left <= right
        if isinstance(exp, LessThan):
            new_column = left < right
        if isinstance(exp, And):
            new_column = (
                (left & right)
                if isinstance(input_data, TableEmulator) or not input_data
                else (left & right) & input_data
            )
        if isinstance(exp, Or):
            new_column = (
                (left | right)
                if isinstance(input_data, TableEmulator) or not input_data
                else (left | right) & input_data
            )
        if isinstance(exp, EqualNullSafe):
            new_column = (
                (left == right)
                | (left.isna() & right.isna())
                | (left.isnull() & right.isnull())
            )
        return new_column
    if isinstance(exp, RegExp):
        column = calculate_expression(exp.expr, input_data, analyzer)
        pattern = str(analyzer.analyze(exp.pattern))
        pattern = f"^{pattern}" if not pattern.startswith("^") else pattern
        pattern = f"{pattern}$" if not pattern.endswith("$") else pattern
        result = column.str.match(pattern)
        result.sf_type = BooleanType()
        return result
    if isinstance(exp, Like):
        column = calculate_expression(exp.expr, input_data, analyzer)
        pattern = convert_wildcard_to_regex(str(analyzer.analyze(exp.pattern)))
        result = column.str.match(pattern)
        result.sf_type = BooleanType()
        return result
    if isinstance(exp, InExpression):
        column = calculate_expression(exp.columns, input_data, analyzer)
        values = [
            calculate_expression(expression, input_data, analyzer)
            for expression in exp.values
        ]
        result = column.isin(values)
        result.sf_type = BooleanType()
        return result
    if isinstance(exp, MultipleExpression):
        raise NotImplementedError("MultipleExpression is to be implemented")
