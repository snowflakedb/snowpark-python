#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import List, Union

from snowflake.snowpark._internal.analyzer.expression import Expression, Star
from snowflake.snowpark._internal.analyzer.select_statement import (
    Selectable,
    SelectSnowflakePlan,
    SelectStatement,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import LogicalPlan
from snowflake.snowpark.mock._analyzer import MockAnalyzer
from snowflake.snowpark.mock._nop_plan import NopExecutionPlan, resolve_attributes
from snowflake.snowpark.mock._plan import MockExecutionPlan
from snowflake.snowpark.mock._select_statement import (
    MockSelectableEntity,
    MockSelectExecutionPlan,
    MockSelectStatement,
    MockSetOperand,
    MockSetStatement,
)


class NopSetStatement(MockSetStatement):
    @property
    def attributes(self):
        return [val.expression for val in self.column_states.data.values()]


class NopSelectStatement(MockSelectStatement):
    @property
    def attributes(self):
        return (
            resolve_attributes(self.from_)
            if isinstance(self.projection[0], Star)
            else self.projection
        )

    def _make_nop_select_statement_copy(self, statement: "MockSelectStatement"):
        if isinstance(statement, NopSelectStatement):
            return statement

        nop_statement = NopSelectStatement(from_=self.from_, analyzer=self.analyzer)
        nop_statement.__dict__.update(statement.__dict__)
        return nop_statement

    def select(self, cols: List[Expression]) -> "MockSelectStatement":
        return self._make_nop_select_statement_copy(super().select(cols))

    def filter(self, col: Expression) -> "MockSelectStatement":
        return self._make_nop_select_statement_copy(super().filter(col))

    def sort(self, cols: List[Expression]) -> "MockSelectStatement":
        return self._make_nop_select_statement_copy(super().sort(cols))

    def distinct(self) -> "MockSelectStatement":
        return self._make_nop_select_statement_copy(super().distinct())

    def exclude(self, exclude_cols, keep_cols) -> "MockSelectStatement":
        return super().exclude(exclude_cols, keep_cols)

    def set_operator(
        self,
        *selectables: Union[
            SelectSnowflakePlan,
            "SelectStatement",
        ],
        operator: str,
    ) -> "SelectStatement":
        new_statement = super().set_operator(*selectables, operator=operator)

        def recursive_copy_helper(
            stmt_or_op: Union[Selectable, MockSetStatement, MockSetOperand]
        ):
            if isinstance(stmt_or_op, MockSetStatement):
                operands = [recursive_copy_helper(op) for op in stmt_or_op.set_operands]
                nop_set = NopSetStatement(*operands, analyzer=self.analyzer)
                nop_set._attributes = nop_set.execution_plan.attributes
                return nop_set
            elif isinstance(stmt_or_op, MockSetOperand):
                stmt_or_op.selectable = recursive_copy_helper(stmt_or_op.selectable)

            return stmt_or_op

        new_statement.from_ = recursive_copy_helper(new_statement.from_)
        return self._make_nop_select_statement_copy(new_statement)

    def limit(self, n: int, *, offset: int = 0) -> "SelectStatement":
        return self._make_nop_select_statement_copy(super().limit(n, offset=offset))

    def to_subqueryable(self) -> "Selectable":
        return self._make_nop_select_statement_copy(super().to_subqueryable())


class NopSelectExecutionPlan(MockSelectExecutionPlan):
    @property
    def attributes(self):
        return (
            self._attributes
            if self._attributes
            else resolve_attributes(self._execution_plan)
        )


class NopSelectableEntity(MockSelectableEntity):
    @property
    def attributes(self):
        return resolve_attributes(self.entity_plan, session=self._session)


class NopAnalyzer(MockAnalyzer):
    def do_resolve(self, logical_plan: LogicalPlan) -> MockExecutionPlan:
        return NopExecutionPlan(logical_plan, self.session)

    def create_select_statement(self, *args, **kwargs):
        return NopSelectStatement(*args, **kwargs)

    def create_select_snowflake_plan(self, *args, **kwargs):
        return NopSelectExecutionPlan(*args, **kwargs)

    def create_selectable_entity(self, *args, **kwargs):
        return NopSelectableEntity(*args, **kwargs)
