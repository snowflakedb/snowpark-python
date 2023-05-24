#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
<<<<<<< HEAD
from abc import ABC
from copy import copy
from typing import TYPE_CHECKING, List, Optional, Union
=======
import importlib
import inspect
from abc import ABC
from copy import copy
from functools import cached_property, partial
from typing import TYPE_CHECKING, Dict, List, NoReturn, Optional, Tuple, Union
>>>>>>> 4855a30 (Clean up)
from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from snowflake.snowpark import Column
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    UNION,
    UNION_ALL,
    quote_name,
)
from snowflake.snowpark._internal.analyzer.binary_plan_node import Join
from snowflake.snowpark._internal.analyzer.select_statement import (
    ColumnChangeState,
    ColumnStateDict,
    Selectable,
    SelectSnowflakePlan,
    SelectStatement,
    can_clause_dependent_columns_flatten,
    can_projection_dependent_columns_be_flattened,
    derive_column_states_from_subquery,
    initiate_column_states,
)
<<<<<<< HEAD
from snowflake.snowpark.types import LongType
=======
from snowflake.snowpark._internal.analyzer.sort_expression import Ascending, NullsFirst
from snowflake.snowpark._internal.analyzer.unary_plan_node import Aggregate
from snowflake.snowpark.mock.functions import _MOCK_FUNCTION_IMPLEMENTATION_MAP
from snowflake.snowpark.mock.snowflake_data_type import ColumnEmulator, TableEmulator
from snowflake.snowpark.mock.util import convert_wildcard_to_regex, custom_comparator
from snowflake.snowpark.types import LongType, _NumericType
>>>>>>> 4855a30 (Clean up)

if TYPE_CHECKING:
    from snowflake.snowpark._internal.analyzer.analyzer import (
        Analyzer,
    )  # pragma: no cover

from snowflake.snowpark._internal.analyzer import analyzer_utils
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
    COLUMN_DEPENDENCY_DOLLAR,
    Attribute,
    Expression,
    FunctionExpression,
    InExpression,
    Like,
    ListAgg,
    Literal,
    MultipleExpression,
    RegExp,
    Star,
    UnresolvedAttribute,
    derive_dependent_columns,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    Range,
    SnowflakeValues,
)
from snowflake.snowpark._internal.analyzer.unary_expression import (
    Alias,
    IsNaN,
    IsNotNull,
    IsNull,
    Not,
    UnresolvedAlias,
)

SET_UNION = analyzer_utils.UNION
SET_UNION_ALL = analyzer_utils.UNION_ALL
SET_INTERSECT = analyzer_utils.INTERSECT
SET_EXCEPT = analyzer_utils.EXCEPT


class MockSelectable(LogicalPlan, ABC):
    """The parent abstract class of a DataFrame's logical plan. It can be converted to and from a SnowflakePlan."""

    def __init__(
        self,
        analyzer: "Analyzer",
    ) -> None:
        super().__init__()
        self.analyzer = analyzer
        self.pre_actions = None
        self.post_actions = None
        self.flatten_disabled: bool = False
        self._column_states: Optional[ColumnStateDict] = None
        self._execution_plan: Optional[SnowflakePlan] = None
        self._attributes = None

    @property
    def execution_plan(self):
        """Convert to a SnowflakePlan"""
<<<<<<< HEAD
        from snowflake.snowpark.mock.mock_plan import MockExecutionPlan

=======
>>>>>>> 4855a30 (Clean up)
        if self._execution_plan is None:
            self._execution_plan = MockExecutionPlan(self, self.analyzer.session)
        return self._execution_plan

    @property
    def attributes(self):
        return self._attributes or self.execution_plan.attributes

    @property
    def column_states(self) -> ColumnStateDict:
        """A dictionary that contains the column states of a query.
        Refer to class ColumnStateDict.
        """
        if self._column_states is None:
            self._column_states = initiate_column_states(
                self.attributes,
                self.analyzer,
            )
        return self._column_states

    def to_subqueryable(self) -> "Selectable":
        """Some queries can be used in a subquery. Some can't. For details, refer to class SelectSQL."""
        return self


class MockSetOperand:
    def __init__(self, selectable: Selectable, operator: Optional[str] = None) -> None:
        super().__init__()
        self.selectable = selectable
        self.operator = operator


class MockSetStatement(MockSelectable):
    def __init__(
        self, *set_operands: MockSetOperand, analyzer: Optional["Analyzer"]
    ) -> None:
        super().__init__(analyzer=analyzer)
        self.set_operands = set_operands
        for operand in set_operands:
            if operand.selectable.pre_actions:
                if not self.pre_actions:
                    self.pre_actions = []
                self.pre_actions.extend(operand.selectable.pre_actions)
            if operand.selectable.post_actions:
                if not self.post_actions:
                    self.post_actions = []
                self.post_actions.extend(operand.selectable.post_actions)

    @property
    def sql_query(self) -> str:
        sql = f"({self.set_operands[0].selectable.sql_query})"
        for i in range(1, len(self.set_operands)):
            sql = f"{sql}{self.set_operands[i].operator}({self.set_operands[i].selectable.sql_query})"
        return sql

    @property
    def schema_query(self) -> str:
        """The first operand decide the column attributes of a query with set operations.
        Refer to https://docs.snowflake.com/en/sql-reference/operators-query.html#general-usage-notes"""
        return self.set_operands[0].selectable.schema_query

    @property
    def column_states(self) -> Optional[ColumnStateDict]:
        if not self._column_states:
            self._column_states = initiate_column_states(
                self.set_operands[0].selectable.column_states.projection, self.analyzer
            )
        return self._column_states


class MockSelectExecutionPlan(MockSelectable):
    """Wrap a SnowflakePlan to a subclass of Selectable."""

    def __init__(self, logical_plan: LogicalPlan, *, analyzer: "Analyzer") -> None:
        super().__init__(analyzer)
        self._execution_plan = analyzer.resolve(logical_plan)

        if isinstance(logical_plan, Range):
            self._attributes = [Attribute('"ID"', LongType(), False)]

        self.api_calls = MagicMock()


class MockSelectStatement(MockSelectable):
    """The main logic plan to be used by a DataFrame.
    It structurally has the parts of a query and uses the ColumnState to decide whether a query can be flattened."""

    def __init__(
        self,
        *,
        projection: Optional[List[Expression]] = None,
        from_: Optional["MockSelectable"] = None,
        where: Optional[Expression] = None,
        order_by: Optional[List[Expression]] = None,
        limit_: Optional[int] = None,
        offset: Optional[int] = None,
        analyzer: "Analyzer",
    ) -> None:
        super().__init__(analyzer)
        self.projection: List[Expression] = projection or [Star([])]
        self.from_: Optional["Selectable"] = from_
        self.where: Optional[Expression] = where
        self.order_by: Optional[List[Expression]] = order_by
        self.limit_: Optional[int] = limit_
        self.offset = offset
        self.pre_actions = self.from_.pre_actions
        self.post_actions = self.from_.post_actions
        self._sql_query = None
        self._schema_query = None
        self._projection_in_str = None
        self.api_calls = (
            self.from_.api_calls.copy() if self.from_.api_calls is not None else None
        )  # will be replaced by new api calls if any operation.

    def __copy__(self):
        new = MockSelectStatement(
            projection=self.projection,
            from_=self.from_,
            where=self.where,
            order_by=self.order_by,
            limit_=self.limit_,
            offset=self.offset,
            analyzer=self.analyzer,
        )
        # The following values will change if they're None in the newly copied one so reset their values here
        # to avoid problems.
        new._column_states = None
        new.flatten_disabled = False  # by default a SelectStatement can be flattened.
        return new

    @property
    def column_states(self) -> ColumnStateDict:
        if self._column_states is None:
            if not self.projection and not self.has_clause:
                self._column_states = self.from_.column_states
            elif isinstance(self.from_, MockSelectExecutionPlan):
                self._column_states = initiate_column_states(
                    self.from_.attributes, self.analyzer
                )
            else:
                super().column_states  # will assign value to self._column_states
        return self._column_states

    @property
    def has_clause_using_columns(self) -> bool:
        return any(
            (
                self.where is not None,
                self.order_by is not None,
            )
        )

    @property
    def has_clause(self) -> bool:
        return self.has_clause_using_columns or self.limit_ is not None

    @property
    def projection_in_str(self) -> str:
        if not self._projection_in_str:
            self._projection_in_str = (
                analyzer_utils.COMMA.join(
                    self.analyzer.analyze(x) for x in self.projection
                )
                if self.projection
                else analyzer_utils.STAR
            )
        return self._projection_in_str

    def select(self, cols: List[Expression]) -> "SelectStatement":
        """Build a new query. This SelectStatement will be the subquery of the new query.
        Possibly flatten the new query and the subquery (self) to form a new flattened query.
        """
        if (
            len(cols) == 1
            and isinstance(cols[0], UnresolvedAlias)
            and isinstance(cols[0].child, Star)
            and not cols[0].child.expressions
            # df.select("*") doesn't have the child.expressions
            # df.select(df["*"]) has the child.expressions
        ):
            new = copy(self)  # it copies the api_calls
            new._projection_in_str = self._projection_in_str
            new._schema_query = self._schema_query
            new._column_states = self._column_states
            new.flatten_disabled = self.flatten_disabled
            new._execution_plan = self._execution_plan
            return new
        final_projection = []
        disable_next_level_flatten = False
        new_column_states = derive_column_states_from_subquery(cols, self)
        if new_column_states is None:
            can_be_flattened = False
            disable_next_level_flatten = True
        elif len(new_column_states.active_columns) != len(new_column_states.projection):
            # There must be duplicate columns in the projection.
            # We don't flatten when there are duplicate columns.
            can_be_flattened = False
            disable_next_level_flatten = True
        elif self.flatten_disabled or self.has_clause_using_columns:
            can_be_flattened = False
        else:
            can_be_flattened = True
            subquery_column_states = self.column_states
            for col, state in new_column_states.items():
                dependent_columns = state.dependent_columns
                if dependent_columns == COLUMN_DEPENDENCY_DOLLAR:
                    can_be_flattened = False
                    break
                subquery_state = subquery_column_states.get(col)
                if state.change_state in (
                    ColumnChangeState.CHANGED_EXP,
                    ColumnChangeState.NEW,
                ):
                    can_be_flattened = can_projection_dependent_columns_be_flattened(
                        dependent_columns, subquery_column_states
                    )
                    if not can_be_flattened:
                        break
                    final_projection.append(copy(state.expression))
                elif state.change_state == ColumnChangeState.UNCHANGED_EXP:
                    # query may change sequence of columns. If subquery has same-level reference, flattened sql may not work.
                    if (
                        col not in subquery_column_states
                        or subquery_column_states[col].depend_on_same_level
                    ):
                        can_be_flattened = False
                        break
                    final_projection.append(
                        copy(subquery_column_states[col].expression)
                    )  # add subquery's expression for this column name
                elif state.change_state == ColumnChangeState.DROPPED:
                    if (
                        subquery_state.change_state == ColumnChangeState.NEW
                        and subquery_state.is_referenced_by_same_level_column
                    ):
                        can_be_flattened = False
                        break
                else:  # pragma: no cover
                    raise ValueError(f"Invalid column state {state}.")
        if can_be_flattened:
            new = copy(self)
            new.projection = final_projection
            new.from_ = self.from_
            new.pre_actions = new.from_.pre_actions
            new.post_actions = new.from_.post_actions
        else:
            new = MockSelectStatement(
                projection=cols, from_=self, analyzer=self.analyzer
            )
        new.flatten_disabled = disable_next_level_flatten
        new._column_states = derive_column_states_from_subquery(
            new.projection, new.from_
        )
        # If new._column_states is None, when property `column_states` is called later,
        # a query will be described and an error like "invalid identifier" will be thrown.

        return new

    def filter(self, col: Expression) -> "MockSelectStatement":
        if self.flatten_disabled:
            can_be_flattened = False
        else:
            dependent_columns = derive_dependent_columns(col)
            can_be_flattened = can_clause_dependent_columns_flatten(
                dependent_columns, self.column_states
            )
        if can_be_flattened:
            new = copy(self)
            new.from_ = self.from_.to_subqueryable()
            new.pre_actions = new.from_.pre_actions
            new.post_actions = new.from_.post_actions
            new.where = And(self.where, col) if self.where is not None else col
            new._column_states = self._column_states
        else:
            new = MockSelectStatement(
                from_=self.to_subqueryable(), where=col, analyzer=self.analyzer
            )
        return new

    def sort(self, cols: List[Expression]) -> "SelectStatement":
        if self.flatten_disabled:
            can_be_flattened = False
        else:
            dependent_columns = derive_dependent_columns(*cols)
            can_be_flattened = can_clause_dependent_columns_flatten(
                dependent_columns, self.column_states
            )
        if can_be_flattened:
            new = copy(self)
            new.from_ = self.from_.to_subqueryable()
            new.pre_actions = new.from_.pre_actions
            new.post_actions = new.from_.post_actions
            new.order_by = cols
            new._column_states = self._column_states
        else:
            new = SelectStatement(
                from_=self.to_subqueryable(), order_by=cols, analyzer=self.analyzer
            )
        return new

    def set_operator(
        self,
        *selectables: Union[
            SelectSnowflakePlan,
            "SelectStatement",
        ],
        operator: str,
    ) -> "SelectStatement":
        if isinstance(self.from_, MockSetStatement) and not self.has_clause:
            last_operator = self.from_.set_operands[-1].operator
            if operator == last_operator:
                existing_set_operands = self.from_.set_operands
                set_operands = tuple(
                    MockSetOperand(x.to_subqueryable(), operator) for x in selectables
                )
            elif operator == SET_INTERSECT:
                # In Snowflake SQL, intersect has higher precedence than other set operators.
                # So we need to put all operands before intersect into a single operand.
                existing_set_operands = (
                    MockSetOperand(
                        MockSetStatement(
                            *self.from_.set_operands, analyzer=self.analyzer
                        )
                    ),
                )
                sub_statement = MockSetStatement(
                    *(
                        MockSetOperand(x.to_subqueryable(), operator)
                        for x in selectables
                    ),
                    analyzer=self.analyzer,
                )
                set_operands = (
                    MockSetOperand(sub_statement.to_subqueryable(), operator),
                )
            else:
                existing_set_operands = self.from_.set_operands
                sub_statement = MockSetStatement(
                    *(
                        MockSetOperand(x.to_subqueryable(), operator)
                        for x in selectables
                    ),
                    analyzer=self.analyzer,
                )
                set_operands = (
                    MockSetOperand(sub_statement.to_subqueryable(), operator),
                )
            set_statement = MockSetStatement(
                *existing_set_operands, *set_operands, analyzer=self.analyzer
            )
        else:
            set_operands = tuple(
                MockSetOperand(x.to_subqueryable(), operator) for x in selectables
            )
            set_statement = MockSetStatement(
                MockSetOperand(self.to_subqueryable()),
                *set_operands,
                analyzer=self.analyzer,
            )
        api_calls = self.api_calls.copy()
        for s in selectables:
            if s.api_calls:
                api_calls.extend(s.api_calls)
        set_statement.api_calls = api_calls
        new = MockSelectStatement(analyzer=self.analyzer, from_=set_statement)
        new._column_states = set_statement.column_states
        return new

    def limit(self, n: int, *, offset: int = 0) -> "SelectStatement":
        new = copy(self)
        new.from_ = self.from_.to_subqueryable()
        new.limit_ = min(self.limit_, n) if self.limit_ else n
        new.offset = (self.offset + offset) if self.offset else offset
        new._column_states = self._column_states
        return new

    def to_subqueryable(self) -> "Selectable":
        """When this SelectStatement's subquery is not subqueryable (can't be used in `from` clause of the sql),
        convert it to subqueryable and create a new SelectStatement with from_ being the new subqueryable。
        An example is "show tables", which will be converted to a pre-action "show tables" and "select from result_scan(query_id_of_show_tables)".
        """
        from_subqueryable = self.from_.to_subqueryable()
        if self.from_ is not from_subqueryable:
            new = copy(self)
            new.pre_actions = from_subqueryable.pre_actions
            new.post_actions = from_subqueryable.post_actions
            new.from_ = from_subqueryable
            new._column_states = self._column_states
            return new
        return self
<<<<<<< HEAD
=======


class MockExecutionPlan(LogicalPlan):
    def __init__(
        self,
        source_plan: LogicalPlan,
        session,
        *,
        child: Optional["MockExecutionPlan"] = None,
    ) -> NoReturn:
        super().__init__()
        self.source_plan = source_plan
        self.session = session
        mock_query = MagicMock()
        mock_query.sql = "SELECT MOCK_TEST_FAKE_QUERY()"
        self.queries = [mock_query]
        self.child = child
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


def execute_mock_plan(
    plan: MockExecutionPlan, expr_to_alias: Optional[Dict[str, str]] = None
) -> TableEmulator:
    if expr_to_alias is None:
        expr_to_alias = {}
    if isinstance(plan, (MockExecutionPlan, SnowflakePlan)):
        source_plan = plan.source_plan
        analyzer = plan.session._analyzer
    else:
        source_plan = plan
        analyzer = plan.analyzer
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
        return execute_mock_plan(source_plan.execution_plan, expr_to_alias)
    if isinstance(source_plan, MockSelectStatement):
        projection: Optional[List[Expression]] = source_plan.projection
        from_: Optional[MockSelectable] = source_plan.from_
        where: Optional[Expression] = source_plan.where
        order_by: Optional[List[Expression]] = source_plan.order_by
        limit_: Optional[int] = source_plan.limit_

        from_df = execute_mock_plan(from_, expr_to_alias)

        if not projection and isinstance(from_, MockSetStatement):
            projection = from_.set_operands[0].selectable.projection

        result_df = TableEmulator()

        for exp in projection:
            if isinstance(exp, Star):
                for col in from_df.columns:
                    result_df[col] = from_df[col]
            elif (
                isinstance(exp, UnresolvedAlias)
                and exp.child
                and isinstance(exp.child, Star)
            ):
                for e in exp.child.expressions:
                    col_name = analyzer.analyze(e, expr_to_alias)
                    result_df[col_name] = calculate_expression(
                        e, from_df, analyzer, expr_to_alias
                    )
            else:
                if isinstance(exp, Alias):
                    column_name = expr_to_alias.get(exp.expr_id, exp.name)
                else:
                    column_name = analyzer.analyze(exp, expr_to_alias)

                column_series = calculate_expression(
                    exp, from_df, analyzer, expr_to_alias
                )
                result_df[column_name] = column_series

                if isinstance(exp, (Alias)):
                    if isinstance(exp.child, Attribute):
                        quoted_name = quote_name(exp.name)
                        expr_to_alias[exp.child.expr_id] = quoted_name
                        for k, v in expr_to_alias.items():
                            if v == exp.child.name:
                                expr_to_alias[k] = quoted_name

        if where:
            condition = calculate_expression(where, result_df, analyzer, expr_to_alias)
            result_df = result_df[condition]

        sort_columns_array = []
        sort_orders_array = []
        null_first_last_array = []
        if order_by:
            for exp in order_by:
                sort_columns_array.append(analyzer.analyze(exp.child, expr_to_alias))
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

        if limit_ is not None:
            result_df = result_df.head(n=limit_)

        return result_df
    if isinstance(source_plan, MockSetStatement):
        first_operand = source_plan.set_operands[0]
        res_df = execute_mock_plan(
            MockExecutionPlan(
                source_plan.analyzer.session, source_plan=first_operand.selectable
            ),
            expr_to_alias,
        )
        for operand in source_plan.set_operands[1:]:
            operator = operand.operator
            if operator in (UNION, UNION_ALL):
                cur_df = execute_mock_plan(
                    MockExecutionPlan(
                        source_plan.analyzer.session, source_plan=operand.selectable
                    ),
                    expr_to_alias,
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
        child_rf = execute_mock_plan(source_plan.child, expr_to_alias)
        column_exps = [
            plan.session._analyzer.analyze(exp, expr_to_alias)
            for exp in source_plan.grouping_expressions
        ]

        # Aggregate may not have column_exps, which is allowed in the case of `Dataframe.agg`, in this case we pass
        # lambda x: True as the `by` parameter
        children_dfs = child_rf.groupby(by=column_exps or (lambda x: True), sort=False)
        # we first define the returning DataFrame with its column names
        result_df = TableEmulator(
            columns=[
                plan.session._analyzer.analyze(exp, expr_to_alias)
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
                    exp, cur_group, analyzer, expr_to_alias
                )
                # and then append the calculated value
                values.append(cal_exp_res.iat[0])
            result_df.loc[len(result_df.index)] = values
        return result_df
    if isinstance(source_plan, Range):
        col = pd.Series(
            data=[
                num
                for num in range(source_plan.start, source_plan.end, source_plan.step)
            ]
        )
        result_df = TableEmulator(
            col,
            columns=['"ID"'],
            sf_types={'"ID"': LongType()},
            dtype=object,
        )
        return result_df

    if isinstance(source_plan, Join):
        L_expr_to_alias = {}
        R_expr_to_alias = {}
        left = execute_mock_plan(source_plan.left, L_expr_to_alias).reset_index(
            drop=True
        )
        right = execute_mock_plan(source_plan.right, R_expr_to_alias).reset_index(
            drop=True
        )
        # Processing ON clause
        using_columns = getattr(source_plan.join_type, "using_columns", None)
        on = using_columns
        if isinstance(on, list):  # USING a list of columns
            if on:
                on = [quote_name(x.upper()) for x in on]
            else:
                on = None
        elif isinstance(on, Column):  # ON a single column
            on = on.name
        elif isinstance(
            on, BinaryExpression
        ):  # ON a condition, apply where to a Cartesian product
            on = None
        else:  # ON clause not specified, SF returns a Cartesian product
            on = None

        # Processing the join type
        how = source_plan.join_type.sql
        if how.startswith("USING "):
            how = how[6:]
        if how.startswith("NATURAL "):
            how = how[8:]
        if how == "LEFT OUTER":
            how = "LEFT"
        elif how == "RIGHT OUTER":
            how = "RIGHT"
        elif "FULL" in how:
            how = "OUTER"
        elif "SEMI" in how:
            how = "INNER"
        elif "ANTI" in how:
            how = "CROSS"

        if (
            "NATURAL" in source_plan.join_type.sql and on is None
        ):  # natural joins use the list of common names as keys
            on = left.columns.intersection(right.columns).values.tolist()

        if on is None:
            how = "CROSS"

        result_df = left.merge(
            right,
            on=on,
            how=how.lower(),
        )

        # Restore sf_types information after merging, there should be better way to do this
        result_df.sf_types.update(left.sf_types)
        result_df.sf_types.update(right.sf_types)

        if on:
            result_df = result_df.reset_index(drop=True)
            if isinstance(on, list):
                # Reorder columns for JOINS with USING clause, where Snowflake puts the key columns to the left
                reordered_cols = on + [
                    col for col in result_df.columns.tolist() if col not in on
                ]
                result_df = result_df[reordered_cols]

        common_columns = set(L_expr_to_alias.keys()).intersection(
            R_expr_to_alias.keys()
        )
        new_expr_to_alias = {
            k: v
            for k, v in {
                **L_expr_to_alias,
                **R_expr_to_alias,
            }.items()
            if k not in common_columns
        }
        expr_to_alias.update(new_expr_to_alias)

        if source_plan.condition:
            condition = calculate_expression(
                source_plan.condition, result_df, analyzer, expr_to_alias
            )

            if "SEMI" in source_plan.join_type.sql:  # left semi
                result_df = left[
                    left.apply(tuple, 1).isin(
                        result_df[condition][left.columns].apply(tuple, 1)
                    )
                ].dropna()
            elif "ANTI" in source_plan.join_type.sql:  # left anti
                result_df = left[
                    ~(
                        left.apply(tuple, 1).isin(
                            result_df[condition][left.columns].apply(tuple, 1)
                        )
                    )
                ].dropna()
            elif "LEFT" in source_plan.join_type.sql:  # left outer join
                # rows from LEFT that did not get matched
                unmatched_left = left[
                    ~left.apply(tuple, 1).isin(
                        result_df[condition][left.columns].apply(tuple, 1)
                    )
                ]
                unmatched_left[right.columns] = None
                result_df = pd.concat([result_df[condition], unmatched_left])
            elif "RIGHT" in source_plan.join_type.sql:  # right outer join
                # rows from RIGHT that did not get matched
                unmatched_right = right[
                    ~right.apply(tuple, 1).isin(
                        result_df[condition][right.columns].apply(tuple, 1)
                    )
                ]
                unmatched_right[left.columns] = None
                result_df = pd.concat([result_df[condition], unmatched_right])
            elif "OUTER" in source_plan.join_type.sql:  # full outer join
                # rows from LEFT that did not get matched
                unmatched_left = left[
                    ~left.apply(tuple, 1).isin(
                        result_df[condition][left.columns].apply(tuple, 1)
                    )
                ]
                unmatched_left[right.columns] = None
                # rows from RIGHT that did not get matched
                unmatched_right = right[
                    ~right.apply(tuple, 1).isin(
                        result_df[condition][right.columns].apply(tuple, 1)
                    )
                ]
                unmatched_right[left.columns] = None
                result_df = pd.concat(
                    [result_df[condition], unmatched_left, unmatched_right]
                )
            else:
                result_df = result_df[condition]

        return result_df.where(result_df.notna(), None)  # Swap np.nan with None


def describe(plan: MockExecutionPlan):
    result = execute_mock_plan(plan)
    return [Attribute(result[c].name, result[c].sf_type) for c in result.columns]


def calculate_expression(
    exp: Expression,
    input_data: Union[TableEmulator, ColumnEmulator],
    analyzer,
    expr_to_alias: Dict[str, str],
) -> Tuple[ColumnEmulator]:
    """Returns the calculated expression evaluated based on input table/column"""
    if isinstance(exp, Attribute):
        return input_data[expr_to_alias.get(exp.expr_id, exp.name)]
    if isinstance(exp, (UnresolvedAttribute, Attribute)):
        return input_data[exp.name]
    if isinstance(exp, (UnresolvedAlias, Alias)):
        return calculate_expression(exp.child, input_data, analyzer, expr_to_alias)
    if isinstance(exp, FunctionExpression):
        # evaluated_children maps to parameters passed to the function call
        evaluated_children = [
            calculate_expression(c, input_data, analyzer, expr_to_alias)
            for c in exp.children
        ]
        original_func = getattr(
            importlib.import_module("snowflake.snowpark.functions"), exp.name
        )
        signatures = inspect.signature(original_func)
        spec = inspect.getfullargspec(original_func)
        if exp.name not in _MOCK_FUNCTION_IMPLEMENTATION_MAP:
            raise NotImplementedError(
                f"Mock function {exp.name} has not been implemented yet."
            )
        to_pass_args = []
        for idx, key in enumerate(signatures.parameters):
            if key == spec.varargs:
                to_pass_args.extend(evaluated_children[idx:])
            else:
                try:
                    to_pass_args.append(evaluated_children[idx])
                except IndexError:
                    to_pass_args.append(None)

        if exp.name == "array_agg":
            to_pass_args[-1] = exp.is_distinct
        return _MOCK_FUNCTION_IMPLEMENTATION_MAP[exp.name](*to_pass_args)
    if isinstance(exp, ListAgg):
        column = calculate_expression(exp.col, input_data, analyzer, expr_to_alias)
        return _MOCK_FUNCTION_IMPLEMENTATION_MAP["listagg"](
            column, is_distinct=exp.is_distinct, delimiter=exp.delimiter
        )
    if isinstance(exp, IsNull):
        child_column = calculate_expression(
            exp.child, input_data, analyzer, expr_to_alias
        )
        return ColumnEmulator(data=[bool(data is None) for data in child_column])
    if isinstance(exp, IsNotNull):
        child_column = calculate_expression(
            exp.child, input_data, analyzer, expr_to_alias
        )
        return ColumnEmulator(data=[bool(data is not None) for data in child_column])
    if isinstance(exp, IsNaN):
        child_column = calculate_expression(
            exp.child, input_data, analyzer, expr_to_alias
        )
        return child_column.isna()
    if isinstance(exp, Not):
        child_column = calculate_expression(
            exp.child, input_data, analyzer, expr_to_alias
        )
        return ~child_column
    if isinstance(exp, UnresolvedAttribute):
        return analyzer.analyze(exp, expr_to_alias)
    if isinstance(exp, Literal):
        return exp.value
    if isinstance(exp, BinaryExpression):
        new_column = None
        left = calculate_expression(exp.left, input_data, analyzer, expr_to_alias)
        right = calculate_expression(exp.right, input_data, analyzer, expr_to_alias)
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
        column = calculate_expression(exp.expr, input_data, analyzer, expr_to_alias)
        pattern = str(analyzer.analyze(exp.pattern, expr_to_alias))
        pattern = f"^{pattern}" if not pattern.startswith("^") else pattern
        pattern = f"{pattern}$" if not pattern.endswith("$") else pattern
        return column.str.match(pattern)
    if isinstance(exp, Like):
        column = calculate_expression(exp.expr, input_data, analyzer, expr_to_alias)
        pattern = convert_wildcard_to_regex(
            str(analyzer.analyze(exp.pattern, expr_to_alias))
        )
        return column.str.match(pattern)
    if isinstance(exp, InExpression):
        column = calculate_expression(exp.columns, input_data, analyzer, expr_to_alias)
        values = [
            calculate_expression(expression, input_data, analyzer, expr_to_alias)
            for expression in exp.values
        ]
        return column.isin(values)
    if isinstance(exp, MultipleExpression):
        raise NotImplementedError("MultipleExpression is to be implemented")
>>>>>>> 4855a30 (Clean up)
