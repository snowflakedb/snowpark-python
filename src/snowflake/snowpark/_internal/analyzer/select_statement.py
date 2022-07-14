#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import re
from abc import ABC, abstractmethod
from copy import copy
from enum import Enum
from typing import Dict, Iterable, List, NamedTuple, Optional, Union

from snowflake.snowpark._internal.analyzer import analyzer_utils
from snowflake.snowpark._internal.analyzer.analyzer_utils import quote_name
from snowflake.snowpark._internal.analyzer.binary_expression import And
from snowflake.snowpark._internal.analyzer.expression import (
    Expression,
    UnresolvedAttribute,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import Query, SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import LogicalPlan


class ColumnChangeState(Enum):
    NEW = "new_exp"
    UNCHANGED_EXP = "unchanged"
    CHANGED_EXP = "changed"
    DROPPED = "dropped"


class ColumnState(NamedTuple):
    change_state: ColumnChangeState
    expression: Optional[Union[str, Expression]]  # None means the expression is unknown


class Selectable(LogicalPlan, ABC):
    def __init__(self, analyzer) -> None:
        super().__init__()
        self.analyzer = analyzer
        self._column_states = dict()

    def pre_actions(self) -> Optional[List["Query"]]:
        return None

    def post_actions(self) -> Optional[List["Query"]]:
        return None

    @abstractmethod
    def final_select_sql(self) -> str:
        ...

    @abstractmethod
    def schema_query(self) -> str:
        ...

    @property
    def snowflake_plan(self):
        queries = [Query(self.final_select_sql())]
        if self.pre_actions():
            queries = self.pre_actions() + queries
        return SnowflakePlan(
            queries,
            self.schema_query(),
            post_actions=self.post_actions(),
            session=self.analyzer.session,
        )

    @property
    def column_states(self) -> Dict[str, ColumnState]:
        if not self._column_states:
            self._column_states = {
                x.name: ColumnState(
                    ColumnChangeState.UNCHANGED_EXP,
                    UnresolvedAttribute(quote_name(x.name)),
                )
                for x in self.snowflake_plan.attributes
            }
        return self._column_states


class SelectableEntity(Selectable):
    def __init__(self, entity_name, *, analyzer=None) -> None:
        super().__init__(analyzer)
        self.entity_name = entity_name

    def final_select_sql(self) -> str:
        return self.entity_name

    def schema_query(self) -> str:
        return f"select * from {self.entity_name}"


class SelectSQL(Selectable):
    def __init__(self, sql, *, analyzer=None) -> None:
        super().__init__(analyzer)
        self.sql = sql

    def final_select_sql(self) -> str:
        return self.sql

    def schema_query(self) -> str:
        return self.sql


class SelectSnowflakePlan(Selectable):
    def __init__(self, snowflake_plan: SnowflakePlan, *, analyzer=None) -> None:
        super().__init__(analyzer)
        self._snowflake_plan = snowflake_plan

    @property
    def snowflake_plan(self):
        return self._snowflake_plan

    def pre_actions(self) -> Optional[List["Query"]]:
        return self._snowflake_plan.queries[:-1]

    def final_select_sql(self) -> str:
        return self._snowflake_plan.queries[-1].sql

    def post_actions(self) -> Optional[List["Query"]]:
        return self._snowflake_plan.post_actions

    def schema_query(self) -> str:
        return self.snowflake_plan.schema_query


class Join:
    def __init__(
        self,
        left: Selectable,
        right: Selectable,
        join_column: Optional[Expression],
        join_type: str,
        *,
        analyzer=None,
    ) -> None:
        self.left = left
        self.right = right
        self.join_type = join_type
        self.join_column: Expression = join_column
        self.analyzer = analyzer

    def final_select_sql(self) -> str:
        return f""" {self.join_type} ({self.right.final_select_sql()}) on {self.analyzer.analyze(self.join_column)}"""


class SelectStatement(Selectable):
    def __init__(
        self,
        *,
        projection_: Optional[List[Union[Expression]]] = None,
        from_: Optional["Selectable"] = None,
        from_entity: Optional[str] = None,
        join_: Optional[List[Join]] = None,
        where_: Optional[Expression] = None,
        order_by_: Optional[List[Expression]] = None,
        limit_: Optional[int] = None,
        analyzer=None,
    ) -> None:
        super().__init__(analyzer)
        self.projection_: Optional[List[Expression]] = projection_
        self.from_: Optional["Selectable"] = from_
        self.from_entity: Optional[
            str
        ] = from_entity  # table, sql, SelectStatement, UnionStatement
        self.join_: Optional[List[Join]] = join_
        self.where_: Optional[Expression] = where_
        self.order_by_: Optional[List[Expression]] = order_by_
        self.limit_: Optional[int] = limit_

    def _has_clause_using_columns(self) -> bool:
        return any(
            (
                self.join_ is not None,
                self.where_ is not None,
                self.order_by_ is not None,
            )
        )

    def _has_clause(self) -> bool:
        return self._has_clause_using_columns() or self.limit_ is not None

    def final_select_sql(self) -> str:
        if not self._has_clause() and not self.projection_:
            return (
                self.from_.final_select_sql()
                if not isinstance(self.from_, SelectableEntity)
                else f"select * from {self.from_.final_select_sql()}"
            )
        projection = (
            ",".join(self.analyzer.analyze(x) for x in self.projection_)
            if self.projection_
            else "*"
        )
        from_clause = (
            f"({self.from_.final_select_sql()})"
            if not isinstance(self.from_, SelectableEntity)
            else self.from_.final_select_sql()
        )
        join_clause = (
            ", ".join(x.final_select_sql() for x in self.join_) if self.join_ else ""
        )
        where_clause = (
            f" where {self.analyzer.analyze(self.where_)}"
            if self.where_ is not None
            else ""
        )
        order_by_clause = (
            f" order by {','.join(self.analyzer.analyze(x) for x in self.order_by_)}"
            if self.order_by_
            else ""
        )
        limit_clause = f" limit {self.limit_}" if self.limit_ else ""
        return f"select {projection} from {from_clause}{join_clause}{where_clause}{order_by_clause}{limit_clause}"

    def schema_query(self) -> str:
        return self.final_select_sql()

    def filter(self, col: Expression) -> "SelectStatement":
        if self.projection_ and has_changed_column_exp(self.column_states):
            return SelectStatement(from_=self, where_=col, analyzer=self.analyzer)
        new = copy(self)
        new.where_ = And(self.where_, col) if self.where_ is not None else col
        return new

    def select(self, cols) -> "SelectStatement":
        """
        // for selection, it needs to compare the projection list and
        select a from (select * from test_table); // can flatten because a exists in both layers and there is no expression change for a in the final query
        select a from (select a, b from test_table); // can flatten, same as above.
        select a + 1 as d from (select a from test_table);  // can flatten because d is new in the previous 2 layers and expression `a + 1` uses a and a doesn't have an expression change.
        select a + 2 as e from (select a, a + 1 as d from test_table); // can flatten because e is new in previous 2 layers and expression `a + 2` uses a and a doesn't have an expression change.
        // Question: How do we detect a + 2 uses column a and no other columns?
        select a, d, a + 2 as e from (select a, a + 1 as d from test_table) where d=1 and e=2; /* can be changed to */ select a, a + 1 as d, a + 2 as e from test_table where d=1 and e=2


        select a, e from (select a, a + 1 as d, d + 1 as e from test_table) /* can't flatten to*/ select a, d + 1 as e // because column d is undefined. Rule: if outside is less than inside and there is column exp change, then don't flatten.
        select a from (select a from test_table where b = 1 order by c);  // can't flatten because there are where and order by clause. Plus, users should barely write this query.
        select a from (select a + 1 as a from test_table);  // can't flatten becasue a has a expression change in subquery.
        select d from (select a as d from test_table);  // can't flatten because d is new in subquery but not in test_table.
        select d from (select a from test_table); // can't flatten because d can't be found in subquery list
        select a + 2 as b, b + 1 as c from (select a + 1 as a from test_table);  // can't flatten because the outer b doesn't exist in subquery but in test_table.

        If there is any clause except limit, we don't flatten.
        If there is any changed column expression, we don't flatten.
        All columns must be able to flatten:
            If a column has no expression, it can flatten by using the expression in from_:
            (If a column has expression changed, it can't flatten)
            (If a column has no expression, and )
            If a new column doesn't exist in the from_'s from_, it can flatten by using the expression.
            (If a new column exists in the from_'s from_, it can't flatten)
            If a column is dropped, and it has no expression in the parent, it can flatten by dropping it.


        What can't be flattened:
            1. There is a column expression (regardless new or old column), and there is column expression in the parent layer, .
            2. There is a column expression on old column, no flatten.

        """
        final_projection = []
        new_column_states = get_column_states(cols, self)
        if self._has_clause_using_columns():
            # If there is any clause except limit, we don't flatten.
            can_flatten = False
        elif has_changed_column_exp(self.column_states):
            # If both this layer and the parent has changed column expression, we don't flatten.
            can_flatten = False
        else:
            can_flatten = True
            parent_column_states = self.column_states
            for col, state in new_column_states.items():
                parent_state = parent_column_states.get(col)
                if state.change_state == ColumnChangeState.CHANGED_EXP:
                    if parent_state.change_state != ColumnChangeState.UNCHANGED_EXP:
                        can_flatten = False
                        break
                    final_projection.append(state.expression)
                elif state.change_state == ColumnChangeState.NEW:
                    if (
                        parent_state
                        and parent_state.change_state != ColumnChangeState.DROPPED
                    ):
                        can_flatten = False
                        break
                    final_projection.append(state.expression)
                elif state.change_state == ColumnChangeState.UNCHANGED_EXP:
                    final_projection.append(
                        parent_column_states[col].expression
                    )  # add parent's expression for this column name
                else:  # state == ColumnChangeState.DROPPED:
                    if parent_state and parent_state.change_state not in (
                        ColumnChangeState.DROPPED,
                        ColumnChangeState.UNCHANGED_EXP,
                    ):
                        can_flatten = False
                        break
        if can_flatten:
            new = copy(self)
            new.projection_ = final_projection
        else:
            final_projection = cols
            new = SelectStatement(projection_=cols, from_=self, analyzer=self.analyzer)
        new._column_states = get_column_states(final_projection, self.from_)
        return new

    def sort(self, cols) -> "SelectStatement":
        if self.projection_ and has_changed_column_exp(self.column_states):
            return SelectStatement(from_=self, order_by_=cols, analyzer=self.analyzer)
        new = copy(self)
        new.order_by_ = cols
        return new

    def join(
        self, other: Selectable, join_column: Optional[Expression], join_type: str
    ) -> "SelectStatement":
        new = copy(self)
        join_ = Join(self, other, join_column, join_type, analyzer=self.analyzer)
        new.join_ = new.join_.append(join_) if new.join_ else [join_]
        return new

    def set_operate(
        self,
        *selectables: Union[
            SelectSnowflakePlan,
            "SelectStatement",
        ],
        operator: str = "union",
    ) -> "SelectStatement":
        if operator == "except":
            except_set_statement = SetStatement(
                *(SetOperand(x, "union") for x in selectables)
            )
            set_operands = (SetOperand(except_set_statement, "except"),)
        else:
            set_operands = tuple(SetOperand(x, operator) for x in selectables)
        if isinstance(self.from_, SetStatement) and not self._has_clause():
            set_statement = SetStatement(
                *self.from_.set_operands, *set_operands, analyzer=self.analyzer
            )
        else:
            set_statement = SetStatement(
                SetOperand(self, operator),
                *set_operands,
                analyzer=self.analyzer,
            )
        new = SelectStatement(analyzer=self.analyzer)
        new.from_ = set_statement
        return new

    def limit(self, n: int):
        new = copy(self)
        new.limit_ = min(self.limit_, n) if self.limit_ else n
        return new


class SetOperand:
    def __init__(self, selectable: Selectable, operator) -> None:
        super().__init__()
        self.selectable = selectable
        self.operator = operator


class SetStatement(Selectable):
    def __init__(self, *set_operands: SetOperand, analyzer=None) -> None:
        super().__init__(analyzer=analyzer)
        self.analyzer = analyzer
        self.set_operands = set_operands

    def final_select_sql(self) -> str:
        sql = self.set_operands[0].selectable.final_select_sql()
        for i in range(1, len(self.set_operands)):
            sql += f" {self.set_operands[i].operator} ({self.set_operands[i].selectable.final_select_sql()})"
        return sql

    def schema_query(self) -> str:
        return self.set_operands[0].selectable.schema_query()

    @property
    def column_states(self) -> Dict[str, bool]:
        if not self._column_states:
            self._column_states = self.set_operands[0].selectable._column_states
        return self._column_states


COLUMN_EXP_PATTERN = re.compile(
    '^"?.*"? as ("?.*"?)$', re.IGNORECASE
)  # this isn't accurate yet


def parse_column_name(column: Union[str, Expression], analyzer):
    column_in_str = (
        analyzer.analyze(column) if isinstance(column, Expression) else column
    )
    match = COLUMN_EXP_PATTERN.match(column_in_str)
    column_name = match.group(1) if match else column_in_str
    return column_name


def get_column_states(
    cols: Iterable[Expression], from_: Selectable
) -> Dict[str, ColumnState]:
    analyzer = from_.analyzer
    column_states = {}
    for c in cols:
        c_name = parse_column_name(c, analyzer)
        quoted_c_name = analyzer_utils.quote_name(c_name)
        from_c_state = from_.column_states.get(quoted_c_name)
        if from_c_state and from_c_state.change_state != ColumnChangeState.DROPPED:
            if c_name != from_.analyzer.analyze(c):
                column_states[c_name] = ColumnState(ColumnChangeState.CHANGED_EXP, c)
            else:
                column_states[c_name] = ColumnState(ColumnChangeState.UNCHANGED_EXP, c)
        else:
            if c_name == analyzer.analyze(c) if isinstance(c, Expression) else c:
                raise ValueError(f"Wrong column name: {c_name}")
            column_states[c_name] = ColumnState(ColumnChangeState.NEW, c)

    dropped_columns = from_.column_states.keys() - column_states.keys()
    for dc in dropped_columns:
        column_states[dc] = ColumnState(ColumnChangeState.DROPPED, None)
    return column_states


def has_changed_column_exp(column_states: Dict[str, ColumnState]):
    return any(
        x
        for x in column_states.values()
        if x.change_state == ColumnChangeState.CHANGED_EXP
    )
