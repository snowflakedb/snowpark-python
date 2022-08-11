#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import re
from abc import ABC, abstractmethod
from collections import UserDict
from copy import copy
from enum import Enum
from typing import Iterable, List, Optional, Set, Union

from snowflake.snowpark._internal.analyzer import analyzer_utils
from snowflake.snowpark._internal.analyzer.analyzer_utils import quote_name
from snowflake.snowpark._internal.analyzer.binary_expression import And
from snowflake.snowpark._internal.analyzer.expression import (
    COLUMN_DEPENDENCY_ALL,
    COLUMN_DEPENDENCY_EMPTY,
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


class ColumnState:
    def __init__(
        self,
        index: Optional[int],
        change_state: ColumnChangeState,
        expression: Optional[Union[str, Expression]],
        dependent_columns: Optional[Set[str]] = COLUMN_DEPENDENCY_ALL,
        depend_on_same_level: bool = False,
        referenced_by_same_level_columns: Optional[Set[str]] = COLUMN_DEPENDENCY_EMPTY,
        state_dict: "ColumnStateDict" = None,
    ) -> None:
        self.index = index
        self.change_state = change_state
        self.expression = expression
        self.dependent_columns = dependent_columns
        self.depend_on_same_level = depend_on_same_level  # where this column's expression depends on other columns of the same level
        self.referenced_by_same_level_columns = referenced_by_same_level_columns  # referenced by other columns explicitly at the same level.
        self.state_dict = state_dict

    def add_referenced_by_same_level_column(self, col_name):
        if self.referenced_by_same_level_columns in (
            COLUMN_DEPENDENCY_ALL,
            COLUMN_DEPENDENCY_EMPTY,
        ):
            self.referenced_by_same_level_columns = set()
        self.referenced_by_same_level_columns.add(col_name)

    @property
    def referenced_by_same_level_column(self):
        return bool(self.state_dict.columns_referencing_all_columns) or bool(
            self.referenced_by_same_level_columns
        )


class ColumnStateDict(UserDict):
    def __init__(self) -> None:
        super().__init__(dict())
        self.has_changed_column_exp: bool = False
        self.dropped_columns: Optional[Set[str]] = None
        self.active_columns: Set[str] = set()
        self.columns_referencing_all_columns: Set[str] = set()

    @property
    def has_dropped_columns(self):
        return bool(self.dropped_columns)

    def __setitem__(self, col_name, col_state: ColumnState):
        super().__setitem__(col_name, col_state)
        if col_state.change_state == ColumnChangeState.DROPPED:
            if self.dropped_columns is None:
                self.dropped_columns = set()
            self.dropped_columns.add(col_name)
        else:
            self.active_columns.add(col_name)
            if col_state.change_state == ColumnChangeState.CHANGED_EXP:
                self.has_changed_column_exp = True

    def get_active_column_state(self, item):
        fetched_item = super().get(item)
        if fetched_item.change_state == ColumnChangeState.DROPPED:
            return None
        return fetched_item


class Selectable(LogicalPlan, ABC):
    def __init__(self, analyzer) -> None:
        super().__init__()
        self.analyzer = analyzer
        self._column_states: Optional[ColumnStateDict] = None

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
    def column_states(self) -> ColumnStateDict:
        if self._column_states is None:
            self._column_states = ColumnStateDict()
            for i, attr in enumerate(self.snowflake_plan.attributes):
                self._column_states[attr.name] = ColumnState(
                    index=i,
                    change_state=ColumnChangeState.UNCHANGED_EXP,
                    expression=UnresolvedAttribute(quote_name(attr.name)),
                    dependent_columns=COLUMN_DEPENDENCY_EMPTY,
                    depend_on_same_level=False,
                    referenced_by_same_level_columns=COLUMN_DEPENDENCY_EMPTY,
                    state_dict=self._column_states,
                )
        return self._column_states


class SelectableEntity(Selectable):
    def __init__(self, entity_name, *, analyzer=None) -> None:
        super().__init__(analyzer)
        self.entity_name = entity_name

    def final_select_sql(self) -> str:
        return self.entity_name

    def schema_query(self) -> str:
        return f"{analyzer_utils.SELECT} * {analyzer_utils.FROM} {self.entity_name}"


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
                else f"{analyzer_utils.SELECT} * {analyzer_utils.FROM} {self.from_.final_select_sql()}"
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
            f" {analyzer_utils.WHERE} {self.analyzer.analyze(self.where_)}"
            if self.where_ is not None
            else ""
        )
        order_by_clause = (
            f" {analyzer_utils.ORDER_BY} {','.join(self.analyzer.analyze(x) for x in self.order_by_)}"
            if self.order_by_
            else ""
        )
        limit_clause = f" {analyzer_utils.LIMIT} {self.limit_}" if self.limit_ else ""
        return f"{analyzer_utils.SELECT} {projection} {analyzer_utils.FROM} {from_clause}{join_clause}{where_clause}{order_by_clause}{limit_clause}"

    def schema_query(self) -> str:
        return self.final_select_sql()

    def filter(self, col: Expression) -> "SelectStatement":
        if self.projection_ and self.column_states.has_changed_column_exp:
            return SelectStatement(from_=self, where_=col, analyzer=self.analyzer)
        new = copy(self)
        new.where_ = And(self.where_, col) if self.where_ is not None else col
        return new

    def select(self, cols) -> "SelectStatement":
        final_projection = []
        new_column_states = get_column_states(cols, self)
        if self._has_clause_using_columns():
            # If there is any clause except limit, we don't flatten.
            can_flatten = False
        else:
            can_flatten = True
            parent_column_states = self.column_states
            parent_has_dropped_column = parent_column_states.has_dropped_columns
            for col, state in new_column_states.items():
                parent_state = parent_column_states.get(col)
                if state.change_state == ColumnChangeState.CHANGED_EXP:
                    dependent_columns = state.dependent_columns
                    if dependent_columns == COLUMN_DEPENDENCY_ALL:
                        if (
                            parent_has_dropped_column  # in case a dependent column is a dropped column
                            or parent_state.change_state
                            != ColumnChangeState.UNCHANGED_EXP
                        ) or parent_column_states.has_changed_column_exp:
                            can_flatten = False
                            break
                    else:
                        for dc in dependent_columns:
                            dc_in_parent_state = parent_column_states.get(dc)
                            if (
                                dc_in_parent_state
                                and dc_in_parent_state.change_state
                                != ColumnChangeState.UNCHANGED_EXP
                            ):
                                # check if any dependent columns have changed value
                                can_flatten = False
                                break
                        if not can_flatten:
                            break
                    final_projection.append(state.expression)
                elif state.change_state == ColumnChangeState.NEW:
                    dependent_columns = state.dependent_columns
                    if dependent_columns == COLUMN_DEPENDENCY_ALL:
                        if (
                            parent_has_dropped_column  # in case the new column has a dependency on a dropped column
                            or self.column_states.has_changed_column_exp
                        ):
                            # When the parent has a dropped column, a new column may use that dropped column.
                            # Flattening the SQL would make a wrong SQL to work unexpectedly.
                            # for instance, select c + 1 as d from (select a from test_table)
                            # d is a new column. `select c + 1 as d from test_table` is wrong.
                            can_flatten = False
                            break
                    else:
                        for dc in dependent_columns:
                            dc_in_parent_state = parent_column_states.get(dc)
                            if (
                                dc_in_parent_state
                                and dc_in_parent_state.change_state
                                != ColumnChangeState.UNCHANGED_EXP
                            ):
                                # check if any dependent columns have changed value
                                can_flatten = False
                                break
                        if not can_flatten:
                            break
                    final_projection.append(state.expression)
                elif state.change_state == ColumnChangeState.UNCHANGED_EXP:
                    # query may change sequence of columns. If subquery has same-level reference, flattened sql may not work.
                    if parent_column_states[col].depend_on_same_level:
                        can_flatten = False
                        break
                    final_projection.append(
                        parent_column_states[col].expression
                    )  # add parent's expression for this column name
                else:  # state == ColumnChangeState.DROPPED:
                    if parent_state.change_state == ColumnChangeState.NEW and (
                        parent_state.referenced_by_same_level_column
                        or parent_state.change_state == ColumnChangeState.DROPPED
                    ):
                        can_flatten = False
                        break
        if can_flatten:
            new = copy(self)
            new.projection_ = final_projection
        else:
            final_projection = cols
            new = SelectStatement(projection_=cols, from_=self, analyzer=self.analyzer)
        new._column_states = get_column_states(final_projection, new.from_)
        return new

    def with_columns(self, cols) -> "SelectStatement":
        can_flatten = True
        for c in cols:
            c_name = parse_column_name(c, self.analyzer)
            parent_state = self.column_states.get(c_name)
            if parent_state and parent_state == ColumnChangeState.CHANGED_EXP:
                can_flatten = False
                break
        if can_flatten:
            new = SelectStatement(
                projection_=[*self.projection_, *cols],
                from_=self,
                analyzer=self.analyzer,
            )
            return new
        else:
            new = copy(self)
            new.projection_ = [*self.projection_, *cols]
        return new

    def sort(self, cols) -> "SelectStatement":
        if self.projection_ and self.column_states.has_changed_column_exp:
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
        operator: str = "UNION",
    ) -> "SelectStatement":
        if operator == "EXCEPT":
            except_set_statement = SetStatement(
                *(SetOperand(x, "UNION") for x in selectables)
            )
            set_operands = (SetOperand(except_set_statement, "EXCEPT"),)
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
    def column_states(self) -> Optional[ColumnStateDict]:
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
    # TODO: convert to upper case if the column_name is an identifier
    return column_name


def get_dependent_columns(column_exp: Union[Expression, str]) -> Set[str]:
    if isinstance(column_exp, Expression):
        return column_exp.dependent_column_names()
    return COLUMN_DEPENDENCY_ALL


def get_column_states(cols: Iterable[Expression], from_: Selectable) -> ColumnStateDict:
    analyzer = from_.analyzer
    column_states = ColumnStateDict()
    column_index = 0
    # populate column status against subquery
    quoted_col_names = []
    for c in cols:
        c_name = parse_column_name(c, analyzer)
        quoted_c_name = analyzer_utils.quote_name(c_name)
        quoted_col_names.append(quoted_c_name)
        from_c_state = from_.column_states.get(quoted_c_name)
        if from_c_state:
            if c_name != from_.analyzer.analyze(c):
                column_states[quoted_c_name] = ColumnState(
                    column_index,
                    ColumnChangeState.CHANGED_EXP,
                    c,
                    state_dict=column_states,
                )
            else:
                column_states[quoted_c_name] = ColumnState(
                    column_index,
                    ColumnChangeState.UNCHANGED_EXP,
                    c,
                    state_dict=column_states,
                )
        else:
            column_states[quoted_c_name] = ColumnState(
                column_index, ColumnChangeState.NEW, c, state_dict=column_states
            )
        column_index += 1
    # end of populate column status against subquery

    # populate column dependency
    for c, quoted_c_name in zip(cols, quoted_col_names):
        dependent_column_names = get_dependent_columns(c)
        column_states[quoted_c_name].dependent_columns = dependent_column_names
        if dependent_column_names == COLUMN_DEPENDENCY_ALL:
            column_states[quoted_c_name].depend_on_same_level = True
            column_states.columns_referencing_all_columns.add(quoted_c_name)
        else:
            for dependent_column in dependent_column_names:
                if dependent_column not in from_.column_states.active_columns:
                    column_states[quoted_c_name].depend_on_same_level = True
                    if dependent_column in column_states:
                        column_states[
                            dependent_column
                        ].add_referenced_by_same_level_column(dependent_column)
                    else:
                        # TODO: change to a new SQL
                        raise ValueError(
                            f"Column name {dependent_column} used in a column expression but it doesn't exist."
                        )
    # end of populate column dependency

    for dc in from_.column_states.active_columns - column_states.active_columns:
        column_states[dc] = ColumnState(
            None, ColumnChangeState.DROPPED, None, None, None, state_dict=column_states
        )
    return column_states
