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
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    quote_name,
    result_scan_statement,
)
from snowflake.snowpark._internal.analyzer.binary_expression import And
from snowflake.snowpark._internal.analyzer.expression import (
    COLUMN_DEPENDENCY_ALL,
    COLUMN_DEPENDENCY_EMPTY,
    Expression,
    Star,
    UnresolvedAttribute,
)
from snowflake.snowpark._internal.analyzer.schema_utils import analyze_attributes
from snowflake.snowpark._internal.analyzer.snowflake_plan import Query, SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import LogicalPlan
from snowflake.snowpark._internal.analyzer.unary_expression import UnresolvedAlias

SET_UNION = "UNION"
SET_UNION_ALL = "UNION ALL"
SET_INTERSECT = "INTERSECT"
SET_EXCEPT = "EXCEPT"


class ColumnChangeState(Enum):
    NEW = "new_exp"
    UNCHANGED_EXP = "unchanged"
    CHANGED_EXP = "changed"
    DROPPED = "dropped"


class ColumnState:
    def __init__(
        self,
        col_name: str,
        index: Optional[int],
        change_state: ColumnChangeState,
        expression: Optional[Union[str, Expression]],
        dependent_columns: Optional[Set[str]] = COLUMN_DEPENDENCY_ALL,
        depend_on_same_level: bool = False,
        referenced_by_same_level_columns: Optional[Set[str]] = COLUMN_DEPENDENCY_EMPTY,
        state_dict: "ColumnStateDict" = None,
    ) -> None:
        self.col_name = col_name
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
        return (
            len(self.state_dict.columns_referencing_all_columns) > 1
            or (
                len(self.state_dict.columns_referencing_all_columns) == 1
                and self.col_name not in self.state_dict.columns_referencing_all_columns
            )
            or bool(self.referenced_by_same_level_columns)
        )


class ColumnStateDict(UserDict):
    def __init__(self) -> None:
        super().__init__(dict())
        self.has_changed_columns: bool = False
        self.has_new_columns: bool = False
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
                self.has_changed_columns = True
            elif col_state.change_state == ColumnChangeState.NEW:
                self.has_new_columns = True

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
        self.pre_actions: Optional[List["Query"]] = None
        self.post_actions: Optional[List["Query"]] = None

    @abstractmethod
    def final_select_sql(self) -> str:
        ...

    @abstractmethod
    def schema_query(self) -> str:
        ...

    def to_subqueryable(self) -> "Selectable":
        return self

    @property
    def snowflake_plan(self):
        queries = [Query(self.final_select_sql())]
        if self.pre_actions:
            queries = self.pre_actions + queries
        return SnowflakePlan(
            queries,
            self.schema_query(),
            post_actions=self.post_actions,
            session=self.analyzer.session,
        )

    @property
    def column_states(self) -> ColumnStateDict:
        if self._column_states is None:
            self._column_states = initiate_column_states(self.snowflake_plan)
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
    def __init__(self, sql: str, *, analyzer=None, to_select: bool = False) -> None:
        super().__init__(analyzer)
        self.to_select = to_select
        self.original_sql = sql
        is_select = sql.strip().lower().startswith("select")
        if to_select and not is_select:
            self.pre_actions = [Query(sql)]
            self.sql = result_scan_statement(self.pre_actions[0].query_id_place_holder)
            self._schema_query = analyzer_utils.schema_value_statement(
                analyze_attributes(sql, self.analyzer.session)
            )
        else:
            self.sql = sql
            self._schema_query = sql

    def final_select_sql(self) -> str:
        return self.sql

    def schema_query(self) -> str:
        return self._schema_query

    @property
    def snowflake_plan(self):
        query_object = Query(self.final_select_sql())
        return SnowflakePlan(
            queries=[self.pre_actions[0], query_object]
            if self.pre_actions
            else [query_object],
            schema_query=self.schema_query(),
            session=self.analyzer.session,
        )

    def to_subqueryable(self):
        if self.to_select:
            return self
        new = SelectSQL(self.sql, to_select=True, analyzer=self.analyzer)
        new._column_states = self.column_states
        return new


class SelectSnowflakePlan(Selectable):
    def __init__(self, snowflake_plan: LogicalPlan, *, analyzer=None) -> None:
        super().__init__(analyzer)
        self._snowflake_plan = (
            snowflake_plan
            if isinstance(snowflake_plan, SnowflakePlan)
            else analyzer.resolve(snowflake_plan)
        )
        self.pre_actions = self._snowflake_plan.queries[:-1]
        self.post_actions = self._snowflake_plan.post_actions

    @property
    def snowflake_plan(self):
        return self._snowflake_plan

    def final_select_sql(self) -> str:
        return self._snowflake_plan.queries[-1].sql

    def schema_query(self) -> str:
        return self.snowflake_plan.schema_query


class Join:
    def __init__(
        self,
        left: Selectable,
        right: Selectable,
        joincolumn: Optional[Expression],
        jointype: str,
        *,
        analyzer=None,
    ) -> None:
        self.left = left
        self.right = right
        self.jointype = jointype
        self.joincolumn: Expression = joincolumn
        self.analyzer = analyzer

    def final_select_sql(self) -> str:
        return f""" {self.jointype} ({self.right.final_select_sql()}) on {self.analyzer.analyze(self.joincolumn)}"""


class SelectStatement(Selectable):
    def __init__(
        self,
        *,
        projection_: Optional[List[Union[Expression]]] = None,
        from_: Optional["Selectable"] = None,
        from_entity: Optional[str] = None,
        join: Optional[List[Join]] = None,
        where: Optional[Expression] = None,
        order_by: Optional[List[Expression]] = None,
        limit_: Optional[int] = None,
        offset: Optional[int] = None,
        analyzer=None,
    ) -> None:
        super().__init__(analyzer)
        self.projection_: Optional[List[Expression]] = projection_
        self.from_: Optional["Selectable"] = from_
        self.from_entity: Optional[
            str
        ] = from_entity  # table, sql, SelectStatement, UnionStatement
        self.join: Optional[List[Join]] = join
        self.where: Optional[Expression] = where
        self.order_by: Optional[List[Expression]] = order_by
        self.limit_: Optional[int] = limit_
        self.offset = offset
        self._schema_query = None
        self.pre_actions = self.from_.pre_actions
        self.post_actions = self.from_.post_actions

    def _has_clause_using_columns(self) -> bool:
        return any(
            (
                self.join is not None,
                self.where is not None,
                self.order_by is not None,
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
            ", ".join(x.final_select_sql() for x in self.join) if self.join else ""
        )
        where_clause = (
            f" {analyzer_utils.WHERE} {self.analyzer.analyze(self.where)}"
            if self.where is not None
            else ""
        )
        order_by_clause = (
            f" {analyzer_utils.ORDER_BY} {','.join(self.analyzer.analyze(x) for x in self.order_by)}"
            if self.order_by
            else ""
        )
        limit_clause = (
            f" {analyzer_utils.LIMIT} {self.limit_}" if self.limit_ is not None else ""
        )
        offset_clause = f" {analyzer_utils.OFFSET} {self.offset}" if self.offset else ""
        self._schema_query = f"{analyzer_utils.SELECT} {projection} {analyzer_utils.FROM}({self.from_.schema_query()}){join_clause}"
        return f"{analyzer_utils.SELECT} {projection} {analyzer_utils.FROM} {from_clause}{join_clause}{where_clause}{order_by_clause}{limit_clause}{offset_clause}"

    def schema_query(self) -> str:
        return self._schema_query or self.from_.schema_query()

    def to_subqueryable(self) -> "Selectable":
        from_subqueryable = self.from_.to_subqueryable()
        if self.from_ != from_subqueryable:
            new = copy(self)
            new.pre_actions = from_subqueryable.pre_actions
            new.post_actions = from_subqueryable.post_actions
            new.from_ = from_subqueryable
            return new
        return self

    def select(self, cols) -> "SelectStatement":
        if (
            len(cols) == 1
            and isinstance(cols[0], UnresolvedAlias)
            and isinstance(cols[0].child, Star)
        ):
            return self
        final_projection = []
        new_column_states = derive_column_states_from_subquery(cols, self)
        if self._has_clause_using_columns():
            can_flatten = False
        else:
            can_flatten = True
            subquery_column_states = self.column_states
            for col, state in new_column_states.items():
                subquery_state = subquery_column_states.get(col)
                if state.change_state in (
                    ColumnChangeState.CHANGED_EXP,
                    ColumnChangeState.NEW,
                ):
                    dependent_columns = state.dependent_columns
                    if dependent_columns == COLUMN_DEPENDENCY_ALL:
                        if (
                            subquery_column_states.has_changed_columns
                            or subquery_column_states.has_new_columns
                            or subquery_column_states.has_dropped_columns
                        ):
                            can_flatten = False
                            break
                    else:
                        can_flatten = can_projection_dependent_columns_flatten(
                            dependent_columns, subquery_column_states
                        )
                        if not can_flatten:
                            break
                    final_projection.append(state.expression)
                elif state.change_state == ColumnChangeState.UNCHANGED_EXP:
                    # query may change sequence of columns. If subquery has same-level reference, flattened sql may not work.
                    if subquery_column_states[col].depend_on_same_level:
                        can_flatten = False
                        break
                    final_projection.append(
                        subquery_column_states[col].expression
                    )  # add subquery's expression for this column name
                else:  # state == ColumnChangeState.DROPPED:
                    if (
                        subquery_state.change_state == ColumnChangeState.NEW
                        and subquery_state.referenced_by_same_level_column
                    ):
                        can_flatten = False
                        break
        if can_flatten:
            new = copy(self)
            new.projection_ = final_projection
            new.from_ = self.from_.to_subqueryable()
            new.pre_actions = new.from_.pre_actions
            new.post_actions = new.from_.post_actions
        else:
            final_projection = cols
            new = SelectStatement(
                projection_=cols, from_=self.to_subqueryable(), analyzer=self.analyzer
            )
        new._column_states = derive_column_states_from_subquery(
            final_projection, new.from_
        )
        return new

    def filter(self, col: Expression) -> "SelectStatement":
        dependent_columns = get_dependent_columns(col)
        can_flatten = can_clause_dependent_columns_flatten(
            dependent_columns, self.column_states
        )
        if can_flatten:
            new = copy(self)
            new.from_ = self.from_.to_subqueryable()
            new.pre_actions = new.from_.pre_actions
            new.post_actions = new.from_.post_actions
            new.where = And(self.where, col) if self.where is not None else col
            return new
        return SelectStatement(
            from_=self.to_subqueryable(), where=col, analyzer=self.analyzer
        )

    def sort(self, cols) -> "SelectStatement":
        dependent_columns = get_dependent_columns(cols)
        can_flatten = can_clause_dependent_columns_flatten(
            dependent_columns, self.column_states
        )
        if can_flatten:
            new = copy(self)
            new.from_ = self.from_.to_subqueryable()
            new.pre_actions = new.from_.pre_actions
            new.post_actions = new.from_.post_actions
            new.order_by = cols
            return new
        return SelectStatement(
            from_=self.to_subqueryable(), order_by=cols, analyzer=self.analyzer
        )

    def join(
        self, other: Selectable, joincolumn: Optional[Expression], jointype: str
    ) -> "SelectStatement":
        new = copy(self)
        join = Join(self, other, joincolumn, jointype, analyzer=self.analyzer)
        new.join = new.join.append(join) if new.join else [join]
        return new

    def set_operate(
        self,
        *selectables: Union[
            SelectSnowflakePlan,
            "SelectStatement",
        ],
        operator: str,
    ) -> "SelectStatement":
        if isinstance(self.from_, SetStatement) and not self._has_clause():
            last_operator = self.from_.set_operands[-1].operator
            if operator == last_operator:
                set_operands = tuple(
                    SetOperand(x.to_subqueryable(), operator) for x in selectables
                )
            else:
                sub_statement = SetStatement(
                    *(SetOperand(x.to_subqueryable(), operator) for x in selectables)
                )
                set_operands = (SetOperand(sub_statement.to_subqueryable(), operator),)
            set_statement = SetStatement(
                *self.from_.set_operands, *set_operands, analyzer=self.analyzer
            )
        else:
            set_operands = tuple(
                SetOperand(x.to_subqueryable(), operator) for x in selectables
            )
            set_statement = SetStatement(
                SetOperand(self.to_subqueryable(), operator),
                *set_operands,
                analyzer=self.analyzer,
            )
        new = SelectStatement(analyzer=self.analyzer, from_=set_statement)
        new._column_states = set_statement.column_states
        return new

    def limit(self, n: int, *, offset: int = 0):
        new = copy(self)
        new.from_ = self.from_.to_subqueryable()
        new.limit_ = min(self.limit_, n) if self.limit_ else n
        new.offset = (self.offset + offset) if self.offset else offset
        return new


class SetOperand:
    def __init__(self, selectable: Selectable, operator: str) -> None:
        super().__init__()
        self.selectable = selectable
        self.operator = operator


class SetStatement(Selectable):
    def __init__(self, *set_operands: SetOperand, analyzer=None) -> None:
        super().__init__(analyzer=analyzer)
        self.analyzer = analyzer
        self.set_operands = set_operands
        self.pre_actions = []
        self.post_actions = []
        for operand in set_operands:
            if operand.selectable.pre_actions:
                self.pre_actions.extend(operand.selectable.pre_actions)
            if operand.selectable.post_actions:
                self.post_actions.extend(operand.selectable.post_actions)

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
            self._column_states = self.set_operands[0].selectable.column_states
        return self._column_states


COLUMN_EXP_PATTERN = re.compile(
    r'^"?.*"?\s*as\s*("?.*"?)\s*$', re.IGNORECASE
)  # TODO: Make this more accurate.


def parse_column_name(column: Union[str, Expression], analyzer):
    column_in_str = (
        analyzer.analyze(column) if isinstance(column, Expression) else column
    )
    match = COLUMN_EXP_PATTERN.match(column_in_str)
    column_name = match.group(1) if match else column_in_str
    # TODO: convert to upper case if the column_name is an identifier
    return column_name


def get_dependent_columns(*column_exp: Union[Expression, str]) -> Set[str]:
    if len(column_exp) == 1:
        if isinstance(column_exp[0], Expression):
            return column_exp[0].dependent_column_names()
        return COLUMN_DEPENDENCY_ALL
    result = set()
    for c in column_exp:
        c_dependent_columns = get_dependent_columns(c)
        if c_dependent_columns == COLUMN_DEPENDENCY_ALL:
            return COLUMN_DEPENDENCY_ALL
        result.update(c_dependent_columns)
    return result


def can_projection_dependent_columns_flatten(
    dependent_columns: Optional[Set[str]], column_states: ColumnStateDict
):
    can_flatten = True
    if (
        column_states.has_changed_columns
        or column_states.has_dropped_columns
        or column_states.has_new_columns
    ):
        if dependent_columns == COLUMN_DEPENDENCY_ALL:
            can_flatten = False
        else:
            for dc in dependent_columns:
                dc_state = column_states.get(dc)
                if dc_state and dc_state.change_state in (
                    (
                        ColumnChangeState.CHANGED_EXP,
                        ColumnChangeState.DROPPED,
                        ColumnChangeState.NEW,
                    )
                ):
                    can_flatten = False
                    break
    return can_flatten


def can_clause_dependent_columns_flatten(
    dependent_columns: Optional[Set[str]], column_states: ColumnStateDict
):
    can_flatten = True
    if column_states.has_changed_columns:
        if dependent_columns == COLUMN_DEPENDENCY_ALL:
            can_flatten = False
        else:
            for dc in dependent_columns:
                dc_state = column_states.get(dc)
                if dc_state and dc_state.change_state == ColumnChangeState.CHANGED_EXP:
                    can_flatten = False
                    break
    return can_flatten


def initiate_column_states(snowflake_plan: SnowflakePlan):
    column_states = ColumnStateDict()
    for i, attr in enumerate(snowflake_plan.attributes):
        column_states[attr.name] = ColumnState(
            attr.name,
            index=i,
            change_state=ColumnChangeState.UNCHANGED_EXP,
            expression=UnresolvedAttribute(quote_name(attr.name)),
            dependent_columns=COLUMN_DEPENDENCY_EMPTY,
            depend_on_same_level=False,
            referenced_by_same_level_columns=COLUMN_DEPENDENCY_EMPTY,
            state_dict=column_states,
        )
    return column_states


def derive_column_states_from_subquery(
    cols: Iterable[Expression], from_: Selectable
) -> ColumnStateDict:
    analyzer = from_.analyzer
    column_states = ColumnStateDict()
    column_index = 0
    # populate column status against subquery
    quoted_col_names = []
    for c in cols:
        if isinstance(c, UnresolvedAlias) and isinstance(c.child, Star):
            column_states.update(from_.column_states)
            continue
        c_name = parse_column_name(c, analyzer)
        quoted_c_name = analyzer_utils.quote_name(c_name)
        quoted_col_names.append(quoted_c_name)
        from_c_state = from_.column_states.get(quoted_c_name)
        if from_c_state:
            if c_name != from_.analyzer.analyze(c):
                column_states[quoted_c_name] = ColumnState(
                    quoted_c_name,
                    column_index,
                    ColumnChangeState.CHANGED_EXP,
                    c,
                    state_dict=column_states,
                )
            else:
                column_states[quoted_c_name] = ColumnState(
                    quoted_c_name,
                    column_index,
                    ColumnChangeState.UNCHANGED_EXP,
                    c,
                    state_dict=column_states,
                )
        else:
            column_states[quoted_c_name] = ColumnState(
                quoted_c_name,
                column_index,
                ColumnChangeState.NEW,
                c,
                state_dict=column_states,
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
                    else:  # A referenced column can't be found.
                        # TODO: change to a new error class
                        raise ValueError(
                            f"Column name {dependent_column} used in a column expression but it doesn't exist."
                        )
    # end of populate column dependency

    for dc in from_.column_states.active_columns - column_states.active_columns:
        column_states[dc] = ColumnState(
            dc,
            None,
            ColumnChangeState.DROPPED,
            None,
            None,
            None,
            state_dict=column_states,
        )
    return column_states
