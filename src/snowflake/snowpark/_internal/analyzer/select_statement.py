#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from abc import ABC, abstractmethod
from collections import UserDict
from copy import copy
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Union

from snowflake.snowpark._internal.analyzer.table_function import (
    TableFunctionExpression,
    TableFunctionJoin,
    TableFunctionRelation,
)

if TYPE_CHECKING:
    from snowflake.snowpark._internal.analyzer.analyzer import Analyzer

from snowflake.snowpark._internal.analyzer import analyzer_utils
from snowflake.snowpark._internal.analyzer.analyzer_utils import result_scan_statement
from snowflake.snowpark._internal.analyzer.binary_expression import And
from snowflake.snowpark._internal.analyzer.expression import (
    COLUMN_DEPENDENCY_ALL,
    COLUMN_DEPENDENCY_DOLLAR,
    COLUMN_DEPENDENCY_EMPTY,
    Attribute,
    Expression,
    Star,
    UnresolvedAttribute,
    derive_dependent_columns,
)
from snowflake.snowpark._internal.analyzer.schema_utils import analyze_attributes
from snowflake.snowpark._internal.analyzer.snowflake_plan import Query, SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import LogicalPlan
from snowflake.snowpark._internal.analyzer.unary_expression import (
    Alias,
    UnresolvedAlias,
)
from snowflake.snowpark._internal.utils import is_sql_select_statement

SET_UNION = analyzer_utils.UNION
SET_UNION_ALL = analyzer_utils.UNION_ALL
SET_INTERSECT = analyzer_utils.INTERSECT
SET_EXCEPT = analyzer_utils.EXCEPT


class ColumnChangeState(Enum):
    """The change state of a column when building a query from its subquery."""

    NEW = "new"  # The column is new in the query. The subquery doesn't have the column name.
    UNCHANGED_EXP = "unchanged"  # The same column name is in both the query and subquery and there is no value change.
    CHANGED_EXP = "changed"  # The same column name is in both the query and subquery and there is value change.
    DROPPED = "dropped"  # The column name doesn't exist in the query but exists in the subquery. So it's dropped.


class ColumnState:
    """The state of a column when building a query from its subquery.
    It's used to rule whether a query and the subquery can be flattened."""

    def __init__(
        self,
        col_name: str,
        change_state: ColumnChangeState,  # The relative status of this column against the subquery
        expression: Optional[
            Union[str, Expression]
        ] = None,  # used to infer dependent columns
        dependent_columns: Optional[
            Set[str]
        ] = COLUMN_DEPENDENCY_ALL,  # columns that this column has a dependency on.
        depend_on_same_level: bool = False,  # Whether this column has dependency on one or more columns of the same level instead of the subquery.
        referenced_by_same_level_columns: Optional[
            Set[str]
        ] = COLUMN_DEPENDENCY_EMPTY,  # Other same-level columns that use this column.
        state_dict: Optional["ColumnStateDict"] = None,  # has states of all columns.
    ) -> None:
        self.col_name = col_name
        self.change_state = change_state
        self.expression = expression
        self.dependent_columns = dependent_columns
        self.depend_on_same_level = depend_on_same_level
        self.referenced_by_same_level_columns = referenced_by_same_level_columns
        self.state_dict = state_dict

    def add_referenced_by_same_level_column(self, col_name: str) -> None:
        """Add a column to the set if the column is referenced by other columns of the same level."""
        if self.referenced_by_same_level_columns in (
            COLUMN_DEPENDENCY_ALL,
            COLUMN_DEPENDENCY_EMPTY,
        ):
            self.referenced_by_same_level_columns = set(COLUMN_DEPENDENCY_EMPTY)
        self.referenced_by_same_level_columns.add(col_name)

    @property
    def is_referenced_by_same_level_column(self) -> bool:
        """Whether this column is referenced by any columns of the same-level query."""
        return (
            len(self.state_dict.columns_referencing_all_columns) > 1
            or (
                len(self.state_dict.columns_referencing_all_columns) == 1
                and self.col_name not in self.state_dict.columns_referencing_all_columns
            )
            or bool(self.referenced_by_same_level_columns)
        )


class ColumnStateDict(UserDict):
    """Store the column states of all columns."""

    def __init__(self) -> None:
        super().__init__(dict())
        self.projection: List[Attribute] = []
        # The following are useful aggregate information of all columns. Used to quickly rule if a query can be flattened.
        self.has_changed_columns: bool = False
        self.has_new_columns: bool = False
        self.dropped_columns: Optional[Set[str]] = None
        self.active_columns: Set[str] = set()
        self.columns_referencing_all_columns: Set[str] = set()

    @property
    def has_dropped_columns(self) -> bool:
        return bool(self.dropped_columns)

    def __setitem__(self, col_name: str, col_state: ColumnState) -> None:
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


class Selectable(LogicalPlan, ABC):
    """The parent abstract class of a DataFrame's logical plan. It can be converted to and from a SnowflakePlan."""

    def __init__(
        self,
        analyzer: "Analyzer",
        api_calls: Optional[
            List[Dict[str, Any]]
        ] = None,  # Use Any because it's recursive.
    ) -> None:
        super().__init__()
        self.analyzer = analyzer
        self.pre_actions: Optional[List["Query"]] = None
        self.post_actions: Optional[List["Query"]] = None
        self.flatten_disabled: bool = False
        self._column_states: Optional[ColumnStateDict] = None
        self._snowflake_plan: Optional[SnowflakePlan] = None
        self.expr_to_alias = {}
        self._api_calls = api_calls.copy() if api_calls is not None else None

    @property
    @abstractmethod
    def sql_query(self) -> str:
        """Returns the sql query of this Selectable logical plan."""
        ...

    @property
    def sql_in_subquery(self) -> str:
        """Return the sql when this Selectable is used in a subquery."""
        return f"{analyzer_utils.LEFT_PARENTHESIS}{self.sql_query}{analyzer_utils.RIGHT_PARENTHESIS}"

    @property
    @abstractmethod
    def schema_query(self) -> str:
        """Returns the schema query that can be used to retrieve the schema information."""
        ...

    def to_subqueryable(self) -> "Selectable":
        """Some queries can be used in a subquery. Some can't. For details, refer to class SelectSQL."""
        return self

    @property
    def api_calls(self) -> Dict[str, Any]:
        self._api_calls = self._api_calls if self._api_calls is not None else []
        return self._api_calls

    @api_calls.setter
    def api_calls(self, value: List[Dict[str, Any]]) -> None:
        self._api_calls = value
        if self._snowflake_plan:
            self._snowflake_plan.api_calls = value

    @property
    def snowflake_plan(self):
        """Convert to a SnowflakePlan"""
        if self._snowflake_plan is None:
            query = Query(self.sql_query)
            queries = [*self.pre_actions, query] if self.pre_actions else [query]
            self._snowflake_plan = SnowflakePlan(
                queries,
                self.schema_query,
                post_actions=self.post_actions,
                session=self.analyzer.session,
                expr_to_alias=self.expr_to_alias,
                source_plan=self,
            )
            # set api_calls to self._snowflake_plan outside of the above constructor
            # because the constructor copy api_calls.
            # We want Selectable and SnowflakePlan to share the same api_calls.
            self._snowflake_plan.api_calls = self.api_calls
        return self._snowflake_plan

    @property
    def column_states(self) -> ColumnStateDict:
        """A dictionary that contains the column states of a query.
        Refer to class ColumnStateDict.
        """
        if self._column_states is None:
            self._column_states = initiate_column_states(
                self.snowflake_plan.attributes, self.analyzer
            )
        return self._column_states


class SelectableEntity(Selectable):
    """Query from a table, view, or any other Snowflake objects.
    Mainly used by session.table().
    """

    def __init__(self, entity_name: str, *, analyzer: "Analyzer") -> None:
        super().__init__(analyzer)
        self.entity_name = entity_name

    @property
    def sql_query(self) -> str:
        return f"{analyzer_utils.SELECT}{analyzer_utils.STAR}{analyzer_utils.FROM}{self.entity_name}"

    @property
    def sql_in_subquery(self) -> str:
        return self.entity_name

    @property
    def schema_query(self) -> str:
        return self.sql_query


class SelectSQL(Selectable):
    """Query from a SQL. Mainly used by session.sql()"""

    def __init__(
        self, sql: str, *, convert_to_select: bool = False, analyzer: "Analyzer"
    ) -> None:
        """
        convert_to_select: If true the passed-in ``sql`` is not a select SQL, convert it to two SQLs in the logical plan.
        One is to execute the ``sql``. Another one is to `select * from result_scan(<query_id_of_the_first_sql>)`.
        So the query can be used in a subquery.
        """
        super().__init__(analyzer)
        self.convert_to_select = convert_to_select
        self.original_sql = sql
        is_select = is_sql_select_statement(sql)
        if not is_select and convert_to_select:
            self.pre_actions = [Query(sql)]
            self._sql_query = result_scan_statement(
                self.pre_actions[0].query_id_place_holder
            )
            self._schema_query = analyzer_utils.schema_value_statement(
                analyze_attributes(sql, self.analyzer.session)
            )  # Change to subqueryable schema query so downstream query plan can describe the SQL
        else:
            self._sql_query = sql
            self._schema_query = sql

    @property
    def sql_query(self) -> str:
        return self._sql_query

    @property
    def schema_query(self) -> str:
        return self._schema_query

    def to_subqueryable(self) -> "SelectSQL":
        """Convert this SelectSQL to a new one that can be used as a subquery. Refer to __init__."""
        if self.convert_to_select:
            return self
        new = SelectSQL(self._sql_query, convert_to_select=True, analyzer=self.analyzer)
        new._column_states = self.column_states
        new._api_calls = self._api_calls
        return new


class SelectSnowflakePlan(Selectable):
    """Wrap a SnowflakePlan to a subclass of Selectable."""

    def __init__(self, snowflake_plan: LogicalPlan, *, analyzer: "Analyzer") -> None:
        super().__init__(analyzer)
        self._snowflake_plan = (
            snowflake_plan
            if isinstance(snowflake_plan, SnowflakePlan)
            else analyzer.resolve(snowflake_plan)
        )
        self.expr_to_alias.update(self._snowflake_plan.expr_to_alias)
        self.pre_actions = self._snowflake_plan.queries[:-1]
        self.post_actions = self._snowflake_plan.post_actions
        self._api_calls = self._snowflake_plan.api_calls

    @property
    def snowflake_plan(self):
        return self._snowflake_plan

    @property
    def sql_query(self) -> str:
        return self._snowflake_plan.queries[-1].sql

    @property
    def schema_query(self) -> str:
        return self.snowflake_plan.schema_query


class SelectStatement(Selectable):
    """The main logic plan to be used by a DataFrame.
    It structurally has the parts of a query and uses the ColumnState to decide whether a query can be flattened."""

    def __init__(
        self,
        *,
        projection: Optional[List[Expression]] = None,
        from_: Optional["Selectable"] = None,
        where: Optional[Expression] = None,
        order_by: Optional[List[Expression]] = None,
        limit_: Optional[int] = None,
        offset: Optional[int] = None,
        analyzer: "Analyzer",
    ) -> None:
        super().__init__(analyzer)
        self.projection: Optional[List[Expression]] = projection
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
        self.expr_to_alias.update(self.from_.expr_to_alias)
        self.api_calls = (
            self.from_.api_calls.copy() if self.from_.api_calls is not None else None
        )  # will be replaced by new api calls if any operation.

    def __copy__(self):
        new = SelectStatement(
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
        new._projection_in_str = None
        new._schema_query = None
        new._column_states = None
        new._snowflake_plan = None
        new.flatten_disabled = False  # by default a SelectStatement can be flattened.
        new._api_calls = self._api_calls.copy() if self._api_calls is not None else None
        return new

    @property
    def column_states(self) -> ColumnStateDict:
        if self._column_states is None:
            if not self.projection and not self.has_clause:
                self._column_states = self.from_.column_states
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

    @property
    def sql_query(self) -> str:
        if self._sql_query:
            return self._sql_query
        if not self.has_clause and not self.projection:
            self._sql_query = self.from_.sql_query
            return self._sql_query
        from_clause = self.from_.sql_in_subquery
        where_clause = (
            f"{analyzer_utils.WHERE}{self.analyzer.analyze(self.where)}"
            if self.where is not None
            else analyzer_utils.EMPTY_STRING
        )
        order_by_clause = (
            f"{analyzer_utils.ORDER_BY}{analyzer_utils.COMMA.join(self.analyzer.analyze(x) for x in self.order_by)}"
            if self.order_by
            else analyzer_utils.EMPTY_STRING
        )
        limit_clause = (
            f"{analyzer_utils.LIMIT}{self.limit_}"
            if self.limit_ is not None
            else analyzer_utils.EMPTY_STRING
        )
        offset_clause = (
            f"{analyzer_utils.OFFSET}{self.offset}"
            if self.offset
            else analyzer_utils.EMPTY_STRING
        )
        self._sql_query = f"{analyzer_utils.SELECT}{self.projection_in_str}{analyzer_utils.FROM}{from_clause}{where_clause}{order_by_clause}{limit_clause}{offset_clause}"
        return self._sql_query

    @property
    def schema_query(self) -> str:
        if self._schema_query:
            return self._schema_query
        if not self.projection:
            self._schema_query = self.from_.schema_query
            return self._schema_query
        self._schema_query = f"{analyzer_utils.SELECT}{self.projection_in_str}{analyzer_utils.FROM}({self.from_.schema_query})"
        return self._schema_query

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
            new._snowflake_plan = self._snowflake_plan
            new.flatten_disabled = self.flatten_disabled
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
                    final_projection.append(state.expression)
                elif state.change_state == ColumnChangeState.UNCHANGED_EXP:
                    # query may change sequence of columns. If subquery has same-level reference, flattened sql may not work.
                    if (
                        col not in subquery_column_states
                        or subquery_column_states[col].depend_on_same_level
                    ):
                        can_be_flattened = False
                        break
                    final_projection.append(
                        subquery_column_states[col].expression
                    )  # add subquery's expression for this column name
                elif state.change_state == ColumnChangeState.DROPPED:
                    if (
                        subquery_state.change_state == ColumnChangeState.NEW
                        and subquery_state.is_referenced_by_same_level_column
                    ):
                        can_be_flattened = False
                        break
                else:
                    raise ValueError(f"Invalid column state {state}.")
        if can_be_flattened:
            new = copy(self)
            new.projection = final_projection
            new.from_ = self.from_.to_subqueryable()
            new.pre_actions = new.from_.pre_actions
            new.post_actions = new.from_.post_actions
        else:
            new = SelectStatement(
                projection=cols, from_=self.to_subqueryable(), analyzer=self.analyzer
            )
        new.flatten_disabled = disable_next_level_flatten
        new._column_states = derive_column_states_from_subquery(
            new.projection, new.from_
        )
        # If new._column_states is None, when property `column_states` is called later,
        # a query will be described and an error like "invalid identifier" will be thrown.

        return new

    def filter(self, col: Expression) -> "SelectStatement":
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
            new = SelectStatement(
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
        if isinstance(self.from_, SetStatement) and not self.has_clause:
            last_operator = self.from_.set_operands[-1].operator
            if operator == last_operator:
                existing_set_operands = self.from_.set_operands
                set_operands = tuple(
                    SetOperand(x.to_subqueryable(), operator) for x in selectables
                )
            elif operator == SET_INTERSECT:
                # In Snowflake SQL, intersect has higher precedence than other set operators.
                # So we need to put all operands before intersect into a single operand.
                existing_set_operands = (
                    SetOperand(
                        SetStatement(*self.from_.set_operands, analyzer=self.analyzer)
                    ),
                )
                sub_statement = SetStatement(
                    *(SetOperand(x.to_subqueryable(), operator) for x in selectables),
                    analyzer=self.analyzer,
                )
                set_operands = (SetOperand(sub_statement.to_subqueryable(), operator),)
            else:
                existing_set_operands = self.from_.set_operands
                sub_statement = SetStatement(
                    *(SetOperand(x.to_subqueryable(), operator) for x in selectables),
                    analyzer=self.analyzer,
                )
                set_operands = (SetOperand(sub_statement.to_subqueryable(), operator),)
            set_statement = SetStatement(
                *existing_set_operands, *set_operands, analyzer=self.analyzer
            )
        else:
            set_operands = tuple(
                SetOperand(x.to_subqueryable(), operator) for x in selectables
            )
            set_statement = SetStatement(
                SetOperand(self.to_subqueryable()),
                *set_operands,
                analyzer=self.analyzer,
            )
        api_calls = self.api_calls.copy()
        for s in selectables:
            if s.api_calls:
                api_calls.extend(s.api_calls)
        set_statement.api_calls = api_calls
        new = SelectStatement(analyzer=self.analyzer, from_=set_statement)
        new._column_states = set_statement.column_states
        return new

    def limit(self, n: int, *, offset: int = 0) -> "SelectStatement":
        new = copy(self)
        new.from_ = self.from_.to_subqueryable()
        new.limit_ = min(self.limit_, n) if self.limit_ else n
        new.offset = (self.offset + offset) if self.offset else offset
        new._column_states = self._column_states
        return new


class SelectTableFunction(Selectable):
    """Wrap table function related plan to a subclass of Selectable."""

    def __init__(
        self,
        func_expr: TableFunctionExpression,
        *,
        other_plan: Optional[LogicalPlan] = None,
        analyzer: "Analyzer",
    ) -> None:
        super().__init__(analyzer)
        self.func_expr = func_expr
        if other_plan:
            self._snowflake_plan = analyzer.resolve(
                TableFunctionJoin(other_plan, func_expr)
            )
        else:
            self._snowflake_plan = analyzer.resolve(TableFunctionRelation(func_expr))
        self._api_calls = self._snowflake_plan.api_calls

    @property
    def snowflake_plan(self):
        return self._snowflake_plan

    @property
    def sql_query(self) -> str:
        return self._snowflake_plan.queries[-1].sql

    @property
    def schema_query(self) -> str:
        return self._snowflake_plan.schema_query


class SetOperand:
    def __init__(self, selectable: Selectable, operator: Optional[str] = None) -> None:
        super().__init__()
        self.selectable = selectable
        self.operator = operator


class SetStatement(Selectable):
    def __init__(
        self, *set_operands: SetOperand, analyzer: Optional["Analyzer"]
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


class DeriveColumnDependencyError(Exception):
    """When deriving column dependencies from the subquery."""


def parse_column_name(column: Expression, analyzer: "Analyzer") -> Optional[str]:
    if isinstance(column, Expression):
        if isinstance(column, Attribute):
            return column.name
        if isinstance(column, UnresolvedAttribute):
            if not column.is_sql_text:
                return column.name
        if isinstance(column, UnresolvedAlias):
            return analyzer.analyze(column)
        if isinstance(column, Alias):
            return column.name
    # We can parse column name from a column's SQL expression in the future.
    # When parsing column name isn't possible, the SelectStatement.select won't flatten and
    # disables the next level SelectStatement to flatten
    return None


def can_projection_dependent_columns_be_flattened(
    dependent_columns: Optional[Set[str]], subquery_column_states: ColumnStateDict
) -> bool:
    can_be_flattened = True
    if dependent_columns == COLUMN_DEPENDENCY_DOLLAR:
        can_be_flattened = False
    elif (
        subquery_column_states.has_changed_columns
        or subquery_column_states.has_dropped_columns
        or subquery_column_states.has_new_columns
    ):
        if dependent_columns == COLUMN_DEPENDENCY_ALL:
            can_be_flattened = False
        else:
            for dc in dependent_columns:
                dc_state = subquery_column_states.get(dc)
                if dc_state and dc_state.change_state in (
                    (
                        ColumnChangeState.CHANGED_EXP,
                        ColumnChangeState.DROPPED,
                        ColumnChangeState.NEW,
                    )
                ):
                    can_be_flattened = False
                    break
    return can_be_flattened


def can_clause_dependent_columns_flatten(
    dependent_columns: Optional[Set[str]], subquery_column_states: ColumnStateDict
) -> bool:
    can_be_flattened = True
    if dependent_columns == COLUMN_DEPENDENCY_DOLLAR:
        can_be_flattened = False
    elif (
        subquery_column_states.has_changed_columns
        or subquery_column_states.has_new_columns
    ):
        if dependent_columns == COLUMN_DEPENDENCY_ALL:
            can_be_flattened = False
        else:
            for dc in dependent_columns:
                dc_state = subquery_column_states.get(dc)
                if dc_state:
                    if dc_state.change_state == ColumnChangeState.CHANGED_EXP:
                        can_be_flattened = False
                        break
                    elif dc_state.change_state == ColumnChangeState.NEW:
                        # Most of the time this can be flatten. But if a new column uses window function and this column
                        # is used in a clause, the sql doesn't work in Snowflake.
                        # For instance `select a, rank() over(order by b) as d from test_table where d = 1` doesn't work.
                        # But `select a, b as d from test_table where d = 1` works
                        # We can inspect whether the referenced new column uses window function. Here we are being
                        # conservative for now to not flatten the SQL.
                        can_be_flattened = False
                        break
    return can_be_flattened


def initiate_column_states(
    column_attrs: List[Attribute], analyzer: "Analyzer"
) -> ColumnStateDict:
    column_states = ColumnStateDict()
    for attr in column_attrs:
        name = analyzer.analyze(attr)
        column_states[name] = ColumnState(
            name,
            change_state=ColumnChangeState.UNCHANGED_EXP,
            expression=attr,
            dependent_columns=COLUMN_DEPENDENCY_EMPTY,
            depend_on_same_level=False,
            referenced_by_same_level_columns=COLUMN_DEPENDENCY_EMPTY,
            state_dict=column_states,
        )
    column_states.projection = column_attrs
    return column_states


def populate_column_dependency(
    exp: Expression,
    quoted_c_name: str,
    column_states: ColumnStateDict,
    subquery_column_states: ColumnStateDict,
) -> None:
    dependent_column_names = derive_dependent_columns(exp)
    column_states[quoted_c_name].dependent_columns = dependent_column_names
    if dependent_column_names == COLUMN_DEPENDENCY_DOLLAR:
        column_states[quoted_c_name].depend_on_same_level = False
    elif dependent_column_names == COLUMN_DEPENDENCY_ALL:
        column_states[quoted_c_name].depend_on_same_level = True
        column_states.columns_referencing_all_columns.add(quoted_c_name)
    else:
        for dependent_column in dependent_column_names:
            if dependent_column not in subquery_column_states.active_columns:
                column_states[quoted_c_name].depend_on_same_level = True
                if dependent_column in column_states:
                    column_states[dependent_column].add_referenced_by_same_level_column(
                        dependent_column
                    )
                else:  # A referenced column can't be found. The query has an error.
                    raise DeriveColumnDependencyError()


def derive_column_states_from_subquery(
    cols: Iterable[Expression], from_: Selectable
) -> Optional[ColumnStateDict]:
    analyzer = from_.analyzer
    column_states = ColumnStateDict()
    for c in cols:
        if isinstance(c, UnresolvedAlias) and isinstance(c.child, Star):
            if c.child.expressions:
                # df.select(df["*"]) will have child expressions. df.select("*") doesn't.
                columns_from_star = c.child.expressions
            else:
                columns_from_star = from_.column_states.projection
            column_states.update(initiate_column_states(columns_from_star, analyzer))
            column_states.projection.extend(columns_from_star)
            continue
        c_name = parse_column_name(c, analyzer)
        if c_name is None:
            return None
        quoted_c_name = analyzer_utils.quote_name(c_name)
        # if c is not an Attribute object, we will only care about the column name,
        # so we can build a dummy Attribute with the column name
        column_states.projection.append(
            c if isinstance(c, Attribute) else Attribute(quoted_c_name)
        )
        from_c_state = from_.column_states.get(quoted_c_name)
        if from_c_state and from_c_state.change_state != ColumnChangeState.DROPPED:
            if c_name != analyzer.analyze(c):
                column_states[quoted_c_name] = ColumnState(
                    quoted_c_name,
                    ColumnChangeState.CHANGED_EXP,
                    c,
                    state_dict=column_states,
                )
            else:
                column_states[quoted_c_name] = ColumnState(
                    quoted_c_name,
                    ColumnChangeState.UNCHANGED_EXP,
                    c,
                    state_dict=column_states,
                )
        else:
            column_states[quoted_c_name] = ColumnState(
                quoted_c_name,
                ColumnChangeState.NEW,
                c,
                state_dict=column_states,
            )
        try:
            populate_column_dependency(
                c, quoted_c_name, column_states, from_.column_states
            )
        except DeriveColumnDependencyError:
            # downstream will not flatten when seeing None and disable next level SelectStatement to flatten.
            # The query will get an invalid column error.
            return None

    for dc in from_.column_states.active_columns - column_states.active_columns:
        # for dropped columns, we only care name
        column_states[dc] = ColumnState(
            col_name=dc,
            change_state=ColumnChangeState.DROPPED,
            state_dict=column_states,
        )
    return column_states
