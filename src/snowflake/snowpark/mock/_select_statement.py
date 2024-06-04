#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from abc import ABC
from copy import copy
from typing import TYPE_CHECKING, Any, List, Optional, Sequence, Union

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
from snowflake.snowpark.types import LongType

if TYPE_CHECKING:
    from snowflake.snowpark._internal.analyzer.analyzer import (
        Analyzer,
    )  # pragma: no cover

from snowflake.snowpark._internal.analyzer import analyzer_utils
from snowflake.snowpark._internal.analyzer.binary_expression import And
from snowflake.snowpark._internal.analyzer.expression import (
    COLUMN_DEPENDENCY_DOLLAR,
    Attribute,
    Expression,
    Star,
    derive_dependent_columns,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import LogicalPlan, Range
from snowflake.snowpark._internal.analyzer.unary_expression import UnresolvedAlias

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
        self.expr_to_alias = {}
        self.df_aliased_col_name_to_real_col_name = {}

    @property
    def sql_query(self) -> str:
        """Returns the sql query of this Selectable logical plan."""
        return ""

    @property
    def schema_query(self) -> str:
        """Returns the schema query that can be used to retrieve the schema information."""
        return ""

    @property
    def query_params(self) -> Optional[Sequence[Any]]:
        """Returns the sql query of this Selectable logical plan."""
        return ""

    @property
    def execution_plan(self):
        """Convert to a SnowflakePlan"""
        from snowflake.snowpark.mock._plan import MockExecutionPlan

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
                {},
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
                self.set_operands[0].selectable.column_states.projection,
                self.analyzer,
                {},
            )
        return self._column_states


class MockSelectExecutionPlan(MockSelectable):
    """Wrap a SnowflakePlan to a subclass of Selectable."""

    def __init__(self, snowflake_plan: LogicalPlan, *, analyzer: "Analyzer") -> None:
        super().__init__(analyzer)
        self._execution_plan = analyzer.resolve(snowflake_plan)

        if isinstance(snowflake_plan, Range):
            self._attributes = [Attribute('"ID"', LongType(), False)]

        self.api_calls = []


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
                    self.from_.attributes, self.analyzer, {}
                )
            elif isinstance(self.from_, MockSelectStatement):
                self._column_states = self.from_.column_states
            else:
                super().column_states  # will assign value to self._column_states
        return self._column_states

    @column_states.setter
    def column_states(self, value: ColumnStateDict):
        """A dictionary that contains the column states of a query.
        Refer to class ColumnStateDict.
        """
        self._column_states = copy(value)
        self._column_states.projection = [copy(attr) for attr in value.projection]

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
            new = MockSelectStatement(
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


class MockSelectableEntity(MockSelectable):
    """Query from a table, view, or any other Snowflake objects.
    Mainly used by session.table().
    """

    def __init__(self, entity_name: str, *, analyzer: "Analyzer") -> None:
        super().__init__(analyzer)
        self.entity_name = entity_name
        self.api_calls = []
