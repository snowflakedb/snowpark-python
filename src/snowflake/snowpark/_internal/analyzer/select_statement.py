#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import sys
from abc import ABC, abstractmethod
from collections import UserDict, defaultdict
from copy import copy, deepcopy
from enum import Enum
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    DefaultDict,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Union,
)

import snowflake.snowpark._internal.utils
from snowflake.snowpark._internal.analyzer.cte_utils import encode_id
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
    PlanState,
    subtract_complexities,
    sum_node_complexities,
)
from snowflake.snowpark._internal.analyzer.table_function import (
    TableFunctionExpression,
    TableFunctionJoin,
    TableFunctionRelation,
)
from snowflake.snowpark._internal.analyzer.window_expression import WindowExpression
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark.types import DataType

if TYPE_CHECKING:
    from snowflake.snowpark._internal.analyzer.analyzer import (
        Analyzer,
    )  # pragma: no cover

from snowflake.snowpark._internal.analyzer import analyzer_utils
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    result_scan_statement,
    schema_value_statement,
)
from snowflake.snowpark._internal.analyzer.binary_expression import And
from snowflake.snowpark._internal.analyzer.expression import (
    COLUMN_DEPENDENCY_ALL,
    COLUMN_DEPENDENCY_DOLLAR,
    COLUMN_DEPENDENCY_EMPTY,
    Attribute,
    Expression,
    FunctionExpression,
    Star,
    UnresolvedAttribute,
    derive_dependent_columns,
)
from snowflake.snowpark._internal.analyzer.schema_utils import analyze_attributes
from snowflake.snowpark._internal.analyzer.snowflake_plan import Query, SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    SnowflakeTable,
    WithQueryBlock,
)
from snowflake.snowpark._internal.analyzer.unary_expression import (
    Alias,
    UnresolvedAlias,
)
from snowflake.snowpark._internal.select_projection_complexity_utils import (
    has_invalid_projection_merge_functions,
)
from snowflake.snowpark._internal.utils import is_sql_select_statement

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

SET_UNION = analyzer_utils.UNION
SET_UNION_ALL = analyzer_utils.UNION_ALL
SET_INTERSECT = analyzer_utils.INTERSECT
SET_EXCEPT = analyzer_utils.EXCEPT
SEQUENCE_DEPENDENT_DATA_GENERATION = (
    "normal",
    "zipf",
    "uniform",
    "seq1",
    "seq2",
    "seq4",
    "seq8",
)


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
            AbstractSet[str]
        ] = COLUMN_DEPENDENCY_ALL,  # columns that this column has a dependency on.
        depend_on_same_level: bool = False,  # Whether this column has dependency on one or more columns of the same level instead of the subquery.
        referenced_by_same_level_columns: Optional[
            AbstractSet[str]
        ] = COLUMN_DEPENDENCY_EMPTY,  # Other same-level columns that use this column.
        *,
        state_dict: "ColumnStateDict",  # has states of all columns.
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
        assert isinstance(self.referenced_by_same_level_columns, set)
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


def _deepcopy_selectable_fields(
    from_selectable: "Selectable", to_selectable: "Selectable"
) -> None:
    """
    Make a deep copy of the fields from the from_selectable to the to_selectable
    """
    to_selectable.pre_actions = deepcopy(from_selectable.pre_actions)
    to_selectable.post_actions = deepcopy(from_selectable.post_actions)
    to_selectable.flatten_disabled = from_selectable.flatten_disabled
    to_selectable._column_states = deepcopy(from_selectable._column_states)
    to_selectable.expr_to_alias = deepcopy(from_selectable.expr_to_alias)
    to_selectable.df_aliased_col_name_to_real_col_name = deepcopy(
        from_selectable.df_aliased_col_name_to_real_col_name
    )
    to_selectable._cumulative_node_complexity = deepcopy(
        from_selectable._cumulative_node_complexity
    )
    # the snowflake plan for selectable typically point to self by default,
    # to avoid run into recursively copy problem, we do not copy the _snowflake_plan
    # field by default and let it rebuild when needed. As far as we have other fields
    # copied correctly, the plan can be recovered properly.
    to_selectable._is_valid_for_replacement = True


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
        self.df_aliased_col_name_to_real_col_name: DefaultDict[
            str, Dict[str, str]
        ] = defaultdict(dict)
        self._api_calls = api_calls.copy() if api_calls is not None else None
        self._cumulative_node_complexity: Optional[Dict[PlanNodeCategory, int]] = None

    def __eq__(self, other: "Selectable") -> bool:
        if not isinstance(other, Selectable):
            return False
        if self._id is not None and other._id is not None:
            return type(self) is type(other) and self._id == other._id
        else:
            return super().__eq__(other)

    def __hash__(self) -> int:
        return hash(self._id) if self._id else super().__hash__()

    @property
    @abstractmethod
    def sql_query(self) -> str:
        """Returns the sql query of this Selectable logical plan."""
        pass

    @property
    @abstractmethod
    def placeholder_query(self) -> Optional[str]:
        """Returns the placeholder query of this Selectable logical plan."""
        pass

    @cached_property
    def _id(self) -> Optional[str]:
        """Returns the id of this Selectable logical plan."""
        return encode_id(self.sql_query, self.query_params)

    @property
    @abstractmethod
    def query_params(self) -> Optional[Sequence[Any]]:
        """Returns the sql query of this Selectable logical plan."""
        pass  # pragma: no cover

    @property
    def sql_in_subquery(self) -> str:
        """Return the sql when this Selectable is used in a subquery."""
        return f"{analyzer_utils.LEFT_PARENTHESIS}{self.sql_query}{analyzer_utils.RIGHT_PARENTHESIS}"

    @property
    @abstractmethod
    def schema_query(self) -> str:
        """Returns the schema query that can be used to retrieve the schema information."""
        pass

    def to_subqueryable(self) -> "Selectable":
        """Some queries can be used in a subquery. Some can't. For details, refer to class SelectSQL."""
        return self

    @property
    def api_calls(self) -> List[Dict[str, Any]]:
        api_calls = self._api_calls if self._api_calls is not None else []
        return api_calls

    @api_calls.setter
    def api_calls(self, value: Optional[List[Dict[str, Any]]]) -> None:
        self._api_calls = value
        if self._snowflake_plan:
            assert value is not None
            self._snowflake_plan.api_calls = value

    @property
    def snowflake_plan(self):
        """Convert to a SnowflakePlan"""
        return self.get_snowflake_plan(skip_schema_query=False)

    def get_snowflake_plan(self, skip_schema_query) -> SnowflakePlan:
        if self._snowflake_plan is None:
            query = Query(self.sql_query, params=self.query_params)
            queries = [*self.pre_actions, query] if self.pre_actions else [query]
            schema_query = None if skip_schema_query else self.schema_query
            self._snowflake_plan = SnowflakePlan(
                queries,
                schema_query,
                post_actions=self.post_actions,
                session=self.analyzer.session,
                expr_to_alias=self.expr_to_alias,
                df_aliased_col_name_to_real_col_name=self.df_aliased_col_name_to_real_col_name,
                source_plan=self,
                placeholder_query=self.placeholder_query,
                referenced_ctes=self.referenced_ctes,
            )
            # set api_calls to self._snowflake_plan outside of the above constructor
            # because the constructor copy api_calls.
            # We want Selectable and SnowflakePlan to share the same api_calls.
            self._snowflake_plan.api_calls = self.api_calls
        return self._snowflake_plan

    @property
    def plan_state(self) -> Dict[PlanState, Any]:
        return self.snowflake_plan.plan_state

    @property
    def num_duplicate_nodes(self) -> int:
        return self.snowflake_plan.num_duplicate_nodes

    @property
    def cumulative_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        if self._cumulative_node_complexity is None:
            self._cumulative_node_complexity = sum_node_complexities(
                self.individual_node_complexity,
                *(node.cumulative_node_complexity for node in self.children_plan_nodes),
            )
        return self._cumulative_node_complexity

    @cumulative_node_complexity.setter
    def cumulative_node_complexity(self, value: Dict[PlanNodeCategory, int]):
        self._cumulative_node_complexity = value

    @property
    def children_plan_nodes(self) -> List[Union["Selectable", SnowflakePlan]]:
        """
        This property is currently only used for traversing the query plan tree
        when performing CTE optimization.
        """
        return (
            self.snowflake_plan.source_plan.children
            if self.snowflake_plan.source_plan
            else []
        )

    @property
    def column_states(self) -> ColumnStateDict:
        """A dictionary that contains the column states of a query.
        Refer to class ColumnStateDict.
        """
        if self._column_states is None:
            self._column_states = initiate_column_states(
                self.snowflake_plan.attributes,
                self.analyzer,
                self.df_aliased_col_name_to_real_col_name,
            )
        return self._column_states

    @column_states.setter
    def column_states(self, value: ColumnStateDict):
        """A dictionary that contains the column states of a query.
        Refer to class ColumnStateDict.
        """
        self._column_states = deepcopy(value)

    @property
    @abstractmethod
    def referenced_ctes(self) -> Dict[WithQueryBlock, int]:
        """Return the set of ctes referenced by the whole selectable subtree, includes its-self and children"""
        pass


class SelectableEntity(Selectable):
    """Query from a table, view, or any other Snowflake objects.
    Mainly used by session.table().
    """

    def __init__(
        self,
        entity: SnowflakeTable,
        *,
        analyzer: "Analyzer",
    ) -> None:
        # currently only selecting from a table or cte is supported
        # to read as entity
        assert isinstance(entity, SnowflakeTable)
        super().__init__(analyzer)
        self.entity = entity

    def __deepcopy__(self, memodict={}) -> "SelectableEntity":  # noqa: B006
        copied = SelectableEntity(
            deepcopy(self.entity, memodict), analyzer=self.analyzer
        )
        _deepcopy_selectable_fields(from_selectable=self, to_selectable=copied)

        return copied

    @property
    def sql_query(self) -> str:
        return f"{analyzer_utils.SELECT}{analyzer_utils.STAR}{analyzer_utils.FROM}{self.entity.name}"

    @property
    def placeholder_query(self) -> Optional[str]:
        return None

    @property
    def sql_in_subquery(self) -> str:
        return self.entity.name

    @property
    def schema_query(self) -> str:
        return self.sql_query

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        # SELECT * FROM entity
        return PlanNodeCategory.COLUMN

    @property
    def query_params(self) -> Optional[Sequence[Any]]:
        return None

    @property
    def referenced_ctes(self) -> Dict[WithQueryBlock, int]:
        # the SelectableEntity only allows select from base table. No
        # CTE table will be referred.
        return dict()


class SelectSQL(Selectable):
    """Query from a SQL. Mainly used by session.sql()"""

    def __init__(
        self,
        sql: str,
        *,
        convert_to_select: bool = False,
        analyzer: "Analyzer",
        params: Optional[Sequence[Any]] = None,
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
            self.pre_actions = [Query(sql, params=params)]
            self._sql_query = result_scan_statement(
                self.pre_actions[0].query_id_place_holder
            )
            self._schema_query = analyzer_utils.schema_value_statement(
                analyze_attributes(sql, self.analyzer.session)
            )  # Change to subqueryable schema query so downstream query plan can describe the SQL
            self._query_param = None
        else:
            self._sql_query = sql
            self._schema_query = sql
            self._query_param = params

    def __deepcopy__(self, memodict={}) -> "SelectSQL":  # noqa: B006
        copied = SelectSQL(
            sql=self.original_sql,
            # when convert_to_select is True, a describe call might be triggered
            # to construct the schema query. Since this is a pure copy method, and all
            # fields can be done with a pure copy, we set this parameter to False on
            # object construct, and correct the fields after.
            convert_to_select=False,
            analyzer=self.analyzer,
            params=deepcopy(self.query_params),
        )
        _deepcopy_selectable_fields(from_selectable=self, to_selectable=copied)
        # copy over the other fields
        copied.convert_to_select = self.convert_to_select
        copied._sql_query = self._sql_query
        copied._schema_query = self._schema_query
        copied._query_param = deepcopy(self._query_param)

        return copied

    @property
    def sql_query(self) -> str:
        return self._sql_query

    @property
    def placeholder_query(self) -> Optional[str]:
        return None

    @property
    def _id(self) -> Optional[str]:
        """
        Returns the id of this SelectSQL logical plan. The original SQL is used to encode its ID,
        which might be a non-select SQL.
        """
        return encode_id(self.original_sql, self.query_params)

    @property
    def query_params(self) -> Optional[Sequence[Any]]:
        return self._query_param

    @property
    def schema_query(self) -> str:
        return self._schema_query

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.COLUMN

    def to_subqueryable(self) -> "SelectSQL":
        """Convert this SelectSQL to a new one that can be used as a subquery. Refer to __init__."""
        if self.convert_to_select or is_sql_select_statement(self._sql_query):
            return self
        new = SelectSQL(
            self._sql_query,
            convert_to_select=True,
            analyzer=self.analyzer,
            params=self.query_params,
        )
        new.column_states = self.column_states
        new._api_calls = self._api_calls
        return new

    @property
    def referenced_ctes(self) -> Dict[WithQueryBlock, int]:
        # SelectSQL directly calls sql query, there will be no
        # auto created CTE tables referenced
        return dict()


class SelectSnowflakePlan(Selectable):
    """Wrap a SnowflakePlan to a subclass of Selectable."""

    def __init__(self, snowflake_plan: LogicalPlan, *, analyzer: "Analyzer") -> None:
        super().__init__(analyzer)
        self._snowflake_plan: SnowflakePlan = (
            snowflake_plan
            if isinstance(snowflake_plan, SnowflakePlan)
            else analyzer.resolve(snowflake_plan)
        )
        self.expr_to_alias.update(self._snowflake_plan.expr_to_alias)
        self.df_aliased_col_name_to_real_col_name.update(
            self._snowflake_plan.df_aliased_col_name_to_real_col_name
        )

        self.pre_actions = self._snowflake_plan.queries[:-1]
        self.post_actions = self._snowflake_plan.post_actions
        self._api_calls = self._snowflake_plan.api_calls
        self._query_params = []
        for query in self._snowflake_plan.queries:
            if query.params:
                self._query_params.extend(query.params)

    def __deepcopy__(self, memodict={}) -> "SelectSnowflakePlan":  # noqa: B006
        copied = SelectSnowflakePlan(
            snowflake_plan=deepcopy(self._snowflake_plan, memodict),
            analyzer=self.analyzer,
        )
        _deepcopy_selectable_fields(from_selectable=self, to_selectable=copied)
        copied._query_params = deepcopy(self._query_params)
        return copied

    @property
    def snowflake_plan(self):
        return self._snowflake_plan

    @property
    def sql_query(self) -> str:
        return self._snowflake_plan.queries[-1].sql

    @property
    def placeholder_query(self) -> Optional[str]:
        return self._snowflake_plan.placeholder_query

    @property
    def _id(self) -> Optional[str]:
        return self._snowflake_plan._id

    @property
    def schema_query(self) -> Optional[str]:
        return self.snowflake_plan.schema_query

    @property
    def query_params(self) -> Optional[Sequence[Any]]:
        return self._query_params

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        return self.snowflake_plan.individual_node_complexity

    @property
    def cumulative_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        if self._cumulative_node_complexity is None:
            self._cumulative_node_complexity = (
                self.snowflake_plan.cumulative_node_complexity
            )
        return self._cumulative_node_complexity

    @cumulative_node_complexity.setter
    def cumulative_node_complexity(self, value: Dict[PlanNodeCategory, int]):
        self._cumulative_node_complexity = value

    def reset_cumulative_node_complexity(self) -> None:
        super().reset_cumulative_node_complexity()
        self.snowflake_plan.reset_cumulative_node_complexity()

    @property
    def referenced_ctes(self) -> Dict[WithQueryBlock, int]:
        return self._snowflake_plan.referenced_ctes


class SelectStatement(Selectable):
    """The main logic plan to be used by a DataFrame.
    It structurally has the parts of a query and uses the ColumnState to decide whether a query can be flattened."""

    def __init__(
        self,
        *,
        projection: Optional[List[Expression]] = None,
        from_: Selectable,
        where: Optional[Expression] = None,
        order_by: Optional[List[Expression]] = None,
        limit_: Optional[int] = None,
        offset: Optional[int] = None,
        analyzer: "Analyzer",
        schema_query: Optional[str] = None,
    ) -> None:
        super().__init__(analyzer)
        self.projection: Optional[List[Expression]] = projection
        self.from_: "Selectable" = from_
        self.where: Optional[Expression] = where
        self.order_by: Optional[List[Expression]] = order_by
        self.limit_: Optional[int] = limit_
        self.offset = offset
        self.pre_actions = self.from_.pre_actions
        self.post_actions = self.from_.post_actions
        self._sql_query = None
        self._schema_query = schema_query
        self._projection_in_str = None
        self._query_params = None
        self.expr_to_alias.update(self.from_.expr_to_alias)
        self.df_aliased_col_name_to_real_col_name.update(
            self.from_.df_aliased_col_name_to_real_col_name
        )
        self.api_calls = (
            self.from_.api_calls.copy() if self.from_.api_calls is not None else None
        )  # will be replaced by new api calls if any operation.
        self._placeholder_query = None
        # indicate whether we should try to merge the projection complexity of the current
        # SelectStatement with the projection complexity of from_ during the calculation of
        # node complexity. For example:
        #   SELECT COL1 + 2 as COL1, COL2 FROM (SELECT COL1 + 3 AS COL1, COL2 FROM TABLE_TEST)
        # can be merged as follows with snowflake:
        #   SELECT (COL1 + 3) + 2 AS COL1, COLS FROM TABLE_TEST
        # Therefore, the plan complexity during compilation will change, and the result plan
        # complexity is can be calculated by merging the projection complexity of the two SELECTS.
        #
        # In Snowpark, we do not generate the query after merging two selects. Flag
        # _merge_projection_complexity_with_subquery is used to indicate that it is valid to merge
        # the projection complexity of current SelectStatement with subquery.
        self._merge_projection_complexity_with_subquery = False
        # cached list of projection complexities, each projection complexity is adjusted
        # with the subquery projection if _merge_projection_complexity_with_subquery is True.
        self._projection_complexities: Optional[
            List[Dict[PlanNodeCategory, int]]
        ] = None

    def __copy__(self):
        new = SelectStatement(
            projection=self.projection,
            from_=self.from_,
            where=self.where,
            order_by=self.order_by,
            limit_=self.limit_,
            offset=self.offset,
            analyzer=self.analyzer,
            schema_query=self.schema_query,
        )
        # The following values will change if they're None in the newly copied one so reset their values here
        # to avoid problems.
        new._projection_in_str = None
        new._schema_query = None
        new._column_states = None
        new._snowflake_plan = None
        new.flatten_disabled = False  # by default a SelectStatement can be flattened.
        new._api_calls = self._api_calls.copy() if self._api_calls is not None else None
        new.df_aliased_col_name_to_real_col_name = (
            self.df_aliased_col_name_to_real_col_name
        )
        new._merge_projection_complexity_with_subquery = (
            self._merge_projection_complexity_with_subquery
        )

        return new

    def __deepcopy__(self, memodict={}) -> "SelectStatement":  # noqa: B006
        copied = SelectStatement(
            projection=deepcopy(self.projection, memodict),
            from_=deepcopy(self.from_, memodict),
            where=deepcopy(self.where, memodict),
            order_by=deepcopy(self.order_by, memodict),
            limit_=self.limit_,
            offset=self.offset,
            analyzer=self.analyzer,
            # directly copy the current schema fields
            schema_query=self._schema_query,
        )

        _deepcopy_selectable_fields(from_selectable=self, to_selectable=copied)
        copied._projection_in_str = self._projection_in_str
        copied._query_params = deepcopy(self._query_params)
        copied._merge_projection_complexity_with_subquery = (
            self._merge_projection_complexity_with_subquery
        )
        copied._projection_complexities = (
            deepcopy(self._projection_complexities)
            if not self._projection_complexities
            else None
        )
        return copied

    @property
    def column_states(self) -> ColumnStateDict:
        if self._column_states is None:
            if not self.projection and not self.has_clause:
                self.column_states = self.from_.column_states
            else:
                super().column_states  # will assign value to self._column_states
        assert self._column_states is not None
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
                    self.analyzer.analyze(x, self.df_aliased_col_name_to_real_col_name)
                    for x in self.projection
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
        if (
            self.analyzer.session._cte_optimization_enabled
            and (not self.analyzer.session._query_compilation_stage_enabled)
            and self.from_._id
        ):
            placeholder = f"{analyzer_utils.LEFT_PARENTHESIS}{self.from_._id}{analyzer_utils.RIGHT_PARENTHESIS}"
            self._sql_query = self.placeholder_query.replace(placeholder, from_clause)
        else:
            where_clause = (
                f"{analyzer_utils.WHERE}{self.analyzer.analyze(self.where, self.df_aliased_col_name_to_real_col_name)}"
                if self.where is not None
                else snowflake.snowpark._internal.utils.EMPTY_STRING
            )
            order_by_clause = (
                f"{analyzer_utils.ORDER_BY}{analyzer_utils.COMMA.join(self.analyzer.analyze(x, self.df_aliased_col_name_to_real_col_name) for x in self.order_by)}"
                if self.order_by
                else snowflake.snowpark._internal.utils.EMPTY_STRING
            )
            limit_clause = (
                f"{analyzer_utils.LIMIT}{self.limit_}"
                if self.limit_ is not None
                else snowflake.snowpark._internal.utils.EMPTY_STRING
            )
            offset_clause = (
                f"{analyzer_utils.OFFSET}{self.offset}"
                if self.offset
                else snowflake.snowpark._internal.utils.EMPTY_STRING
            )
            self._sql_query = f"{analyzer_utils.SELECT}{self.projection_in_str}{analyzer_utils.FROM}{from_clause}{where_clause}{order_by_clause}{limit_clause}{offset_clause}"
        return self._sql_query

    @property
    def placeholder_query(self) -> str:
        if self._placeholder_query:
            return self._placeholder_query
        from_clause = f"{analyzer_utils.LEFT_PARENTHESIS}{self.from_._id}{analyzer_utils.RIGHT_PARENTHESIS}"
        if not self.has_clause and not self.projection:
            self._placeholder_query = from_clause
            return self._placeholder_query

        where_clause = (
            f"{analyzer_utils.WHERE}{self.analyzer.analyze(self.where, self.df_aliased_col_name_to_real_col_name)}"
            if self.where is not None
            else snowflake.snowpark._internal.utils.EMPTY_STRING
        )
        order_by_clause = (
            f"{analyzer_utils.ORDER_BY}{analyzer_utils.COMMA.join(self.analyzer.analyze(x, self.df_aliased_col_name_to_real_col_name) for x in self.order_by)}"
            if self.order_by
            else snowflake.snowpark._internal.utils.EMPTY_STRING
        )
        limit_clause = (
            f"{analyzer_utils.LIMIT}{self.limit_}"
            if self.limit_ is not None
            else snowflake.snowpark._internal.utils.EMPTY_STRING
        )
        offset_clause = (
            f"{analyzer_utils.OFFSET}{self.offset}"
            if self.offset
            else snowflake.snowpark._internal.utils.EMPTY_STRING
        )
        self._placeholder_query = f"{analyzer_utils.SELECT}{self.projection_in_str}{analyzer_utils.FROM}{from_clause}{where_clause}{order_by_clause}{limit_clause}{offset_clause}"
        return self._placeholder_query

    @property
    def query_params(self) -> Optional[Sequence[Any]]:
        return self.from_.query_params

    @property
    def schema_query(self) -> str:
        if self._schema_query:
            return self._schema_query
        if not self.projection:
            self._schema_query = self.from_.schema_query
            return self._schema_query
        self._schema_query = f"{analyzer_utils.SELECT}{self.projection_in_str}{analyzer_utils.FROM}({self.from_.schema_query})"
        return self._schema_query

    @property
    def children_plan_nodes(self) -> List[Union["Selectable", SnowflakePlan]]:
        return [self.from_]

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        complexity = {}
        # projection component
        complexity = (
            sum_node_complexities(*self.projection_complexities)
            if self.projection
            else complexity
        )

        # filter component - add +1 for WHERE clause and sum of expression complexity for where expression
        complexity = (
            sum_node_complexities(
                complexity,
                {PlanNodeCategory.FILTER: 1},
                self.where.cumulative_node_complexity,
            )
            if self.where
            else complexity
        )

        # order by component - add complexity for each sort expression
        complexity = (
            sum_node_complexities(
                complexity,
                *(expr.cumulative_node_complexity for expr in self.order_by),
                {PlanNodeCategory.ORDER_BY: 1},
            )
            if self.order_by
            else complexity
        )

        # limit/offset component
        complexity = (
            sum_node_complexities(complexity, {PlanNodeCategory.LOW_IMPACT: 1})
            if self.limit_
            else complexity
        )
        complexity = (
            sum_node_complexities(complexity, {PlanNodeCategory.LOW_IMPACT: 1})
            if self.offset
            else complexity
        )
        return complexity

    @property
    def cumulative_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        if self._cumulative_node_complexity is None:
            self._cumulative_node_complexity = super().cumulative_node_complexity
            if self._merge_projection_complexity_with_subquery:
                # if _merge_projection_complexity_with_subquery is true, the subquery
                # projection complexity has already been merged with the current projection
                # complexity, and we need to adjust the cumulative_node_complexity by
                # subtracting the from_ projection complexity.
                assert isinstance(self.from_, SelectStatement)
                self._cumulative_node_complexity = subtract_complexities(
                    self._cumulative_node_complexity,
                    sum_node_complexities(*self.from_.projection_complexities),
                )

        return self._cumulative_node_complexity

    @cumulative_node_complexity.setter
    def cumulative_node_complexity(self, value: Dict[PlanNodeCategory, int]):
        self._cumulative_node_complexity = value

    @property
    def referenced_ctes(self) -> Dict[WithQueryBlock, int]:
        return self.from_.referenced_ctes

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
            new.column_states = self.column_states
            return new
        return self

    def get_projection_name_complexity_map(
        self,
    ) -> Optional[Dict[str, Dict[PlanNodeCategory, int]]]:
        """
        Get a map between the projection column name and its complexity. If name or
        projection complexity is missing for any column, None is returned.
        """
        if (
            (not self._column_states)
            or (not self.projection)
            or (not self._column_states.projection)
        ):
            return None

        if len(self.projection) != len(self._column_states.projection):
            return None

        projection_complexities = self.projection_complexities
        if len(self._column_states.projection) != len(projection_complexities):
            return None
        else:
            return {
                attribute.name: complexity
                for complexity, attribute in zip(
                    projection_complexities, self._column_states.projection
                )
            }

    @property
    def projection_complexities(self) -> List[Dict[PlanNodeCategory, int]]:
        """
        Return the cumulative complexity for each projection expression. The
        complexity is merged with the subquery projection complexity if
        _merge_projection_complexity_with_subquery is True.
        """
        if self.projection is None:
            return []

        if self._projection_complexities is None:
            if self._merge_projection_complexity_with_subquery:
                assert isinstance(
                    self.from_, SelectStatement
                ), "merge with none SelectStatement is not valid"
                subquery_projection_name_complexity_map = (
                    self.from_.get_projection_name_complexity_map()
                )
                assert (
                    subquery_projection_name_complexity_map is not None
                ), "failed to extract dependent column map from subquery"
                self._projection_complexities = []
                for proj in self.projection:
                    # For a projection expression that dependents on columns [col1, col2, col1],
                    # and whose original cumulative_node_complexity is proj_complexity, the
                    # new complexity can be calculated as
                    # proj_complexity - {PlanNodeCategory.COLUMN: 1} + col1_complexity
                    #       - {PlanNodeCategory.COLUMN: 1} + col2_complexity
                    #       - {PlanNodeCategory.COLUMN: 1} + col1_complexity
                    dependent_columns = proj.dependent_column_names_with_duplication()
                    projection_complexity = proj.cumulative_node_complexity
                    for dependent_column in dependent_columns:
                        dependent_column_complexity = (
                            subquery_projection_name_complexity_map[dependent_column]
                        )
                        projection_complexity[PlanNodeCategory.COLUMN] -= 1
                        projection_complexity = sum_node_complexities(
                            projection_complexity, dependent_column_complexity
                        )

                    self._projection_complexities.append(projection_complexity)
            else:
                self._projection_complexities = [
                    expr.cumulative_node_complexity for expr in self.projection
                ]

        return self._projection_complexities

    def select(self, cols: List[Expression]) -> "SelectStatement":
        """Build a new query. This SelectStatement will be the subquery of the new query.
        Possibly flatten the new query and the subquery (self) to form a new flattened query.
        """
        if (
            len(cols) == 1
            and isinstance(cols[0], UnresolvedAlias)
            and isinstance(cols[0].child, Star)
            and not cols[0].child.expressions
            and not cols[0].child.df_alias
            # df.select("*") doesn't have the child.expressions
            # df.select(df["*"]) has the child.expressions
        ):
            new = copy(self)  # it copies the api_calls
            new._projection_in_str = self._projection_in_str
            new._schema_query = self._schema_query
            new.column_states = self.column_states
            new._snowflake_plan = (
                None
                # To allow the original dataframe and the dataframe created from `df.select("*") to join,
                # They shouldn't share the same snowflake_plan.
                # Setting it to None so the new._snowflake_plan will be created later.
            )
            new.expr_to_alias = copy(
                self.expr_to_alias
            )  # use copy because we don't want two plans to share the same list. If one mutates, the other ones won't be impacted.
            new.flatten_disabled = self.flatten_disabled
            # no need to flatten the projection complexity since the select projection is already flattened.
            new._merge_projection_complexity_with_subquery = False
            return new
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
        elif self.flatten_disabled:
            can_be_flattened = False
        elif (
            self.has_clause_using_columns
            and self.snowflake_plan.session
            and not self.snowflake_plan.session.conf.get(
                "flatten_select_after_filter_and_orderby"
            )
        ):
            # TODO: Clean up, this entire if case is parameter protection
            can_be_flattened = False
        elif (self.where or self.order_by or self.limit_) and has_data_generator_exp(
            cols
        ):
            can_be_flattened = False
        elif self.where and (
            (subquery_dependent_columns := derive_dependent_columns(self.where))
            in (COLUMN_DEPENDENCY_DOLLAR, COLUMN_DEPENDENCY_ALL)
            or any(
                new_column_states[_col].change_state == ColumnChangeState.NEW
                for _col in (
                    subquery_dependent_columns & new_column_states.active_columns
                )
            )
        ):
            can_be_flattened = False
        elif self.order_by and (
            (subquery_dependent_columns := derive_dependent_columns(*self.order_by))
            in (COLUMN_DEPENDENCY_DOLLAR, COLUMN_DEPENDENCY_ALL)
            or any(
                new_column_states[_col].change_state
                in (ColumnChangeState.CHANGED_EXP, ColumnChangeState.NEW)
                for _col in (
                    subquery_dependent_columns & new_column_states.active_columns
                )
            )
        ):
            can_be_flattened = False
        else:
            can_be_flattened = can_select_statement_be_flattened(
                self.column_states, new_column_states
            )

        if can_be_flattened:
            new = copy(self)
            final_projection = []

            assert new_column_states is not None
            for col, state in new_column_states.items():
                if state.change_state in (
                    ColumnChangeState.CHANGED_EXP,
                    ColumnChangeState.NEW,
                ):
                    final_projection.append(copy(state.expression))
                elif state.change_state == ColumnChangeState.UNCHANGED_EXP:
                    final_projection.append(
                        copy(self.column_states[col].expression)
                    )  # add subquery's expression for this column name

            new.projection = final_projection
            new.from_ = self.from_.to_subqueryable()
            new.pre_actions = new.from_.pre_actions
            new.post_actions = new.from_.post_actions
            # there is no need to flatten the projection complexity since the child
            # select projection is already flattened with the current select.
            new._merge_projection_complexity_with_subquery = False
        else:
            new = SelectStatement(
                projection=cols, from_=self.to_subqueryable(), analyzer=self.analyzer
            )
            new._merge_projection_complexity_with_subquery = (
                can_select_projection_complexity_be_merged(
                    cols,
                    new_column_states,
                    self,
                )
            )

        new.flatten_disabled = disable_next_level_flatten
        assert new.projection is not None
        new._column_states = derive_column_states_from_subquery(
            new.projection, new.from_
        )
        # If new._column_states is None, when property `column_states` is called later,
        # a query will be described and an error like "invalid identifier" will be thrown.

        return new

    def filter(self, col: Expression) -> "SelectStatement":
        can_be_flattened = (
            (not self.flatten_disabled)
            and can_clause_dependent_columns_flatten(
                derive_dependent_columns(col), self.column_states
            )
            and not has_data_generator_exp(self.projection)
            and not (self.order_by and self.limit_ is not None)
        )
        if can_be_flattened:
            new = copy(self)
            new.from_ = self.from_.to_subqueryable()
            new.pre_actions = new.from_.pre_actions
            new.post_actions = new.from_.post_actions
            new.column_states = self.column_states
            new.where = And(self.where, col) if self.where is not None else col
            new._merge_projection_complexity_with_subquery = False
        else:
            new = SelectStatement(
                from_=self.to_subqueryable(), where=col, analyzer=self.analyzer
            )

        return new

    def sort(self, cols: List[Expression]) -> "SelectStatement":
        can_be_flattened = (
            (not self.flatten_disabled)
            and can_clause_dependent_columns_flatten(
                derive_dependent_columns(*cols), self.column_states
            )
            and not has_data_generator_exp(self.projection)
        )
        if can_be_flattened:
            new = copy(self)
            new.from_ = self.from_.to_subqueryable()
            new.pre_actions = new.from_.pre_actions
            new.post_actions = new.from_.post_actions
            new.order_by = cols + (self.order_by or [])
            new.column_states = self.column_states
            new._merge_projection_complexity_with_subquery = False
        else:
            new = SelectStatement(
                from_=self.to_subqueryable(),
                order_by=cols,
                analyzer=self.analyzer,
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
        if (
            isinstance(self.from_, SetStatement)
            and not self.has_clause
            and not self.projection
        ):
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
        new.column_states = set_statement.column_states
        return new

    def limit(self, n: int, *, offset: int = 0) -> "SelectStatement":
        if (
            offset and self.limit_
        ):  # The new offset would impact the previous layer limit if flattened so no flatten.
            new = SelectStatement(
                from_=self.to_subqueryable(),
                limit_=n,
                offset=offset,
                analyzer=self.analyzer,
            )
        else:
            new = copy(self)
            new.from_ = self.from_.to_subqueryable()
            new.limit_ = min(self.limit_, n) if self.limit_ else n
            new.offset = offset or self.offset
            new.column_states = self.column_states
            new.pre_actions = new.from_.pre_actions
            new.post_actions = new.from_.post_actions
            new._merge_projection_complexity_with_subquery = False
        return new


class SelectTableFunction(Selectable):
    """Wrap table function related plan to a subclass of Selectable."""

    def __init__(
        self,
        func_expr: TableFunctionExpression,
        *,
        other_plan: Optional[LogicalPlan] = None,
        left_cols: Optional[List[str]] = None,
        right_cols: Optional[List[str]] = None,
        # snowflake_plan for SelectTableFunction if already known. This is
        # used during copy to avoid extra resolving step.
        snowflake_plan: Optional[SnowflakePlan] = None,
        analyzer: "Analyzer",
    ) -> None:
        super().__init__(analyzer)
        self.func_expr = func_expr
        self._snowflake_plan: SnowflakePlan
        if snowflake_plan is not None:
            self._snowflake_plan = snowflake_plan
        else:
            if other_plan:
                self._snowflake_plan = analyzer.resolve(
                    TableFunctionJoin(other_plan, func_expr, left_cols, right_cols)
                )
            else:
                self._snowflake_plan = analyzer.resolve(
                    TableFunctionRelation(func_expr)
                )
        self.pre_actions = self._snowflake_plan.queries[:-1]
        self.post_actions = self._snowflake_plan.post_actions
        self._api_calls = self._snowflake_plan.api_calls

    def __deepcopy__(self, memodict={}) -> "SelectTableFunction":  # noqa: B006
        copied = SelectTableFunction(
            func_expr=deepcopy(self.func_expr, memodict),
            snowflake_plan=deepcopy(self._snowflake_plan, memodict),
            analyzer=self.analyzer,
        )
        # copy over the other selectable fields, the snowflake plan has already been set correctly.
        _deepcopy_selectable_fields(from_selectable=self, to_selectable=copied)
        return copied

    @property
    def snowflake_plan(self):
        return self._snowflake_plan

    @property
    def sql_query(self) -> str:
        return self._snowflake_plan.queries[-1].sql

    @property
    def placeholder_query(self) -> Optional[str]:
        return self._snowflake_plan.placeholder_query

    @property
    def schema_query(self) -> Optional[str]:
        return self._snowflake_plan.schema_query

    @property
    def query_params(self) -> Optional[Sequence[Any]]:
        return self.snowflake_plan.queries[-1].params

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        return self.snowflake_plan.individual_node_complexity

    @property
    def cumulative_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        if self._cumulative_node_complexity is None:
            self._cumulative_node_complexity = (
                self.snowflake_plan.cumulative_node_complexity
            )
        return self._cumulative_node_complexity

    @cumulative_node_complexity.setter
    def cumulative_node_complexity(self, value: Dict[PlanNodeCategory, int]):
        self._cumulative_node_complexity = value

    def reset_cumulative_node_complexity(self) -> None:
        super().reset_cumulative_node_complexity()
        self.snowflake_plan.reset_cumulative_node_complexity()

    @property
    def referenced_ctes(self) -> Dict[WithQueryBlock, int]:
        return self._snowflake_plan.referenced_ctes


class SetOperand:
    def __init__(self, selectable: Selectable, operator: Optional[str] = None) -> None:
        super().__init__()
        self.selectable = selectable
        self.operator = operator


class SetStatement(Selectable):
    def __init__(self, *set_operands: SetOperand, analyzer: "Analyzer") -> None:
        super().__init__(analyzer=analyzer)
        self._sql_query = None
        self._placeholder_query = None
        self.set_operands = set_operands
        self._nodes = []
        for operand in set_operands:
            if operand.selectable.pre_actions:
                if not self.pre_actions:
                    self.pre_actions = []
                for action in operand.selectable.pre_actions:
                    if action not in self.pre_actions:
                        self.pre_actions.append(copy(action))
            if operand.selectable.post_actions:
                if not self.post_actions:
                    self.post_actions = []
                for action in operand.selectable.post_actions:
                    if action not in self.post_actions:
                        self.post_actions.append(copy(action))
            self._nodes.append(operand.selectable)

    def __deepcopy__(self, memodict={}) -> "SetStatement":  # noqa: B006
        copied = SetStatement(
            *deepcopy(self.set_operands, memodict), analyzer=self.analyzer
        )
        _deepcopy_selectable_fields(from_selectable=self, to_selectable=copied)
        copied._placeholder_query = self._placeholder_query
        copied._sql_query = self._sql_query

        return copied

    @property
    def sql_query(self) -> str:
        if not self._sql_query:
            sql = f"({self.set_operands[0].selectable.sql_query})"
            for i in range(1, len(self.set_operands)):
                sql = f"{sql}{self.set_operands[i].operator}({self.set_operands[i].selectable.sql_query})"
            self._sql_query = sql
        return self._sql_query

    @property
    def placeholder_query(self) -> Optional[str]:
        if not self._placeholder_query:
            sql = f"({self.set_operands[0].selectable._id})"
            for i in range(1, len(self.set_operands)):
                sql = f"{sql}{self.set_operands[i].operator}({self.set_operands[i].selectable._id})"
            self._placeholder_query = sql
        return self._placeholder_query

    @property
    def schema_query(self) -> str:
        """The first operand decide the column attributes of a query with set operations.
        Refer to https://docs.snowflake.com/en/sql-reference/operators-query.html#general-usage-notes"""
        attributes = self.set_operands[0].selectable.snowflake_plan.attributes
        sql = f"({schema_value_statement(attributes)})"
        for i in range(1, len(self.set_operands)):
            attributes = self.set_operands[i].selectable.snowflake_plan.attributes
            sql = f"{sql}{self.set_operands[i].operator}({schema_value_statement(attributes)})"
        return sql

    @property
    def column_states(self) -> ColumnStateDict:
        if not self._column_states:
            self._column_states = initiate_column_states(
                self.set_operands[0].selectable.column_states.projection,
                self.analyzer,
                self.df_aliased_col_name_to_real_col_name,
            )
        return self._column_states

    @property
    def query_params(self) -> Optional[Sequence[Any]]:
        query_params = None
        for operand in self.set_operands:
            if operand.selectable.query_params:
                if query_params is None:
                    query_params = []
                query_params.extend(operand.selectable.query_params)
        return query_params

    @property
    def children_plan_nodes(self) -> List[Union["Selectable", SnowflakePlan]]:
        return self._nodes

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # we add #set_operands - 1 additional operators in sql query
        return {PlanNodeCategory.SET_OPERATION: len(self.set_operands) - 1}

    @property
    def referenced_ctes(self) -> Dict[WithQueryBlock, int]:
        # get a union of referenced cte tables from all child nodes
        merged_ctes = dict()
        for node in self._nodes:
            for with_query_block, count in node.referenced_ctes.items():
                if with_query_block in merged_ctes:
                    merged_ctes[with_query_block] += count
                else:
                    merged_ctes[with_query_block] = count
        return merged_ctes


class DeriveColumnDependencyError(Exception):
    """When deriving column dependencies from the subquery."""


def parse_column_name(
    column: Expression,
    analyzer: "Analyzer",
    df_aliased_col_name_to_real_col_name: DefaultDict[str, Dict[str, str]],
) -> Optional[str]:
    if isinstance(column, Expression):
        if isinstance(column, Attribute):
            # Use analyze for the case of
            #     df1 = session.create_dataframe([[1]], schema=["a"])
            #     df2 = df1.select(df1["a"].alias("b"))
            #     df3 = df2.select(df1["a"])  # df1["a"] converted to column name "b" instead of "a"
            #     df3.show()
            # some expressions converted to SQL text with extra preceeding and trailing spaces.
            # Snowflake SQL removes the spaces in the returned column names.
            # So we remove it at the client too.
            return analyzer.analyze(
                column, df_aliased_col_name_to_real_col_name, parse_local_name=True
            ).strip(" ")
        if isinstance(column, UnresolvedAttribute):
            if not column.is_sql_text:
                return column.name
        if isinstance(column, UnresolvedAlias):
            return analyzer.analyze(
                column, df_aliased_col_name_to_real_col_name, parse_local_name=True
            ).strip(" ")
        if isinstance(column, Alias):
            return column.name
    # We can parse column name from a column's SQL expression in the future.
    # When parsing column name isn't possible, the SelectStatement.select won't flatten and
    # disables the next level SelectStatement to flatten
    return None


def can_select_statement_be_flattened(
    subquery_column_states: ColumnStateDict, new_column_states: ColumnStateDict
) -> bool:
    for col, state in new_column_states.items():
        dependent_columns = state.dependent_columns
        if dependent_columns == COLUMN_DEPENDENCY_DOLLAR:
            return False
        if state.change_state in (
            ColumnChangeState.CHANGED_EXP,
            ColumnChangeState.NEW,
        ) and not can_projection_dependent_columns_be_flattened(
            dependent_columns, subquery_column_states
        ):
            return False
        elif state.change_state == ColumnChangeState.UNCHANGED_EXP and (
            col not in subquery_column_states
            or subquery_column_states[col].depend_on_same_level
        ):
            # query may change sequence of columns. If subquery has same-level reference, flattened sql may not work.
            return False
        elif (
            state.change_state == ColumnChangeState.DROPPED
            and (subquery_state := subquery_column_states.get(col))
            and subquery_state.change_state == ColumnChangeState.NEW
            and subquery_state.is_referenced_by_same_level_column
        ):
            return False
    return True


def can_projection_dependent_columns_be_flattened(
    dependent_columns: Optional[AbstractSet[str]],
    subquery_column_states: ColumnStateDict,
) -> bool:
    # COLUMN_DEPENDENCY_DOLLAR should already be handled before calling this function
    if dependent_columns == COLUMN_DEPENDENCY_DOLLAR:  # pragma: no cover
        return False
    elif (
        subquery_column_states.has_changed_columns
        or subquery_column_states.has_dropped_columns
        or subquery_column_states.has_new_columns
    ):
        if dependent_columns == COLUMN_DEPENDENCY_ALL:
            return False
        else:
            assert dependent_columns is not None
            for dc in dependent_columns:
                dc_state = subquery_column_states.get(dc)
                if dc_state and dc_state.change_state in (
                    (
                        ColumnChangeState.CHANGED_EXP,
                        ColumnChangeState.DROPPED,
                        ColumnChangeState.NEW,
                    )
                ):
                    return False
    return True


def can_clause_dependent_columns_flatten(
    dependent_columns: Optional[AbstractSet[str]],
    subquery_column_states: ColumnStateDict,
) -> bool:
    if dependent_columns == COLUMN_DEPENDENCY_DOLLAR:
        return False
    elif (
        subquery_column_states.has_changed_columns
        or subquery_column_states.has_new_columns
    ):
        if dependent_columns == COLUMN_DEPENDENCY_ALL:
            return False

        assert dependent_columns is not None
        for dc in dependent_columns:
            dc_state = subquery_column_states.get(dc)
            if dc_state:
                if dc_state.change_state == ColumnChangeState.CHANGED_EXP:
                    return False
                elif dc_state.change_state == ColumnChangeState.NEW:
                    # Most of the time this can be flattened. But if a new column uses window function and this column
                    # is used in a clause, the sql doesn't work in Snowflake.
                    # For instance `select a, rank() over(order by b) as d from test_table where d = 1` doesn't work.
                    # But `select a, b as d from test_table where d = 1` works
                    # We can inspect whether the referenced new column uses window function. Here we are being
                    # conservative for now to not flatten the SQL.
                    return False
    return True


def can_select_projection_complexity_be_merged(
    cols: List[Expression],
    column_states: Optional[ColumnStateDict],
    subquery: Selectable,
) -> bool:
    """
    Check whether projection complexity of subquery can be merged with the current
    projection columns.

    Args:
        cols: the projection column expressions of the current select
        column_states: the column states extracted out of the current projection column
            on top of subquery.
        subquery: the subquery where the current select is performed on top of
    """
    if not subquery.analyzer.session._large_query_breakdown_enabled:
        return False

    # only merge of nested select statement is supported, and subquery must be
    # a SelectStatement
    if column_states is None or (not isinstance(subquery, SelectStatement)):
        return False  # pragma: no cover

    if len(cols) != len(column_states.projection):
        # Failed to extract the attributes of some columns
        return False  # pragma: no cover

    if subquery._column_states is None:
        return False  # pragma: no cover

    # It is not valid to merge the projection complexity if:
    # 1) exist a column without state extracted
    # 2) exist a column that dependents on columns from the same level
    # 3) exist a column that dependents on $. Theoretically, this could be
    #       valid, but extra analysis is required to check the validness.
    # 4) all dependent column in the projection expression is an active column
    #    from the subquery
    for proj in column_states.projection:
        column_state = column_states.get(proj.name)
        if column_state is None:
            return False  # pragma: no cover
        if column_state.depend_on_same_level:
            return False
        if column_state.dependent_columns == COLUMN_DEPENDENCY_DOLLAR:
            return False
        if column_state.dependent_columns != COLUMN_DEPENDENCY_ALL:
            for dependent_col in column_state.dependent_columns:
                if dependent_col not in subquery._column_states.active_columns:
                    return False  # pragma: no cover

    # check if the current select have filter, order by, or limit
    if subquery.where or subquery.order_by or subquery.limit_ or subquery.offset:
        return False

    # check if the projection expression contain invalid functions
    if has_invalid_projection_merge_functions(cols):
        return False

    # check if subquery projection expression contain invalid functions
    if has_invalid_projection_merge_functions(subquery.projection):
        return False

    return True


def initiate_column_states(
    column_attrs: List[Attribute],
    analyzer: "Analyzer",
    df_aliased_col_name_to_real_col_name: DefaultDict[str, Dict[str, str]],
) -> ColumnStateDict:
    column_states = ColumnStateDict()
    for attr in column_attrs:
        # review later. should use parse_column_name
        name = analyzer.analyze(
            attr, df_aliased_col_name_to_real_col_name, parse_local_name=True
        ).strip(" ")
        column_states[name] = ColumnState(
            name,
            change_state=ColumnChangeState.UNCHANGED_EXP,
            expression=attr,
            dependent_columns=COLUMN_DEPENDENCY_EMPTY,
            depend_on_same_level=False,
            referenced_by_same_level_columns=COLUMN_DEPENDENCY_EMPTY,
            state_dict=column_states,
        )
    column_states.projection = [
        copy(attr) for attr in column_attrs
    ]  # copy to re-generate expr_id
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
        assert dependent_column_names is not None
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
                columns_from_star = [copy(e) for e in c.child.expressions]
            elif c.child.df_alias:
                if c.child.df_alias not in from_.df_aliased_col_name_to_real_col_name:
                    raise SnowparkClientExceptionMessages.DF_ALIAS_NOT_RECOGNIZED(
                        c.child.df_alias
                    )
                aliased_cols = from_.df_aliased_col_name_to_real_col_name[
                    c.child.df_alias
                ].values()
                columns_from_star = [
                    copy(e)
                    for e in from_.column_states.projection
                    if e.name in aliased_cols
                ]
            else:
                columns_from_star = [copy(e) for e in from_.column_states.projection]
            column_states.update(
                initiate_column_states(
                    columns_from_star,
                    analyzer,
                    from_.df_aliased_col_name_to_real_col_name,
                )
            )
            column_states.projection.extend(
                [c for c in columns_from_star]
            )  # columns_from_star has copied exps.
            continue
        c_name = parse_column_name(
            c, analyzer, from_.df_aliased_col_name_to_real_col_name
        )
        if c_name is None:
            return None
        quoted_c_name = snowflake.snowpark._internal.utils.quote_name(c_name)
        # if c is not an Attribute object, we will only care about the column name,
        # so we can build a dummy Attribute with the column name and dummy type
        column_states.projection.append(
            copy(c)
            if isinstance(c, Attribute)
            else Attribute(quoted_c_name, DataType())
        )
        from_c_state = from_.column_states.get(quoted_c_name)
        if from_c_state and from_c_state.change_state != ColumnChangeState.DROPPED:
            # review later. should use parse_column_name
            if c_name != analyzer.analyze(
                c, from_.df_aliased_col_name_to_real_col_name, parse_local_name=True
            ).strip(" "):
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


def has_data_generator_exp(expressions: Optional[List["Expression"]]) -> bool:
    if expressions is None:
        return False
    for exp in expressions:
        if isinstance(exp, WindowExpression):
            return True
        if isinstance(exp, FunctionExpression) and (
            exp.is_data_generator
            or exp.name.lower() in SEQUENCE_DEPENDENT_DATA_GENERATION
        ):
            # https://docs.snowflake.com/en/sql-reference/functions-data-generation
            return True
        if exp is not None and has_data_generator_exp(exp.children):
            return True
    return False
