#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import copy
import re
import sys
import uuid
from collections import defaultdict
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    DefaultDict,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from snowflake.snowpark._internal.analyzer.table_function import (
    GeneratorTableFunction,
    TableFunctionRelation,
)

if TYPE_CHECKING:
    from snowflake.snowpark._internal.analyzer.select_statement import (
        Selectable,
    )  # pragma: no cover
    import snowflake.snowpark.session
    import snowflake.snowpark.dataframe

import snowflake.connector
import snowflake.snowpark
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    aggregate_statement,
    attribute_to_schema_string,
    batch_insert_into_statement,
    copy_into_location,
    copy_into_table,
    create_file_format_statement,
    create_or_replace_dynamic_table_statement,
    create_or_replace_view_statement,
    create_table_as_select_statement,
    create_table_statement,
    delete_statement,
    drop_file_format_if_exists_statement,
    drop_table_if_exists_statement,
    file_operation_statement,
    filter_statement,
    insert_into_statement,
    join_statement,
    join_table_function_statement,
    lateral_statement,
    limit_statement,
    merge_statement,
    pivot_statement,
    project_statement,
    rename_statement,
    result_scan_statement,
    sample_statement,
    schema_cast_named,
    schema_cast_seq,
    schema_value_statement,
    select_from_path_with_format_statement,
    set_operator_statement,
    sort_statement,
    table_function_statement,
    unpivot_statement,
    update_statement,
)
from snowflake.snowpark._internal.analyzer.binary_plan_node import (
    JoinType,
    SetOperation,
)
from snowflake.snowpark._internal.analyzer.cte_utils import (
    create_cte_query,
    encode_id,
    find_duplicate_subtrees,
)
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.schema_utils import analyze_attributes
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    SaveMode,
)
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.utils import (
    INFER_SCHEMA_FORMAT_TYPES,
    TempObjectType,
    generate_random_alphanumeric,
    get_copy_into_table_options,
    is_sql_select_statement,
    random_name_for_temp_object,
)
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import StructType

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable


class SnowflakePlan(LogicalPlan):
    class Decorator:
        __wrap_exception_regex_match = re.compile(
            r"""(?s).*invalid identifier '"?([^'"]*)"?'.*"""
        )
        __wrap_exception_regex_sub = re.compile(r"""^"|"$""")

        @staticmethod
        def wrap_exception(func):
            def wrap(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except snowflake.connector.errors.ProgrammingError as e:
                    query = getattr(e, "query", None)
                    tb = sys.exc_info()[2]
                    assert e.msg is not None
                    if "unexpected 'as'" in e.msg.lower():
                        ne = SnowparkClientExceptionMessages.SQL_PYTHON_REPORT_UNEXPECTED_ALIAS(
                            query
                        )
                        raise ne.with_traceback(tb) from None
                    elif e.sqlstate == "42000" and "invalid identifier" in e.msg:
                        match = (
                            SnowflakePlan.Decorator.__wrap_exception_regex_match.match(
                                e.msg
                            )
                        )
                        if not match:  # pragma: no cover
                            ne = SnowparkClientExceptionMessages.SQL_EXCEPTION_FROM_PROGRAMMING_ERROR(
                                e
                            )
                            raise ne.with_traceback(tb) from None
                        col = match.group(1)
                        children = [
                            arg for arg in args if isinstance(arg, SnowflakePlan)
                        ]
                        remapped = [
                            SnowflakePlan.Decorator.__wrap_exception_regex_sub.sub(
                                "", val
                            )
                            for child in children
                            for val in child.expr_to_alias.values()
                        ]
                        if col in remapped:
                            unaliased_cols = (
                                snowflake.snowpark.dataframe._get_unaliased(col)
                            )
                            orig_col_name = (
                                unaliased_cols[0] if unaliased_cols else "<colname>"
                            )
                            ne = SnowparkClientExceptionMessages.SQL_PYTHON_REPORT_INVALID_ID(
                                orig_col_name, query
                            )
                            raise ne.with_traceback(tb) from None
                        elif (
                            len(
                                [
                                    unaliased
                                    for item in remapped
                                    for unaliased in snowflake.snowpark.dataframe._get_unaliased(
                                        item
                                    )
                                    if unaliased == col
                                ]
                            )
                            > 1
                        ):
                            ne = SnowparkClientExceptionMessages.SQL_PYTHON_REPORT_JOIN_AMBIGUOUS(
                                col, col, query
                            )
                            raise ne.with_traceback(tb) from None
                        else:
                            ne = SnowparkClientExceptionMessages.SQL_EXCEPTION_FROM_PROGRAMMING_ERROR(
                                e
                            )
                            raise ne.with_traceback(tb) from None
                    else:
                        ne = SnowparkClientExceptionMessages.SQL_EXCEPTION_FROM_PROGRAMMING_ERROR(
                            e
                        )
                        raise ne.with_traceback(tb) from None

            return wrap

    def __init__(
        self,
        queries: List["Query"],
        schema_query: str,
        post_actions: Optional[List["Query"]] = None,
        expr_to_alias: Optional[Dict[uuid.UUID, str]] = None,
        source_plan: Optional[LogicalPlan] = None,
        is_ddl_on_temp_object: bool = False,
        api_calls: Optional[List[Dict]] = None,
        df_aliased_col_name_to_real_col_name: Optional[
            DefaultDict[str, Dict[str, str]]
        ] = None,
        placeholder_query: Optional[str] = None,
        *,
        session: "snowflake.snowpark.session.Session",
    ) -> None:
        super().__init__()
        self.queries = queries
        self.schema_query = schema_query
        self.post_actions = post_actions if post_actions else []
        self.expr_to_alias = expr_to_alias if expr_to_alias else {}
        self.session = session
        self.source_plan = source_plan
        self.is_ddl_on_temp_object = is_ddl_on_temp_object
        # We need to copy this list since we don't want to change it for the
        # previous SnowflakePlan objects
        self.api_calls = api_calls.copy() if api_calls else []
        self._output_dict = None
        # Used for dataframe alias
        if df_aliased_col_name_to_real_col_name:
            self.df_aliased_col_name_to_real_col_name = (
                df_aliased_col_name_to_real_col_name
            )
        else:
            self.df_aliased_col_name_to_real_col_name = defaultdict(dict)
        # In the placeholder query, subquery (child) is held by the ID of query plan
        # It is used for optimization, by replacing a subquery with a CTE
        self.placeholder_query = placeholder_query
        # encode an id for CTE optimization
        self._id = encode_id(queries[-1].sql, queries[-1].params)

    def __eq__(self, other: "SnowflakePlan") -> bool:
        if self._id is not None and other._id is not None:
            return isinstance(other, SnowflakePlan) and self._id == other._id
        else:
            return super().__eq__(other)

    def __hash__(self) -> int:
        return hash(self._id) if self._id else super().__hash__()

    @property
    def children_plan_nodes(self) -> List[Union["Selectable", "SnowflakePlan"]]:
        """
        This property is currently only used for traversing the query plan tree
        when performing CTE optimization.
        """
        from snowflake.snowpark._internal.analyzer.select_statement import Selectable

        if self.source_plan:
            if isinstance(self.source_plan, Selectable):
                return self.source_plan.children_plan_nodes
            else:
                return self.source_plan.children
        else:
            return []

    def replace_repeated_subquery_with_cte(self) -> "SnowflakePlan":
        # parameter protection
        if not self.session._cte_optimization_enabled:
            return self

        # if source_plan or placeholder_query is none, it must be a leaf node,
        # no optimization is needed
        if self.source_plan is None or self.placeholder_query is None:
            return self

        # only select statement can be converted to CTEs
        if not is_sql_select_statement(self.queries[-1].sql):
            return self

        # if there is no duplicate node, no optimization will be performed
        duplicate_plan_set = find_duplicate_subtrees(self)
        if not duplicate_plan_set:
            return self

        # create CTE query
        final_query = create_cte_query(self, duplicate_plan_set)

        # all other parts of query are unchanged, but just replace the original query
        plan = copy.copy(self)
        plan.queries[-1].sql = final_query
        return plan

    def with_subqueries(self, subquery_plans: List["SnowflakePlan"]) -> "SnowflakePlan":
        pre_queries = self.queries[:-1]
        new_schema_query = self.schema_query
        new_post_actions = [*self.post_actions]
        api_calls = [*self.api_calls]

        for plan in subquery_plans:
            for query in plan.queries[:-1]:
                if query not in pre_queries:
                    pre_queries.append(query)
            new_schema_query = new_schema_query.replace(
                plan.queries[-1].sql, plan.schema_query
            )
            for action in plan.post_actions:
                if action not in new_post_actions:
                    new_post_actions.append(action)
            api_calls.extend(plan.api_calls)

        return SnowflakePlan(
            pre_queries + [self.queries[-1]],
            new_schema_query,
            post_actions=new_post_actions,
            expr_to_alias=self.expr_to_alias,
            session=self.session,
            source_plan=self.source_plan,
            api_calls=api_calls,
            df_aliased_col_name_to_real_col_name=self.df_aliased_col_name_to_real_col_name,
        )

    @cached_property
    def attributes(self) -> List[Attribute]:
        output = analyze_attributes(self.schema_query, self.session)
        # No simplifier case relies on this schema_query change to update SHOW TABLES to a nested sql friendly query.
        if not self.schema_query or not self.session.sql_simplifier_enabled:
            self.schema_query = schema_value_statement(output)
        return output

    @cached_property
    def output(self) -> List[Attribute]:
        return [Attribute(a.name, a.datatype, a.nullable) for a in self.attributes]

    @property
    def output_dict(self) -> Dict[str, Any]:
        if self._output_dict is None:
            self._output_dict = {
                attr.name: (attr.datatype, attr.nullable) for attr in self.output
            }
        return self._output_dict

    @cached_property
    def plan_height(self) -> int:
        height = 0
        current_level = [self]
        while len(current_level) > 0:
            next_level = []
            for node in current_level:
                next_level.extend(node.children_plan_nodes)
            height += 1
            current_level = next_level
        return height

    @cached_property
    def num_duplicate_nodes(self) -> int:
        return len(find_duplicate_subtrees(self))

    def __copy__(self) -> "SnowflakePlan":
        if self.session._cte_optimization_enabled:
            return SnowflakePlan(
                copy.deepcopy(self.queries) if self.queries else [],
                self.schema_query,
                copy.deepcopy(self.post_actions) if self.post_actions else None,
                dict(self.expr_to_alias) if self.expr_to_alias else None,
                self.source_plan,
                self.is_ddl_on_temp_object,
                copy.deepcopy(self.api_calls) if self.api_calls else None,
                self.df_aliased_col_name_to_real_col_name,
                session=self.session,
                placeholder_query=self.placeholder_query,
            )
        else:
            return SnowflakePlan(
                self.queries.copy() if self.queries else [],
                self.schema_query,
                self.post_actions.copy() if self.post_actions else None,
                dict(self.expr_to_alias) if self.expr_to_alias else None,
                self.source_plan,
                self.is_ddl_on_temp_object,
                self.api_calls.copy() if self.api_calls else None,
                self.df_aliased_col_name_to_real_col_name,
                session=self.session,
                placeholder_query=self.placeholder_query,
            )

    def add_aliases(self, to_add: Dict) -> None:
        self.expr_to_alias = {**self.expr_to_alias, **to_add}


class SnowflakePlanBuilder:
    def __init__(self, session: "snowflake.snowpark.session.Session") -> None:
        self.session = session

    @SnowflakePlan.Decorator.wrap_exception
    def build(
        self,
        sql_generator: Callable[[str], str],
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
        schema_query: Optional[str] = None,
        is_ddl_on_temp_object: bool = False,
    ) -> SnowflakePlan:
        select_child = self.add_result_scan_if_not_select(child)
        queries = select_child.queries[:-1] + [
            Query(
                sql_generator(select_child.queries[-1].sql),
                query_id_place_holder="",
                is_ddl_on_temp_object=is_ddl_on_temp_object,
                params=select_child.queries[-1].params,
            )
        ]
        new_schema_query = (
            schema_query if schema_query else sql_generator(child.schema_query)
        )
        placeholder_query = (
            sql_generator(select_child._id)
            if self.session._cte_optimization_enabled and select_child._id is not None
            else None
        )

        return SnowflakePlan(
            queries,
            new_schema_query,
            select_child.post_actions,
            select_child.expr_to_alias,
            source_plan,
            is_ddl_on_temp_object,
            api_calls=select_child.api_calls,
            df_aliased_col_name_to_real_col_name=child.df_aliased_col_name_to_real_col_name,
            session=self.session,
            placeholder_query=placeholder_query,
        )

    @SnowflakePlan.Decorator.wrap_exception
    def build_from_multiple_queries(
        self,
        multi_sql_generator: Callable[["Query"], List["Query"]],
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
        schema_query: str,
        is_ddl_on_temp_object: bool = False,
    ) -> SnowflakePlan:
        select_child = self.add_result_scan_if_not_select(child)
        queries = select_child.queries[0:-1] + [
            Query(
                query.sql,
                is_ddl_on_temp_object=is_ddl_on_temp_object,
                params=query.params,
            )
            for query in multi_sql_generator(select_child.queries[-1])
        ]
        new_schema_query = (
            schema_query
            if schema_query is not None
            else multi_sql_generator(Query(select_child.schema_query))[-1].sql
        )
        placeholder_query = (
            multi_sql_generator(Query(select_child._id))[-1].sql
            if self.session._cte_optimization_enabled and select_child._id is not None
            else None
        )

        return SnowflakePlan(
            queries,
            new_schema_query,
            select_child.post_actions,
            select_child.expr_to_alias,
            source_plan,
            api_calls=select_child.api_calls,
            session=self.session,
            placeholder_query=placeholder_query,
        )

    @SnowflakePlan.Decorator.wrap_exception
    def build_binary(
        self,
        sql_generator: Callable[[str, str], str],
        left: SnowflakePlan,
        right: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        select_left = self.add_result_scan_if_not_select(left)
        select_right = self.add_result_scan_if_not_select(right)
        queries = (
            select_left.queries[:-1]
            + select_right.queries[:-1]
            + [
                Query(
                    sql_generator(
                        select_left.queries[-1].sql, select_right.queries[-1].sql
                    ),
                    params=[
                        *select_left.queries[-1].params,
                        *select_right.queries[-1].params,
                    ],
                )
            ]
        )

        left_schema_query = schema_value_statement(select_left.attributes)
        right_schema_query = schema_value_statement(select_right.attributes)
        schema_query = sql_generator(left_schema_query, right_schema_query)
        placeholder_query = (
            sql_generator(select_left._id, select_right._id)
            if self.session._cte_optimization_enabled
            and select_left._id is not None
            and select_right._id is not None
            else None
        )

        common_columns = set(select_left.expr_to_alias.keys()).intersection(
            select_right.expr_to_alias.keys()
        )
        new_expr_to_alias = {
            k: v
            for k, v in {
                **select_left.expr_to_alias,
                **select_right.expr_to_alias,
            }.items()
            if k not in common_columns
        }
        api_calls = [*select_left.api_calls, *select_right.api_calls]

        return SnowflakePlan(
            queries,
            schema_query,
            select_left.post_actions + select_right.post_actions,
            new_expr_to_alias,
            source_plan,
            api_calls=api_calls,
            session=self.session,
            placeholder_query=placeholder_query,
        )

    def query(
        self,
        sql: str,
        source_plan: Optional[LogicalPlan],
        api_calls: Optional[List[Dict]] = None,
        params: Optional[Sequence[Any]] = None,
        schema_query: Optional[str] = None,
    ) -> SnowflakePlan:
        return SnowflakePlan(
            queries=[Query(sql, params=params)],
            schema_query=schema_query or sql,
            session=self.session,
            source_plan=source_plan,
            api_calls=api_calls,
        )

    def large_local_relation_plan(
        self,
        output: List[Attribute],
        data: List[Row],
        source_plan: Optional[LogicalPlan],
        schema_query: Optional[str],
    ) -> SnowflakePlan:
        temp_table_name = random_name_for_temp_object(TempObjectType.TABLE)
        attributes = [
            Attribute(attr.name, attr.datatype, attr.nullable) for attr in output
        ]
        create_table_stmt = create_table_statement(
            temp_table_name,
            attribute_to_schema_string(attributes),
            replace=True,
            table_type="temporary",
            use_scoped_temp_objects=self.session._use_scoped_temp_objects,
            is_generated=True,
        )
        insert_stmt = batch_insert_into_statement(
            temp_table_name,
            [attr.name for attr in attributes],
            self.session._conn._conn._paramstyle,
        )
        select_stmt = project_statement([], temp_table_name)
        drop_table_stmt = drop_table_if_exists_statement(temp_table_name)
        schema_query = schema_query or schema_value_statement(attributes)
        queries = [
            Query(create_table_stmt, is_ddl_on_temp_object=True),
            BatchInsertQuery(insert_stmt, data),
            Query(select_stmt),
        ]
        return SnowflakePlan(
            queries=queries,
            schema_query=schema_query,
            post_actions=[Query(drop_table_stmt, is_ddl_on_temp_object=True)],
            session=self.session,
            source_plan=source_plan,
        )

    def table(self, table_name: str) -> SnowflakePlan:
        return self.query(project_statement([], table_name), None)

    def file_operation_plan(
        self, command: str, file_name: str, stage_location: str, options: Dict[str, str]
    ) -> SnowflakePlan:
        return self.query(
            file_operation_statement(command, file_name, stage_location, options),
            None,
        )

    def project(
        self,
        project_list: List[str],
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
        is_distinct: bool = False,
    ) -> SnowflakePlan:
        return self.build(
            lambda x: project_statement(project_list, x, is_distinct=is_distinct),
            child,
            source_plan,
        )

    def aggregate(
        self,
        grouping_exprs: List[str],
        aggregate_exprs: List[str],
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        return self.build(
            lambda x: aggregate_statement(grouping_exprs, aggregate_exprs, x),
            child,
            source_plan,
        )

    def filter(
        self,
        condition: str,
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        return self.build(lambda x: filter_statement(condition, x), child, source_plan)

    def sample(
        self,
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
        probability_fraction: Optional[float] = None,
        row_count: Optional[int] = None,
    ) -> SnowflakePlan:
        """Builds the sample part of the resultant sql statement"""
        return self.build(
            lambda x: sample_statement(
                x, probability_fraction=probability_fraction, row_count=row_count
            ),
            child,
            source_plan,
        )

    def sort(
        self,
        order: List[str],
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        return self.build(lambda x: sort_statement(order, x), child, source_plan)

    def set_operator(
        self,
        left: SnowflakePlan,
        right: SnowflakePlan,
        op: str,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        return self.build_binary(
            lambda x, y: set_operator_statement(x, y, op),
            left,
            right,
            source_plan,
        )

    def join(
        self,
        left: SnowflakePlan,
        right: SnowflakePlan,
        join_type: JoinType,
        join_condition: str,
        match_condition: str,
        source_plan: Optional[LogicalPlan],
        use_constant_subquery_alias: bool,
    ):
        return self.build_binary(
            lambda x, y: join_statement(
                x,
                y,
                join_type,
                join_condition,
                match_condition,
                use_constant_subquery_alias,
            ),
            left,
            right,
            source_plan,
        )

    def save_as_table(
        self,
        table_name: Iterable[str],
        column_names: Optional[Iterable[str]],
        mode: SaveMode,
        table_type: str,
        clustering_keys: Iterable[str],
        comment: Optional[str],
        child: SnowflakePlan,
    ) -> SnowflakePlan:
        full_table_name = ".".join(table_name)

        # here get the column definition from the child attributes. In certain cases we have
        # the attributes set to ($1, VariantType()) which cannot be used as valid column name
        # in save as table. So we rename ${number} with COL{number}.
        hidden_column_pattern = r"\"\$(\d+)\""
        column_definition_with_hidden_columns = attribute_to_schema_string(
            child.attributes
        )
        column_definition = re.sub(
            hidden_column_pattern,
            lambda match: f'"COL{match.group(1)}"',
            column_definition_with_hidden_columns,
        )

        child = child.replace_repeated_subquery_with_cte()

        def get_create_and_insert_plan(child: SnowflakePlan, replace=False, error=True):
            create_table = create_table_statement(
                full_table_name,
                column_definition,
                replace=replace,
                error=error,
                table_type=table_type,
                clustering_key=clustering_keys,
                comment=comment,
            )

            # so that dataframes created from non-select statements,
            # such as table sprocs, work
            child = self.add_result_scan_if_not_select(child)
            return SnowflakePlan(
                [
                    *child.queries[0:-1],
                    Query(create_table),
                    Query(
                        insert_into_statement(
                            table_name=full_table_name,
                            child=child.queries[-1].sql,
                            column_names=column_names,
                        ),
                        params=child.queries[-1].params,
                    ),
                ],
                create_table,
                child.post_actions,
                {},
                None,
                api_calls=child.api_calls,
                session=self.session,
            )

        if mode == SaveMode.APPEND:
            if self.session._table_exists(table_name):
                return self.build(
                    lambda x: insert_into_statement(
                        table_name=full_table_name,
                        child=x,
                        column_names=column_names,
                    ),
                    child,
                    None,
                )
            else:
                return get_create_and_insert_plan(child, replace=False, error=False)
        elif mode == SaveMode.TRUNCATE:
            if self.session._table_exists(table_name):
                return self.build(
                    lambda x: insert_into_statement(
                        full_table_name, x, [x.name for x in child.attributes], True
                    ),
                    child,
                    None,
                )
            else:
                return self.build(
                    lambda x: create_table_as_select_statement(
                        full_table_name,
                        x,
                        column_definition,
                        replace=True,
                        table_type=table_type,
                        clustering_key=clustering_keys,
                        comment=comment,
                    ),
                    child,
                    None,
                )
        elif mode == SaveMode.OVERWRITE:
            return self.build(
                lambda x: create_table_as_select_statement(
                    full_table_name,
                    x,
                    column_definition,
                    replace=True,
                    table_type=table_type,
                    clustering_key=clustering_keys,
                    comment=comment,
                ),
                child,
                None,
            )
        elif mode == SaveMode.IGNORE:
            return self.build(
                lambda x: create_table_as_select_statement(
                    full_table_name,
                    x,
                    column_definition,
                    error=False,
                    table_type=table_type,
                    clustering_key=clustering_keys,
                    comment=comment,
                ),
                child,
                None,
            )
        elif mode == SaveMode.ERROR_IF_EXISTS:
            return self.build(
                lambda x: create_table_as_select_statement(
                    full_table_name,
                    x,
                    column_definition,
                    table_type=table_type,
                    clustering_key=clustering_keys,
                    comment=comment,
                ),
                child,
                None,
            )

    def limit(
        self,
        limit_expr: str,
        offset_expr: str,
        child: SnowflakePlan,
        on_top_of_oder_by: bool,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        return self.build(
            lambda x: limit_statement(limit_expr, offset_expr, x, on_top_of_oder_by),
            child,
            source_plan,
        )

    def pivot(
        self,
        pivot_column: str,
        pivot_values: Optional[Union[str, List[str]]],
        aggregate: str,
        default_on_null: Optional[str],
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        return self.build(
            lambda x: pivot_statement(
                pivot_column, pivot_values, aggregate, default_on_null, x
            ),
            child,
            source_plan,
        )

    def unpivot(
        self,
        value_column: str,
        name_column: str,
        column_list: List[str],
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        return self.build(
            lambda x: unpivot_statement(value_column, name_column, column_list, x),
            child,
            source_plan,
        )

    def rename(
        self,
        column_map: Dict[str, str],
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        return self.build(
            lambda x: rename_statement(column_map, x),
            child,
            source_plan,
        )

    def create_or_replace_view(
        self, name: str, child: SnowflakePlan, is_temp: bool, comment: Optional[str]
    ) -> SnowflakePlan:
        if len(child.queries) != 1:
            raise SnowparkClientExceptionMessages.PLAN_CREATE_VIEW_FROM_DDL_DML_OPERATIONS()

        if not is_sql_select_statement(child.queries[0].sql.lower().strip()):
            raise SnowparkClientExceptionMessages.PLAN_CREATE_VIEWS_FROM_SELECT_ONLY()

        child = child.replace_repeated_subquery_with_cte()
        return self.build(
            lambda x: create_or_replace_view_statement(name, x, is_temp, comment),
            child,
            None,
        )

    def create_or_replace_dynamic_table(
        self,
        name: str,
        warehouse: str,
        lag: str,
        comment: Optional[str],
        child: SnowflakePlan,
    ) -> SnowflakePlan:
        if len(child.queries) != 1:
            raise SnowparkClientExceptionMessages.PLAN_CREATE_DYNAMIC_TABLE_FROM_DDL_DML_OPERATIONS()

        if not is_sql_select_statement(child.queries[0].sql.lower().strip()):
            raise SnowparkClientExceptionMessages.PLAN_CREATE_DYNAMIC_TABLE_FROM_SELECT_ONLY()

        child = child.replace_repeated_subquery_with_cte()
        return self.build(
            lambda x: create_or_replace_dynamic_table_statement(
                name, warehouse, lag, comment, x
            ),
            child,
            None,
        )

    def create_temp_table(
        self,
        name: str,
        child: SnowflakePlan,
        *,
        use_scoped_temp_objects: bool = False,
        is_generated: bool = False,
    ) -> SnowflakePlan:
        child = child.replace_repeated_subquery_with_cte()
        return self.build_from_multiple_queries(
            lambda x: self.create_table_and_insert(
                self.session,
                name,
                child.schema_query,
                x,
                use_scoped_temp_objects=use_scoped_temp_objects,
                is_generated=is_generated,
            ),
            child,
            None,
            child.schema_query,
            is_ddl_on_temp_object=True,
        )

    def create_table_and_insert(
        self,
        session,
        name: str,
        schema_query: str,
        query: "Query",
        *,
        use_scoped_temp_objects: bool = False,
        is_generated: bool = False,
    ) -> List["Query"]:
        attributes = session._get_result_attributes(schema_query)
        create_table = create_table_statement(
            name,
            attribute_to_schema_string(attributes),
            table_type="temporary",
            use_scoped_temp_objects=use_scoped_temp_objects,
            is_generated=is_generated,
        )

        return [
            Query(create_table),
            Query(
                insert_into_statement(
                    table_name=name, column_names=None, child=query.sql
                ),
                params=query.params,
            ),
        ]

    def read_file(
        self,
        path: str,
        format: str,
        options: Dict[str, str],
        schema: List[Attribute],
        schema_to_cast: Optional[List[Tuple[str, str]]] = None,
        transformations: Optional[List[str]] = None,
        metadata_project: Optional[List[str]] = None,
        metadata_schema: Optional[List[Attribute]] = None,
    ):
        format_type_options, copy_options = get_copy_into_table_options(options)
        pattern = options.get("PATTERN")
        # Can only infer the schema for parquet, orc and avro
        # csv and json in preview
        infer_schema = (
            options.get("INFER_SCHEMA", True)
            if format in INFER_SCHEMA_FORMAT_TYPES
            else False
        )
        # tracking usage of pattern, will refactor this function in future
        if pattern:
            self.session._conn._telemetry_client.send_copy_pattern_telemetry()

        if format_type_options.get("PARSE_HEADER", False):
            # This option is only available for CSV file format
            # The options is used when specified with INFER_SCHEMA( ..., FILE_FORMAT => (.., PARSE_HEADER)) see
            # https://docs.snowflake.com/en/sql-reference/sql/create-file-format#format-type-options-formattypeoptions
            # PARSE_HEADER does not work with FILE_FORMAT when used with SELECT FROM LOCATION(FILE_FORMAT ...). Thus,
            # if user has set option("PARSE_HEADER", True), we have already read the header in
            # DataframeReader._infer_schema_for_file_format so now we must set skip_header = 1 to skip the header line.
            format_type_options["SKIP_HEADER"] = 1
        format_type_options.pop("PARSE_HEADER", None)

        if not copy_options:  # use select
            queries: List[Query] = []
            post_queries: List[Query] = []
            use_temp_file_format: bool = "FORMAT_NAME" not in options
            if use_temp_file_format:
                format_name = self.session.get_fully_qualified_name_if_possible(
                    random_name_for_temp_object(TempObjectType.FILE_FORMAT)
                )
                queries.append(
                    Query(
                        create_file_format_statement(
                            format_name,
                            format,
                            format_type_options,
                            temp=True,
                            if_not_exist=True,
                            use_scoped_temp_objects=self.session._use_scoped_temp_objects,
                            is_generated=True,
                        ),
                        is_ddl_on_temp_object=True,
                    )
                )
                post_queries.append(
                    Query(
                        drop_file_format_if_exists_statement(format_name),
                        is_ddl_on_temp_object=True,
                    )
                )
            else:
                format_name = options["FORMAT_NAME"]

            if infer_schema:
                assert schema_to_cast is not None
                schema_project: List[str] = schema_cast_named(schema_to_cast)
            else:
                schema_project: List[str] = schema_cast_seq(schema)

            queries.append(
                Query(
                    select_from_path_with_format_statement(
                        (metadata_project or []) + schema_project,
                        path,
                        format_name,
                        pattern,
                    )
                )
            )

            return SnowflakePlan(
                queries,
                schema_value_statement((metadata_schema or []) + schema),
                post_queries,
                {},
                None,
                session=self.session,
            )
        else:  # otherwise use COPY
            if "FORCE" in copy_options and str(copy_options["FORCE"]).lower() != "true":
                raise SnowparkClientExceptionMessages.PLAN_COPY_DONT_SUPPORT_SKIP_LOADED_FILES(
                    copy_options["FORCE"]
                )

            # set force to true.
            # it is useless since we always create new temp table.
            # setting it helps users to understand generated queries.

            copy_options_with_force = {**copy_options, "FORCE": True}

            # If we have inferred the schema, we want to use those column names
            temp_table_schema = (
                schema
                if infer_schema
                else [
                    Attribute(f'"COL{index}"', att.datatype, att.nullable)
                    for index, att in enumerate(schema)
                ]
            )

            temp_table_name = self.session.get_fully_qualified_name_if_possible(
                random_name_for_temp_object(TempObjectType.TABLE)
            )
            queries = [
                Query(
                    create_table_statement(
                        temp_table_name,
                        attribute_to_schema_string(temp_table_schema),
                        replace=True,
                        table_type="temporary",
                        use_scoped_temp_objects=self.session._use_scoped_temp_objects,
                        is_generated=True,
                    ),
                    is_ddl_on_temp_object=True,
                ),
                Query(
                    copy_into_table(
                        temp_table_name,
                        path,
                        format,
                        format_type_options,
                        copy_options_with_force,
                        pattern,
                        transformations=transformations,
                    )
                ),
                Query(
                    project_statement(
                        [
                            f"{new_att.name} AS {input_att.name}"
                            for new_att, input_att in zip(temp_table_schema, schema)
                        ],
                        temp_table_name,
                    )
                ),
            ]

            post_actions = [
                Query(
                    drop_table_if_exists_statement(temp_table_name),
                    is_ddl_on_temp_object=True,
                )
            ]
            return SnowflakePlan(
                queries,
                schema_value_statement(schema),
                post_actions,
                {},
                None,
                session=self.session,
            )

    def copy_into_table(
        self,
        file_format: str,
        table_name: Iterable[str],
        path: str,
        files: Optional[str] = None,
        pattern: Optional[str] = None,
        validation_mode: Optional[str] = None,
        column_names: Optional[List[str]] = None,
        transformations: Optional[List[str]] = None,
        user_schema: Optional[StructType] = None,
        create_table_from_infer_schema: bool = False,
        *,
        copy_options: Dict[str, Any],
        format_type_options: Dict[str, Any],
    ) -> SnowflakePlan:
        # tracking usage of pattern, will refactor this function in future
        if pattern:
            self.session._conn._telemetry_client.send_copy_pattern_telemetry()

        full_table_name = ".".join(table_name)
        copy_command = copy_into_table(
            table_name=full_table_name,
            file_path=path,
            files=files,
            file_format_type=file_format,
            format_type_options=format_type_options,
            copy_options=copy_options,
            pattern=pattern,
            validation_mode=validation_mode,
            column_names=column_names,
            transformations=transformations,
        )
        if self.session._table_exists(table_name):
            queries = [Query(copy_command)]
        elif user_schema and (
            (file_format.upper() == "CSV" and not transformations)
            or (
                create_table_from_infer_schema
                and file_format.upper() in INFER_SCHEMA_FORMAT_TYPES
            )
        ):
            attributes = user_schema._to_attributes()
            queries = [
                Query(
                    create_table_statement(
                        full_table_name,
                        attribute_to_schema_string(attributes),
                    ),
                    # This is an exception. The principle is to avoid surprising behavior and most of the time
                    # it applies to temp object. But this perm table creation is also one place where we create
                    # table on behalf of the user automatically.
                    is_ddl_on_temp_object=True,
                ),
                Query(copy_command),
            ]
        else:
            raise SnowparkClientExceptionMessages.DF_COPY_INTO_CANNOT_CREATE_TABLE(
                full_table_name
            )
        return SnowflakePlan(queries, copy_command, [], {}, None, session=self.session)

    def copy_into_location(
        self,
        query: SnowflakePlan,
        stage_location: str,
        partition_by: Optional[str] = None,
        file_format_name: Optional[str] = None,
        file_format_type: Optional[str] = None,
        format_type_options: Optional[Dict[str, Any]] = None,
        header: bool = False,
        **copy_options: Optional[Any],
    ) -> SnowflakePlan:
        query = query.replace_repeated_subquery_with_cte()
        return self.build(
            lambda x: copy_into_location(
                query=x,
                stage_location=stage_location,
                partition_by=partition_by,
                file_format_name=file_format_name,
                file_format_type=file_format_type,
                format_type_options=format_type_options,
                header=header,
                **copy_options,
            ),
            query,
            None,
            query.schema_query,
        )

    def update(
        self,
        table_name: str,
        assignments: Dict[str, str],
        condition: Optional[str],
        source_data: Optional[SnowflakePlan],
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        if source_data:
            source_data = source_data.replace_repeated_subquery_with_cte()
            return self.build(
                lambda x: update_statement(
                    table_name,
                    assignments,
                    condition,
                    x,
                ),
                source_data,
                source_plan,
            )
        else:
            return self.query(
                update_statement(
                    table_name,
                    assignments,
                    condition,
                    None,
                ),
                source_plan,
            )

    def delete(
        self,
        table_name: str,
        condition: Optional[str],
        source_data: Optional[SnowflakePlan],
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        if source_data:
            source_data = source_data.replace_repeated_subquery_with_cte()
            return self.build(
                lambda x: delete_statement(
                    table_name,
                    condition,
                    x,
                ),
                source_data,
                source_plan,
            )
        else:
            return self.query(
                delete_statement(
                    table_name,
                    condition,
                    None,
                ),
                source_plan,
            )

    def merge(
        self,
        table_name: str,
        source_data: SnowflakePlan,
        join_expr: str,
        clauses: List[str],
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        source_data = source_data.replace_repeated_subquery_with_cte()
        return self.build(
            lambda x: merge_statement(table_name, x, join_expr, clauses),
            source_data,
            source_plan,
        )

    def lateral(
        self,
        table_function: str,
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        return self.build(
            lambda x: lateral_statement(table_function, x),
            child,
            source_plan,
        )

    def from_table_function(
        self, func: str, source_plan: TableFunctionRelation
    ) -> SnowflakePlan:
        if isinstance(source_plan.table_function, GeneratorTableFunction):
            return self.query(
                table_function_statement(func, source_plan.table_function.operators),
                source_plan,
            )
        return self.query(table_function_statement(func), None)

    def join_table_function(
        self,
        func: str,
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
        left_cols: List[str],
        right_cols: List[str],
        use_constant_subquery_alias: bool,
    ) -> SnowflakePlan:
        return self.build(
            lambda x: join_table_function_statement(
                func, x, left_cols, right_cols, use_constant_subquery_alias
            ),
            child,
            source_plan,
        )

    def select_statement(self, selectable: "Selectable") -> SnowflakePlan:
        return selectable.snowflake_plan

    def add_result_scan_if_not_select(self, plan: SnowflakePlan) -> SnowflakePlan:
        if isinstance(plan.source_plan, SetOperation):
            return plan
        elif is_sql_select_statement(plan.queries[-1].sql):
            return plan
        else:
            new_queries = plan.queries + [
                Query(
                    result_scan_statement(plan.queries[-1].query_id_place_holder),
                )
            ]
            return SnowflakePlan(
                new_queries,
                schema_value_statement(plan.attributes),
                plan.post_actions,
                plan.expr_to_alias,
                plan.source_plan,
                api_calls=plan.api_calls,
                session=self.session,
            )


class Query:
    def __init__(
        self,
        sql: str,
        *,
        query_id_place_holder: Optional[str] = None,
        is_ddl_on_temp_object: bool = False,
        params: Optional[Sequence[Any]] = None,
    ) -> None:
        self.sql = sql
        self.query_id_place_holder = (
            query_id_place_holder
            if query_id_place_holder
            else f"query_id_place_holder_{generate_random_alphanumeric()}"
        )
        self.is_ddl_on_temp_object = is_ddl_on_temp_object
        self.params = params or []

    def __repr__(self) -> str:
        return (
            "Query("
            + f"{self.sql!r}, "
            + f"query_id_place_holder={self.query_id_place_holder!r}, "
            + f"is_ddl_on_temp_object={self.is_ddl_on_temp_object}, "
            + f"params={self.params}"
            + ")"
        )

    def __eq__(self, other: "Query") -> bool:
        return (
            self.sql == other.sql
            and self.query_id_place_holder == other.query_id_place_holder
            and self.is_ddl_on_temp_object == other.is_ddl_on_temp_object
        )


class BatchInsertQuery(Query):
    def __init__(
        self,
        sql: str,
        rows: Optional[List[Row]] = None,
    ) -> None:
        super().__init__(sql)
        self.rows = rows
