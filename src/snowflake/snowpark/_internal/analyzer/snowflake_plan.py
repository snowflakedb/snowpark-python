#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import re
import sys
import uuid
from functools import cached_property, reduce
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional, Tuple

if TYPE_CHECKING:
    from snowflake.snowpark._internal.analyzer.select_statement import Selectable

import snowflake.connector
import snowflake.snowpark
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    aggregate_statement,
    attribute_to_schema_string,
    batch_insert_into_statement,
    copy_into_location,
    copy_into_table,
    create_file_format_statement,
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
                    tb = sys.exc_info()[2]
                    if "unexpected 'as'" in e.msg.lower():
                        ne = (
                            SnowparkClientExceptionMessages.SQL_PYTHON_REPORT_UNEXPECTED_ALIAS()
                        )
                        raise ne.with_traceback(tb) from None
                    elif e.sqlstate == "42000" and "invalid identifier" in e.msg:
                        match = (
                            SnowflakePlan.Decorator.__wrap_exception_regex_match.match(
                                e.msg
                            )
                        )
                        if not match:
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
                                orig_col_name
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
                                col, col
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
        session: Optional["snowflake.snowpark.session.Session"] = None,
        source_plan: Optional[LogicalPlan] = None,
        is_ddl_on_temp_object: bool = False,
        api_calls: Optional[List[Dict]] = None,
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
        )

    @cached_property
    def attributes(self) -> List[Attribute]:
        output = analyze_attributes(self.schema_query, self.session)
        self.schema_query = schema_value_statement(output)
        return output

    @cached_property
    def output(self) -> List[Attribute]:
        return [Attribute(a.name, a.datatype, a.nullable) for a in self.attributes]

    def __copy__(self) -> "SnowflakePlan":
        return SnowflakePlan(
            self.queries.copy() if self.queries else [],
            self.schema_query,
            self.post_actions.copy() if self.post_actions else None,
            dict(self.expr_to_alias) if self.expr_to_alias else None,
            self.session,
            self.source_plan,
            self.is_ddl_on_temp_object,
            self.api_calls.copy() if self.api_calls else None,
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
            )
        ]
        new_schema_query = (
            schema_query if schema_query else sql_generator(child.schema_query)
        )

        return SnowflakePlan(
            queries,
            new_schema_query,
            select_child.post_actions,
            select_child.expr_to_alias,
            self.session,
            source_plan,
            is_ddl_on_temp_object,
            api_calls=select_child.api_calls,
        )

    @SnowflakePlan.Decorator.wrap_exception
    def build_from_multiple_queries(
        self,
        multi_sql_generator: Callable[[str], str],
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
        schema_query: Optional[str] = None,
        is_ddl_on_temp_object: bool = False,
    ) -> SnowflakePlan:
        select_child = self.add_result_scan_if_not_select(child)
        queries = select_child.queries[0:-1] + [
            Query(msg, is_ddl_on_temp_object=is_ddl_on_temp_object)
            for msg in multi_sql_generator(select_child.queries[-1].sql)
        ]
        new_schema_query = (
            schema_query
            if schema_query is not None
            else multi_sql_generator(child.schema_query)[-1]
        )

        return SnowflakePlan(
            queries,
            new_schema_query,
            select_child.post_actions,
            select_child.expr_to_alias,
            self.session,
            source_plan,
            api_calls=select_child.api_calls,
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
                    None,
                )
            ]
        )

        left_schema_query = schema_value_statement(select_left.attributes)
        right_schema_query = schema_value_statement(select_right.attributes)
        schema_query = sql_generator(left_schema_query, right_schema_query)

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
            self.session,
            source_plan,
            api_calls=api_calls,
        )

    def query(
        self,
        sql: str,
        source_plan: Optional[LogicalPlan],
        api_calls: Optional[List[Dict]] = None,
    ) -> SnowflakePlan:
        return SnowflakePlan(
            queries=[Query(sql)],
            schema_query=sql,
            session=self.session,
            source_plan=source_plan,
            api_calls=api_calls,
        )

    def large_local_relation_plan(
        self,
        output: List[Attribute],
        data: List[Row],
        source_plan: Optional[LogicalPlan],
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
            temp_table_name, [attr.name for attr in attributes]
        )
        select_stmt = project_statement([], temp_table_name)
        drop_table_stmt = drop_table_if_exists_statement(temp_table_name)
        schema_query = schema_value_statement(attributes)
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
        self, condition: str, child: SnowflakePlan, source_plan: Optional[LogicalPlan]
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
        self, order: List[str], child: SnowflakePlan, source_plan: Optional[LogicalPlan]
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

    def union(
        self, children: List[SnowflakePlan], source_plan: Optional[LogicalPlan]
    ) -> SnowflakePlan:
        return reduce(
            lambda x, y: self.set_operator(x, y, "UNION ALL ", source_plan), children
        )

    def join(
        self,
        left: SnowflakePlan,
        right: SnowflakePlan,
        join_type: JoinType,
        condition: str,
        source_plan: Optional[LogicalPlan],
    ):
        return self.build_binary(
            lambda x, y: join_statement(x, y, join_type, condition),
            left,
            right,
            source_plan,
        )

    def save_as_table(
        self,
        table_name: str,
        column_names: Optional[Iterable[str]],
        mode: SaveMode,
        table_type: str,
        child: SnowflakePlan,
    ) -> SnowflakePlan:
        if mode == SaveMode.APPEND:
            if self.session._table_exists(table_name):
                return self.build(
                    lambda x: insert_into_statement(
                        table_name=table_name,
                        child=x,
                        column_names=column_names,
                    ),
                    child,
                    None,
                )
            else:
                create_table = create_table_statement(
                    table_name,
                    attribute_to_schema_string(child.attributes),
                    error=False,
                    table_type=table_type,
                )

                return SnowflakePlan(
                    [
                        *child.queries[0:-1],
                        Query(create_table),
                        Query(
                            insert_into_statement(
                                table_name=table_name,
                                child=child.queries[-1].sql,
                                column_names=column_names,
                            )
                        ),
                    ],
                    create_table,
                    child.post_actions,
                    {},
                    self.session,
                    None,
                    api_calls=child.api_calls,
                )
        elif mode == SaveMode.OVERWRITE:
            return self.build(
                lambda x: create_table_as_select_statement(
                    table_name, x, replace=True, table_type=table_type
                ),
                child,
                None,
            )
        elif mode == SaveMode.IGNORE:
            return self.build(
                lambda x: create_table_as_select_statement(
                    table_name, x, error=False, table_type=table_type
                ),
                child,
                None,
            )
        elif mode == SaveMode.ERROR_IF_EXISTS:
            return self.build(
                lambda x: create_table_as_select_statement(
                    table_name, x, table_type=table_type
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
        pivot_values: List[str],
        aggregate: str,
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        return self.build(
            lambda x: pivot_statement(pivot_column, pivot_values, aggregate, x),
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

    def create_or_replace_view(
        self, name: str, child: SnowflakePlan, is_temp: bool
    ) -> SnowflakePlan:
        if len(child.queries) != 1:
            raise SnowparkClientExceptionMessages.PLAN_CREATE_VIEW_FROM_DDL_DML_OPERATIONS()

        if not is_sql_select_statement(child.queries[0].sql.lower().strip()):
            raise SnowparkClientExceptionMessages.PLAN_CREATE_VIEWS_FROM_SELECT_ONLY()

        return self.build(
            lambda x: create_or_replace_view_statement(name, x, is_temp),
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
        query: str,
        *,
        use_scoped_temp_objects: bool = False,
        is_generated: bool = False,
    ) -> List[str]:
        attributes = session._get_result_attributes(schema_query)
        create_table = create_table_statement(
            name,
            attribute_to_schema_string(attributes),
            table_type="temporary",
            use_scoped_temp_objects=use_scoped_temp_objects,
            is_generated=is_generated,
        )

        return [
            create_table,
            insert_into_statement(table_name=name, column_names=None, child=query),
        ]

    def read_file(
        self,
        path: str,
        format: str,
        options: Dict[str, str],
        fully_qualified_schema: str,
        schema: List[Attribute],
        schema_to_cast: Optional[List[Tuple[str, str]]] = None,
        transformations: Optional[List[str]] = None,
    ):
        format_type_options, copy_options = get_copy_into_table_options(options)
        pattern = options.get("PATTERN", None)
        # Can only infer the schema for parquet, orc and avro
        infer_schema = (
            options.get("INFER_SCHEMA", True)
            if format in INFER_SCHEMA_FORMAT_TYPES
            else False
        )
        # tracking usage of pattern, will refactor this function in future
        if pattern:
            self.session._conn._telemetry_client.send_copy_pattern_telemetry()

        if not copy_options:  # use select
            queries: List[Query] = []
            post_queries: List[Query] = []
            use_temp_file_format: bool = "FORMAT_NAME" not in options
            if use_temp_file_format:
                format_name = (
                    fully_qualified_schema
                    + "."
                    + random_name_for_temp_object(TempObjectType.FILE_FORMAT)
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

            queries.append(
                Query(
                    select_from_path_with_format_statement(
                        schema_cast_named(schema_to_cast)
                        if infer_schema
                        else schema_cast_seq(schema),
                        path,
                        format_name,
                        pattern,
                    )
                )
            )
            return SnowflakePlan(
                queries,
                schema_value_statement(schema),
                post_queries,
                {},
                self.session,
                None,
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

            temp_table_name = (
                fully_qualified_schema
                + "."
                + random_name_for_temp_object(TempObjectType.TABLE)
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
                self.session,
                None,
            )

    def copy_into_table(
        self,
        file_format: str,
        table_name: str,
        path: Optional[str] = None,
        files: Optional[str] = None,
        pattern: Optional[str] = None,
        format_type_options: Optional[Dict[str, Any]] = None,
        copy_options: Optional[Dict[str, Any]] = None,
        validation_mode: Optional[str] = None,
        column_names: Optional[List[str]] = None,
        transformations: Optional[List[str]] = None,
        user_schema: Optional[StructType] = None,
        create_table_from_infer_schema: bool = False,
    ) -> SnowflakePlan:
        # tracking usage of pattern, will refactor this function in future
        if pattern:
            self.session._conn._telemetry_client.send_copy_pattern_telemetry()

        copy_command = copy_into_table(
            table_name=table_name,
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
                        table_name,
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
                table_name
            )
        return SnowflakePlan(queries, copy_command, [], {}, self.session, None)

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

    def from_table_function(self, func: str) -> SnowflakePlan:
        return self.query(table_function_statement(func), None)

    def join_table_function(
        self, func: str, child: SnowflakePlan, source_plan: Optional[LogicalPlan]
    ) -> SnowflakePlan:
        return self.build(
            lambda x: join_table_function_statement(func, x),
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
                    None,
                )
            ]
            return SnowflakePlan(
                new_queries,
                schema_value_statement(plan.attributes),
                plan.post_actions,
                plan.expr_to_alias,
                self.session,
                plan.source_plan,
                api_calls=plan.api_calls,
            )


class Query:
    def __init__(
        self,
        sql: str,
        query_id_place_holder: Optional[str] = None,
        is_ddl_on_temp_object: bool = False,
    ) -> None:
        self.sql = sql
        self.query_id_place_holder = (
            query_id_place_holder
            if query_id_place_holder
            else f"query_id_place_holder_{generate_random_alphanumeric()}"
        )
        self.is_ddl_on_temp_object = is_ddl_on_temp_object

    def __repr__(self) -> str:
        return f"Query({self.sql}, {self.query_id_place_holder}, {self.is_ddl_on_temp_object})"


class BatchInsertQuery(Query):
    def __init__(
        self,
        sql: str,
        rows: Optional[List[Row]] = None,
    ) -> None:
        super().__init__(sql)
        self.rows = rows
