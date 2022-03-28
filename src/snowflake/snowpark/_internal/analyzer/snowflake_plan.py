#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import re
from functools import reduce
from typing import Any, Callable, Dict, List, Optional

import snowflake.connector
import snowflake.snowpark.dataframe
from snowflake.snowpark import Column
from snowflake.snowpark._internal.analyzer.analyzer_package import AnalyzerPackage
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.plans.logical.basic_logical_operators import (
    SetOperation,
)
from snowflake.snowpark._internal.plans.logical.logical_plan import (
    LeafNode,
    LogicalPlan,
)
from snowflake.snowpark._internal.sp_expressions import (
    Attribute,
    Expression as SPExpression,
)
from snowflake.snowpark._internal.utils import (
    SchemaUtils,
    TempObjectType,
    Utils,
    _SaveMode,
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
                    if "unexpected 'as'" in e.msg.lower():
                        raise SnowparkClientExceptionMessages.SQL_PYTHON_REPORT_UNEXPECTED_ALIAS() from e
                    elif e.sqlstate == "42000" and "invalid identifier" in e.msg:
                        match = (
                            SnowflakePlan.Decorator.__wrap_exception_regex_match.match(
                                e.msg
                            )
                        )
                        if not match:
                            raise e
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
                                snowflake.snowpark.dataframe.DataFrame.get_unaliased(
                                    col
                                )
                            )
                            orig_col_name = (
                                unaliased_cols[0] if unaliased_cols else "<colname>"
                            )
                            raise SnowparkClientExceptionMessages.SQL_PYTHON_REPORT_INVALID_ID(
                                orig_col_name
                            ) from e
                        elif (
                            len(
                                [
                                    unaliased
                                    for item in remapped
                                    for unaliased in snowflake.snowpark.dataframe.DataFrame.get_unaliased(
                                        item
                                    )
                                    if unaliased == col
                                ]
                            )
                            > 1
                        ):
                            raise SnowparkClientExceptionMessages.SQL_PYTHON_REPORT_JOIN_AMBIGUOUS(
                                col, col
                            ) from e
                        else:
                            raise e
                    else:
                        raise e

            return wrap

    # for read_file()
    __copy_option = {
        "ON_ERROR",
        "SIZE_LIMIT",
        "PURGE",
        "RETURN_FAILED_ONLY",
        "MATCH_BY_COLUMN_NAME",
        "ENFORCE_LENGTH",
        "TRUNCATECOLUMNS",
        "FORCE",
        "LOAD_UNCERTAIN_FILES",
    }

    def __init__(
        self,
        queries,
        schema_query,
        post_actions=None,
        expr_to_alias=None,
        session=None,
        source_plan=None,
        is_ddl_on_temp_object=False,
    ):
        super().__init__()
        self.queries: List[Query] = queries
        self._schema_query: str = schema_query
        self.post_actions = post_actions if post_actions else []
        self.expr_to_alias = expr_to_alias if expr_to_alias else {}
        self.session = session
        self.source_plan = source_plan
        self.is_ddl_on_temp_object = is_ddl_on_temp_object

        # use this to simulate scala's lazy val
        self.__placeholder_for_attributes = None
        self.__placeholder_for_output = None

    # TODO
    @Decorator.wrap_exception
    def analyze_if_needed(self):
        pass

    def with_subqueries(self, subquery_plans: List["SnowflakePlan"]) -> "SnowflakePlan":
        pre_queries = self.queries[:-1]
        new_schema_query = self._schema_query
        new_post_actions = [*self.post_actions]

        for plan in subquery_plans:
            for query in plan.queries[:-1]:
                if query not in pre_queries:
                    pre_queries.append(query)
            new_schema_query = new_schema_query.replace(
                plan.queries[-1].sql, plan._schema_query
            )
            for action in plan.post_actions:
                if action not in new_post_actions:
                    new_post_actions.append(action)

        return SnowflakePlan(
            pre_queries + [self.queries[-1]],
            new_schema_query,
            post_actions=new_post_actions,
            expr_to_alias=self.expr_to_alias,
            session=self.session,
            source_plan=self.source_plan,
        )

    def attributes(self) -> List[Attribute]:
        if not self.__placeholder_for_attributes:
            output = SchemaUtils.analyze_attributes(self._schema_query, self.session)
            pkg = AnalyzerPackage()
            self._schema_query = pkg.schema_value_statement(output)
            self.__placeholder_for_attributes = output
        return self.__placeholder_for_attributes

    # Convert to 'Spark' AttributeReference
    def output(self) -> List[Attribute]:
        if not self.__placeholder_for_output:
            self.__placeholder_for_output = [
                Attribute(a.name, a.datatype, a.nullable) for a in self.attributes()
            ]
        return self.__placeholder_for_output

    def __copy__(self) -> "SnowflakePlan":
        return SnowflakePlan(
            self.queries.copy() if self.queries else [],
            self._schema_query,
            self.post_actions.copy() if self.post_actions else None,
            dict(self.expr_to_alias) if self.expr_to_alias else None,
            self.session,
            self.source_plan,
        )

    def add_aliases(self, to_add: Dict):
        self.expr_to_alias = {**self.expr_to_alias, **to_add}


class SnowflakePlanBuilder:
    CopyOption = {
        "ON_ERROR",
        "SIZE_LIMIT",
        "PURGE",
        "RETURN_FAILED_ONLY",
        "MATCH_BY_COLUMN_NAME",
        "ENFORCE_LENGTH",
        "TRUNCATECOLUMNS",
        "FORCE",
        "LOAD_UNCERTAIN_FILES",
    }

    def __init__(self, session):
        self.__session = session
        self.pkg = AnalyzerPackage()

    @SnowflakePlan.Decorator.wrap_exception
    def build(
        self,
        sql_generator,
        child,
        source_plan,
        schema_query=None,
        is_ddl_on_temp_object=False,
    ):
        select_child = self._add_result_scan_if_not_select(child)
        queries = select_child.queries[:-1] + [
            Query(
                sql_generator(select_child.queries[-1].sql),
                query_id_place_holder="",
                is_ddl_on_temp_object=is_ddl_on_temp_object,
            )
        ]
        new_schema_query = (
            schema_query if schema_query else sql_generator(child._schema_query)
        )

        return SnowflakePlan(
            queries,
            new_schema_query,
            select_child.post_actions,
            select_child.expr_to_alias,
            self.__session,
            source_plan,
            is_ddl_on_temp_object,
        )

    @SnowflakePlan.Decorator.wrap_exception
    def build_from_multiple_queries(
        self,
        multi_sql_generator,
        child,
        source_plan,
        schema_query=None,
        is_ddl_on_temp_object=False,
    ):
        select_child = self._add_result_scan_if_not_select(child)
        queries = select_child.queries[0:-1] + [
            Query(msg, is_ddl_on_temp_object=is_ddl_on_temp_object)
            for msg in multi_sql_generator(select_child.queries[-1].sql)
        ]
        new_schema_query = (
            schema_query
            if schema_query is not None
            else multi_sql_generator(child._schema_query)[-1]
        )

        return SnowflakePlan(
            queries,
            new_schema_query,
            select_child.post_actions,
            select_child.expr_to_alias,
            self.__session,
            source_plan,
        )

    @SnowflakePlan.Decorator.wrap_exception
    def __build_binary(
        self,
        sql_generator: Callable[[str, str], str],
        left: SnowflakePlan,
        right: SnowflakePlan,
        source_plan: LogicalPlan,
    ):
        select_left = self._add_result_scan_if_not_select(left)
        select_right = self._add_result_scan_if_not_select(right)
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

        left_schema_query = self.pkg.schema_value_statement(select_left.attributes())
        right_schema_query = self.pkg.schema_value_statement(select_right.attributes())
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

        return SnowflakePlan(
            queries,
            schema_query,
            select_left.post_actions + select_right.post_actions,
            new_expr_to_alias,
            self.__session,
            source_plan,
        )

    def query(self, sql, source_plan):
        """
        :rtype: SnowflakePlan
        """
        return SnowflakePlan(
            queries=[Query(sql, None)],
            schema_query=sql,
            session=self.__session,
            source_plan=source_plan,
        )

    def large_local_relation_plan(
        self,
        output: List[Attribute],
        data: List[Row],
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        temp_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        attributes = [
            Attribute(attr.name, attr.datatype, attr.nullable) for attr in output
        ]
        create_table_stmt = self.pkg.create_temp_table_statement(
            temp_table_name, self.pkg.attribute_to_schema_string(attributes)
        )
        insert_stmt = self.pkg.batch_insert_into_statement(
            temp_table_name, [attr.name for attr in attributes]
        )
        select_stmt = self.pkg.project_statement([], temp_table_name)
        drop_table_stmt = self.pkg.drop_table_if_exists_statement(temp_table_name)
        schema_query = self.pkg.schema_value_statement(attributes)
        queries = [
            Query(create_table_stmt, is_ddl_on_temp_object=True),
            BatchInsertQuery(insert_stmt, data),
            Query(select_stmt),
        ]
        return SnowflakePlan(
            queries=queries,
            schema_query=schema_query,
            post_actions=[Query(drop_table_stmt, is_ddl_on_temp_object=True)],
            session=self.__session,
            source_plan=source_plan,
        )

    def table(self, table_name):
        return self.query(self.pkg.project_statement([], table_name), None)

    def file_operation_plan(
        self, command, file_name, stage_location, options: Dict[str, str]
    ) -> SnowflakePlan:
        return self.query(
            self.pkg.file_operation_statement(
                command, file_name, stage_location, options
            ),
            None,
        )

    def project(self, project_list, child, source_plan, is_distinct=False):
        return self.build(
            lambda x: self.pkg.project_statement(
                project_list, x, is_distinct=is_distinct
            ),
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
            lambda x: self.pkg.aggregate_statement(grouping_exprs, aggregate_exprs, x),
            child,
            source_plan,
        )

    def filter(self, condition, child, source_plan):
        return self.build(
            lambda x: self.pkg.filter_statement(condition, x), child, source_plan
        )

    def sample(
        self,
        child: SnowflakePlan,
        source_plan: LogicalPlan,
        probability_fraction: Optional[float] = None,
        row_count: Optional[int] = None,
    ):
        """Builds the sample part of the resultant sql statement"""
        return self.build(
            lambda x: self.pkg.sample_statement(
                x, probability_fraction=probability_fraction, row_count=row_count
            ),
            child,
            source_plan,
        )

    def sort(
        self, order: List[str], child: SnowflakePlan, source_plan: Optional[LogicalPlan]
    ):
        return self.build(
            lambda x: self.pkg.sort_statement(order, x), child, source_plan
        )

    def set_operator(
        self,
        left: SnowflakePlan,
        right: SnowflakePlan,
        op: str,
        source_plan: Optional[LogicalPlan],
    ):
        return self.__build_binary(
            lambda x, y: self.pkg.set_operator_statement(x, y, op),
            left,
            right,
            source_plan,
        )

    def union(self, children: List[SnowflakePlan], source_plan: Optional[LogicalPlan]):
        func = lambda x, y: self.set_operator(x, y, "UNION ALL ", source_plan)
        return reduce(func, children)

    def join(self, left, right, join_type, condition, source_plan):
        return self.__build_binary(
            lambda x, y: self.pkg.join_statement(x, y, join_type, condition),
            left,
            right,
            source_plan,
        )

    def save_as_table(
        self,
        table_name: str,
        mode: _SaveMode,
        create_temp_table: bool,
        child: SnowflakePlan,
    ) -> SnowflakePlan:
        if mode == _SaveMode.APPEND:
            create_table = self.pkg.create_table_statement(
                table_name,
                self.pkg.attribute_to_schema_string(child.attributes()),
                error=False,
                temp=create_temp_table,
            )
            return SnowflakePlan(
                [
                    *child.queries[0:-1],
                    Query(create_table),
                    Query(
                        self.pkg.insert_into_statement(
                            table_name, child.queries[-1].sql
                        )
                    ),
                ],
                create_table,
                child.post_actions,
                {},
                self.__session,
                None,
            )
        elif mode == _SaveMode.OVERWRITE:
            return self.build(
                lambda x: self.pkg.create_table_as_select_statement(
                    table_name, x, replace=True, temp=create_temp_table
                ),
                child,
                None,
            )
        elif mode == _SaveMode.IGNORE:
            return self.build(
                lambda x: self.pkg.create_table_as_select_statement(
                    table_name, x, error=False, temp=create_temp_table
                ),
                child,
                None,
            )
        elif mode == _SaveMode.ERROR_IF_EXISTS:
            return self.build(
                lambda x: self.pkg.create_table_as_select_statement(
                    table_name, x, temp=create_temp_table
                ),
                child,
                None,
            )

    def limit(
        self,
        limit_expr: str,
        child: SnowflakePlan,
        on_top_of_oder_by: bool,
        source_plan: Optional[LogicalPlan],
    ):
        return self.build(
            lambda x: self.pkg.limit_statement(limit_expr, x, on_top_of_oder_by),
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
            lambda x: self.pkg.pivot_statement(
                pivot_column, pivot_values, aggregate, x
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
            lambda x: self.pkg.unpivot_statement(
                value_column, name_column, column_list, x
            ),
            child,
            source_plan,
        )

    def create_or_replace_view(
        self, name: str, child: SnowflakePlan, is_temp: bool
    ) -> SnowflakePlan:
        if len(child.queries) != 1:
            raise SnowparkClientExceptionMessages.PLAN_CREATE_VIEW_FROM_DDL_DML_OPERATIONS()

        if not child.queries[0].sql.lower().strip().startswith("select"):
            raise SnowparkClientExceptionMessages.PLAN_CREATE_VIEWS_FROM_SELECT_ONLY()

        return self.build(
            lambda x: self.pkg.create_or_replace_view_statement(name, x, is_temp),
            child,
            None,
        )

    def create_temp_table(self, name: str, child: SnowflakePlan) -> SnowflakePlan:
        return self.build_from_multiple_queries(
            lambda x: self.create_table_and_insert(
                self.__session, name, child._schema_query, x
            ),
            child,
            None,
            child._schema_query,
            is_ddl_on_temp_object=True,
        )

    def create_table_and_insert(
        self, session, name: str, schema_query: str, query: str
    ) -> List[str]:
        attributes = session._get_result_attributes(schema_query)
        create_table = self.pkg.create_temp_table_statement(
            name, self.pkg.attribute_to_schema_string(attributes)
        )

        return [create_table, self.pkg.insert_into_statement(name, query)]

    def read_file(
        self,
        path: str,
        format: str,
        options: Dict,
        fully_qualified_schema: str,
        schema: List[Attribute],
    ):
        copy_options = {}
        format_type_options = {}

        for k, v in options.items():
            if k != "PATTERN":
                if k in self.CopyOption:
                    copy_options[k] = v
                else:
                    format_type_options[k] = v

        pattern = options.get("PATTERN", None)
        # TODO track usage of pattern, will refactor this function in future
        # Telemetry: https://snowflakecomputing.atlassian.net/browse/SNOW-363951
        # if pattern:
        #   session._conn.telemetry.reportUsageOfCopyPattern()

        pkg = AnalyzerPackage()
        if not copy_options:  # use select
            temp_file_format_name = (
                fully_qualified_schema
                + "."
                + Utils.random_name_for_temp_object(TempObjectType.FILE_FORMAT)
            )
            queries = [
                Query(
                    pkg.create_file_format_statement(
                        temp_file_format_name,
                        format,
                        format_type_options,
                        temp=True,
                        if_not_exist=True,
                    ),
                    is_ddl_on_temp_object=True,
                ),
                Query(
                    pkg.select_from_path_with_format_statement(
                        pkg.schema_cast_seq(schema),
                        path,
                        temp_file_format_name,
                        pattern,
                    )
                ),
            ]
            return SnowflakePlan(
                queries,
                pkg.schema_value_statement(schema),
                [
                    Query(
                        pkg.drop_file_format_if_exists_statement(temp_file_format_name),
                        is_ddl_on_temp_object=True,
                    )
                ],
                {},
                self.__session,
                None,
            )
        else:  # otherwise use COPY
            if "FORCE" in copy_options and copy_options["FORCE"].lower() != "true":
                raise SnowparkClientExceptionMessages.PLAN_COPY_DONT_SUPPORT_SKIP_LOADED_FILES(
                    copy_options["FORCE"]
                )

            # set force to true.
            # it is useless since we always create new temp table.
            # setting it helps users to understand generated queries.

            copy_options_with_force = {**copy_options, "FORCE": True}

            temp_table_schema = []
            for index, att in enumerate(schema):
                temp_table_schema.append(
                    Attribute(f'"COL{index}"', att.datatype, att.nullable)
                )

            temp_table_name = (
                fully_qualified_schema
                + "."
                + Utils.random_name_for_temp_object(TempObjectType.TABLE)
            )
            queries = [
                Query(
                    pkg.create_temp_table_statement(
                        temp_table_name,
                        pkg.attribute_to_schema_string(temp_table_schema),
                    ),
                    is_ddl_on_temp_object=True,
                ),
                Query(
                    pkg.copy_into_table(
                        temp_table_name,
                        path,
                        format,
                        format_type_options,
                        copy_options_with_force,
                        pattern,
                    )
                ),
                Query(
                    pkg.project_statement(
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
                    pkg.drop_table_if_exists_statement(temp_table_name),
                    is_ddl_on_temp_object=True,
                )
            ]
            return SnowflakePlan(
                queries,
                pkg.schema_value_statement(schema),
                post_actions,
                {},
                self.__session,
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
    ) -> SnowflakePlan:
        copy_command = self.pkg.copy_into_table(
            table_name=table_name,
            file_path=path,
            files=files,
            file_format=file_format,
            format_type_options=format_type_options,
            copy_options=copy_options,
            pattern=pattern,
            validation_mode=validation_mode,
            column_names=column_names,
            transformations=transformations,
        )
        if self.__session._table_exists(table_name):
            queries = [Query(copy_command)]
        elif user_schema and not transformations:
            attributes = user_schema._to_attributes()
            queries = [
                Query(
                    self.pkg.create_table_statement(
                        table_name,
                        self.pkg.attribute_to_schema_string(attributes),
                        False,
                        False,
                    )
                ),
                Query(copy_command),
            ]
        else:
            raise SnowparkClientExceptionMessages.DF_COPY_INTO_CANNOT_CREATE_TABLE(
                table_name
            )
        return SnowflakePlan(queries, copy_command, [], {}, self.__session, None)

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
            lambda x: self.pkg.copy_into_location(
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
            query._schema_query,
        )

    def update(
        self,
        table_name: str,
        assignments: Dict[str, str],
        condition: Optional[str],
        source_data: Optional[SnowflakePlan],
    ) -> SnowflakePlan:
        return self.query(
            self.pkg.update_statement(
                table_name,
                assignments,
                condition,
                source_data.queries[-1].sql
                if source_data and source_data.queries
                else None,
            ),
            None,
        )

    def delete(
        self,
        table_name: str,
        condition: Optional[str],
        source_data: Optional[SnowflakePlan],
    ) -> SnowflakePlan:
        return self.query(
            self.pkg.delete_statement(
                table_name,
                condition,
                source_data.queries[-1].sql
                if source_data and source_data.queries
                else None,
            ),
            None,
        )

    def merge(
        self, table_name: str, source: SnowflakePlan, join_expr: str, clauses: List[str]
    ) -> SnowflakePlan:
        return self.query(
            self.pkg.merge_statement(
                table_name, source.queries[-1].sql, join_expr, clauses
            ),
            None,
        )

    def lateral(
        self,
        table_function: str,
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        return self.build(
            lambda x: self.pkg.lateral_statement(table_function, x), child, source_plan
        )

    def from_table_function(self, func: str) -> SnowflakePlan:
        return self.query(self.pkg.table_function_statement(func), None)

    def join_table_function(
        self, func: str, child: SnowflakePlan, source_plan: Optional[LogicalPlan]
    ) -> SnowflakePlan:
        return self.build(
            lambda x: self.pkg.join_table_function_statement(func, x),
            child,
            source_plan,
        )

    def _add_result_scan_if_not_select(self, plan):
        if isinstance(plan.source_plan, SetOperation):
            return plan
        elif plan.queries[-1].sql.strip().lower().startswith("select"):
            return plan
        else:
            new_queries = plan.queries + [
                Query(
                    self.pkg.result_scan_statement(
                        plan.queries[-1].query_id_place_holder
                    ),
                    None,
                )
            ]
            return SnowflakePlan(
                new_queries,
                self.pkg.schema_value_statement(plan.attributes()),
                plan.post_actions,
                plan.expr_to_alias,
                self.__session,
                plan.source_plan,
            )


class Query:
    def __init__(
        self,
        sql: str,
        query_id_place_holder: Optional[str] = None,
        is_ddl_on_temp_object: bool = False,
    ):
        self.sql = sql
        self.query_id_place_holder = (
            query_id_place_holder
            if query_id_place_holder
            else f"query_id_place_holder_{Utils.generate_random_alphanumeric()}"
        )
        self.is_ddl_on_temp_object = is_ddl_on_temp_object


class BatchInsertQuery(Query):
    def __init__(
        self,
        sql: str,
        rows: Optional[List[Row]] = None,
    ):
        super().__init__(sql)
        self.rows = rows


# TODO: this class was taken from SnowflakePlanNonde.scala, we might have to move it to a new file
class SnowflakeValues(LeafNode):
    def __init__(self, output: List[Attribute], data: List[Row]):
        super(SnowflakeValues, self).__init__()
        self.output = output
        self.data = data


# TODO: Similar to the above SnowflakeValues, this should be moved to a different file
class SnowflakeCreateTable(LogicalPlan):
    def __init__(
        self,
        table_name: str,
        mode: "_SaveMode",
        query: Optional[LogicalPlan],
        create_temp_table: bool = False,
    ):
        super().__init__()
        self.table_name = table_name
        self.mode = mode
        self.create_temp_table = create_temp_table
        self.children.append(query)


class CopyIntoNode(LeafNode):
    def __init__(
        self,
        table_name: str,
        *,
        file_path: Optional[str] = None,
        files: Optional[str] = None,
        pattern: Optional[str] = None,
        file_format: Optional[str] = None,
        format_type_options: Optional[Dict[str, Any]],
        column_names: Optional[List[str]] = None,
        transformations: Optional[List[Column]] = None,
        copy_options: Optional[Dict[str, Any]] = None,
        validation_mode: Optional[str] = None,
        user_schema: Optional[StructType] = None,
        cur_options: Optional[Dict[str, Any]] = None,  # the options of DataFrameReader
    ):
        super().__init__()
        self.table_name = table_name
        self.file_path = file_path
        self.files = files
        self.pattern = pattern
        self.file_format = file_format
        self.column_names = column_names
        self.transformations = transformations
        self.copy_options = copy_options
        self.format_type_options = format_type_options
        self.validation_mode = validation_mode
        self.user_schema = user_schema
        self.cur_options = cur_options


class CopyIntoLocationNode(LeafNode):
    def __init__(
        self,
        child: SnowflakePlan,
        stage_location: str,
        *,
        partition_by: Optional[SPExpression] = None,
        file_format_name: Optional[str] = None,
        file_format_type: Optional[str] = None,
        format_type_options: Optional[str] = None,
        header: bool = False,
        copy_options: Dict[str, Any],
    ):
        super().__init__()
        self.child = child
        self.stage_location = stage_location
        self.partition_by = partition_by
        self.format_type_options = format_type_options
        self.header = header
        self.file_format_name = file_format_name
        self.file_format_type = file_format_type
        self.copy_options = copy_options


class TableUpdate(LogicalPlan):
    def __init__(
        self,
        table_name: str,
        assignments: Dict[SPExpression, SPExpression],
        condition: Optional[SPExpression],
        source_data: Optional[LogicalPlan],
    ):
        super().__init__()
        self.table_name = table_name
        self.assignments = assignments
        self.condition = condition
        self.source_data = source_data
        self.children = [source_data] if source_data else []


class TableDelete(LogicalPlan):
    def __init__(
        self,
        table_name: str,
        condition: Optional[SPExpression],
        source_data: Optional[LogicalPlan],
    ):
        super().__init__()
        self.table_name = table_name
        self.condition = condition
        self.source_data = source_data
        self.children = [source_data] if source_data else []


class TableMerge(LogicalPlan):
    def __init__(
        self,
        table_name: str,
        source: LogicalPlan,
        join_expr: SPExpression,
        clauses: List[SPExpression],
    ):
        super().__init__()
        self.table_name = table_name
        self.source = source
        self.join_expr = join_expr
        self.clauses = clauses
        self.children = [source] if source else []
