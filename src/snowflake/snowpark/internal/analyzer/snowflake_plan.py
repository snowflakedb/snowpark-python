#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from functools import reduce
from typing import Callable, Dict, List, Optional

from snowflake.snowpark.internal.analyzer.analyzer_package import AnalyzerPackage
from snowflake.snowpark.internal.analyzer.sf_attribute import Attribute
from snowflake.snowpark.internal.schema_utils import SchemaUtils
from snowflake.snowpark.internal.sp_expressions import (
    Attribute as SPAttribute,
    AttributeReference as SPAttributeReference,
)
from snowflake.snowpark.internal.utils import Utils
from snowflake.snowpark.plans.logical.basic_logical_operators import SetOperation
from snowflake.snowpark.plans.logical.logical_plan import LeafNode, LogicalPlan
from snowflake.snowpark.row import Row
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from snowflake.snowpark.types.types_package import snow_type_to_sp_type


class SnowflakePlan(LogicalPlan):
    def __init__(
        self,
        queries,
        schema_query,
        post_actions=None,
        expr_to_alias=None,
        session=None,
        source_plan=None,
    ):
        super().__init__()
        self.queries: List[Query] = queries
        self._schema_query: Query = schema_query
        self.post_actions = post_actions if post_actions else []
        self.expr_to_alias = expr_to_alias if expr_to_alias else {}
        self.session = session
        self.source_plan = source_plan

        # use this to simulate scala's lazy val
        self.__placeholder_for_attributes = None
        self.__placeholder_for_output = None

    # TODO
    def wrap_exception(self):
        pass

    # TODO
    def analyze_if_needed(self):
        pass

    def attributes(self) -> List["Attribute"]:
        if not self.__placeholder_for_attributes:
            output = SchemaUtils.analyze_attributes(self._schema_query, self.session)
            pkg = AnalyzerPackage()
            self._schema_query = pkg.schema_value_statement(output)
            self.__placeholder_for_attributes = output
        return self.__placeholder_for_attributes

    # Convert to 'Spark' AttributeReference
    def output(self) -> List[SPAttributeReference]:
        if not self.__placeholder_for_output:
            self.__placeholder_for_output = [
                SPAttributeReference(
                    a.name, snow_type_to_sp_type(a.datatype), a.nullable
                )
                for a in self.attributes()
            ]
        return self.__placeholder_for_output

    def clone(self):
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

    def build(self, sql_generator, child, source_plan, schema_query=None):
        select_child = self._add_result_scan_if_not_select(child)
        queries = select_child.queries[:-1] + [
            Query(sql_generator(select_child.queries[-1].sql), "")
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
        )

    def __build_binary(
        self,
        sql_generator: Callable[[str, str], str],
        left: SnowflakePlan,
        right: SnowflakePlan,
        source_plan: LogicalPlan,
    ):
        try:
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

            left_schema_query = self.pkg.schema_value_statement(
                select_left.attributes()
            )
            right_schema_query = self.pkg.schema_value_statement(
                select_right.attributes()
            )
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

        except Exception as ex:
            # TODO
            # self.__wrap_exception(ex, left, right)
            raise ex

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

    def table(self, table_name):
        return self.query(self.pkg.project_statement([], table_name), None)

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

    def create_or_replace_view(
        self, name: str, child: SnowflakePlan, is_temp: bool
    ) -> SnowflakePlan:
        if len(child.queries) != 1:
            raise SnowparkClientException(
                "Your dataframe may include DDL or DML operations. "
                + "Creating a view from this DataFrame is currently not supported."
            )

        if not child.queries[0].sql.lower().strip().startswith("select"):
            raise SnowparkClientException(
                "Creating views from SELECT queries supported only."
            )

        return self.build(
            lambda x: self.pkg.create_or_replace_view_statement(name, x, is_temp),
            child,
            None,
        )

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
        #   session.conn.telemetry.reportUsageOfCopyPattern()

        pkg = AnalyzerPackage()
        if not copy_options:  # use select
            temp_file_format_name = fully_qualified_schema + "." + Utils.random_name_for_temp_object(Utils.FileFormat)

            queries = [
                Query(
                    pkg.create_file_format_statement(
                        temp_file_format_name,
                        format,
                        format_type_options,
                        temp=True,
                        if_not_exist=True,
                    )
                ),
                Query(
                    pkg.select_from_path_with_format_statement(
                        pkg.schema_cast_seq(schema), path, temp_file_format_name, pattern
                    )
                ),
            ]

            self.__session._Session__record_temp_object(Utils.FileFormat, temp_file_format_name)

            return SnowflakePlan(
                queries,
                pkg.schema_value_statement(schema),
                [],
                {},
                self.__session,
                None,
            )
        else:  # otherwise use COPY
            temp_table_name = fully_qualified_schema + "." + Utils.random_name_for_temp_object(Utils.Table)

            if "FORCE" in copy_options and copy_options["FORCE"].lower() != "true":
                raise SnowparkClientException(
                    f"Copy option 'FORCE = {copy_options['FORCE']}' is not supported. Snowpark doesn't skip any loaded files in COPY."
                )

            # set force to true.
            # it is useless since we always create new temp table.
            # setting it helps users to understand generated queries.

            copy_options_with_force = {**copy_options, "FORCE": "TRUE"}

            temp_table_schema = []
            for index, att in enumerate(schema):
                temp_table_schema.append(
                    Attribute(f'"COL{index}"', att.datatype, att.nullable)
                )

            queries = [
                Query(
                    pkg.create_temp_table_statement(
                        temp_table_name,
                        pkg.attribute_to_schema_string(temp_table_schema),
                    )
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

            self.__session._Session__record_temp_object(Utils.Table, temp_table_name)

            post_actions = [pkg.drop_table_if_exists_statement(temp_table_name)]
            return SnowflakePlan(
                queries,
                pkg.schema_value_statement(schema),
                post_actions,
                {},
                self.__session,
                None,
            )

    def copy_into(self, table_name:str, path:str, format:str, options:Dict[str,str], fully_qualified_shcema:str, schema:List[Attribute]):
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
        #   session.conn.telemetry.reportUsageOfCopyPattern()

        # If target table doesn't exist, create target table from schema automatically,
        # and then run COPY
        pkg = AnalyzerPackage()
        queries = [
            Query(pkg.create_table_statement(table_name,
                                             pkg.attribute_to_schema_string(schema), False, False)),
            Query(pkg.copy_into_table(table_name, path, format, format_type_options, copy_options, pattern))
        ]

        return SnowflakePlan(queries, pkg.schema_value_statement(schema), [], {}, self.__session, None)

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
    def __init__(self, query_string, query_id_placeholder=None):
        self.sql = query_string
        self.query_id_place_holder = (
            query_id_placeholder
            if query_id_placeholder
            else f"query_id_place_holder_{SchemaUtils.random_string()}"
        )


# TODO: this class was taken from SnowflakePlanNonde.scala, we might have to move it to a new file
class SnowflakeValues(LeafNode):
    def __init__(self, output: List["SPAttribute"], data: List["Row"]):
        super(SnowflakeValues, self).__init__()
        self.output = output
        self.data = data
