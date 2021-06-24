#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from typing import Callable, Dict, List, Optional

from src.snowflake.snowpark.internal.analyzer.analyzer_package import AnalyzerPackage
from src.snowflake.snowpark.internal.schema_utils import SchemaUtils
from src.snowflake.snowpark.plans.logical.logical_plan import LeafNode, LogicalPlan
from src.snowflake.snowpark.row import Row
from src.snowflake.snowpark.types.types_package import snow_type_to_sp_type

from ..sp_expressions import (
    Attribute as SPAttribute,
    AttributeReference as SPAttributeReference,
)
from .sf_attribute import Attribute

from ...snowpark_client_exception import SnowparkClientException


class SnowflakePlan(LogicalPlan):
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

    def join(self, left, right, join_type, condition, source_plan):
        return self.__build_binary(
            lambda x, y: self.pkg.join_statement(x, y, join_type, condition),
            left,
            right,
            source_plan,
        )

    def sort(
        self, order: List[str], child: SnowflakePlan, source_plan: Optional[LogicalPlan]
    ):
        return self.build(
            lambda x: self.pkg.sort_statement(order, x), child, source_plan
        )


    def create_or_replace_view(self, name: str, child: SnowflakePlan, is_temp: bool) -> SnowflakePlan:
        if len(child.queries) != 1:
            raise SnowparkClientException("Your dataframe may include DDL or DML operations. " +
                                           "Creating a view from this DataFrame is currently not supported.")

        if not child.queries[0].sql.lower().strip().startswith('select'):
            raise SnowparkClientException("Creating views from SELECT queries supported only.")

        return self.build(
            lambda x: self.pkg.create_or_replace_view_statement(name, x, is_temp), child, None)

    def _add_result_scan_if_not_select(self, plan):
        if plan.queries[-1].sql.strip().lower().startswith("select"):
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
    def __init__(self, query_string, query_id_placeholder):
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
