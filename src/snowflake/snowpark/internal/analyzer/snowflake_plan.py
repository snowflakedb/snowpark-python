from src.snowflake.snowpark.internal.sp_expressions import AttributeReference
from src.snowflake.snowpark.internal.schema_utils import SchemaUtils
from src.snowflake.snowpark.plans.logical.logical_plan import LogicalPlan
from src.snowflake.snowpark.internal.analyzer.analyzer_package import AnalyzerPackage
from src.snowflake.snowpark.types.types_package import snow_type_to_sp_type


class SnowflakePlan(LogicalPlan):
    # for read_file()
    __copy_option = {"ON_ERROR", "SIZE_LIMIT", "PURGE", "RETURN_FAILED_ONLY",
                     "MATCH_BY_COLUMN_NAME", "ENFORCE_LENGTH", "TRUNCATECOLUMNS", "FORCE",
                     "LOAD_UNCERTAIN_FILES"}

    def __init__(self, queries, schema_query, post_actions=None, expr_to_alias=None, session=None,
                 source_plan=None):
        super().__init__()
        self.queries = queries
        self._schema_query = schema_query
        self.post_actions = post_actions if post_actions else []
        self.expr_to_alias = expr_to_alias if expr_to_alias else {}
        self.session = session
        self.source_plan = source_plan

    # TODO
    def wrap_exception(self):
        pass

    # TODO
    def analyze_if_needed(self):
        pass

    def attributes(self):
        output = SchemaUtils.analyze_attributes(self._schema_query, self.session)
        pkg = AnalyzerPackage()
        self._schema_query = pkg.schema_value_statement(output)
        return output

    # Convert to 'Spark' AttributeReference
    def output(self) -> list:
        return [AttributeReference(a.name, snow_type_to_sp_type(a.data_type), a.nullable)
                for a in self.attributes()]


class SnowflakePlanBuilder:

    def __init__(self, session):
        self.__session = session
        self.pkg = AnalyzerPackage()

    def build(self, sql_generator, child, source_plan, schema_query=None):
        select_child = self._add_result_scan_if_not_select(child)
        queries = select_child.queries[:-1] + [
            Query(sql_generator(select_child.queries[-1].sql), "")]
        new_schema_query = schema_query if schema_query else sql_generator(child._schema_query)

        return SnowflakePlan(queries, new_schema_query, select_child.post_actions,
                             select_child.expr_to_alias, self.__session, source_plan)

    def query(self, sql, source_plan):
        """
        :rtype: SnowflakePlan
        """
        return SnowflakePlan(queries=[Query(sql, None)], schema_query=sql, session=self.__session,
                             source_plan=source_plan)

    def table(self, table_name):
        return self.query(self.pkg.project_statement([], table_name), None)

    def project(self, project_list, child, source_plan, is_distinct=False):
        return self.build(
            lambda x: self.pkg.project_statement(project_list, x, is_distinct=is_distinct),
            child, source_plan)

    def filter(self, condition, child, source_plan):
        return self.build(
            lambda x: self.pkg.filter_statement(condition, x), child, source_plan)

    def _add_result_scan_if_not_select(self, plan):
        if plan.queries[-1].sql.strip().lower().startswith("select"):
            return plan
        else:
            new_queries = plan.queries + [
                Query(self.pkg.result_scan_statement(plan.queries[-1].query_id_plance_holder))]
            return SnowflakePlan(new_queries, self.pkg.schema_value_statement(plan.attributes),
                                 plan.post_actions, plan.expr_to_alias, self.__session,
                                 plan.source_plan)


class Query:

    def __init__(self, query_string, query_placeholder):
        self.sql = query_string
        self.place_holder = query_placeholder
