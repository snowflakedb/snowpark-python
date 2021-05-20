from src.snowflake.snowpark.internal.Expression import UnresolvedAttribute
from src.snowflake.snowpark.plans.logical.BasicLogicalOperators import Range

# TODO fix import
import src.snowflake.snowpark.internal.analyzer.SnowflakePlan as SP
from src.snowflake.snowpark.internal.analyzer.package import Package
from src.snowflake.snowpark.plans.logical.LogicalPlan import Project, Filter, UnresolvedRelation


class Analyzer:

    def __init__(self, session):
        self.generate_alias_maps = []
        self.subquery_plans = []
        self.session = session
        self.plan_builder = SP.SnowflakePlanBuilder(self.session)
        self.package = Package()


    def analyze(self, expr):

        # unresolved expression
        if type(expr) is UnresolvedAttribute:
            if len(expr.name_parts) == 1:
                return expr.name_parts[0]
            else:
                raise Exception(f"Invalid name {'.'.join(expr.name_parts)}")
        raise Exception(f"Invalid type, analyze. {str(expr)}")

    # TODO
    def resolve(self, logical_plan):
        self.subquery_plans = []
        self.generate_alias_maps = []
        result = self.do_resolve(logical_plan, is_lazy_mode=True)

        # TODO add aliases to result
        # result.add_aliases()

        # TODO add subquery plans

        result.analyze_if_needed()
        return result

    def do_resolve(self, logical_plan, is_lazy_mode=True):
        resolved_children = {}
        for c in logical_plan.children:
            resolved_children[c] = self.resolve(c)
        # TODO maps

        return self.do_resolve_inner(logical_plan, resolved_children)

    def do_resolve_inner(self, logical_plan, resolved_children):
        lp = logical_plan

        if type(lp) == SP.SnowflakePlan:
            return lp

        if type(lp) == Project:
            return self.plan_builder.project(
                map(self.analyze, lp.project_list),
                resolved_children[lp.child],
                lp)

        if type(lp) == Filter:
            return self.plan_builder.filter(
                self.analyze(lp.condition), resolved_children[lp.child], lp)

        if type(lp) == Range:
            # The column name id lower-case is hard-coded by Spark as the output
            # schema of Range. Since this corresponds to the Snowflake column "id"
            # (quoted lower-case) it's a little hard for users. So we switch it to
            # the column name "ID" == id == Id
            return self.plan_builder.query(
                self.package.range_statement(lp.start, lp.end, lp.step, "id"),
                lp)

        if type(lp) == UnresolvedRelation:
            return self.plan_builder.table('.'.join(lp.multipart_identifier))


