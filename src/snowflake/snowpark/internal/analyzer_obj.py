from src.snowflake.snowpark.internal.analyzer.datatype_mapper import DataTypeMapper
from src.snowflake.snowpark.internal.sp_expressions import Expression as SPExpression, \
    UnresolvedAttribute as SPUnresolvedAttribute, UnresolvedFunction as SPUnresolvedFunction, \
    UnresolvedAlias as SPUnresolvedAlias, UnaryExpression as SPUnaryExpression, \
    LeafExpression as SPLeafExpression, Literal as SPLiteral, BinaryExpression as SPBinaryExpression
from src.snowflake.snowpark.plans.logical.basic_logical_operators import Range

# TODO fix import
import src.snowflake.snowpark.internal.analyzer.snowflake_plan as SP
from src.snowflake.snowpark.internal.analyzer.analyzer_package import AnalyzerPackage
from src.snowflake.snowpark.plans.logical.logical_plan import Project, Filter, UnresolvedRelation


class Analyzer:

    def __init__(self, session):
        self.generate_alias_maps = []
        self.subquery_plans = []
        self.session = session
        self.plan_builder = SP.SnowflakePlanBuilder(self.session)
        self.package = AnalyzerPackage()

    def analyze(self, expr):

        if type(expr) is SPLiteral:
            return DataTypeMapper.to_sql(expr.value, expr.datatype)

        # unresolved expression
        if type(expr) is SPUnresolvedAttribute:
            if len(expr.name_parts) == 1:
                return expr.name_parts[0]
            else:
                raise Exception(f"Invalid name {'.'.join(expr.name_parts)}")
        if type(expr) is SPUnresolvedFunction:
            # TODO expr.name should return FunctionIdentifier, and we should pass expr.name.funcName
            return self.package.function_expression(expr.name,
                                                    list(map(self.analyze, expr.children)),
                                                    expr.is_distinct)

        # Extractors

        res = self.unary_expression_extractor(expr)
        if res:
            return res
        res = self.spark_binary_operator_extractor(expr)
        if res:
            return res

        raise Exception(f"Invalid type, analyze. {str(expr)}")

    # TODO
    def table_function_expression_extractor(self, expr):
        pass

    # TODO
    def leaf_expression_extractor(self, expr):
        if not isinstance(expr, SPLeafExpression):
            return None

    # TODO
    def string_to_trim_expression_extractor(self, expr):
        pass

    # TODO
    def complex_type_merging_expressing_extractor(self, expr):
        pass

    # TODO
    def ternary_expression_extractor(self, expr):
        pass

    # TODO complete
    def unary_expression_extractor(self, expr):
        if not isinstance(expr, SPUnaryExpression):
            return None

        if isinstance(expr, SPUnresolvedAlias):
            return self.analyze(expr.child)

    # TODO
    def special_frame_boundary_extractor(self, expr):
        pass

    # TODO
    def offset_window_function_extractor(self, expr):
        pass

    # TODO
    def binary_operator_extractor(self, expr):
        pass

    # TODO
    def spark_binary_operator_extractor(self, expr):
        if not isinstance(expr, SPBinaryExpression):
            return None

        if isinstance(expr, SPBinaryExpression):
            return self.package.binary_comparison(self.analyze(expr.left), self.analyze(expr.right),
                                                  expr.symbol)

    # TODO
    def aggregate_extractor(self, expr):
        pass

    # TODO
    def grouping_extractor(self, expr: SPExpression):
        pass

    # TODO
    def window_frame_boundary(self, offset: str) -> str:
        pass

    # TODO
    def to_sql_avoid_offset(self, expr: SPExpression) -> str:
        pass

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
