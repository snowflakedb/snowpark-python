#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from typing import Optional

from src.snowflake.snowpark.internal.analyzer.analyzer_package import AnalyzerPackage
from src.snowflake.snowpark.internal.analyzer.datatype_mapper import DataTypeMapper

# TODO fix import
from src.snowflake.snowpark.internal.analyzer.snowflake_plan import (
    SnowflakePlan,
    SnowflakePlanBuilder,
    SnowflakeValues,
)
from src.snowflake.snowpark.internal.sp_expressions import (
    AggregateExpression as SPAggregateExpression,
    AggregateFunction as SPAggregateFunction,
    Alias as SPAlias,
    AttributeReference as SPAttributeReference,
    BinaryArithmeticExpression as SPBinaryArithmeticExpression,
    BinaryExpression as SPBinaryExpression,
    Cast as SPCast,
    Expression as SPExpression,
    IsNaN as SPIsNaN,
    IsNotNull as SPIsNotNull,
    IsNull as SPIsNull,
    LeafExpression as SPLeafExpression,
    Literal as SPLiteral,
    Not as SPNot,
    ResolvedStar as SPResolvedStar,
    SortOrder as SPSortOrder,
    UnaryExpression as SPUnaryExpression,
    UnaryMinus as SPUnaryMinus,
    UnresolvedAlias as SPUnresolvedAlias,
    UnresolvedAttribute as SPUnresolvedAttribute,
    UnresolvedFunction as SPUnresolvedFunction,
    UnresolvedStar as SPUnresolvedStar,
)
from src.snowflake.snowpark.plans.logical.basic_logical_operators import (
    Aggregate as SPAggregate,
    Join as SPJoin,
    Range as SPRange,
    Sort as SPSort,
)
from src.snowflake.snowpark.plans.logical.logical_plan import (
    Filter as SPFilter,
    Project as SPProject,
    UnresolvedRelation as SPUnresolvedRelation,
)
from src.snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from src.snowflake.snowpark.types.sp_data_types import (
    ByteType as SPByteType,
    IntegerType as SPIntegerType,
    LongType as SPLongType,
    ShortType as SPShortType,
)


class Analyzer:
    def __init__(self, session):
        self.session = session
        self.plan_builder = SnowflakePlanBuilder(self.session)
        self.package = AnalyzerPackage()

        self.generated_alias_maps = {}
        self.subquery_plans = {}
        self.alias_maps_to_use = None

    def analyze(self, expr):

        # aggregate
        if type(expr) == SPAggregateExpression:
            return self.aggr_extractor_convert_expr(
                expr.aggregate_function, expr.is_distinct
            )

        if type(expr) is SPLiteral:
            return DataTypeMapper.to_sql(expr.value, expr.datatype)

        if type(expr) is SPAttributeReference:
            name = self.alias_maps_to_use.get(expr.expr_id, expr.name)
            return self.package.quote_name(name)

        # unresolved expression
        if type(expr) is SPUnresolvedAttribute:
            if len(expr.name_parts) == 1:
                return expr.name_parts[0]
            else:
                raise SnowparkClientException(
                    f"Invalid name {'.'.join(expr.name_parts)}"
                )
        if type(expr) is SPUnresolvedFunction:
            # TODO expr.name should return FunctionIdentifier, and we should pass expr.name.funcName
            return self.package.function_expression(
                expr.name, list(map(self.analyze, expr.children)), expr.is_distinct
            )

        if type(expr) == SPAlias:
            quoted_name = self.package.quote_name(expr.name)
            if isinstance(expr.child, SPAttributeReference):
                self.generated_alias_maps[expr.child.expr_id] = quoted_name
                for k, v in self.alias_maps_to_use.items():
                    if v == expr.child.name:
                        self.generated_alias_maps[k] = quoted_name
            return self.package.alias_expression(self.analyze(expr.child), quoted_name)

        if type(expr) == SPResolvedStar:
            return "*"
        if type(expr) == SPUnresolvedStar:
            return expr.to_string()

        # Extractors
        if isinstance(expr, SPUnaryExpression):
            return self.unary_expression_extractor(expr)

        if isinstance(expr, SPBinaryExpression):
            return self.binary_operator_extractor(expr)

        raise SnowparkClientException(f"Invalid type, analyze. {str(expr)}")

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

    def unary_expression_extractor(self, expr) -> Optional[str]:
        if type(expr) == SPUnresolvedAlias:
            return self.analyze(expr.child)
        elif type(expr) == SPCast:
            return self.package.cast_expression(self.analyze(expr.child), expr.to)
        elif type(expr) == SPSortOrder:
            return self.package.order_expression(
                self.analyze(expr.child), expr.direction.sql, expr.null_ordering.sql
            )
        elif type(expr) == SPUnaryMinus:
            return self.package.unary_minus_expression(self.analyze(expr.child))
        elif type(expr) == SPNot:
            return self.package.not_expression(self.analyze(expr.child))
        elif type(expr) == SPIsNaN:
            return self.package.is_nan_expression(self.analyze(expr.child))
        elif type(expr) == SPIsNull:
            return self.package.is_null_expression(self.analyze(expr.child))
        elif type(expr) == SPIsNotNull:
            return self.package.is_not_null_expression(self.analyze(expr.child))
        else:
            # TODO: SNOW-369125: pretty_name of Expression
            return self.package.function_expression(
                expr.pretty_name, [self.analyze(expr.child)], False
            )

    # TODO
    def special_frame_boundary_extractor(self, expr):
        pass

    # TODO
    def offset_window_function_extractor(self, expr):
        pass

    def binary_operator_extractor(self, expr):
        if isinstance(expr, SPBinaryArithmeticExpression):
            return self.package.binary_arithmetic_expression(
                expr.sql_operator, self.analyze(expr.left), self.analyze(expr.right)
            )
        else:
            return self.package.function_expression(
                expr.sql_operator,
                [self.analyze(expr.left), self.analyze(expr.right)],
                False,
            )

    # TODO
    def aggregate_extractor(self, expr):
        if not isinstance(expr, SPAggregateFunction):
            return None
        else:
            return self.aggr_extractor_convert_expr(expr, is_distinct=False)

    def aggr_extractor_convert_expr(
        self, expr: SPAggregateFunction, is_distinct: bool
    ) -> str:
        # if type(expr) == SPSkewness:
        #   TODO
        # if type(expr) == SPNTile:
        #   TODO
        # if type(expr) == aggregateWindow:
        #   TODO
        # else:
        return self.package.function_expression(
            expr.pretty_name(), [self.analyze(c) for c in expr.children], is_distinct
        )

    # TODO
    def grouping_extractor(self, expr: SPExpression):
        pass

    # TODO
    def window_frame_boundary(self, offset: str) -> str:
        pass

    def __to_sql_avoid_offset(self, expr: SPExpression) -> str:
        # if expression is integral literal, return the number without casting,
        # otherwise process as normal
        if type(expr) == SPLiteral:
            if isinstance(
                expr.datatype, (SPIntegerType, SPLongType, SPShortType, SPByteType)
            ):
                return DataTypeMapper.to_sql_without_cast(expr.value, expr.datatype)
        else:
            return self.analyze(expr)

    # TODO
    def resolve(self, logical_plan) -> SnowflakePlan:
        self.subquery_plans = []
        self.generated_alias_maps = {}
        result = self.do_resolve(logical_plan, is_lazy_mode=True)

        result.add_aliases(self.generated_alias_maps)

        # TODO add subquery plans

        result.analyze_if_needed()
        return result

    def do_resolve(self, logical_plan, is_lazy_mode=True):
        resolved_children = {}
        for c in logical_plan.children:
            resolved_children[c] = self.resolve(c)

        use_maps = {}
        for k, v in resolved_children.items():
            if v.expr_to_alias:
                use_maps.update(v.expr_to_alias)

        self.alias_maps_to_use = use_maps
        return self.do_resolve_inner(logical_plan, resolved_children)

    def do_resolve_inner(self, logical_plan, resolved_children) -> SnowflakePlan:
        if type(logical_plan) == SnowflakePlan:
            return logical_plan

        if type(logical_plan) == SPAggregate:
            return self.plan_builder.aggregate(
                list(
                    map(self.__to_sql_avoid_offset, logical_plan.grouping_expressions)
                ),
                list(map(self.analyze, logical_plan.aggregate_expressions)),
                resolved_children[logical_plan.child],
                logical_plan,
            )

        if type(logical_plan) == SPProject:
            return self.plan_builder.project(
                list(map(self.analyze, logical_plan.project_list)),
                resolved_children[logical_plan.child],
                logical_plan,
            )

        if type(logical_plan) == SPFilter:
            return self.plan_builder.filter(
                self.analyze(logical_plan.condition),
                resolved_children[logical_plan.child],
                logical_plan,
            )

        if type(logical_plan) == SPJoin:
            return self.plan_builder.join(
                resolved_children[logical_plan.left],
                resolved_children[logical_plan.right],
                logical_plan.join_type,
                self.analyze(logical_plan.condition) if logical_plan.condition else "",
                logical_plan,
            )

        if type(logical_plan) == SPSort:
            return self.plan_builder.sort(
                list(map(self.analyze, logical_plan.order)),
                resolved_children[logical_plan.child],
                logical_plan,
            )

        if type(logical_plan) == SPRange:
            # The column name id lower-case is hard-coded by Spark as the output
            # schema of Range. Since this corresponds to the Snowflake column "id"
            # (quoted lower-case) it's a little hard for users. So we switch it to
            # the column name "ID" == id == Id
            return self.plan_builder.query(
                self.package.range_statement(
                    logical_plan.start, logical_plan.end, logical_plan.step, "id"
                ),
                logical_plan,
            )

        if type(logical_plan) == SPUnresolvedRelation:
            return self.plan_builder.table(".".join(logical_plan.multipart_identifier))

        if type(logical_plan) == SnowflakeValues:
            if logical_plan.data:
                # TODO: SNOW-367105 handle large values with largeLocalRelationPlan
                return self.plan_builder.query(
                    self.package.values_statement(
                        logical_plan.output, logical_plan.data
                    ),
                    logical_plan,
                )
            else:
                return self.plan_builder.query(
                    self.package.empty_values_statement(logical_plan.output),
                    logical_plan,
                )
