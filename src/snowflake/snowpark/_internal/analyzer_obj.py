#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from collections import Counter
from typing import Optional

from snowflake.snowpark._internal.analyzer.analyzer_package import AnalyzerPackage
from snowflake.snowpark._internal.analyzer.datatype_mapper import DataTypeMapper
from snowflake.snowpark._internal.analyzer.lateral import Lateral as SPLateral
from snowflake.snowpark._internal.analyzer.limit import Limit as SPLimit
from snowflake.snowpark._internal.analyzer.sf_attribute import Attribute
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    CopyIntoNode,
    SnowflakeCreateTable,
    SnowflakePlan,
    SnowflakePlanBuilder,
    SnowflakeValues,
)
from snowflake.snowpark._internal.analyzer.sp_views import (
    CreateViewCommand as SPCreateViewCommand,
    LocalTempView as SPLocalTempView,
    PersistedView as SPPersistedView,
)
from snowflake.snowpark._internal.analyzer.table_function import (
    TableFunctionJoin as SPTableFunctionJoin,
    TableFunctionRelation as SPTableFunctionRelation,
)
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.plans.logical.basic_logical_operators import (
    Aggregate as SPAggregate,
    Except as SPExcept,
    Intersect as SPIntersect,
    Join as SPJoin,
    Pivot as SPPivot,
    Range as SPRange,
    Sort as SPSort,
    Union as SPUnion,
)
from snowflake.snowpark._internal.plans.logical.logical_plan import (
    Filter as SPFilter,
    Project as SPProject,
    Sample as SPSample,
    UnresolvedRelation as SPUnresolvedRelation,
)
from snowflake.snowpark._internal.sp_expressions import (
    AggregateExpression as SPAggregateExpression,
    AggregateFunction as SPAggregateFunction,
    Alias as SPAlias,
    AttributeReference as SPAttributeReference,
    BaseGroupingSets as SPBaseGroupingSets,
    BinaryArithmeticExpression as SPBinaryArithmeticExpression,
    BinaryExpression as SPBinaryExpression,
    CaseWhen as SPCaseWhen,
    Cast as SPCast,
    Collate as SPCollate,
    Expression as SPExpression,
    FlattenFunction as SPFlattenFunction,
    FunctionExpression as SPFunctionExpression,
    GroupingSetsExpression as SPGroupingSetsExpression,
    InExpression as SPInExpression,
    IsNaN as SPIsNaN,
    IsNotNull as SPIsNotNull,
    IsNull as SPIsNull,
    LeafExpression as SPLeafExpression,
    Like as SPLike,
    Literal as SPLiteral,
    MultipleExpression as SPMultipleExpression,
    NamedArgumentsTableFunction as SPNamedArgumentsTableFunction,
    Not as SPNot,
    RegExp as SPRegExp,
    ScalarSubquery as SPScalarSubquery,
    SnowflakeUDF as SPSnowflakeUDF,
    SortOrder as SPSortOrder,
    SpecialFrameBoundary as SPSpecialFrameBoundary,
    SpecifiedWindowFrame as SPSpecifiedWindowFrame,
    Star as SPStar,
    SubfieldInt as SPSubfieldInt,
    SubfieldString as SPSubfieldString,
    TableFunction as SPTableFunction,
    TableFunctionExpression as SPTableFunctionExpression,
    UnaryExpression as SPUnaryExpression,
    UnaryMinus as SPUnaryMinus,
    UnresolvedAlias as SPUnresolvedAlias,
    UnresolvedAttribute as SPUnresolvedAttribute,
    UnspecifiedFrame as SPUnspecifiedFrame,
    WindowExpression as SPWindowExpression,
    WindowSpecDefinition as SPWindowSpecDefinition,
)
from snowflake.snowpark._internal.sp_types.sp_data_types import (
    IntegralType as SPIntegralType,
    VariantType as SPVariantType,
)

ARRAY_BIND_THRESHOLD = 512


class Analyzer:
    def __init__(self, session):
        self.session = session
        self.plan_builder = SnowflakePlanBuilder(self.session)
        self.package = AnalyzerPackage()

        self.generated_alias_maps = {}
        self.subquery_plans = []
        self.alias_maps_to_use = None

    def analyze(self, expr) -> str:
        if isinstance(expr, SPGroupingSetsExpression):
            return self.package.grouping_set_expression(
                [[self.analyze(a) for a in arg] for arg in expr.args]
            )

        if isinstance(expr, SPLike):
            return self.package.like_expression(
                self.analyze(expr.expr), self.analyze(expr.pattern)
            )

        if isinstance(expr, SPRegExp):
            return self.package.regexp_expression(
                self.analyze(expr.expr), self.analyze(expr.pattern)
            )

        if isinstance(expr, SPCollate):
            return self.package.collate_expression(
                self.analyze(expr.expr), expr.collation_spec
            )

        if isinstance(expr, (SPSubfieldString, SPSubfieldInt)):
            return self.package.subfield_expression(self.analyze(expr.expr), expr.field)

        if isinstance(expr, SPCaseWhen):
            return self.package.case_when_expression(
                [
                    (self.analyze(condition), self.analyze(value))
                    for condition, value in expr.branches
                ],
                self.analyze(expr.else_value) if expr.else_value else "NULL",
            )

        if isinstance(expr, SPMultipleExpression):
            return self.package.block_expression(
                [self.analyze(expression) for expression in expr.expressions]
            )

        if isinstance(expr, SPInExpression):
            return self.package.in_expression(
                self.analyze(expr.columns),
                [self.analyze(expression) for expression in expr.values],
            )

        # aggregate
        if isinstance(expr, SPAggregateExpression):
            return self.aggr_extractor_convert_expr(
                expr.aggregate_function, expr.is_distinct
            )
        if isinstance(expr, SPBaseGroupingSets):
            return self.grouping_extractor(expr)

        # window
        if isinstance(expr, SPWindowExpression):
            return self.package.window_expression(
                self.analyze(expr.window_function), self.analyze(expr.window_spec)
            )
        if isinstance(expr, SPWindowSpecDefinition):
            return self.package.window_spec_expression(
                list(map(self.analyze, expr.partition_spec)),
                list(map(self.analyze, expr.order_spec)),
                self.analyze(expr.frame_spec),
            )
        if isinstance(expr, SPSpecifiedWindowFrame):
            return self.package.specified_window_frame_expression(
                expr.frame_type.sql,
                self.window_frame_boundary(self.to_sql_avoid_offset(expr.lower)),
                self.window_frame_boundary(self.to_sql_avoid_offset(expr.upper)),
            )
        if isinstance(expr, SPUnspecifiedFrame):
            return ""
        if isinstance(expr, SPSpecialFrameBoundary):
            return expr.sql()

        if isinstance(expr, SPLiteral):
            return DataTypeMapper.to_sql(expr.value, expr.datatype)

        if isinstance(expr, SPAttributeReference):
            name = self.alias_maps_to_use.get(expr.expr_id, expr.name)
            return self.package.quote_name(name)

        # unresolved expression
        if isinstance(expr, SPUnresolvedAttribute):
            if len(expr.name_parts) == 1:
                return expr.name_parts[0]
            else:
                raise SnowparkClientExceptionMessages.PLAN_ANALYZER_INVALID_IDENTIFIER(
                    ".".join(expr.name_parts)
                )

        if isinstance(expr, SPAlias):
            quoted_name = self.package.quote_name(expr.name)
            if isinstance(expr.child, SPAttributeReference):
                self.generated_alias_maps[expr.child.expr_id] = quoted_name
                for k, v in self.alias_maps_to_use.items():
                    if v == expr.child.name:
                        self.generated_alias_maps[k] = quoted_name
            return self.package.alias_expression(self.analyze(expr.child), quoted_name)

        if isinstance(expr, SPFunctionExpression):
            return self.package.function_expression(
                expr.name,
                [self.to_sql_avoid_offset(c) for c in expr.children],
                expr.is_distinct,
            )

        if isinstance(expr, SPStar):
            if not expr.expressions:
                return "*"
            else:
                return ",".join(list(map(self.analyze, expr.expressions)))

        if isinstance(expr, SPSnowflakeUDF):
            return self.package.function_expression(
                expr.udf_name, list(map(self.analyze, expr.children)), False
            )

        # Extractors
        if isinstance(expr, SPTableFunctionExpression):
            return self.table_function_expression_extractor(expr)

        if isinstance(expr, SPUnaryExpression):
            return self.unary_expression_extractor(expr)

        if isinstance(expr, SPScalarSubquery):
            self.subquery_plans.append(expr.plan)
            return self.package.subquery_expression(expr.plan.queries[-1].sql)

        if isinstance(expr, SPBinaryExpression):
            return self.binary_operator_extractor(expr)

        raise SnowparkClientExceptionMessages.PLAN_INVALID_TYPE(str(expr))

    def table_function_expression_extractor(self, expr):
        if isinstance(expr, SPFlattenFunction):
            return self.package.flatten_expression(
                self.analyze(expr.input),
                expr.path,
                expr.outer,
                expr.recursive,
                expr.mode,
            )

        if isinstance(expr, SPTableFunction):
            return self.package.function_expression(
                expr.func_name, [self.analyze(x) for x in expr.args], False
            )

        if isinstance(expr, SPNamedArgumentsTableFunction):
            return self.package.named_arguments_function(
                expr.func_name,
                {key: self.analyze(value) for key, value in expr.args.items()},
            )

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
        if isinstance(expr, SPUnresolvedAlias):
            return self.analyze(expr.child)
        elif isinstance(expr, SPCast):
            return self.package.cast_expression(self.analyze(expr.child), expr.to)
        elif isinstance(expr, SPSortOrder):
            return self.package.order_expression(
                self.analyze(expr.child), expr.direction.sql, expr.null_ordering.sql
            )
        elif isinstance(expr, SPUnaryMinus):
            return self.package.unary_minus_expression(self.analyze(expr.child))
        elif isinstance(expr, SPNot):
            return self.package.not_expression(self.analyze(expr.child))
        elif isinstance(expr, SPIsNaN):
            return self.package.is_nan_expression(self.analyze(expr.child))
        elif isinstance(expr, SPIsNull):
            return self.package.is_null_expression(self.analyze(expr.child))
        elif isinstance(expr, SPIsNotNull):
            return self.package.is_not_null_expression(self.analyze(expr.child))
        else:
            return self.package.function_expression(
                expr.pretty_name, [self.analyze(expr.child)], False
            )

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

    def grouping_extractor(self, expr: SPExpression):
        return self.analyze(
            SPFunctionExpression(
                expr.pretty_name().upper(),
                [c.child if isinstance(c, SPAlias) else c for c in expr.children],
                False,
            )
        )

    def window_frame_boundary(self, offset: str) -> str:
        try:
            num = int(offset)
            return self.package.window_frame_boundary_expression(
                str(abs(num)), num >= 0
            )
        except:
            return offset

    def to_sql_avoid_offset(self, expr: SPExpression) -> str:
        # if expression is an integral literal, return the number without casting,
        # otherwise process as normal
        if isinstance(expr, SPLiteral) and isinstance(expr.datatype, SPIntegralType):
            return DataTypeMapper.to_sql_without_cast(expr.value, expr.datatype)
        else:
            return self.analyze(expr)

    def resolve(self, logical_plan) -> SnowflakePlan:
        self.subquery_plans = []
        self.generated_alias_maps = {}
        result = self.do_resolve(logical_plan, is_lazy_mode=True)

        result.add_aliases(self.generated_alias_maps)

        if self.subquery_plans:
            result = result.with_subqueries(self.subquery_plans)

        result.analyze_if_needed()
        return result

    def do_resolve(self, logical_plan, is_lazy_mode=True):
        resolved_children = {}
        for c in logical_plan.children:
            resolved_children[c] = self.resolve(c)

        use_maps = {}
        # get counts of expr_to_alias keys
        counts = Counter()
        for k, v in resolved_children.items():
            if v.expr_to_alias:
                counts.update(list(v.expr_to_alias.keys()))

        # Keep only non-shared expr_to_alias keys
        # let (df1.join(df2)).join(df2.join(df3)).select(df2) report error
        for k, v in resolved_children.items():
            if v.expr_to_alias:
                use_maps.update(
                    {p: q for p, q in v.expr_to_alias.items() if counts[p] < 2}
                )

        self.alias_maps_to_use = use_maps
        return self.do_resolve_inner(logical_plan, resolved_children)

    def do_resolve_inner(self, logical_plan, resolved_children) -> SnowflakePlan:
        if isinstance(logical_plan, SnowflakePlan):
            return logical_plan

        if isinstance(logical_plan, SPTableFunctionJoin):
            return self.plan_builder.join_table_function(
                self.analyze(logical_plan.table_function),
                resolved_children[logical_plan.children[0]],
                logical_plan,
            )

        if isinstance(logical_plan, SPTableFunctionRelation):
            return self.plan_builder.from_table_function(
                self.analyze(logical_plan.table_function)
            )

        if isinstance(logical_plan, SPLateral):
            return self.plan_builder.lateral(
                self.analyze(logical_plan.table_function),
                resolved_children[logical_plan.children[0]],
                logical_plan,
            )

        if isinstance(logical_plan, SPAggregate):
            return self.plan_builder.aggregate(
                list(map(self.to_sql_avoid_offset, logical_plan.grouping_expressions)),
                list(map(self.analyze, logical_plan.aggregate_expressions)),
                resolved_children[logical_plan.child],
                logical_plan,
            )

        if isinstance(logical_plan, SPProject):
            return self.plan_builder.project(
                list(map(self.analyze, logical_plan.project_list)),
                resolved_children[logical_plan.child],
                logical_plan,
            )

        if isinstance(logical_plan, SPFilter):
            return self.plan_builder.filter(
                self.analyze(logical_plan.condition),
                resolved_children[logical_plan.child],
                logical_plan,
            )

        # Add a sample stop to the plan being built
        if isinstance(logical_plan, SPSample):
            return self.plan_builder.sample(
                resolved_children[logical_plan.child],
                logical_plan,
                logical_plan.probability_fraction,
                logical_plan.row_count,
            )

        if isinstance(logical_plan, SPJoin):
            return self.plan_builder.join(
                resolved_children[logical_plan.left],
                resolved_children[logical_plan.right],
                logical_plan.join_type,
                self.analyze(logical_plan.condition) if logical_plan.condition else "",
                logical_plan,
            )

        if isinstance(logical_plan, SPSort):
            return self.plan_builder.sort(
                list(map(self.analyze, logical_plan.order)),
                resolved_children[logical_plan.child],
                logical_plan,
            )

        if isinstance(logical_plan, (SPIntersect, SPUnion, SPExcept)):
            return self.plan_builder.set_operator(
                resolved_children[logical_plan.left],
                resolved_children[logical_plan.right],
                logical_plan.sql,
                logical_plan,
            )

        if isinstance(logical_plan, SPRange):
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

        if isinstance(logical_plan, SnowflakeValues):
            if logical_plan.data:
                if (
                    len(logical_plan.output) * len(logical_plan.data)
                    < ARRAY_BIND_THRESHOLD
                ):
                    return self.plan_builder.query(
                        self.package.values_statement(
                            logical_plan.output, logical_plan.data
                        ),
                        logical_plan,
                    )
                else:
                    return self.plan_builder.large_local_relation_plan(
                        logical_plan.output, logical_plan.data, logical_plan
                    )
            else:
                return self.plan_builder.query(
                    self.package.empty_values_statement(logical_plan.output),
                    logical_plan,
                )

        if isinstance(logical_plan, SPUnresolvedRelation):
            return self.plan_builder.table(".".join(logical_plan.multipart_identifier))

        if isinstance(logical_plan, SnowflakeCreateTable):
            return self.plan_builder.save_as_table(
                logical_plan.table_name,
                logical_plan.mode,
                resolved_children[logical_plan.children[0]],
            )

        if isinstance(logical_plan, SPLimit):
            if isinstance(logical_plan.child, SPSort):
                on_top_of_order_by = True
            elif (
                isinstance(logical_plan.child, SnowflakePlan)
                and logical_plan.child.source_plan
            ):
                on_top_of_order_by = isinstance(logical_plan.child.source_plan, SPSort)
            else:
                on_top_of_order_by = False

            return self.plan_builder.limit(
                self.to_sql_avoid_offset(logical_plan.limit_expr),
                resolved_children[logical_plan.child],
                on_top_of_order_by,
                logical_plan,
            )

        if isinstance(logical_plan, SPPivot):
            if len(logical_plan.aggregates) != 1:
                raise ValueError("Only one aggregate is supported with pivot")

            return self.plan_builder.pivot(
                self.analyze(logical_plan.pivot_column),
                [self.analyze(pv) for pv in logical_plan.pivot_values],
                self.analyze(logical_plan.aggregates[0]),
                self.resolve(logical_plan.child),
                logical_plan,
            )

        if isinstance(logical_plan, SPCreateViewCommand):
            if isinstance(logical_plan.view_type, SPPersistedView):
                is_temp = False
            elif isinstance(logical_plan.view_type, SPLocalTempView):
                is_temp = True
            else:
                raise SnowparkClientExceptionMessages.PLAN_ANALYZER_UNSUPPORTED_VIEW_TYPE(
                    str(logical_plan.view_type)
                )

            return self.plan_builder.create_or_replace_view(
                logical_plan.name.table, self.resolve(logical_plan.child), is_temp
            )

        if isinstance(logical_plan, CopyIntoNode):
            if logical_plan.table_name:
                return self.plan_builder.copy_into_table(
                    path=logical_plan.file_path,
                    table_name=logical_plan.table_name,
                    files=logical_plan.files,
                    pattern=logical_plan.pattern,
                    file_format=logical_plan.file_format,
                    format_type_options=logical_plan.format_type_options,
                    copy_options=logical_plan.copy_options,
                    validation_mode=logical_plan.validation_mode,
                    column_names=logical_plan.column_names,
                    transformations=[
                        self.analyze(x) for x in logical_plan.transformations
                    ]
                    if logical_plan.transformations
                    else None,
                    user_schema=logical_plan.user_schema,
                )
            elif logical_plan.file_format and logical_plan.file_format.upper() == "CSV":
                if not logical_plan.user_schema:
                    raise SnowparkClientExceptionMessages.DF_MUST_PROVIDE_SCHEMA_FOR_READING_FILE()
                else:
                    return self.plan_builder.read_file(
                        logical_plan.files,
                        logical_plan.file_format,
                        logical_plan.cur_options,
                        self.session.getFullyQualifiedCurrentSchema(),
                        logical_plan.user_schema._to_attributes(),
                    )
            else:
                return self.plan_builder.read_file(
                    logical_plan.files,
                    logical_plan.file_format,
                    logical_plan.cur_options,
                    self.session.getFullyQualifiedCurrentSchema(),
                    [Attribute('"$1"', SPVariantType())],
                )
