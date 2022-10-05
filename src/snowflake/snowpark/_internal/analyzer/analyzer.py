#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from collections import Counter
from typing import Dict, Union

import snowflake.snowpark
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    alias_expression,
    binary_arithmetic_expression,
    block_expression,
    case_when_expression,
    cast_expression,
    collate_expression,
    delete_merge_statement,
    empty_values_statement,
    flatten_expression,
    function_expression,
    grouping_set_expression,
    in_expression,
    insert_merge_statement,
    like_expression,
    list_agg,
    named_arguments_function,
    order_expression,
    quote_name,
    range_statement,
    rank_related_function_expression,
    regexp_expression,
    specified_window_frame_expression,
    subfield_expression,
    subquery_expression,
    table_function_partition_spec,
    unary_expression,
    update_merge_statement,
    values_statement,
    window_expression,
    window_frame_boundary_expression,
    window_spec_expression,
    within_group_expression,
)
from snowflake.snowpark._internal.analyzer.binary_expression import (
    BinaryArithmeticExpression,
    BinaryExpression,
)
from snowflake.snowpark._internal.analyzer.binary_plan_node import Join, SetOperation
from snowflake.snowpark._internal.analyzer.datatype_mapper import (
    str_to_sql,
    to_sql,
    to_sql_without_cast,
)
from snowflake.snowpark._internal.analyzer.expression import (
    Attribute,
    CaseWhen,
    Collate,
    Expression,
    FunctionExpression,
    InExpression,
    Like,
    ListAgg,
    Literal,
    MultipleExpression,
    NamedExpression,
    RegExp,
    ScalarSubquery,
    SnowflakeUDF,
    Star,
    SubfieldInt,
    SubfieldString,
    UnresolvedAttribute,
    WithinGroup,
)
from snowflake.snowpark._internal.analyzer.grouping_set import (
    GroupingSet,
    GroupingSetsExpression,
)
from snowflake.snowpark._internal.analyzer.select_statement import Selectable
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    SnowflakePlan,
    SnowflakePlanBuilder,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    CopyIntoLocationNode,
    CopyIntoTableNode,
    Limit,
    LogicalPlan,
    Range,
    SnowflakeCreateTable,
    SnowflakeValues,
    UnresolvedRelation,
)
from snowflake.snowpark._internal.analyzer.sort_expression import SortOrder
from snowflake.snowpark._internal.analyzer.table_function import (
    FlattenFunction,
    Lateral,
    NamedArgumentsTableFunction,
    PosArgumentsTableFunction,
    TableFunctionExpression,
    TableFunctionJoin,
    TableFunctionPartitionSpecDefinition,
    TableFunctionRelation,
)
from snowflake.snowpark._internal.analyzer.table_merge_expression import (
    DeleteMergeExpression,
    InsertMergeExpression,
    TableDelete,
    TableMerge,
    TableUpdate,
    UpdateMergeExpression,
)
from snowflake.snowpark._internal.analyzer.unary_expression import (
    Alias,
    Cast,
    UnaryExpression,
    UnresolvedAlias,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import (
    Aggregate,
    CreateViewCommand,
    Filter,
    LocalTempView,
    PersistedView,
    Pivot,
    Project,
    Sample,
    Sort,
    Unpivot,
)
from snowflake.snowpark._internal.analyzer.window_expression import (
    RankRelatedFunctionExpression,
    SpecialFrameBoundary,
    SpecifiedWindowFrame,
    UnspecifiedFrame,
    WindowExpression,
    WindowSpecDefinition,
)
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.telemetry import TelemetryField
from snowflake.snowpark.types import VariantType, _NumericType

ARRAY_BIND_THRESHOLD = 512


class Analyzer:
    def __init__(self, session: "snowflake.snowpark.session.Session") -> None:
        self.session = session
        self.plan_builder = SnowflakePlanBuilder(self.session)
        self.generated_alias_maps = {}
        self.subquery_plans = []
        self.alias_maps_to_use = None

    def analyze(self, expr: Union[Expression, NamedExpression]) -> str:
        if isinstance(expr, GroupingSetsExpression):
            return grouping_set_expression(
                [[self.analyze(a) for a in arg] for arg in expr.args]
            )

        if isinstance(expr, Like):
            return like_expression(self.analyze(expr.expr), self.analyze(expr.pattern))

        if isinstance(expr, RegExp):
            return regexp_expression(
                self.analyze(expr.expr), self.analyze(expr.pattern)
            )

        if isinstance(expr, Collate):
            return collate_expression(self.analyze(expr.expr), expr.collation_spec)

        if isinstance(expr, (SubfieldString, SubfieldInt)):
            return subfield_expression(self.analyze(expr.expr), expr.field)

        if isinstance(expr, CaseWhen):
            return case_when_expression(
                [
                    (self.analyze(condition), self.analyze(value))
                    for condition, value in expr.branches
                ],
                self.analyze(expr.else_value) if expr.else_value else "NULL",
            )

        if isinstance(expr, MultipleExpression):
            return block_expression(
                [self.analyze(expression) for expression in expr.expressions]
            )

        if isinstance(expr, InExpression):
            return in_expression(
                self.analyze(expr.columns),
                [self.analyze(expression) for expression in expr.values],
            )

        if isinstance(expr, GroupingSet):
            return self.grouping_extractor(expr)

        if isinstance(expr, WindowExpression):
            return window_expression(
                self.analyze(expr.window_function), self.analyze(expr.window_spec)
            )
        if isinstance(expr, WindowSpecDefinition):
            return window_spec_expression(
                list(map(self.analyze, expr.partition_spec)),
                list(map(self.analyze, expr.order_spec)),
                self.analyze(expr.frame_spec),
            )
        if isinstance(expr, SpecifiedWindowFrame):
            return specified_window_frame_expression(
                expr.frame_type.sql,
                self.window_frame_boundary(self.to_sql_avoid_offset(expr.lower)),
                self.window_frame_boundary(self.to_sql_avoid_offset(expr.upper)),
            )
        if isinstance(expr, UnspecifiedFrame):
            return ""
        if isinstance(expr, SpecialFrameBoundary):
            return expr.sql

        if isinstance(expr, Literal):
            return to_sql(expr.value, expr.datatype)

        if isinstance(expr, Attribute):
            name = self.alias_maps_to_use.get(expr.expr_id, expr.name)
            return quote_name(name)

        if isinstance(expr, UnresolvedAttribute):
            return expr.name

        if isinstance(expr, Alias):
            quoted_name = quote_name(expr.name)
            if isinstance(expr.child, Attribute):
                self.generated_alias_maps[expr.child.expr_id] = quoted_name
                for k, v in self.alias_maps_to_use.items():
                    if v == expr.child.name:
                        self.generated_alias_maps[k] = quoted_name
            return alias_expression(self.analyze(expr.child), quoted_name)

        if isinstance(expr, FunctionExpression):
            if expr.api_call_source is not None:
                self.session._conn._telemetry_client.send_function_usage_telemetry(
                    expr.api_call_source, TelemetryField.FUNC_CAT_USAGE.value
                )
            return function_expression(
                expr.name,
                [self.to_sql_avoid_offset(c) for c in expr.children],
                expr.is_distinct,
            )

        if isinstance(expr, Star):
            if not expr.expressions:
                return "*"
            else:
                return ",".join(list(map(self.analyze, expr.expressions)))

        if isinstance(expr, SnowflakeUDF):
            if expr.api_call_source is not None:
                self.session._conn._telemetry_client.send_function_usage_telemetry(
                    expr.api_call_source, TelemetryField.FUNC_CAT_USAGE.value
                )
            return function_expression(
                expr.udf_name, list(map(self.analyze, expr.children)), False
            )

        if isinstance(expr, TableFunctionExpression):
            if expr.api_call_source is not None:
                self.session._conn._telemetry_client.send_function_usage_telemetry(
                    expr.api_call_source, TelemetryField.FUNC_CAT_USAGE.value
                )
            return self.table_function_expression_extractor(expr)

        if isinstance(expr, TableFunctionPartitionSpecDefinition):
            return table_function_partition_spec(
                expr.over,
                list(map(self.analyze, expr.partition_spec))
                if expr.partition_spec
                else [],
                list(map(self.analyze, expr.order_spec)) if expr.order_spec else [],
            )

        if isinstance(expr, UnaryExpression):
            return self.unary_expression_extractor(expr)

        if isinstance(expr, SortOrder):
            return order_expression(
                self.analyze(expr.child), expr.direction.sql, expr.null_ordering.sql
            )

        if isinstance(expr, ScalarSubquery):
            self.subquery_plans.append(expr.plan)
            return subquery_expression(expr.plan.queries[-1].sql)

        if isinstance(expr, WithinGroup):
            return within_group_expression(
                self.analyze(expr.expr), [self.analyze(e) for e in expr.order_by_cols]
            )

        if isinstance(expr, BinaryExpression):
            return self.binary_operator_extractor(expr)

        if isinstance(expr, InsertMergeExpression):
            return insert_merge_statement(
                self.analyze(expr.condition) if expr.condition else None,
                [self.analyze(k) for k in expr.keys],
                [self.analyze(v) for v in expr.values],
            )

        if isinstance(expr, UpdateMergeExpression):
            return update_merge_statement(
                self.analyze(expr.condition) if expr.condition else None,
                {self.analyze(k): self.analyze(v) for k, v in expr.assignments.items()},
            )

        if isinstance(expr, DeleteMergeExpression):
            return delete_merge_statement(
                self.analyze(expr.condition) if expr.condition else None
            )

        if isinstance(expr, ListAgg):
            return list_agg(
                self.analyze(expr.col),
                str_to_sql(expr.delimiter),
                expr.is_distinct,
            )

        if isinstance(expr, RankRelatedFunctionExpression):
            return rank_related_function_expression(
                expr.sql,
                self.analyze(expr.expr),
                expr.offset,
                self.analyze(expr.default) if expr.default else None,
                expr.ignore_nulls,
            )

        raise SnowparkClientExceptionMessages.PLAN_INVALID_TYPE(str(expr))

    def table_function_expression_extractor(self, expr: TableFunctionExpression) -> str:
        if isinstance(expr, FlattenFunction):
            return flatten_expression(
                self.analyze(expr.input),
                expr.path,
                expr.outer,
                expr.recursive,
                expr.mode,
            )
        elif isinstance(expr, PosArgumentsTableFunction):
            sql = function_expression(
                expr.func_name, [self.analyze(x) for x in expr.args], False
            )
        elif isinstance(expr, NamedArgumentsTableFunction):
            sql = named_arguments_function(
                expr.func_name,
                {key: self.analyze(value) for key, value in expr.args.items()},
            )
        else:
            raise TypeError(
                "A table function expression should be any of PosArgumentsTableFunction, NamedArgumentsTableFunction, or FlattenFunction."
            )
        partition_spec_sql = (
            self.analyze(expr.partition_spec) if expr.partition_spec else ""
        )
        return f"{sql} {partition_spec_sql}"

    def unary_expression_extractor(self, expr: UnaryExpression) -> str:
        if isinstance(expr, Alias):
            quoted_name = quote_name(expr.name)
            if isinstance(expr.child, Attribute):
                self.generated_alias_maps[expr.child.expr_id] = quoted_name
                for k, v in self.alias_maps_to_use.items():
                    if v == expr.child.name:
                        self.generated_alias_maps[k] = quoted_name
            return alias_expression(self.analyze(expr.child), quoted_name)
        if isinstance(expr, UnresolvedAlias):
            return self.analyze(expr.child)
        elif isinstance(expr, Cast):
            return cast_expression(self.analyze(expr.child), expr.to, expr.try_)
        else:
            return unary_expression(
                self.analyze(expr.child), expr.sql_operator, expr.operator_first
            )

    def binary_operator_extractor(self, expr: BinaryExpression) -> str:
        if isinstance(expr, BinaryArithmeticExpression):
            return binary_arithmetic_expression(
                expr.sql_operator, self.analyze(expr.left), self.analyze(expr.right)
            )
        else:
            return function_expression(
                expr.sql_operator,
                [self.analyze(expr.left), self.analyze(expr.right)],
                False,
            )

    def grouping_extractor(self, expr: GroupingSet) -> str:
        return self.analyze(
            FunctionExpression(
                expr.pretty_name.upper(),
                [c.child if isinstance(c, Alias) else c for c in expr.children],
                False,
            )
        )

    def window_frame_boundary(self, offset: str) -> str:
        try:
            num = int(offset)
            return window_frame_boundary_expression(str(abs(num)), num >= 0)
        except Exception:
            return offset

    def to_sql_avoid_offset(self, expr: Expression) -> str:
        # if expression is a numeric literal, return the number without casting,
        # otherwise process as normal
        if isinstance(expr, Literal) and isinstance(expr.datatype, _NumericType):
            return to_sql_without_cast(expr.value, expr.datatype)
        else:
            return self.analyze(expr)

    def resolve(self, logical_plan: LogicalPlan) -> SnowflakePlan:
        self.subquery_plans = []
        self.generated_alias_maps = {}
        result = self.do_resolve(logical_plan)

        result.add_aliases(self.generated_alias_maps)

        if self.subquery_plans:
            result = result.with_subqueries(self.subquery_plans)

        return result

    def do_resolve(self, logical_plan: LogicalPlan) -> SnowflakePlan:
        resolved_children = {}
        for c in logical_plan.children:
            resolved_children[c] = self.resolve(c)

        if isinstance(logical_plan, Selectable):
            # Selectable doesn't have children. It already has the expr_to_alias dict.
            self.alias_maps_to_use = logical_plan.expr_to_alias
        else:
            use_maps = {}
            # get counts of expr_to_alias keys
            counts = Counter()
            for v in resolved_children.values():
                if v.expr_to_alias:
                    counts.update(list(v.expr_to_alias.keys()))

            # Keep only non-shared expr_to_alias keys
            # let (df1.join(df2)).join(df2.join(df3)).select(df2) report error
            for v in resolved_children.values():
                if v.expr_to_alias:
                    use_maps.update(
                        {p: q for p, q in v.expr_to_alias.items() if counts[p] < 2}
                    )

            self.alias_maps_to_use = use_maps
        return self.do_resolve_with_resolved_children(logical_plan, resolved_children)

    def do_resolve_with_resolved_children(
        self,
        logical_plan: LogicalPlan,
        resolved_children: Dict[LogicalPlan, SnowflakePlan],
    ) -> SnowflakePlan:
        if isinstance(logical_plan, SnowflakePlan):
            return logical_plan

        if isinstance(logical_plan, TableFunctionJoin):
            return self.plan_builder.join_table_function(
                self.analyze(logical_plan.table_function),
                resolved_children[logical_plan.children[0]],
                logical_plan,
            )

        if isinstance(logical_plan, TableFunctionRelation):
            return self.plan_builder.from_table_function(
                self.analyze(logical_plan.table_function)
            )

        if isinstance(logical_plan, Lateral):
            return self.plan_builder.lateral(
                self.analyze(logical_plan.table_function),
                resolved_children[logical_plan.children[0]],
                logical_plan,
            )

        if isinstance(logical_plan, Aggregate):
            return self.plan_builder.aggregate(
                list(map(self.to_sql_avoid_offset, logical_plan.grouping_expressions)),
                list(map(self.analyze, logical_plan.aggregate_expressions)),
                resolved_children[logical_plan.child],
                logical_plan,
            )

        if isinstance(logical_plan, Project):
            return self.plan_builder.project(
                list(map(self.analyze, logical_plan.project_list)),
                resolved_children[logical_plan.child],
                logical_plan,
            )

        if isinstance(logical_plan, Filter):
            return self.plan_builder.filter(
                self.analyze(logical_plan.condition),
                resolved_children[logical_plan.child],
                logical_plan,
            )

        # Add a sample stop to the plan being built
        if isinstance(logical_plan, Sample):
            return self.plan_builder.sample(
                resolved_children[logical_plan.child],
                logical_plan,
                logical_plan.probability_fraction,
                logical_plan.row_count,
            )

        if isinstance(logical_plan, Join):
            return self.plan_builder.join(
                resolved_children[logical_plan.left],
                resolved_children[logical_plan.right],
                logical_plan.join_type,
                self.analyze(logical_plan.condition) if logical_plan.condition else "",
                logical_plan,
            )

        if isinstance(logical_plan, Sort):
            return self.plan_builder.sort(
                list(map(self.analyze, logical_plan.order)),
                resolved_children[logical_plan.child],
                logical_plan,
            )

        if isinstance(logical_plan, SetOperation):
            return self.plan_builder.set_operator(
                resolved_children[logical_plan.left],
                resolved_children[logical_plan.right],
                logical_plan.sql,
                logical_plan,
            )

        if isinstance(logical_plan, Range):
            # schema of Range. Since this corresponds to the Snowflake column "id"
            # (quoted lower-case) it's a little hard for users. So we switch it to
            # the column name "ID" == id == Id
            return self.plan_builder.query(
                range_statement(
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
                        values_statement(logical_plan.output, logical_plan.data),
                        logical_plan,
                    )
                else:
                    return self.plan_builder.large_local_relation_plan(
                        logical_plan.output, logical_plan.data, logical_plan
                    )
            else:
                return self.plan_builder.query(
                    empty_values_statement(logical_plan.output),
                    logical_plan,
                )

        if isinstance(logical_plan, UnresolvedRelation):
            return self.plan_builder.table(logical_plan.name)

        if isinstance(logical_plan, SnowflakeCreateTable):
            return self.plan_builder.save_as_table(
                logical_plan.table_name,
                logical_plan.column_names,
                logical_plan.mode,
                logical_plan.table_type,
                resolved_children[logical_plan.children[0]],
            )

        if isinstance(logical_plan, Limit):
            if isinstance(logical_plan.child, Sort):
                on_top_of_order_by = True
            elif (
                isinstance(logical_plan.child, SnowflakePlan)
                and logical_plan.child.source_plan
            ):
                on_top_of_order_by = isinstance(logical_plan.child.source_plan, Sort)
            else:
                on_top_of_order_by = False

            return self.plan_builder.limit(
                self.to_sql_avoid_offset(logical_plan.limit_expr),
                self.to_sql_avoid_offset(logical_plan.offset_expr),
                resolved_children[logical_plan.child],
                on_top_of_order_by,
                logical_plan,
            )

        if isinstance(logical_plan, Pivot):
            if len(logical_plan.aggregates) != 1:
                raise ValueError("Only one aggregate is supported with pivot")

            return self.plan_builder.pivot(
                self.analyze(logical_plan.pivot_column),
                [self.analyze(pv) for pv in logical_plan.pivot_values],
                self.analyze(logical_plan.aggregates[0]),
                resolved_children[logical_plan.child],
                logical_plan,
            )

        if isinstance(logical_plan, Unpivot):
            return self.plan_builder.unpivot(
                logical_plan.value_column,
                logical_plan.name_column,
                [self.analyze(c) for c in logical_plan.column_list],
                resolved_children[logical_plan.child],
                logical_plan,
            )

        if isinstance(logical_plan, CreateViewCommand):
            if isinstance(logical_plan.view_type, PersistedView):
                is_temp = False
            elif isinstance(logical_plan.view_type, LocalTempView):
                is_temp = True
            else:
                raise SnowparkClientExceptionMessages.PLAN_ANALYZER_UNSUPPORTED_VIEW_TYPE(
                    str(logical_plan.view_type)
                )

            return self.plan_builder.create_or_replace_view(
                logical_plan.name, resolved_children[logical_plan.child], is_temp
            )

        if isinstance(logical_plan, CopyIntoTableNode):
            if logical_plan.format_type_options is None:
                format_type_options = dict()
            else:
                format_type_options = logical_plan.format_type_options.copy()
            format_name = logical_plan.cur_options.get("FORMAT_NAME")
            if format_name is not None:
                format_type_options["FORMAT_NAME"] = format_name
            if logical_plan.table_name:
                return self.plan_builder.copy_into_table(
                    path=logical_plan.file_path,
                    table_name=logical_plan.table_name,
                    files=logical_plan.files,
                    pattern=logical_plan.pattern,
                    file_format=logical_plan.file_format,
                    format_type_options=format_type_options,
                    copy_options=logical_plan.copy_options,
                    validation_mode=logical_plan.validation_mode,
                    column_names=logical_plan.column_names,
                    transformations=[
                        self.analyze(x) for x in logical_plan.transformations
                    ]
                    if logical_plan.transformations
                    else None,
                    user_schema=logical_plan.user_schema,
                    create_table_from_infer_schema=logical_plan.create_table_from_infer_schema,
                )
            elif logical_plan.file_format and logical_plan.file_format.upper() == "CSV":
                if not logical_plan.user_schema:
                    raise SnowparkClientExceptionMessages.DF_MUST_PROVIDE_SCHEMA_FOR_READING_FILE()
                else:
                    return self.plan_builder.read_file(
                        logical_plan.files,
                        logical_plan.file_format,
                        logical_plan.cur_options,
                        self.session.get_fully_qualified_current_schema(),
                        logical_plan.user_schema._to_attributes(),
                    )
            else:
                schema = (
                    logical_plan.user_schema._to_attributes()
                    if logical_plan.user_schema
                    else [Attribute('"$1"', VariantType())]
                )
                return self.plan_builder.read_file(
                    logical_plan.files,
                    logical_plan.file_format,
                    logical_plan.cur_options,
                    self.session.get_fully_qualified_current_schema(),
                    schema,
                )

        if isinstance(logical_plan, CopyIntoLocationNode):
            return self.plan_builder.copy_into_location(
                query=resolved_children[logical_plan.child],
                stage_location=logical_plan.stage_location,
                partition_by=self.analyze(logical_plan.partition_by)
                if logical_plan.partition_by
                else None,
                file_format_name=logical_plan.file_format_name,
                file_format_type=logical_plan.file_format_type,
                format_type_options=logical_plan.format_type_options,
                header=logical_plan.header,
                **logical_plan.copy_options,
            )

        if isinstance(logical_plan, TableUpdate):
            return self.plan_builder.update(
                logical_plan.table_name,
                {
                    self.analyze(k): self.analyze(v)
                    for k, v in logical_plan.assignments.items()
                },
                self.analyze(logical_plan.condition)
                if logical_plan.condition
                else None,
                resolved_children.get(logical_plan.source_data, None),
                logical_plan,
            )

        if isinstance(logical_plan, TableDelete):
            return self.plan_builder.delete(
                logical_plan.table_name,
                self.analyze(logical_plan.condition)
                if logical_plan.condition
                else None,
                resolved_children.get(logical_plan.source_data, None),
                logical_plan,
            )

        if isinstance(logical_plan, TableMerge):
            return self.plan_builder.merge(
                logical_plan.table_name,
                resolved_children.get(logical_plan.source),
                self.analyze(logical_plan.join_expr),
                [self.analyze(c) for c in logical_plan.clauses],
                logical_plan,
            )

        if isinstance(logical_plan, Selectable):
            return self.plan_builder.select_statement(logical_plan)
