#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import uuid
from collections import Counter, defaultdict
from typing import TYPE_CHECKING, DefaultDict, Dict, List, Union
from logging import getLogger

from snowflake.connector import IntegrityError

import snowflake.snowpark
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    alias_expression,
    binary_arithmetic_expression,
    block_expression,
    case_when_expression,
    cast_expression,
    collate_expression,
    column_sum,
    delete_merge_statement,
    empty_values_statement,
    flatten_expression,
    function_expression,
    grouping_set_expression,
    in_expression,
    insert_merge_statement,
    like_expression,
    list_agg,
    model_expression,
    service_expression,
    named_arguments_function,
    order_expression,
    range_statement,
    rank_related_function_expression,
    regexp_expression,
    schema_query_for_values_statement,
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
from snowflake.snowpark._internal.analyzer.binary_plan_node import (
    FullOuter,
    Join,
    SetOperation,
    UsingJoin,
)
from snowflake.snowpark._internal.analyzer.datatype_mapper import (
    numeric_to_sql_without_cast,
    str_to_sql,
    to_sql,
)
from snowflake.snowpark._internal.analyzer.expression import (
    Attribute,
    CaseWhen,
    Collate,
    ColumnSum,
    Expression,
    FunctionExpression,
    InExpression,
    Interval,
    Like,
    ListAgg,
    Literal,
    ModelExpression,
    ServiceExpression,
    MultipleExpression,
    NamedExpression,
    NamedFunctionExpression,
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
from snowflake.snowpark._internal.analyzer.select_statement import (
    Selectable,
    SelectableEntity,
    SelectSnowflakePlan,
    SelectStatement,
    SelectTableFunction,
)
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
    ReadFileNode,
    SnowflakeCreateTable,
    SnowflakeTable,
    SnowflakeValues,
)
from snowflake.snowpark._internal.analyzer.sort_expression import SortOrder
from snowflake.snowpark._internal.analyzer.table_function import (
    FlattenFunction,
    GeneratorTableFunction,
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
    _InternalAlias,
    Alias,
    Cast,
    UnaryExpression,
    UnaryMinus,
    UnresolvedAlias,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import (
    Aggregate,
    CreateDynamicTableCommand,
    CreateViewCommand,
    Distinct,
    Filter,
    LocalTempView,
    PersistedView,
    Pivot,
    Project,
    Rename,
    Sample,
    SampleBy,
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
from snowflake.snowpark._internal.utils import (
    quote_name,
    merge_multiple_snowflake_plan_expr_to_alias,
    ExprAliasUpdateDict,
)
from snowflake.snowpark.types import BooleanType, _NumericType

ARRAY_BIND_THRESHOLD = 512

if TYPE_CHECKING:
    import snowflake.snowpark.session


_logger = getLogger(__name__)


class Analyzer:
    def __init__(self, session: "snowflake.snowpark.session.Session") -> None:
        self.session = session
        self.plan_builder = SnowflakePlanBuilder(self.session)
        self.subquery_plans = []
        if session._join_alias_fix:
            self.generated_alias_maps = ExprAliasUpdateDict()
            self.alias_maps_to_use = ExprAliasUpdateDict()
        else:
            self.generated_alias_maps = {}
            self.alias_maps_to_use: Dict[uuid.UUID, str] = {}

    def analyze(
        self,
        expr: Union[Expression, NamedExpression],
        df_aliased_col_name_to_real_col_name: Union[
            DefaultDict[str, Dict[str, str]], DefaultDict[str, ExprAliasUpdateDict]
        ],
        parse_local_name=False,
    ) -> str:
        if isinstance(expr, GroupingSetsExpression):
            return grouping_set_expression(
                [
                    [
                        self.analyze(
                            a, df_aliased_col_name_to_real_col_name, parse_local_name
                        )
                        for a in arg
                    ]
                    for arg in expr.args
                ]
            )

        if isinstance(expr, Like):
            return like_expression(
                self.analyze(
                    self.internal_alias_extractor(expr.expr),
                    df_aliased_col_name_to_real_col_name,
                    parse_local_name,
                ),
                self.analyze(
                    expr.pattern, df_aliased_col_name_to_real_col_name, parse_local_name
                ),
            )

        if isinstance(expr, RegExp):
            return regexp_expression(
                self.analyze(
                    self.internal_alias_extractor(expr.expr),
                    df_aliased_col_name_to_real_col_name,
                    parse_local_name,
                ),
                self.analyze(
                    expr.pattern, df_aliased_col_name_to_real_col_name, parse_local_name
                ),
                self.analyze(
                    expr.parameters,
                    df_aliased_col_name_to_real_col_name,
                    parse_local_name,
                )
                if expr.parameters is not None
                else None,
            )

        if isinstance(expr, Collate):
            collation_spec = (
                expr.collation_spec.upper() if parse_local_name else expr.collation_spec
            )
            return collate_expression(
                self.analyze(
                    self.internal_alias_extractor(expr.expr),
                    df_aliased_col_name_to_real_col_name,
                    parse_local_name,
                ),
                collation_spec,
            )

        if isinstance(expr, (SubfieldString, SubfieldInt)):
            field = expr.field
            if parse_local_name and isinstance(field, str):
                field = field.upper()
            return subfield_expression(
                self.analyze(
                    self.internal_alias_extractor(expr.expr),
                    df_aliased_col_name_to_real_col_name,
                    parse_local_name,
                ),
                field,
            )

        if isinstance(expr, CaseWhen):
            return case_when_expression(
                [
                    (
                        self.analyze(
                            self.internal_alias_extractor(condition),
                            df_aliased_col_name_to_real_col_name,
                            parse_local_name,
                        ),
                        self.analyze(
                            value,
                            df_aliased_col_name_to_real_col_name,
                            parse_local_name,
                        ),
                    )
                    for condition, value in expr.branches
                ],
                self.analyze(
                    self.internal_alias_extractor(expr.else_value),
                    df_aliased_col_name_to_real_col_name,
                    parse_local_name,
                )
                if expr.else_value
                else "NULL",
            )

        if isinstance(expr, MultipleExpression):
            block_expressions = []
            for expression in expr.expressions:
                if self.session.eliminate_numeric_sql_value_cast_enabled:
                    resolved_expr = self.to_sql_try_avoid_cast(
                        expression,
                        df_aliased_col_name_to_real_col_name,
                        parse_local_name,
                    )
                else:
                    resolved_expr = self.analyze(
                        expression,
                        df_aliased_col_name_to_real_col_name,
                        parse_local_name,
                    )

                block_expressions.append(resolved_expr)
            return block_expression(block_expressions)

        if isinstance(expr, InExpression):
            in_values = []
            for expression in expr.values:
                if self.session.eliminate_numeric_sql_value_cast_enabled:
                    in_value = self.to_sql_try_avoid_cast(
                        self.internal_alias_extractor(expression),
                        df_aliased_col_name_to_real_col_name,
                        parse_local_name,
                    )
                else:
                    in_value = self.analyze(
                        self.internal_alias_extractor(expression),
                        df_aliased_col_name_to_real_col_name,
                        parse_local_name,
                    )

                in_values.append(in_value)
            return in_expression(
                self.analyze(
                    self.internal_alias_extractor(expr.columns),
                    df_aliased_col_name_to_real_col_name,
                    parse_local_name,
                ),
                in_values,
            )

        if isinstance(expr, GroupingSet):
            return self.grouping_extractor(expr, df_aliased_col_name_to_real_col_name)

        if isinstance(expr, WindowExpression):
            return window_expression(
                self.analyze(
                    expr.window_function,
                    df_aliased_col_name_to_real_col_name,
                    parse_local_name,
                ),
                self.analyze(
                    expr.window_spec,
                    df_aliased_col_name_to_real_col_name,
                    parse_local_name,
                ),
            )
        if isinstance(expr, WindowSpecDefinition):
            return window_spec_expression(
                [
                    self.analyze(
                        x, df_aliased_col_name_to_real_col_name, parse_local_name
                    )
                    for x in expr.partition_spec
                ],
                [
                    self.analyze(
                        x, df_aliased_col_name_to_real_col_name, parse_local_name
                    )
                    for x in expr.order_spec
                ],
                self.analyze(
                    expr.frame_spec,
                    df_aliased_col_name_to_real_col_name,
                    parse_local_name,
                ),
            )
        if isinstance(expr, SpecifiedWindowFrame):
            return specified_window_frame_expression(
                expr.frame_type.sql,
                self.window_frame_boundary(
                    expr.lower, df_aliased_col_name_to_real_col_name
                ),
                self.window_frame_boundary(
                    expr.upper, df_aliased_col_name_to_real_col_name
                ),
            )
        if isinstance(expr, UnspecifiedFrame):
            return ""
        if isinstance(expr, SpecialFrameBoundary):
            return expr.sql

        if isinstance(expr, Literal):
            sql = to_sql(expr.value, expr.datatype)
            if parse_local_name:
                sql = sql.upper()
            return sql

        if isinstance(expr, Interval):
            return expr.sql

        if isinstance(expr, Attribute):
            name = self.alias_maps_to_use.get(expr.expr_id, expr.name)
            return quote_name(name)

        if isinstance(expr, UnresolvedAttribute):
            if expr.df_alias:
                if expr.df_alias in df_aliased_col_name_to_real_col_name:
                    return df_aliased_col_name_to_real_col_name[expr.df_alias].get(
                        expr.name, expr.name
                    )
                else:
                    raise SnowparkClientExceptionMessages.DF_ALIAS_NOT_RECOGNIZED(
                        expr.df_alias
                    )
            return expr.name

        if isinstance(expr, ModelExpression):
            return model_expression(
                expr.model_name,
                expr.version_or_alias_name,
                expr.method_name,
                [
                    self.to_sql_try_avoid_cast(c, df_aliased_col_name_to_real_col_name)
                    for c in expr.children
                ],
            )

        if isinstance(expr, ServiceExpression):
            return service_expression(
                expr.service_name,
                expr.method_name,
                [
                    self.to_sql_try_avoid_cast(c, df_aliased_col_name_to_real_col_name)
                    for c in expr.children
                ],
            )

        if isinstance(expr, FunctionExpression):
            if expr.api_call_source is not None:
                self.session._conn._telemetry_client.send_function_usage_telemetry(
                    expr.api_call_source, TelemetryField.FUNC_CAT_USAGE.value
                )
            func_name = expr.name.upper() if parse_local_name else expr.name
            return function_expression(
                func_name,
                [
                    self.to_sql_try_avoid_cast(c, df_aliased_col_name_to_real_col_name)
                    for c in expr.children
                ],
                expr.is_distinct,
            )

        if isinstance(expr, NamedFunctionExpression):
            if expr.api_call_source is not None:
                self.session._conn._telemetry_client.send_function_usage_telemetry(
                    expr.api_call_source, TelemetryField.FUNC_CAT_USAGE.value
                )
            func_name = expr.name.upper() if parse_local_name else expr.name
            return named_arguments_function(
                func_name,
                {
                    key: self.to_sql_try_avoid_cast(
                        value, df_aliased_col_name_to_real_col_name
                    )
                    for key, value in expr.named_arguments.items()
                },
            )

        if isinstance(expr, Star):
            if expr.df_alias:
                # This is only hit by col(<df_alias>)
                if expr.df_alias not in df_aliased_col_name_to_real_col_name:
                    raise SnowparkClientExceptionMessages.DF_ALIAS_NOT_RECOGNIZED(
                        expr.df_alias
                    )
                columns = df_aliased_col_name_to_real_col_name[expr.df_alias]
                return ",".join(columns.values())
            if not expr.expressions:
                return "*"
            else:
                # This case is hit by df.col("*")
                return ",".join(
                    [
                        self.analyze(e, df_aliased_col_name_to_real_col_name)
                        for e in expr.expressions
                    ]
                )

        if isinstance(expr, SnowflakeUDF):
            if expr.api_call_source is not None:
                self.session._conn._telemetry_client.send_function_usage_telemetry(
                    expr.api_call_source, TelemetryField.FUNC_CAT_USAGE.value
                )
            func_name = expr.udf_name.upper() if parse_local_name else expr.udf_name
            return function_expression(
                func_name,
                [
                    self.analyze(
                        self.internal_alias_extractor(x),
                        df_aliased_col_name_to_real_col_name,
                        parse_local_name,
                    )
                    for x in expr.children
                ],
                False,
            )

        if isinstance(expr, TableFunctionExpression):
            if expr.api_call_source is not None:
                self.session._conn._telemetry_client.send_function_usage_telemetry(
                    expr.api_call_source, TelemetryField.FUNC_CAT_USAGE.value
                )
            return self.table_function_expression_extractor(
                expr, df_aliased_col_name_to_real_col_name
            )

        if isinstance(expr, TableFunctionPartitionSpecDefinition):
            return table_function_partition_spec(
                expr.over,
                [
                    self.analyze(
                        x, df_aliased_col_name_to_real_col_name, parse_local_name
                    )
                    for x in expr.partition_spec
                ]
                if expr.partition_spec
                else [],
                [
                    self.analyze(
                        x, df_aliased_col_name_to_real_col_name, parse_local_name
                    )
                    for x in expr.order_spec
                ]
                if expr.order_spec
                else [],
            )

        if isinstance(expr, UnaryExpression):
            return self.unary_expression_extractor(
                expr, df_aliased_col_name_to_real_col_name, parse_local_name
            )

        if isinstance(expr, SortOrder):
            return order_expression(
                self.analyze(
                    self.internal_alias_extractor(expr.child),
                    df_aliased_col_name_to_real_col_name,
                    parse_local_name,
                ),
                expr.direction.sql,
                expr.null_ordering.sql,
            )

        if isinstance(expr, ScalarSubquery):
            self.subquery_plans.append(expr.plan)
            return subquery_expression(expr.plan.queries[-1].sql)

        if isinstance(expr, WithinGroup):
            return within_group_expression(
                self.analyze(
                    self.internal_alias_extractor(expr.expr),
                    df_aliased_col_name_to_real_col_name,
                    parse_local_name,
                ),
                [
                    self.analyze(e, df_aliased_col_name_to_real_col_name)
                    for e in expr.order_by_cols
                ],
            )

        if isinstance(expr, BinaryExpression):
            return self.binary_operator_extractor(
                expr, df_aliased_col_name_to_real_col_name, parse_local_name
            )

        if isinstance(expr, InsertMergeExpression):
            return insert_merge_statement(
                self.analyze(expr.condition, df_aliased_col_name_to_real_col_name)
                if expr.condition
                else None,
                [
                    self.analyze(k, df_aliased_col_name_to_real_col_name)
                    for k in expr.keys
                ],
                [
                    self.analyze(v, df_aliased_col_name_to_real_col_name)
                    for v in expr.values
                ],
            )

        if isinstance(expr, UpdateMergeExpression):
            return update_merge_statement(
                self.analyze(expr.condition, df_aliased_col_name_to_real_col_name)
                if expr.condition
                else None,
                {
                    self.analyze(k, df_aliased_col_name_to_real_col_name): self.analyze(
                        v, df_aliased_col_name_to_real_col_name
                    )
                    for k, v in expr.assignments.items()
                },
            )

        if isinstance(expr, DeleteMergeExpression):
            return delete_merge_statement(
                self.analyze(expr.condition, df_aliased_col_name_to_real_col_name)
                if expr.condition
                else None
            )

        if isinstance(expr, ListAgg):
            return list_agg(
                self.analyze(
                    self.internal_alias_extractor(expr.col),
                    df_aliased_col_name_to_real_col_name,
                    parse_local_name,
                ),
                str_to_sql(expr.delimiter),
                expr.is_distinct,
            )

        if isinstance(expr, ColumnSum):
            return column_sum(
                [
                    self.analyze(
                        col, df_aliased_col_name_to_real_col_name, parse_local_name
                    )
                    for col in expr.exprs
                ]
            )

        if isinstance(expr, RankRelatedFunctionExpression):
            return rank_related_function_expression(
                expr.sql,
                self.analyze(
                    self.internal_alias_extractor(expr.expr),
                    df_aliased_col_name_to_real_col_name,
                    parse_local_name,
                ),
                expr.offset,
                self.analyze(
                    expr.default, df_aliased_col_name_to_real_col_name, parse_local_name
                )
                if expr.default
                else None,
                expr.ignore_nulls,
            )

        raise SnowparkClientExceptionMessages.PLAN_INVALID_TYPE(
            str(expr)
        )  # pragma: no cover

    def internal_alias_extractor(self, expr: Expression) -> Expression:
        """
        This function is used to extract the internal alias of an expression. This function
        needs to be called whenever an expr is coming from a Column object. This is done because
        _InternalAlias is generated for implementing a few functions on the client-side with
        the final output column being aliased internally. Such internal aliases need to be
        dropped when they are not applied at the top level in a sql nesting level.
        """
        if isinstance(expr, _InternalAlias):
            return expr.child
        return expr

    def table_function_expression_extractor(
        self,
        expr: TableFunctionExpression,
        df_aliased_col_name_to_real_col_name: Union[
            DefaultDict[str, Dict[str, str]], DefaultDict[str, ExprAliasUpdateDict]
        ],
        parse_local_name=False,
    ) -> str:
        if isinstance(expr, FlattenFunction):
            return flatten_expression(
                self.analyze(
                    expr.input, df_aliased_col_name_to_real_col_name, parse_local_name
                ),
                expr.path,
                expr.outer,
                expr.recursive,
                expr.mode,
            )
        elif isinstance(expr, PosArgumentsTableFunction):
            sql = function_expression(
                expr.func_name,
                [
                    self.analyze(
                        x, df_aliased_col_name_to_real_col_name, parse_local_name
                    )
                    for x in expr.args
                ],
                False,
            )
        elif isinstance(expr, (NamedArgumentsTableFunction, GeneratorTableFunction)):
            sql = named_arguments_function(
                expr.func_name,
                {
                    key: self.to_sql_try_avoid_cast(
                        value, df_aliased_col_name_to_real_col_name, parse_local_name
                    )
                    for key, value in expr.args.items()
                },
            )
        else:  # pragma: no cover
            raise TypeError(
                "A table function expression should be any of PosArgumentsTableFunction, "
                "NamedArgumentsTableFunction, GeneratorTableFunction, or FlattenFunction."
            )
        partition_spec_sql = (
            self.analyze(expr.partition_spec, df_aliased_col_name_to_real_col_name)
            if expr.partition_spec
            else ""
        )
        return f"{sql} {partition_spec_sql}"

    def unary_expression_extractor(
        self,
        expr: UnaryExpression,
        df_aliased_col_name_to_real_col_name: Union[
            DefaultDict[str, Dict[str, str]], DefaultDict[str, ExprAliasUpdateDict]
        ],
        parse_local_name=False,
    ) -> str:
        if isinstance(expr, Alias):
            quoted_name = quote_name(expr.name)
            if isinstance(expr.child, Attribute):
                # When resolving an alias, we not only track the current (expr_id,alias) info, but also update the existing
                # (expr_id,alias) pairs whose alias is equal to expr.child.name in the alias_maps_to_use map.
                # This is because we create new attribute with new expr id in the new plan,
                # and we want to make sure that the existing attribute can still be resolved to the latest alias when
                # we resolve the child plan.
                # an example is the test case: test_name_alias_on_multiple_join
                # with the _join_alias_fix enabled, we will track also whether an entry is updated (boolean)
                # due to inheritance from other plans. this info helps decide which entries to keep during deduplication
                updated_due_to_inheritance = (
                    (quoted_name, True) if self.session._join_alias_fix else quoted_name
                )
                self.generated_alias_maps[expr.child.expr_id] = quoted_name
                assert self.alias_maps_to_use is not None
                for k, v in self.alias_maps_to_use.items():
                    if v == expr.child.name:
                        self.generated_alias_maps[k] = updated_due_to_inheritance

                for df_alias_dict in df_aliased_col_name_to_real_col_name.values():
                    for k, v in df_alias_dict.items():
                        if v == expr.child.name:
                            df_alias_dict[k] = updated_due_to_inheritance  # type: ignore
            return alias_expression(
                self.analyze(
                    expr.child, df_aliased_col_name_to_real_col_name, parse_local_name
                ),
                quoted_name,
            )

        child = self.internal_alias_extractor(expr.child)
        if isinstance(expr, UnresolvedAlias):
            expr_str = self.analyze(
                child, df_aliased_col_name_to_real_col_name, parse_local_name
            )
            if parse_local_name:
                expr_str = expr_str.upper()
            return expr_str
        elif isinstance(expr, Cast):
            return cast_expression(
                self.analyze(
                    child, df_aliased_col_name_to_real_col_name, parse_local_name
                ),
                expr.to,
                expr.try_,
                expr.is_rename,
                expr.is_add,
            )
        else:
            return unary_expression(
                self.analyze(
                    child, df_aliased_col_name_to_real_col_name, parse_local_name
                ),
                expr.sql_operator,
                expr.operator_first,
            )

    def binary_operator_extractor(
        self,
        expr: BinaryExpression,
        df_aliased_col_name_to_real_col_name,
        parse_local_name=False,
    ) -> str:
        left = self.internal_alias_extractor(expr.left)
        right = self.internal_alias_extractor(expr.right)
        if self.session.eliminate_numeric_sql_value_cast_enabled:
            left_sql_expr = self.to_sql_try_avoid_cast(
                left, df_aliased_col_name_to_real_col_name, parse_local_name
            )
            right_sql_expr = self.to_sql_try_avoid_cast(
                right,
                df_aliased_col_name_to_real_col_name,
                parse_local_name,
            )
        else:
            left_sql_expr = self.analyze(
                left, df_aliased_col_name_to_real_col_name, parse_local_name
            )
            right_sql_expr = self.analyze(
                right, df_aliased_col_name_to_real_col_name, parse_local_name
            )
        if isinstance(expr, BinaryArithmeticExpression):
            return binary_arithmetic_expression(
                expr.sql_operator,
                left_sql_expr,
                right_sql_expr,
            )
        else:
            return function_expression(
                expr.sql_operator,
                [
                    left_sql_expr,
                    right_sql_expr,
                ],
                False,
            )

    def grouping_extractor(
        self, expr: GroupingSet, df_aliased_col_name_to_real_col_name
    ) -> str:
        return self.analyze(
            FunctionExpression(
                expr.pretty_name.upper(),
                [c.child if isinstance(c, Alias) else c for c in expr.children],
                False,
            ),
            df_aliased_col_name_to_real_col_name,
        )

    def window_frame_boundary(
        self,
        boundary: Expression,
        df_aliased_col_name_to_real_col_name: Union[
            DefaultDict[str, Dict[str, str]], DefaultDict[str, ExprAliasUpdateDict]
        ],
    ) -> str:
        # it means interval preceding
        if isinstance(boundary, UnaryMinus) and isinstance(boundary.child, Interval):
            return window_frame_boundary_expression(
                boundary.child.sql, is_following=False
            )
        elif isinstance(boundary, Interval):
            return window_frame_boundary_expression(boundary.sql, is_following=True)
        else:
            # boundary should be an integer
            offset = self.to_sql_try_avoid_cast(
                boundary, df_aliased_col_name_to_real_col_name
            )
            try:
                num = int(offset)
                return window_frame_boundary_expression(str(abs(num)), num >= 0)
            except Exception:
                return offset

    def to_sql_try_avoid_cast(
        self,
        expr: Expression,
        df_aliased_col_name_to_real_col_name: Union[
            DefaultDict[str, Dict[str, str]], DefaultDict[str, ExprAliasUpdateDict]
        ],
        parse_local_name: bool = False,
    ) -> str:
        """
        Convert the expression to sql and try to avoid cast expression if possible when
        the expression is:
        1) a literal expression
        2) the literal expression is numeric type
        """
        # if expression is a numeric literal, return the number without casting,
        # otherwise process as normal
        if isinstance(expr, Literal) and isinstance(expr.datatype, _NumericType):
            return numeric_to_sql_without_cast(expr.value, expr.datatype)
        elif (
            isinstance(expr, Literal)
            and isinstance(expr.datatype, BooleanType)
            and isinstance(expr.value, bool)
        ):
            return str(expr.value).upper()
        else:
            return self.analyze(
                self.internal_alias_extractor(expr),
                df_aliased_col_name_to_real_col_name,
                parse_local_name,
            )

    def resolve(self, logical_plan: LogicalPlan) -> SnowflakePlan:
        self.subquery_plans = []
        self.generated_alias_maps = (
            ExprAliasUpdateDict() if self.session._join_alias_fix else {}
        )

        result = self.do_resolve(logical_plan)

        result.add_aliases(self.generated_alias_maps)

        if self.subquery_plans:
            result = result.with_subqueries(self.subquery_plans)
            # Perform in-place update of the pre and post actions for selectable
            # if it has subqueries. Also updated attached resolved snowflake plan
            # for the selectable
            if isinstance(logical_plan, Selectable):
                logical_plan.with_subqueries(self.subquery_plans, result)

        return result

    def do_resolve(self, logical_plan: LogicalPlan) -> SnowflakePlan:
        resolved_children = {}
        df_aliased_col_name_to_real_col_name = (
            defaultdict(ExprAliasUpdateDict)
            if self.session._join_alias_fix
            else defaultdict(dict)
        )

        for c in logical_plan.children:  # post-order traversal of the tree
            resolved = self.resolve(c)
            df_aliased_col_name_to_real_col_name.update(resolved.df_aliased_col_name_to_real_col_name)  # type: ignore
            resolved_children[c] = resolved

        if isinstance(logical_plan, Selectable):
            # Selectable doesn't have children. It already has the expr_to_alias dict.
            self.alias_maps_to_use = logical_plan.expr_to_alias.copy()
        else:
            if self.session._join_alias_fix:
                self.alias_maps_to_use = merge_multiple_snowflake_plan_expr_to_alias(
                    list(resolved_children.values())
                )
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

        res = self.do_resolve_with_resolved_children(
            logical_plan, resolved_children, df_aliased_col_name_to_real_col_name
        )
        res.df_aliased_col_name_to_real_col_name.update(
            df_aliased_col_name_to_real_col_name
        )
        return res

    def do_resolve_with_resolved_children(
        self,
        logical_plan: LogicalPlan,
        resolved_children: Dict[LogicalPlan, SnowflakePlan],
        df_aliased_col_name_to_real_col_name: Union[
            DefaultDict[str, Dict[str, str]], DefaultDict[str, ExprAliasUpdateDict]
        ],
    ) -> SnowflakePlan:
        if isinstance(logical_plan, SnowflakePlan):
            return logical_plan

        if isinstance(logical_plan, TableFunctionJoin):
            return self.plan_builder.join_table_function(
                self.analyze(
                    logical_plan.table_function, df_aliased_col_name_to_real_col_name
                ),
                resolved_children[logical_plan.children[0]],
                logical_plan,
                logical_plan.left_cols,
                logical_plan.right_cols,
                self.session.conf.get("use_constant_subquery_alias", False),
            )

        if isinstance(logical_plan, TableFunctionRelation):
            return self.plan_builder.from_table_function(
                self.analyze(
                    logical_plan.table_function, df_aliased_col_name_to_real_col_name
                ),
                logical_plan,
            )

        if isinstance(logical_plan, Lateral):
            return self.plan_builder.lateral(
                self.analyze(
                    logical_plan.table_function, df_aliased_col_name_to_real_col_name
                ),
                resolved_children[logical_plan.children[0]],
                logical_plan,
            )

        if isinstance(logical_plan, Aggregate):
            return self.plan_builder.aggregate(
                [
                    self.to_sql_try_avoid_cast(
                        expr, df_aliased_col_name_to_real_col_name
                    )
                    for expr in logical_plan.grouping_expressions
                ],
                [
                    self.analyze(expr, df_aliased_col_name_to_real_col_name)
                    for expr in logical_plan.aggregate_expressions
                ],
                resolved_children[logical_plan.child],
                logical_plan,
            )

        if isinstance(logical_plan, Project):
            return self.plan_builder.project(
                list(
                    map(
                        lambda x: self.analyze(x, df_aliased_col_name_to_real_col_name),
                        logical_plan.project_list,
                    )
                ),
                resolved_children[logical_plan.child],
                logical_plan,
                ilike_pattern=logical_plan.ilike_pattern,
            )

        if isinstance(logical_plan, Distinct):
            return self.plan_builder.project(
                [],
                child=resolved_children[logical_plan.child],
                source_plan=logical_plan,
                is_distinct=True,
            )

        if isinstance(logical_plan, Filter):
            return self.plan_builder.filter(
                self.analyze(
                    logical_plan.condition, df_aliased_col_name_to_real_col_name
                ),
                logical_plan.is_having,
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

        if isinstance(logical_plan, SampleBy):
            return self.plan_builder.sample_by(
                resolved_children[logical_plan.child],
                logical_plan,
                self.analyze(logical_plan.col, df_aliased_col_name_to_real_col_name),
                logical_plan.fractions,
            )

        if isinstance(logical_plan, Join):
            join_condition = (
                self.analyze(
                    logical_plan.join_condition, df_aliased_col_name_to_real_col_name
                )
                if logical_plan.join_condition
                else ""
            )
            match_condition = (
                self.analyze(
                    logical_plan.match_condition, df_aliased_col_name_to_real_col_name
                )
                if logical_plan.match_condition
                else ""
            )
            return self.plan_builder.join(
                resolved_children[logical_plan.left],
                resolved_children[logical_plan.right],
                logical_plan.join_type,
                join_condition,
                match_condition,
                logical_plan,
                self.session.conf.get("use_constant_subquery_alias", False),
            )

        if isinstance(logical_plan, Sort):
            return self.plan_builder.sort(
                [
                    self.analyze(x, df_aliased_col_name_to_real_col_name)
                    for x in logical_plan.order
                ],
                logical_plan.is_order_by_append,
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
            if logical_plan.schema_query:
                schema_query = logical_plan.schema_query
            else:
                schema_query = schema_query_for_values_statement(logical_plan.output)

            if logical_plan.data:
                if not logical_plan.is_large_local_data:
                    if logical_plan.is_contain_illegal_null_value:
                        raise IntegrityError("NULL result in a non-nullable column")
                    return self.plan_builder.query(
                        values_statement(logical_plan.output, logical_plan.data),
                        logical_plan,
                        schema_query=schema_query,
                    )
                else:
                    return self.plan_builder.large_local_relation_plan(
                        logical_plan.output,
                        logical_plan.data,
                        logical_plan,
                        schema_query=schema_query,
                    )
            else:
                return self.plan_builder.query(
                    empty_values_statement(logical_plan.output),
                    logical_plan,
                    schema_query=schema_query,
                )

        if isinstance(logical_plan, SnowflakeTable):
            return self.plan_builder.table(logical_plan.name, logical_plan)

        if isinstance(logical_plan, SnowflakeCreateTable):
            resolved_child = resolved_children[logical_plan.children[0]]
            return self.plan_builder.save_as_table(
                table_name=logical_plan.table_name,
                column_names=logical_plan.column_names,
                mode=logical_plan.mode,
                table_type=logical_plan.table_type,
                clustering_keys=[
                    self.analyze(x, df_aliased_col_name_to_real_col_name)
                    for x in logical_plan.clustering_exprs
                ],
                comment=logical_plan.comment,
                enable_schema_evolution=logical_plan.enable_schema_evolution,
                data_retention_time=logical_plan.data_retention_time,
                max_data_extension_time=logical_plan.max_data_extension_time,
                change_tracking=logical_plan.change_tracking,
                copy_grants=logical_plan.copy_grants,
                child=resolved_child,
                source_plan=logical_plan,
                use_scoped_temp_objects=self.session._use_scoped_temp_objects,
                creation_source=logical_plan.creation_source,
                child_attributes=resolved_child.attributes,
                iceberg_config=logical_plan.iceberg_config,
                table_exists=logical_plan.table_exists,
            )

        if isinstance(logical_plan, Limit):
            on_top_of_order_by = logical_plan.is_limit_append or (
                isinstance(logical_plan.child, SnowflakePlan)
                and isinstance(logical_plan.child.source_plan, Sort)
            )
            return self.plan_builder.limit(
                self.to_sql_try_avoid_cast(
                    logical_plan.limit_expr, df_aliased_col_name_to_real_col_name
                ),
                self.to_sql_try_avoid_cast(
                    logical_plan.offset_expr, df_aliased_col_name_to_real_col_name
                ),
                resolved_children[logical_plan.child],
                on_top_of_order_by,
                logical_plan,
            )

        if isinstance(logical_plan, Pivot):
            # We retrieve the pivot_values for generating SQL using types:
            # List[str] => explicit list of pivot values
            # ScalarSubquery => dynamic pivot subquery
            # None => dynamic pivot ANY subquery

            if isinstance(logical_plan.pivot_values, List):
                pivot_values = [
                    self.analyze(pv, df_aliased_col_name_to_real_col_name)
                    for pv in logical_plan.pivot_values
                ]
            elif isinstance(logical_plan.pivot_values, ScalarSubquery):
                pivot_values = self.analyze(
                    logical_plan.pivot_values, df_aliased_col_name_to_real_col_name
                )
            else:
                pivot_values = None

            plan = None

            for agg_expr in logical_plan.aggregates:
                # We only allow pivot on more than one aggregates when it on a groupby clause
                join_columns: List[str] | None = None
                if (
                    len(logical_plan.grouping_columns) != 0
                    and agg_expr.children is not None
                ):
                    # Currently snowflake pivot creates a group by from all columns outside of
                    # pivot column and aggregate column. In order to implement df.group_by().pivot(),
                    # we need to first select only the columns that need to be involved in this
                    # operation, and then apply pivot operation. Here, we will first use project
                    # plan to select group_by, pivot and aggregate column and then apply the pivot
                    # logic.
                    #     project_cols = grouping_cols + pivot_col + aggregate_col
                    # It is possible that pivot_col and aggregate_col references the same column,
                    # so we need to deduplicate
                    project_exprs = [
                        *logical_plan.grouping_columns,
                        agg_expr.children[0],
                    ]
                    if not (
                        isinstance(agg_expr.children[0], NamedExpression)
                        and isinstance(logical_plan.pivot_column, NamedExpression)
                        and agg_expr.children[0].name == logical_plan.pivot_column.name
                    ):
                        project_exprs.append(
                            logical_plan.pivot_column,
                        )
                    join_columns = [
                        self.analyze(expression, df_aliased_col_name_to_real_col_name)
                        for expression in logical_plan.grouping_columns
                    ]
                    child = self.plan_builder.project(
                        [
                            self.analyze(col, df_aliased_col_name_to_real_col_name)
                            for col in project_exprs
                        ],
                        resolved_children[logical_plan.child],
                        logical_plan,
                    )
                else:
                    child = resolved_children[logical_plan.child]

                pivot_plan = self.plan_builder.pivot(
                    self.analyze(
                        logical_plan.pivot_column, df_aliased_col_name_to_real_col_name
                    ),
                    pivot_values,
                    self.analyze(agg_expr, df_aliased_col_name_to_real_col_name),
                    self.analyze(
                        logical_plan.default_on_null,
                        df_aliased_col_name_to_real_col_name,
                    )
                    if logical_plan.default_on_null
                    else None,
                    child,
                    logical_plan,
                    len(logical_plan.aggregates)
                    > 1,  # we need to alias the names with agg function when we have more than one agg functions on the pivot
                )

                # If this is a dynamic pivot, then we can't use child.schema_query which is used in the schema_query
                # sql generator by default because it is simplified and won't fetch the output columns from the underlying
                # source.  So in this case we use the actual pivot query as the schema query.
                if logical_plan.pivot_values is None or isinstance(
                    logical_plan.pivot_values, ScalarSubquery
                ):
                    # TODO (SNOW-916744): Using the original query here does not work if the query depends on a temp
                    # table as it may not exist at later point in time when dataframe.schema is called.
                    pivot_plan.schema_query = pivot_plan.queries[-1].sql

                # using join here to have the output similar to what spark have
                # both the aggregations are happening over the same set of columns and pivot values
                # we will receive left and right both pivot table with same set of groupby columns and columns corresponding to pivot values
                # to differentiate between columns corresponding to pivot values for aggregation function they will have name suffixed by agg fun
                # join would keep the group by column same and append the columns corresponding to pivot values for multiple agg functions
                # output would look similar to below for a statement like
                # df.groupBy("name").pivot("department", ["Sales", "Marketing"]).sum("year", "salary").show()
                # +-------+---------------+---------------+-------------------+-------------------+
                # |   name|Sales_sum(year)|Sales_sum(year)|Marketing_sum(year)|Marketing_sum(year)|
                # +-------+---------------+---------------+-------------------+-------------------+
                # |  Scott|           NULL|           NULL|               NULL|               NULL|
                # |  James|           4039|           4039|               NULL|               NULL|
                # |    Jen|           NULL|           NULL|               NULL|               NULL|
                # |Michael|           2020|           2020|               NULL|               NULL|

                if plan is None:
                    plan = pivot_plan
                elif join_columns is not None:
                    plan = self.plan_builder.join(
                        plan,
                        pivot_plan,
                        UsingJoin(FullOuter(), join_columns),
                        "",
                        "",
                        logical_plan,
                        self.session.conf.get("use_constant_subquery_alias", False),
                    )
                # we have a check in relational_grouped_dataframe.py which will prevent a case where there are more than one aggregate
                # without having a grouping condition which is essential to create join_columns

            assert plan is not None
            return plan

        if isinstance(logical_plan, Unpivot):
            return self.plan_builder.unpivot(
                logical_plan.value_column,
                logical_plan.name_column,
                [
                    self.analyze(c, df_aliased_col_name_to_real_col_name)
                    for c in logical_plan.column_list
                ],
                logical_plan.include_nulls,
                resolved_children[logical_plan.child],
                logical_plan,
            )

        if isinstance(logical_plan, Rename):
            return self.plan_builder.rename(
                logical_plan.column_map,
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
                name=logical_plan.name,
                child=resolved_children[logical_plan.child],
                is_temp=is_temp,
                comment=logical_plan.comment,
                replace=logical_plan.replace,
                copy_grants=logical_plan.copy_grants,
                source_plan=logical_plan,
            )

        if isinstance(logical_plan, CreateDynamicTableCommand):
            return self.plan_builder.create_or_replace_dynamic_table(
                name=logical_plan.name,
                warehouse=logical_plan.warehouse,
                lag=logical_plan.lag,
                comment=logical_plan.comment,
                create_mode=logical_plan.create_mode,
                refresh_mode=logical_plan.refresh_mode,
                initialize=logical_plan.initialize,
                clustering_keys=[
                    self.analyze(x, df_aliased_col_name_to_real_col_name)
                    for x in logical_plan.clustering_exprs
                ],
                is_transient=logical_plan.is_transient,
                data_retention_time=logical_plan.data_retention_time,
                max_data_extension_time=logical_plan.max_data_extension_time,
                child=resolved_children[logical_plan.child],
                source_plan=logical_plan,
                iceberg_config=logical_plan.iceberg_config,
                copy_grants=logical_plan.copy_grants,
            )

        if isinstance(logical_plan, ReadFileNode):
            return self.plan_builder.read_file(
                path=logical_plan.path,
                format=logical_plan.format,
                options=logical_plan.options,
                schema=logical_plan.schema,
                schema_to_cast=logical_plan.schema_to_cast,
                transformations=logical_plan.transformations,
                metadata_project=logical_plan.metadata_project,
                metadata_schema=logical_plan.metadata_schema,
                use_user_schema=logical_plan.use_user_schema,
                xml_reader_udtf=logical_plan.xml_reader_udtf,
                source_plan=logical_plan,
            )

        if isinstance(logical_plan, CopyIntoTableNode):
            format_type_options = (
                logical_plan.format_type_options.copy()
                if logical_plan.format_type_options
                else {}
            )
            format_name = (logical_plan.cur_options or {}).get("FORMAT_NAME")
            if format_name is not None:
                format_type_options["FORMAT_NAME"] = format_name
            assert logical_plan.file_format is not None
            return self.plan_builder.copy_into_table(
                path=logical_plan.file_path,
                table_name=logical_plan.table_name,
                files=logical_plan.files,
                source_plan=logical_plan,
                pattern=logical_plan.pattern,
                file_format=logical_plan.file_format,
                format_type_options=format_type_options,
                copy_options=logical_plan.copy_options,
                validation_mode=logical_plan.validation_mode,
                column_names=logical_plan.column_names,
                transformations=[
                    self.analyze(x, df_aliased_col_name_to_real_col_name)
                    for x in logical_plan.transformations
                ]
                if logical_plan.transformations
                else None,
                user_schema=logical_plan.user_schema,
                create_table_from_infer_schema=logical_plan.create_table_from_infer_schema,
                iceberg_config=logical_plan.iceberg_config,
            )

        if isinstance(logical_plan, CopyIntoLocationNode):
            return self.plan_builder.copy_into_location(
                query=resolved_children[logical_plan.child],
                stage_location=logical_plan.stage_location,
                source_plan=logical_plan,
                partition_by=self.analyze(
                    logical_plan.partition_by, df_aliased_col_name_to_real_col_name
                )
                if logical_plan.partition_by
                else None,
                file_format_name=logical_plan.file_format_name,
                file_format_type=logical_plan.file_format_type,
                format_type_options=logical_plan.format_type_options,
                header=logical_plan.header,
                validation_mode=logical_plan.validation_mode,
                storage_integration=logical_plan.storage_integration,
                credentials=logical_plan.credentials,
                encryption=logical_plan.encryption,
                **logical_plan.copy_options,
            )

        if isinstance(logical_plan, TableUpdate):
            return self.plan_builder.update(
                logical_plan.table_name,
                {
                    self.analyze(k, df_aliased_col_name_to_real_col_name): self.analyze(
                        v, df_aliased_col_name_to_real_col_name
                    )
                    for k, v in logical_plan.assignments.items()
                },
                self.analyze(
                    logical_plan.condition, df_aliased_col_name_to_real_col_name
                )
                if logical_plan.condition
                else None,
                resolved_children[logical_plan.source_data]
                if logical_plan.source_data
                else None,
                logical_plan,
            )

        if isinstance(logical_plan, TableDelete):
            return self.plan_builder.delete(
                logical_plan.table_name,
                self.analyze(
                    logical_plan.condition, df_aliased_col_name_to_real_col_name
                )
                if logical_plan.condition
                else None,
                # source_data is marked as child of the logical_plan
                resolved_children[logical_plan.source_data]
                if logical_plan.source_data
                else None,
                logical_plan,
            )

        if isinstance(logical_plan, TableMerge):
            return self.plan_builder.merge(
                logical_plan.table_name,
                resolved_children[logical_plan.source]
                if logical_plan.source
                else logical_plan.source,
                self.analyze(
                    logical_plan.join_expr, df_aliased_col_name_to_real_col_name
                ),
                [
                    self.analyze(c, df_aliased_col_name_to_real_col_name)
                    for c in logical_plan.clauses
                ],
                logical_plan,
            )

        if isinstance(logical_plan, Selectable):
            return self.plan_builder.select_statement(logical_plan)

        raise TypeError(
            f"Cannot resolve type logical_plan of {type(logical_plan).__name__} to a SnowflakePlan"
        )

    def create_select_statement(self, *args, **kwargs):
        return SelectStatement(*args, **kwargs)

    def create_selectable_entity(self, *args, **kwargs):
        return SelectableEntity(*args, **kwargs)

    def create_select_snowflake_plan(self, *args, **kwargs):
        return SelectSnowflakePlan(*args, **kwargs)

    def create_select_table_function(self, *args, **kwargs):
        return SelectTableFunction(*args, **kwargs)
