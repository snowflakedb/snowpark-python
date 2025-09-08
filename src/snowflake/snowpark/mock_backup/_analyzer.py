#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from collections import Counter, defaultdict
from typing import DefaultDict, Dict, List, Optional, Union

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
    flatten_expression,
    function_expression,
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
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
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
    Alias,
    Cast,
    UnaryExpression,
    UnresolvedAlias,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import (
    Aggregate,
    CreateDynamicTableCommand,
    CreateViewCommand,
    Filter,
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
from snowflake.snowpark.mock._plan import MockExecutionPlan
from snowflake.snowpark.mock._plan_builder import MockSnowflakePlanBuilder
from snowflake.snowpark.mock._select_statement import (
    MockSelectable,
    MockSelectableEntity,
    MockSelectExecutionPlan,
    MockSelectStatement,
)
from snowflake.snowpark.types import _NumericType


class MockAnalyzer:
    def __init__(self, session: "snowflake.snowpark.session.Session") -> None:
        self.session = session
        self.plan_builder = MockSnowflakePlanBuilder(self.session)
        self.subquery_plans = []
        self.generated_alias_maps = {}
        self.alias_maps_to_use = None
        self._conn = self.session._conn

    def analyze(
        self,
        expr: Union[Expression, NamedExpression],
        df_aliased_col_name_to_real_col_name: Optional[
            DefaultDict[str, Dict[str, str]]
        ] = None,
        parse_local_name=False,
        keep_alias=True,
    ) -> Union[str, List[str]]:
        """
        Args:
            keep_alias: if true, return the column name as "aa as bb", else return the desired column name.
            e.g., analyzing an expression sum(col('b')).as_("totB"), we want keep_alias to be true in the
            sql simplifier process, which returns column name as sum('b') as 'totB',
            so that it will detect column name change.
            however, in the result calculation, we want to column name to be the output name, which is 'totB',
            so we set keep_alias to False in the execution.
        """
        if isinstance(expr, GroupingSetsExpression):
            self._conn.log_not_supported_error(
                external_feature_name="DataFrame.group_by_grouping_sets",
                raise_error=NotImplementedError,
            )

        if isinstance(expr, Like):
            return like_expression(
                self.analyze(
                    expr.expr, df_aliased_col_name_to_real_col_name, parse_local_name
                ),
                self.analyze(
                    expr.pattern, df_aliased_col_name_to_real_col_name, parse_local_name
                ),
            )

        if isinstance(expr, RegExp):
            return regexp_expression(
                self.analyze(
                    expr.expr, df_aliased_col_name_to_real_col_name, parse_local_name
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
                    expr.expr, df_aliased_col_name_to_real_col_name, parse_local_name
                ),
                collation_spec,
            )

        if isinstance(expr, (SubfieldString, SubfieldInt)):
            field = expr.field
            if parse_local_name and isinstance(field, str):
                field = field.upper()
            return subfield_expression(
                self.analyze(
                    expr.expr, df_aliased_col_name_to_real_col_name, parse_local_name
                ),
                field,
            )

        if isinstance(expr, CaseWhen):
            return case_when_expression(
                [
                    (
                        self.analyze(
                            condition,
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
                    expr.else_value,
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
                        expression,
                        df_aliased_col_name_to_real_col_name,
                        parse_local_name,
                    )
                else:
                    in_value = self.analyze(
                        expression,
                        df_aliased_col_name_to_real_col_name,
                        parse_local_name,
                    )

                in_values.append(in_value)
            return in_expression(
                self.analyze(
                    expr.columns, df_aliased_col_name_to_real_col_name, parse_local_name
                ),
                in_values,
            )

        if isinstance(expr, GroupingSet):
            self._conn.log_not_supported_error(
                external_feature_name="DataFrame.group_by_grouping_sets",
                raise_error=NotImplementedError,
            )

        if isinstance(expr, WindowExpression):
            return window_expression(
                self.analyze(
                    expr.window_function,
                    df_aliased_col_name_to_real_col_name=df_aliased_col_name_to_real_col_name,
                    parse_local_name=parse_local_name,
                ),
                self.analyze(
                    expr.window_spec,
                    df_aliased_col_name_to_real_col_name=df_aliased_col_name_to_real_col_name,
                    parse_local_name=parse_local_name,
                ),
            )

        if isinstance(expr, WindowSpecDefinition):
            return window_spec_expression(
                [
                    self.analyze(
                        x,
                        df_aliased_col_name_to_real_col_name=df_aliased_col_name_to_real_col_name,
                        parse_local_name=parse_local_name,
                    )
                    for x in expr.partition_spec
                ],
                [
                    self.analyze(
                        x,
                        df_aliased_col_name_to_real_col_name=df_aliased_col_name_to_real_col_name,
                        parse_local_name=parse_local_name,
                    )
                    for x in expr.order_spec
                ],
                self.analyze(
                    expr.frame_spec,
                    df_aliased_col_name_to_real_col_name=df_aliased_col_name_to_real_col_name,
                    parse_local_name=parse_local_name,
                ),
            )

        if isinstance(expr, SpecifiedWindowFrame):
            return specified_window_frame_expression(
                expr.frame_type.sql,
                self.window_frame_boundary(self.to_sql_try_avoid_cast(expr.lower, {})),
                self.window_frame_boundary(self.to_sql_try_avoid_cast(expr.upper, {})),
            )

        if isinstance(expr, UnspecifiedFrame):
            return ""
        if isinstance(expr, SpecialFrameBoundary):
            return expr.sql

        if isinstance(expr, Literal):
            sql = to_sql(expr.value, expr.datatype)
            if parse_local_name:
                sql = sql.upper()
            return f"{sql}"

        if isinstance(expr, Attribute):
            # this is different from live connection, in live connection we assert alias_maps_to_use is not None.
            # However, it's not the case in local testing because we don't have the alias map when we use
            # the plan to get attributes. So we need to check if alias_maps_to_use is None here.
            if self.alias_maps_to_use:
                # this is the case when we resolve plan
                name = self.alias_maps_to_use.get(expr.expr_id, expr.name)
            else:
                # this is the case when we describe plan to get attributes
                name = expr.name
            return quote_name(name)

        if isinstance(expr, UnresolvedAttribute):
            return expr.name

        if isinstance(expr, FunctionExpression):
            if expr.api_call_source is not None:
                self.session._conn._telemetry_client.send_function_usage_telemetry(
                    expr.api_call_source, TelemetryField.FUNC_CAT_USAGE.value
                )
            func_name = expr.name.upper()

            children = []
            for c in expr.children:
                extracted = self.to_sql_try_avoid_cast(
                    c, df_aliased_col_name_to_real_col_name
                )
                if isinstance(extracted, list):
                    children.extend(extracted)
                else:
                    children.append(extracted)

            return function_expression(
                func_name,
                children,
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
            if not expr.expressions:
                return "*"
            else:
                return [
                    self.analyze(e, df_aliased_col_name_to_real_col_name)
                    for e in expr.expressions
                ]

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
                        x, df_aliased_col_name_to_real_col_name, parse_local_name
                    )
                    for x in expr.children
                ],
                False,
            )

        if isinstance(expr, TableFunctionExpression):
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
                expr,
                df_aliased_col_name_to_real_col_name,
                parse_local_name,
                keep_alias=keep_alias,
            )

        if isinstance(expr, SortOrder):
            return order_expression(
                self.analyze(
                    expr.child, df_aliased_col_name_to_real_col_name, parse_local_name
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
                    expr.expr, df_aliased_col_name_to_real_col_name, parse_local_name
                ),
                [
                    self.analyze(e, df_aliased_col_name_to_real_col_name)
                    for e in expr.order_by_cols
                ],
            )

        if isinstance(expr, BinaryExpression):
            return self.binary_operator_extractor(
                expr,
                df_aliased_col_name_to_real_col_name,
                parse_local_name,
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
                    expr.col, df_aliased_col_name_to_real_col_name, parse_local_name
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
                    expr.expr, df_aliased_col_name_to_real_col_name, parse_local_name
                ),
                expr.offset,
                self.analyze(
                    expr.default, df_aliased_col_name_to_real_col_name, parse_local_name
                )
                if expr.default
                else None,
                expr.ignore_nulls,
            )

        if isinstance(expr, Interval):
            return str(expr)

        raise SnowparkClientExceptionMessages.PLAN_INVALID_TYPE(
            str(expr)
        )  # pragma: no cover

    def table_function_expression_extractor(
        self,
        expr: TableFunctionExpression,
        df_aliased_col_name_to_real_col_name: DefaultDict[str, Dict[str, str]],
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
                    key: self.analyze(
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
        df_aliased_col_name_to_real_col_name: DefaultDict[str, Dict[str, str]],
        parse_local_name=False,
        keep_alias=True,
    ) -> str:
        if isinstance(expr, Alias):
            quoted_name = quote_name(expr.name)
            if isinstance(expr.child, Attribute):
                self.generated_alias_maps[expr.child.expr_id] = quoted_name
                assert self.alias_maps_to_use is not None
                for k, v in self.alias_maps_to_use.items():
                    if v == expr.child.name:
                        self.generated_alias_maps[k] = quoted_name

                if df_aliased_col_name_to_real_col_name:
                    for df_alias_dict in df_aliased_col_name_to_real_col_name.values():
                        for k, v in df_alias_dict.items():
                            if v == expr.child.name:
                                df_alias_dict[k] = quoted_name

            alias_exp = alias_expression(
                self.analyze(
                    expr.child, df_aliased_col_name_to_real_col_name, parse_local_name
                ),
                quoted_name,
            )

            expr_str = alias_exp if keep_alias else expr.name or keep_alias
            expr_str = expr_str.upper() if parse_local_name else expr_str
            return expr_str
        if isinstance(expr, UnresolvedAlias):
            expr_str = self.analyze(
                expr.child, df_aliased_col_name_to_real_col_name, parse_local_name
            )
            assert isinstance(expr_str, (str, list))
            if isinstance(expr_str, str):
                if parse_local_name:
                    expr_str = expr_str.upper()
                return quote_name(expr_str.strip())
            else:  # expr_str is a list
                assert all(isinstance(e, str) for e in expr_str)
                return ",".join([quote_name(e.strip()) for e in expr_str])
        elif isinstance(expr, Cast):
            return cast_expression(
                self.analyze(
                    expr.child, df_aliased_col_name_to_real_col_name, parse_local_name
                ),
                expr.to,
                expr.try_,
            )
        else:
            return unary_expression(
                self.analyze(
                    expr.child, df_aliased_col_name_to_real_col_name, parse_local_name
                ),
                expr.sql_operator,
                expr.operator_first,
            )

    def binary_operator_extractor(
        self,
        expr: BinaryExpression,
        df_aliased_col_name_to_real_col_name: DefaultDict[str, Dict[str, str]],
        parse_local_name=False,
    ) -> str:
        if self.session.eliminate_numeric_sql_value_cast_enabled:
            left_sql_expr = self.to_sql_try_avoid_cast(
                expr.left, df_aliased_col_name_to_real_col_name, parse_local_name
            )
            right_sql_expr = self.to_sql_try_avoid_cast(
                expr.right,
                df_aliased_col_name_to_real_col_name,
                parse_local_name,
            )
        else:
            left_sql_expr = self.analyze(
                expr.left, df_aliased_col_name_to_real_col_name, parse_local_name
            )
            right_sql_expr = self.analyze(
                expr.right, df_aliased_col_name_to_real_col_name, parse_local_name
            )

        operator = expr.sql_operator.lower()
        if isinstance(expr, BinaryArithmeticExpression):
            return binary_arithmetic_expression(
                operator,
                left_sql_expr,
                right_sql_expr,
            )
        else:
            return function_expression(
                operator,
                [
                    left_sql_expr,
                    right_sql_expr,
                ],
                False,
            )

    def grouping_extractor(
        self,
        expr: GroupingSet,
        df_aliased_col_name_to_real_col_name: DefaultDict[str, Dict[str, str]],
    ) -> str:
        return self.analyze(
            FunctionExpression(
                expr.pretty_name.upper(),
                [c.child if isinstance(c, Alias) else c for c in expr.children],
                False,
            ),
            df_aliased_col_name_to_real_col_name,
        )

    def window_frame_boundary(self, offset: str) -> str:
        try:
            num = int(offset)
            return window_frame_boundary_expression(str(abs(num)), num >= 0)
        except Exception:
            return offset

    def to_sql_try_avoid_cast(
        self,
        expr: Expression,
        df_aliased_col_name_to_real_col_name: DefaultDict[str, Dict[str, str]],
        parse_local_name=False,
    ) -> str:
        # if expression is a numeric literal, return the number without casting,
        # otherwise process as normal
        if isinstance(expr, Literal) and isinstance(expr.datatype, _NumericType):
            return numeric_to_sql_without_cast(expr.value, expr.datatype)
        else:
            return self.analyze(
                expr, df_aliased_col_name_to_real_col_name, parse_local_name
            )

    def resolve(self, logical_plan: LogicalPlan) -> MockExecutionPlan:
        self.subquery_plans = []
        self.generated_alias_maps = {}
        result = self.do_resolve(logical_plan)
        result.add_aliases(self.generated_alias_maps)
        return result

    def do_resolve(self, logical_plan: LogicalPlan) -> MockExecutionPlan:
        resolved_children = {}
        df_aliased_col_name_to_real_col_name = defaultdict(dict)
        for c in logical_plan.children:
            resolved = self.resolve(c)
            df_aliased_col_name_to_real_col_name.update(
                resolved.df_aliased_col_name_to_real_col_name
            )
            resolved_children[c] = resolved

        if isinstance(logical_plan, MockSelectable):
            # Selectable doesn't have children. It already has the expr_to_alias dict.
            assert logical_plan.expr_to_alias is not None
            self.alias_maps_to_use = logical_plan.expr_to_alias.copy()
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
        assert hasattr(res, "df_aliased_col_name_to_real_col_name"), (
            f"The resolved plan {res!r} should have the attribute "
            "df_aliased_col_name_to_real_col_name"
        )
        res.df_aliased_col_name_to_real_col_name.update(
            df_aliased_col_name_to_real_col_name
        )
        return res

    def do_resolve_with_resolved_children(
        self,
        logical_plan: LogicalPlan,
        resolved_children: Dict[LogicalPlan, SnowflakePlan],
        df_aliased_col_name_to_real_col_name: DefaultDict[str, Dict[str, str]],
    ) -> MockExecutionPlan:
        if isinstance(logical_plan, MockExecutionPlan):
            return logical_plan

        if isinstance(logical_plan, (Rename, TableFunctionJoin, TableFunctionRelation)):
            return MockExecutionPlan(
                logical_plan,
                self.session,
            )

        if isinstance(logical_plan, Lateral):
            self._conn.log_not_supported_error(
                external_feature_name="table_function.Lateral",
                raise_error=NotImplementedError,
            )

        if isinstance(logical_plan, Aggregate):
            return MockExecutionPlan(
                logical_plan,
                self.session,
            )

        if isinstance(logical_plan, Project):
            return MockExecutionPlan(logical_plan, self.session)

        if isinstance(logical_plan, Filter):
            return MockExecutionPlan(logical_plan, self.session)

        # Add a sample stop to the plan being built
        if isinstance(logical_plan, Sample):
            return MockExecutionPlan(logical_plan, self.session)

        if isinstance(logical_plan, SampleBy):
            return MockExecutionPlan(logical_plan, self.session)

        if isinstance(logical_plan, Join):
            left = self.do_resolve(logical_plan.children[0])
            right = self.do_resolve(logical_plan.children[1])
            # the following two attributes call are used to resolve the alias map
            # this aligns with the live connection behavior where for Join expression analysis, the analyzer calls
            # plan_builder.join -> SnowflakePlanBuilder.build_binary which issues sql to evaluate the schema
            left.attributes
            right.attributes
            common_columns = set(left.expr_to_alias.keys()).intersection(
                right.expr_to_alias.keys()
            )
            new_expr_to_alias = {
                k: v
                for k, v in {
                    **left.expr_to_alias,
                    **right.expr_to_alias,
                }.items()
                if k not in common_columns
            }
            return MockExecutionPlan(
                logical_plan, self.session, expr_to_alias=new_expr_to_alias
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
            return MockExecutionPlan(logical_plan, self.session)

        if isinstance(logical_plan, SnowflakeTable):
            return MockExecutionPlan(logical_plan, self.session)

        if isinstance(logical_plan, SnowflakeCreateTable):
            return MockExecutionPlan(logical_plan, self.session)

        if isinstance(logical_plan, SnowflakePlan):
            return MockExecutionPlan(logical_plan, self.session)

        if isinstance(logical_plan, Limit):
            on_top_of_order_by = isinstance(
                logical_plan.child, SnowflakePlan
            ) and isinstance(logical_plan.child.source_plan, Sort)
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
            return MockExecutionPlan(logical_plan, self.session)

        if isinstance(logical_plan, Unpivot):
            self._conn.log_not_supported_error(
                external_feature_name="RelationalGroupedDataFrame.Unpivot",
                raise_error=NotImplementedError,
            )

        if isinstance(logical_plan, CreateViewCommand):
            return MockExecutionPlan(logical_plan, self.session)

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
                source_plan=logical_plan,
            )

        if isinstance(logical_plan, CopyIntoTableNode):
            self._conn.log_not_supported_error(
                external_feature_name="DateFrame.copy_into_table",
                raise_error=NotImplementedError,
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
                **logical_plan.copy_options,
            )

        if isinstance(logical_plan, TableUpdate):
            return MockExecutionPlan(logical_plan, self.session)

        if isinstance(logical_plan, TableDelete):
            return MockExecutionPlan(logical_plan, self.session)

        if isinstance(logical_plan, CreateDynamicTableCommand):
            self._conn.log_not_supported_error(
                external_feature_name="DateFrame.create_or_replace_dynamic_table",
                raise_error=NotImplementedError,
            )

        if isinstance(logical_plan, TableMerge):
            return MockExecutionPlan(logical_plan, self.session)

        if isinstance(logical_plan, MockSelectable):
            if isinstance(logical_plan, MockSelectStatement):
                # align with the live connection behavior where the analyzer calls
                # logical_plan.projection_in_str to materialize an lazy property to resolve the alias map
                logical_plan.projection_in_str
            return MockExecutionPlan(logical_plan, self.session)

    def create_select_statement(self, *args, **kwargs):
        return MockSelectStatement(*args, **kwargs)

    def create_select_snowflake_plan(self, *args, **kwargs):
        return MockSelectExecutionPlan(*args, **kwargs)

    def create_selectable_entity(self, *args, **kwargs):
        return MockSelectableEntity(*args, **kwargs)
