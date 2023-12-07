#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import importlib
import inspect
import math
import re
import typing
import uuid
from enum import Enum
from functools import cached_property, partial
from typing import TYPE_CHECKING, Dict, List, NoReturn, Optional, Union
from unittest.mock import MagicMock

from snowflake.snowpark._internal.analyzer.table_merge_expression import (
    DeleteMergeExpression,
    InsertMergeExpression,
    TableDelete,
    TableMerge,
    TableUpdate,
    UpdateMergeExpression,
)
from snowflake.snowpark._internal.analyzer.window_expression import (
    FirstValue,
    Lag,
    LastValue,
    Lead,
    RangeFrame,
    RowFrame,
    SpecifiedWindowFrame,
    UnboundedFollowing,
    UnboundedPreceding,
    WindowExpression,
)
from snowflake.snowpark.mock._window_utils import (
    EntireWindowIndexer,
    RowFrameIndexer,
    is_rank_related_window_function,
)

if TYPE_CHECKING:
    from snowflake.snowpark.mock._analyzer import MockAnalyzer

import snowflake.snowpark.mock._file_operation as mock_file_operation
from snowflake.connector.options import pandas as pd
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    EXCEPT,
    INTERSECT,
    UNION,
    UNION_ALL,
    quote_name,
)
from snowflake.snowpark._internal.analyzer.binary_expression import (
    Add,
    And,
    BinaryExpression,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    Divide,
    EqualNullSafe,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Multiply,
    NotEqualTo,
    Or,
    Pow,
    Remainder,
    Subtract,
)
from snowflake.snowpark._internal.analyzer.binary_plan_node import Join
from snowflake.snowpark._internal.analyzer.expression import (
    Attribute,
    CaseWhen,
    Expression,
    FunctionExpression,
    InExpression,
    Like,
    ListAgg,
    Literal,
    MultipleExpression,
    RegExp,
    ScalarSubquery,
    Star,
    SubfieldInt,
    SubfieldString,
    UnresolvedAttribute,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    Range,
    SaveMode,
    SnowflakeCreateTable,
    SnowflakeValues,
    UnresolvedRelation,
)
from snowflake.snowpark._internal.analyzer.sort_expression import (
    Ascending,
    NullsFirst,
    SortOrder,
)
from snowflake.snowpark._internal.analyzer.unary_expression import (
    Alias,
    Cast,
    IsNaN,
    IsNotNull,
    IsNull,
    Not,
    UnaryMinus,
    UnresolvedAlias,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import (
    Aggregate,
    CreateViewCommand,
    Sample,
)
from snowflake.snowpark._internal.type_utils import infer_type
from snowflake.snowpark._internal.utils import generate_random_alphanumeric, parse_table_name
from snowflake.snowpark.column import Column
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.mock._functions import _MOCK_FUNCTION_IMPLEMENTATION_MAP
from snowflake.snowpark.mock._select_statement import (
    MockSelectable,
    MockSelectableEntity,
    MockSelectExecutionPlan,
    MockSelectStatement,
    MockSetStatement,
)
from snowflake.snowpark.mock._snowflake_data_type import (
    ColumnEmulator,
    ColumnType,
    TableEmulator,
)
from snowflake.snowpark.mock._util import (
    convert_wildcard_to_regex,
    custom_comparator,
    fix_drift_between_column_sf_type_and_dtype,
)
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    TimestampType,
    TimeType,
    VariantType,
    _NumericType,
)


class MockExecutionPlan(LogicalPlan):
    def __init__(
        self,
        source_plan: LogicalPlan,
        session,
        *,
        child: Optional["MockExecutionPlan"] = None,
        expr_to_alias: Optional[Dict[uuid.UUID, str]] = None,
        df_aliased_col_name_to_real_col_name: Optional[Dict[str, str]] = None,
    ) -> NoReturn:
        super().__init__()
        self.source_plan = source_plan
        self.session = session
        mock_query = MagicMock()
        mock_query.sql = "SELECT MOCK_TEST_FAKE_QUERY()"
        self.queries = [mock_query]
        self.child = child
        self.expr_to_alias = expr_to_alias if expr_to_alias is not None else {}
        self.df_aliased_col_name_to_real_col_name = (
            df_aliased_col_name_to_real_col_name or {}
        )
        self.api_calls = []

    @property
    def attributes(self) -> List[Attribute]:
        output = describe(self)
        return output

    @cached_property
    def output(self) -> List[Attribute]:
        return [Attribute(a.name, a.datatype, a.nullable) for a in self.attributes]


class MockFileOperation(MockExecutionPlan):
    class Operator(str, Enum):
        PUT = "put"
        READ_FILE = "read_file"
        # others are not supported yet

    def __init__(
        self,
        session,
        operator: Union[str, Operator],
        *,
        options: Dict[str, str],
        local_file_name: Optional[str] = None,
        stage_location: Optional[str] = None,
        child: Optional["MockExecutionPlan"] = None,
        source_plan: Optional[LogicalPlan] = None,
        format: Optional[str] = None,
        schema: Optional[List[Attribute]] = None,
    ) -> None:
        super().__init__(session=session, child=child, source_plan=source_plan)
        self.operator = operator
        self.local_file_name = local_file_name
        self.stage_location = stage_location
        self.api_calls = self.api_calls or []
        self.format = format
        self.schema = schema
        self.options = options


def handle_order_by_clause(
    order_by: List[SortOrder],
    result_df: TableEmulator,
    analyzer: "MockAnalyzer",
    expr_to_alias: Optional[Dict[str, str]],
) -> TableEmulator:
    """Given an input dataframe `result_df` and a list of SortOrder expressions `order_by`, return the sorted dataframe."""
    sort_columns_array = []
    sort_orders_array = []
    null_first_last_array = []
    added_columns = []
    for exp in order_by:
        exp_name = analyzer.analyze(exp.child, expr_to_alias)
        if exp_name not in result_df.columns:
            result_df[exp_name] = calculate_expression(
                exp.child, result_df, analyzer, expr_to_alias
            )
            added_columns.append(exp_name)
        sort_columns_array.append(exp_name)
        sort_orders_array.append(isinstance(exp.direction, Ascending))
        null_first_last_array.append(
            isinstance(exp.null_ordering, NullsFirst) or exp.null_ordering == NullsFirst
        )
    for column, ascending, null_first in reversed(
        list(zip(sort_columns_array, sort_orders_array, null_first_last_array))
    ):
        comparator = partial(custom_comparator, ascending, null_first)
        result_df = result_df.sort_values(by=column, key=comparator)
    result_df = result_df.drop(columns=added_columns)
    return result_df


def handle_range_frame_indexing(
    order_spec: List[SortOrder],
    res_index: "pd.Index",
    res: "pd.api.typing.DataFrameGroupBy",
    analyzer: "MockAnalyzer",
    expr_to_alias: Optional[Dict[str, str]],
    unbounded_preceding: bool,
    unbounded_following: bool,
) -> "pd.api.typing.RollingGroupby":
    """Return a list of range between window frames based on the dataframe paritions `res` and the ORDER BY clause `order_spec`."""
    if order_spec:
        windows = []
        for current_row, win in zip(res_index, res.rolling(EntireWindowIndexer())):
            _win = handle_order_by_clause(order_spec, win, analyzer, expr_to_alias)
            row_idx = list(_win.index).index(current_row)
            start_idx = 0 if unbounded_preceding else row_idx
            end_idx = len(_win) - 1 if unbounded_following else row_idx

            def search_boundary_idx(idx, delta, _win):
                while 0 <= idx + delta < len(_win):
                    cur_expr = list(
                        calculate_expression(
                            exp.child, _win.iloc[idx], analyzer, expr_to_alias
                        )
                        for exp in order_spec
                    )
                    next_expr = list(
                        calculate_expression(
                            exp.child, _win.iloc[idx + delta], analyzer, expr_to_alias
                        )
                        for exp in order_spec
                    )
                    if not cur_expr == next_expr:
                        break
                    idx += delta
                return idx

            start_idx = search_boundary_idx(start_idx, -1, _win)
            end_idx = search_boundary_idx(end_idx, 1, _win)
            windows.append(_win[start_idx : end_idx + 1])
    else:  # If order by is not specified, just use the entire window
        windows = res.rolling(EntireWindowIndexer())
    return windows


def handle_function_expression(
    exp: FunctionExpression,
    input_data: Union[TableEmulator, ColumnEmulator],
    analyzer: "MockAnalyzer",
    expr_to_alias: Optional[Dict[str, str]],
    current_row=None,
):
    # Special case for count_distinct
    if exp.name.lower() == "count" and exp.is_distinct:
        func_name = "count_distinct"
    else:
        func_name = exp.name.lower()

    try:
        original_func = getattr(
            importlib.import_module("snowflake.snowpark.functions"), func_name
        )
    except AttributeError:
        raise NotImplementedError(
            f"[Local Testing] Mocking function {func_name} is not supported."
        )

    signatures = inspect.signature(original_func)
    spec = inspect.getfullargspec(original_func)
    if func_name not in _MOCK_FUNCTION_IMPLEMENTATION_MAP:
        raise NotImplementedError(
            f"[Local Testing] Mocking function {func_name} is not implemented."
        )
    to_pass_args = []
    type_hints = typing.get_type_hints(original_func)
    for idx, key in enumerate(signatures.parameters):
        type_hint = str(type_hints[key])
        keep_literal = "Column" not in type_hint
        if key == spec.varargs:
            to_pass_args.extend(
                [
                    calculate_expression(
                        c,
                        input_data,
                        analyzer,
                        expr_to_alias,
                        keep_literal=keep_literal,
                    )
                    for c in exp.children[idx:]
                ]
            )
        else:
            try:
                to_pass_args.append(
                    calculate_expression(
                        exp.children[idx],
                        input_data,
                        analyzer,
                        expr_to_alias,
                        keep_literal=keep_literal,
                    )
                )
            except IndexError:
                to_pass_args.append(None)
    if func_name == "array_agg":
        to_pass_args[-1] = exp.is_distinct
    if func_name == "sum" and exp.is_distinct:
        to_pass_args[0] = ColumnEmulator(
            data=to_pass_args[0].unique(), sf_type=to_pass_args[0].sf_type
        )
    # Rank related function specific arguments
    if func_name == "row_number":
        # for window functions, input_data is the current window
        to_pass_args.append(input_data)
        row_idx = list(input_data.index).index(
            current_row
        )  # the row's 0-base index in the window
        to_pass_args.append(row_idx)
    return _MOCK_FUNCTION_IMPLEMENTATION_MAP[func_name](*to_pass_args)


def execute_mock_plan(
    plan: MockExecutionPlan,
    expr_to_alias: Optional[Dict[str, str]] = None,
) -> Union[TableEmulator, List[Row]]:
    import numpy as np

    if expr_to_alias is None:
        expr_to_alias = {}
    if isinstance(plan, (MockExecutionPlan, SnowflakePlan)):
        source_plan = plan.source_plan
        analyzer = plan.session._analyzer
    else:
        source_plan = plan
        analyzer = plan.analyzer

    entity_registry = analyzer.session._conn.entity_registry

    if isinstance(source_plan, SnowflakeValues):
        table = TableEmulator(
            source_plan.data,
            columns=[x.name for x in source_plan.output],
            sf_types={
                x.name: ColumnType(x.datatype, x.nullable) for x in source_plan.output
            },
            dtype=object,
        )
        for column_name in table.columns:
            sf_type = table.sf_types[column_name]
            table[column_name].sf_type = table.sf_types[column_name]
            if not isinstance(sf_type.datatype, _NumericType):
                table[column_name].replace(np.nan, None, inplace=True)
        return table
    if isinstance(source_plan, MockSelectExecutionPlan):
        return execute_mock_plan(source_plan.execution_plan, expr_to_alias)
    if isinstance(source_plan, MockSelectStatement):
        projection: Optional[List[Expression]] = source_plan.projection or []
        from_: Optional[MockSelectable] = source_plan.from_
        where: Optional[Expression] = source_plan.where
        order_by: Optional[List[Expression]] = source_plan.order_by
        limit_: Optional[int] = source_plan.limit_
        offset: Optional[int] = source_plan.offset

        from_df = execute_mock_plan(from_, expr_to_alias)

        result_df = TableEmulator()

        for exp in projection:
            if isinstance(exp, Star):
                for i in range(len(from_df.columns)):
                    result_df.insert(len(result_df.columns), str(i), from_df.iloc[:, i])
                result_df.columns = from_df.columns
                result_df.sf_types = from_df.sf_types
                result_df.sf_types_by_col_index = from_df.sf_types_by_col_index
            elif (
                isinstance(exp, UnresolvedAlias)
                and exp.child
                and isinstance(exp.child, Star)
            ):
                for e in exp.child.expressions:
                    col_name = analyzer.analyze(e, expr_to_alias)
                    result_df[col_name] = calculate_expression(
                        e, from_df, analyzer, expr_to_alias
                    )
            else:
                if isinstance(exp, Alias):
                    column_name = expr_to_alias.get(exp.expr_id, exp.name)
                else:
                    column_name = analyzer.analyze(
                        exp, expr_to_alias, parse_local_name=True
                    )

                column_series = calculate_expression(
                    exp, from_df, analyzer, expr_to_alias
                )
                result_df[column_name] = column_series

                if isinstance(exp, (Alias)):
                    if isinstance(exp.child, Attribute):
                        quoted_name = quote_name(exp.name)
                        expr_to_alias[exp.child.expr_id] = quoted_name
                        for k, v in expr_to_alias.items():
                            if v == exp.child.name:
                                expr_to_alias[k] = quoted_name

        if where:
            condition = calculate_expression(where, result_df, analyzer, expr_to_alias)
            result_df = result_df[condition]

        if order_by:
            result_df = handle_order_by_clause(
                order_by, result_df, analyzer, expr_to_alias
            )

        if limit_ is not None:
            if offset is not None:
                result_df = result_df.iloc[offset:]
            result_df = result_df.head(n=limit_)

        return result_df
    if isinstance(source_plan, MockSetStatement):
        first_operand = source_plan.set_operands[0]
        res_df = execute_mock_plan(
            MockExecutionPlan(
                first_operand.selectable,
                source_plan.analyzer.session,
            ),
            expr_to_alias,
        )
        for i in range(1, len(source_plan.set_operands)):
            operand = source_plan.set_operands[i]
            operator = operand.operator
            cur_df = execute_mock_plan(
                MockExecutionPlan(operand.selectable, source_plan.analyzer.session),
                expr_to_alias,
            )
            if len(res_df.columns) != len(cur_df.columns):
                raise SnowparkSQLException(
                    f"SQL compilation error: invalid number of result columns for set operator input branches, expected {len(res_df.columns)}, got {len(cur_df.columns)} in branch {i + 1}"
                )
            cur_df.columns = res_df.columns
            if operator in (UNION, UNION_ALL):
                res_df = pd.concat([res_df, cur_df], ignore_index=True)
                res_df = (
                    res_df.drop_duplicates().reset_index(drop=True)
                    if operator == UNION
                    else res_df
                )
                res_df.sf_types = cur_df.sf_types
            elif operator in (EXCEPT, INTERSECT):
                # NaN == NaN evaluates to False in pandas, so we need to manually process rows that are all None/NaN
                if (
                    res_df.isnull().all(axis=1).where(lambda x: x).count() > 1
                ):  # Dedup rows that are all None/NaN
                    res_df = res_df.drop(index=res_df.isnull().all(axis=1).index[1:])

                any_null_rows_in_cur_df = cur_df.isnull().all(axis=1).any()
                null_rows_in_res_df = res_df.isnull().all(axis=1)
                if operator == INTERSECT:
                    res_df = res_df[
                        (res_df.isin(cur_df.values.ravel()).all(axis=1)).values  # IS IN
                        | (
                            any_null_rows_in_cur_df & null_rows_in_res_df.values
                        )  # Rows that are all None/NaN in both sets
                    ]
                elif operator == EXCEPT:
                    res_df = res_df[
                        ~(
                            res_df.isin(cur_df.values.ravel()).all(axis=1)
                        ).values  # NOT IS IN
                        | (
                            ~any_null_rows_in_cur_df & null_rows_in_res_df.values
                        )  # Rows that are all None/NaN only in LEFT
                    ]

                # Compute drop duplicates
                res_df = res_df.drop_duplicates()
            else:
                raise NotImplementedError(
                    f"[Local Testing] SetStatement operator {operator} is currently not implemented."
                )
        return res_df
    if isinstance(source_plan, MockSelectableEntity):
        entity_name = source_plan.entity_name
        if entity_registry.is_existing_table(entity_name):
            return entity_registry.read_table(entity_name)
        elif entity_registry.is_existing_view(entity_name):
            execution_plan = entity_registry.get_review(entity_name)
            res_df = execute_mock_plan(execution_plan)
            return res_df
        else:
            db_schme_table = parse_table_name(entity_name)
            raise SnowparkSQLException(
                f"Object '{db_schme_table[0][1:-1]}.{db_schme_table[1][1:-1]}.{db_schme_table[2][1:-1]}' does not exist or not authorized."
            )
    if isinstance(source_plan, Aggregate):
        child_rf = execute_mock_plan(source_plan.child)
        if (
            not source_plan.aggregate_expressions
            and not source_plan.grouping_expressions
        ):
            return (
                TableEmulator(child_rf.iloc[0].to_frame().T, sf_types=child_rf.sf_types)
                if len(child_rf)
                else TableEmulator(
                    data=None,
                    dtype=object,
                    columns=child_rf.columns,
                    sf_types=child_rf.sf_types,
                )
            )
        aggregate_columns = [
            plan.session._analyzer.analyze(exp, keep_alias=False)
            for exp in source_plan.aggregate_expressions
        ]
        intermediate_mapped_column = [
            f"<local_test_internal_{str(i + 1)}>" for i in range(len(aggregate_columns))
        ]
        for i in range(len(intermediate_mapped_column)):
            agg_expr = source_plan.aggregate_expressions[i]
            if isinstance(agg_expr, Alias):
                if isinstance(agg_expr.child, Literal) and isinstance(
                    agg_expr.child.datatype, _NumericType
                ):
                    child_rf.insert(
                        len(child_rf.columns),
                        intermediate_mapped_column[i],
                        ColumnEmulator(
                            data=[agg_expr.child.value] * len(child_rf),
                            sf_type=ColumnType(
                                agg_expr.child.datatype, agg_expr.child.nullable
                            ),
                        ),
                    )
                elif isinstance(
                    agg_expr.child, (ListAgg, FunctionExpression, BinaryExpression)
                ):
                    # function expression will be evaluated later
                    child_rf.insert(
                        len(child_rf.columns),
                        intermediate_mapped_column[i],
                        ColumnEmulator(
                            data=[None] * len(child_rf),
                            dtype=object,
                            sf_type=None,  # it will be set later when evaluating the function.
                        ),
                    )
                else:
                    raise NotImplementedError(
                        f"[Local Testing] Aggregate expression {type(agg_expr.child).__name__} is not implemented."
                    )
            elif isinstance(agg_expr, (Attribute, UnresolvedAlias)):
                column_name = plan.session._analyzer.analyze(agg_expr)
                try:
                    child_rf.insert(
                        len(child_rf.columns),
                        intermediate_mapped_column[i],
                        child_rf[column_name],
                    )
                except KeyError:
                    raise SnowparkSQLException(
                        f"[Local Testing] invalid identifier {column_name}"
                    )
            else:
                raise NotImplementedError(
                    f"[Local Testing] Aggregate expression {type(agg_expr).__name__} is not implemented."
                )

        result_df_sf_Types = {}
        result_df_sf_Types_by_col_idx = {}

        column_exps = [
            (
                plan.session._analyzer.analyze(exp),
                bool(isinstance(exp, Literal)),
                calculate_expression(
                    exp, child_rf, plan.session._analyzer, expr_to_alias
                ).sf_type,
            )
            for exp in source_plan.grouping_expressions
        ]
        for idx, (column_name, _, column_type) in enumerate(column_exps):
            result_df_sf_Types[
                column_name
            ] = column_type  # TODO: fix this, this does not work
            result_df_sf_Types_by_col_idx[idx] = column_type
        # Aggregate may not have column_exps, which is allowed in the case of `Dataframe.agg`, in this case we pass
        # lambda x: True as the `by` parameter
        # also pandas group by takes None and nan as the same, so we use .astype to differentiate the two
        by_column_expression = []
        try:
            for exp in source_plan.grouping_expressions:
                if isinstance(exp, Literal) and isinstance(exp.datatype, _NumericType):
                    col_name = f"<local_test_internal_{str(exp.value)}>"
                    by_column_expression.append(child_rf[col_name])
                else:
                    by_column_expression.append(
                        child_rf[plan.session._analyzer.analyze(exp)]
                    )
        except KeyError as e:
            raise SnowparkSQLException(
                f"This is not a valid group by expression due to exception {e!r}"
            )

        children_dfs = child_rf.groupby(
            by=by_column_expression or (lambda x: True), sort=False, dropna=False
        )
        # we first define the returning DataFrame with its column names
        columns = [
            quote_name(plan.session._analyzer.analyze(exp, keep_alias=False))
            for exp in source_plan.aggregate_expressions
        ]
        intermediate_mapped_column = [str(i) for i in range(len(columns))]
        result_df = TableEmulator(columns=intermediate_mapped_column, dtype=object)
        data = []

        def aggregate_by_groups(cur_group: TableEmulator):
            values = []

            if column_exps:
                for idx, (expr, is_literal, _) in enumerate(column_exps):
                    if is_literal:
                        values.append(source_plan.grouping_expressions[idx].value)
                    elif not cur_group.empty:
                        values.append(cur_group.iloc[0][expr])

            # the first len(column_exps) items of calculate_expression are the group_by column expressions,
            # the remaining are the aggregation function expressions
            for idx, exp in enumerate(
                source_plan.aggregate_expressions[len(column_exps) :]
            ):
                cal_exp_res = calculate_expression(
                    exp,
                    cur_group,
                    plan.session._analyzer,
                    expr_to_alias,
                )
                # and then append the calculated value
                if isinstance(cal_exp_res, ColumnEmulator):
                    values.append(cal_exp_res.iat[0])
                    result_df_sf_Types[
                        columns[idx + len(column_exps)]
                    ] = result_df_sf_Types_by_col_idx[
                        idx + len(column_exps)
                    ] = cal_exp_res.sf_type
                else:
                    values.append(cal_exp_res)
                    result_df_sf_Types[
                        columns[idx + len(column_exps)]
                    ] = result_df_sf_Types_by_col_idx[
                        idx + len(column_exps)
                    ] = ColumnType(
                        infer_type(cal_exp_res), nullable=True
                    )
            data.append(values)

        if not children_dfs.indices:
            aggregate_by_groups(child_rf)
        else:
            for _, indices in children_dfs.indices.items():
                # we construct row by row
                cur_group = child_rf.iloc[indices]
                # each row starts with group keys/column expressions, if there is no group keys/column expressions
                # it means aggregation without group (Datagrame.agg)
                aggregate_by_groups(cur_group)

        if len(data):
            for col in range(len(data[0])):
                series_data = ColumnEmulator(
                    data=[data[row][col] for row in range(len(data))],
                    dtype=object,
                )
                result_df[intermediate_mapped_column[col]] = series_data

        result_df.sf_types = result_df_sf_Types
        result_df.sf_types_by_col_index = result_df_sf_Types_by_col_idx
        result_df.columns = columns
        return result_df
    if isinstance(source_plan, Range):
        col = ColumnEmulator(
            data=[
                num
                for num in range(
                    source_plan.start, source_plan.end, int(source_plan.step)
                )
            ],
            sf_type=ColumnType(LongType(), False),
        )
        result_df = TableEmulator(
            col,
            columns=['"ID"'],
            sf_types={'"ID"': col.sf_type},
            dtype=object,
        )
        return result_df
    if isinstance(source_plan, Join):
        L_expr_to_alias = {}
        R_expr_to_alias = {}
        left = execute_mock_plan(source_plan.left, L_expr_to_alias).reset_index(
            drop=True
        )
        right = execute_mock_plan(source_plan.right, R_expr_to_alias).reset_index(
            drop=True
        )
        # Processing ON clause
        using_columns = getattr(source_plan.join_type, "using_columns", None)
        on = using_columns
        if isinstance(on, list):  # USING a list of columns
            if on:
                on = [quote_name(x.upper()) for x in on]
            else:
                on = None
        elif isinstance(on, Column):  # ON a single column
            on = on.name
        elif isinstance(
            on, BinaryExpression
        ):  # ON a condition, apply where to a Cartesian product
            on = None
        else:  # ON clause not specified, SF returns a Cartesian product
            on = None

        # Processing the join type
        how = source_plan.join_type.sql
        if how.startswith("USING "):
            how = how[6:]
        if how.startswith("NATURAL "):
            how = how[8:]
        if how == "LEFT OUTER":
            how = "LEFT"
        elif how == "RIGHT OUTER":
            how = "RIGHT"
        elif "FULL" in how:
            how = "OUTER"
        elif "SEMI" in how:
            how = "INNER"
        elif "ANTI" in how:
            how = "CROSS"

        if (
            "NATURAL" in source_plan.join_type.sql and on is None
        ):  # natural joins use the list of common names as keys
            on = left.columns.intersection(right.columns).values.tolist()

        if on is None:
            how = "CROSS"

        result_df = left.merge(
            right,
            on=on,
            how=how.lower(),
        )

        # Restore sf_types information after merging, there should be better way to do this
        result_df.sf_types.update(left.sf_types)
        result_df.sf_types.update(right.sf_types)

        if on:
            result_df = result_df.reset_index(drop=True)
            if isinstance(on, list):
                # Reorder columns for JOINS with USING clause, where Snowflake puts the key columns to the left
                reordered_cols = on + [
                    col for col in result_df.columns.tolist() if col not in on
                ]
                result_df = result_df[reordered_cols]

        common_columns = set(L_expr_to_alias.keys()).intersection(
            R_expr_to_alias.keys()
        )
        new_expr_to_alias = {
            k: v
            for k, v in {
                **L_expr_to_alias,
                **R_expr_to_alias,
            }.items()
            if k not in common_columns
        }
        expr_to_alias.update(new_expr_to_alias)

        if source_plan.condition:

            def outer_join(base_df):
                ret = base_df.apply(tuple, 1).isin(
                    result_df[condition][base_df.columns].apply(tuple, 1)
                )
                ret.sf_type = ColumnType(BooleanType(), True)
                return ret

            condition = calculate_expression(
                source_plan.condition, result_df, analyzer, expr_to_alias
            )
            sf_types = result_df.sf_types
            if "SEMI" in source_plan.join_type.sql:  # left semi
                result_df = left[outer_join(left)].dropna()
            elif "ANTI" in source_plan.join_type.sql:  # left anti
                result_df = left[~outer_join(left)].dropna()
            elif "LEFT" in source_plan.join_type.sql:  # left outer join
                # rows from LEFT that did not get matched
                unmatched_left = left[~outer_join(left)]
                unmatched_left[right.columns] = None
                result_df = pd.concat(
                    [result_df[condition], unmatched_left], ignore_index=True
                )
                for right_column in right.columns.values:
                    ct = sf_types[right_column]
                    sf_types[right_column] = ColumnType(ct.datatype, True)
            elif "RIGHT" in source_plan.join_type.sql:  # right outer join
                # rows from RIGHT that did not get matched
                unmatched_right = right[~outer_join(right)]
                unmatched_right[left.columns] = None
                result_df = pd.concat(
                    [result_df[condition], unmatched_right], ignore_index=True
                )
                for left_column in left.columns.values:
                    ct = sf_types[left_column]
                    sf_types[left_column] = ColumnType(ct.datatype, True)
            elif "OUTER" in source_plan.join_type.sql:  # full outer join
                # rows from LEFT that did not get matched
                unmatched_left = left[~outer_join(left)]
                unmatched_left[right.columns] = None
                # rows from RIGHT that did not get matched
                unmatched_right = right[~outer_join(right)]
                unmatched_right[left.columns] = None
                result_df = pd.concat(
                    [result_df[condition], unmatched_left, unmatched_right],
                    ignore_index=True,
                )
                for col_name, col_type in sf_types.items():
                    sf_types[col_name] = ColumnType(col_type.datatype, True)
            else:
                result_df = result_df[condition]
            result_df.sf_types = sf_types

        return result_df.where(result_df.notna(), None)  # Swap np.nan with None
    if isinstance(source_plan, MockFileOperation):
        return execute_file_operation(source_plan, analyzer)
    if isinstance(source_plan, SnowflakeCreateTable):
        if source_plan.column_names is not None:
            raise NotImplementedError(
                "[Local Testing] Inserting data into table by matching column names is currently not supported."
            )
        res_df = execute_mock_plan(source_plan.query)
        return entity_registry.write_table(
            source_plan.table_name, res_df, source_plan.mode
        )
    if isinstance(source_plan, UnresolvedRelation):
        entity_name = source_plan.name
        if entity_registry.is_existing_table(entity_name):
            return entity_registry.read_table(entity_name)
        elif entity_registry.is_existing_view(entity_name):
            execution_plan = entity_registry.get_review(entity_name)
            res_df = execute_mock_plan(execution_plan)
            return res_df
        else:
            db_schme_table = parse_table_name(entity_name)
            raise SnowparkSQLException(
                f"Object '{db_schme_table[0][1:-1]}.{db_schme_table[1][1:-1]}.{db_schme_table[2][1:-1]}' does not exist or not authorized."
            )
    if isinstance(source_plan, Sample):
        res_df = execute_mock_plan(source_plan.child)

        if source_plan.row_count and (
            source_plan.row_count < 0 or source_plan.row_count > 100000
        ):
            raise SnowparkSQLException(
                "parameter value out of range: size of fixed sample. Must be between 0 and 1,000,000."
            )

        return res_df.sample(
            n=None
            if source_plan.row_count is None
            else min(source_plan.row_count, len(res_df)),
            frac=source_plan.probability_fraction,
            random_state=source_plan.seed,
        )
    if isinstance(source_plan, CreateViewCommand):
        from_df = execute_mock_plan(source_plan.child, expr_to_alias)
        view_name = source_plan.name
        entity_registry.create_or_replace_view(source_plan.child, view_name)
        return from_df

    if isinstance(source_plan, TableUpdate):
        target = entity_registry.read_table(source_plan.table_name)
        ROW_ID = "row_id_" + generate_random_alphanumeric()
        target.insert(0, ROW_ID, range(len(target)))

        if source_plan.source_data:
            # Calculate cartesian product
            source = execute_mock_plan(source_plan.source_data, expr_to_alias)
            cartesian_product = target.merge(source, on=None, how="cross")
            cartesian_product.sf_types.update(target.sf_types)
            cartesian_product.sf_types.update(source.sf_types)
            intermediate = cartesian_product
        else:
            intermediate = target

        if source_plan.condition:
            # Select rows to be updated based on condition
            condition = calculate_expression(
                source_plan.condition, intermediate, analyzer, expr_to_alias
            )

            matched = target.apply(tuple, 1).isin(
                intermediate[condition][target.columns].apply(tuple, 1)
            )
            matched.sf_type = ColumnType(BooleanType(), True)
            matched_rows = target[matched]
            intermediate = intermediate[condition]
        else:
            matched_rows = target

        # Calculate multi_join
        matched_count = intermediate[target.columns].value_counts()[
            matched_rows.apply(tuple, 1)
        ]
        multi_joins = matched_count.where(lambda x: x > 1).count()

        # Select rows that match the condition to be updated
        rows_to_update = intermediate.drop_duplicates(
            subset=matched_rows.columns, keep="first"
        ).reset_index(  # ERROR_ON_NONDETERMINISTIC_UPDATE is by default False, pick one row to update
            drop=True
        )
        rows_to_update.sf_types = intermediate.sf_types

        # Update rows in place
        for attr, new_expr in source_plan.assignments.items():
            column_name = analyzer.analyze(attr, expr_to_alias)
            target_index = target.loc[rows_to_update[ROW_ID]].index
            new_val = calculate_expression(
                new_expr, rows_to_update, analyzer, expr_to_alias
            )
            new_val.index = target_index
            target.loc[rows_to_update[ROW_ID], column_name] = new_val

        # Delete row_id
        target = target.drop(ROW_ID, axis=1)

        # Write result back to table
        entity_registry.write_table(source_plan.table_name, target, SaveMode.OVERWRITE)
        return [Row(len(rows_to_update), multi_joins)]
    elif isinstance(source_plan, TableDelete):
        target = entity_registry.read_table(source_plan.table_name)

        if source_plan.source_data:
            # Calculate cartesian product
            source = execute_mock_plan(source_plan.source_data, expr_to_alias)
            cartesian_product = target.merge(source, on=None, how="cross")
            cartesian_product.sf_types.update(target.sf_types)
            cartesian_product.sf_types.update(source.sf_types)
            intermediate = cartesian_product
        else:
            intermediate = target

        # Select rows to keep based on condition
        if source_plan.condition:
            condition = calculate_expression(
                source_plan.condition, intermediate, analyzer, expr_to_alias
            )
            intermediate = intermediate[condition]
            matched = target.apply(tuple, 1).isin(
                intermediate[target.columns].apply(tuple, 1)
            )
            matched.sf_type = ColumnType(BooleanType(), True)
            rows_to_keep = target[~matched]
        else:
            rows_to_keep = target.head(0)

        # Write rows to keep to table registry
        entity_registry.write_table(
            source_plan.table_name, rows_to_keep, SaveMode.OVERWRITE
        )
        return [Row(len(target) - len(rows_to_keep))]
    elif isinstance(source_plan, TableMerge):
        target = entity_registry.read_table(source_plan.table_name)
        ROW_ID = "row_id_" + generate_random_alphanumeric()
        SOURCE_ROW_ID = "source_row_id_" + generate_random_alphanumeric()
        # Calculate cartesian product
        source = execute_mock_plan(source_plan.source, expr_to_alias)

        # Insert row_id and source row_id
        target.insert(0, ROW_ID, range(len(target)))
        source.insert(0, SOURCE_ROW_ID, range(len(source)))

        cartesian_product = target.merge(source, on=None, how="cross")
        cartesian_product.sf_types.update(target.sf_types)
        cartesian_product.sf_types.update(source.sf_types)
        join_condition = calculate_expression(
            source_plan.join_expr, cartesian_product, analyzer, expr_to_alias
        )
        join_result = cartesian_product[join_condition]
        join_result.sf_types = cartesian_product.sf_types

        # TODO [GA]: # ERROR_ON_NONDETERMINISTIC_MERGE is by default True, raise error if
        # (1) A target row is selected to be updated with multiple values OR
        # (2) A target row is selected to be both updated and deleted

        inserted_rows = []
        inserted_row_idx = set()  # source_row_id
        deleted_row_idx = set()
        updated_row_idx = set()
        for clause in source_plan.clauses:
            if isinstance(clause, UpdateMergeExpression):
                # Select rows to update
                if clause.condition:
                    condition = calculate_expression(
                        clause.condition, join_result, analyzer, expr_to_alias
                    )
                    rows_to_update = join_result[condition]
                else:
                    rows_to_update = join_result

                rows_to_update = rows_to_update[
                    ~rows_to_update[ROW_ID]
                    .isin(updated_row_idx.union(deleted_row_idx))
                    .values
                ]

                # Update rows in place
                for attr, new_expr in clause.assignments.items():
                    column_name = analyzer.analyze(attr, expr_to_alias)
                    target_index = target.loc[rows_to_update[ROW_ID]].index
                    new_val = calculate_expression(
                        new_expr, rows_to_update, analyzer, expr_to_alias
                    )
                    new_val.index = target_index
                    target.loc[rows_to_update[ROW_ID], column_name] = new_val

                # Update updated row id set
                for _, row in rows_to_update.iterrows():
                    updated_row_idx.add(row[ROW_ID])

            elif isinstance(clause, DeleteMergeExpression):
                # Select rows to delete
                if clause.condition:
                    condition = calculate_expression(
                        clause.condition, join_result, analyzer, expr_to_alias
                    )
                    intermediate = join_result[condition]
                else:
                    intermediate = join_result

                matched = target.apply(tuple, 1).isin(
                    intermediate[target.columns].apply(tuple, 1)
                )
                matched.sf_type = ColumnType(BooleanType(), True)

                # Update deleted row id set
                for _, row in target[matched].iterrows():
                    deleted_row_idx.add(row[ROW_ID])

                # Delete rows in place
                target = target[~matched]

            elif isinstance(clause, InsertMergeExpression):
                # calculate unmatched rows in the source
                matched = source.apply(tuple, 1).isin(
                    join_result[source.columns].apply(tuple, 1)
                )
                matched.sf_type = ColumnType(BooleanType(), True)
                unmatched_rows_in_source = source[~matched]

                # select unmatched rows that qualify the condition
                if clause.condition:
                    condition = calculate_expression(
                        clause.condition,
                        unmatched_rows_in_source,
                        analyzer,
                        expr_to_alias,
                    )
                    unmatched_rows_in_source = unmatched_rows_in_source[condition]

                # filter out the unmatched rows that have been inserted in previous clauses
                unmatched_rows_in_source = unmatched_rows_in_source[
                    ~unmatched_rows_in_source[SOURCE_ROW_ID]
                    .isin(inserted_row_idx)
                    .values
                ]

                # update inserted row idx set
                for _, row in unmatched_rows_in_source.iterrows():
                    inserted_row_idx.add(row[SOURCE_ROW_ID])

                # Calculate rows to insert
                rows_to_insert = TableEmulator(
                    [], columns=target.drop(ROW_ID, axis=1).columns
                )
                rows_to_insert.sf_types = target.sf_types
                if clause.keys:
                    # Keep track of specified columns
                    inserted_columns = set()
                    for k, v in zip(clause.keys, clause.values):
                        column_name = analyzer.analyze(k, expr_to_alias)
                        if column_name not in rows_to_insert.columns:
                            raise SnowparkSQLException(
                                f"Error: invalid identifier '{column_name}'"
                            )
                        inserted_columns.add(column_name)
                        new_val = calculate_expression(
                            v, unmatched_rows_in_source, analyzer, expr_to_alias
                        )
                        rows_to_insert[column_name] = new_val

                    # For unspecified columns, use None as default value
                    for unspecified_col in set(rows_to_insert.columns).difference(
                        inserted_columns
                    ):
                        rows_to_insert[unspecified_col].replace(
                            np.nan, None, inplace=True
                        )

                else:
                    if len(clause.values) != len(rows_to_insert.columns):
                        raise SnowparkSQLException(
                            f"Insert value list does not match column list expecting {len(rows_to_insert.columns)} but got {len(clause.values)}"
                        )
                    for col, v in zip(rows_to_insert.columns, clause.values):
                        new_val = calculate_expression(
                            v, unmatched_rows_in_source, analyzer, expr_to_alias
                        )
                        rows_to_insert[col] = new_val

                inserted_rows.append(rows_to_insert)

        # Remove inserted ROW ID column
        target = target.drop(ROW_ID, axis=1)

        # Process inserted rows
        if inserted_rows:
            res = pd.concat([target] + inserted_rows)
            res.sf_types = target.sf_types
        else:
            res = target

        # Write the result back to table
        entity_registry.write_table(source_plan.table_name, res, SaveMode.OVERWRITE)

        # Generate metadata result
        res = []
        if inserted_rows:
            res.append(len(inserted_row_idx))
        if updated_row_idx:
            res.append(len(updated_row_idx))
        if deleted_row_idx:
            res.append(len(deleted_row_idx))

        return [Row(*res)]

    raise NotImplementedError(
        f"[Local Testing] Mocking SnowflakePlan {type(source_plan).__name__} is not implemented."
    )


def describe(plan: MockExecutionPlan) -> List[Attribute]:
    result = execute_mock_plan(plan)
    ret = []
    for c in result.columns:
        # Raising an exception here will cause infinite recursion
        if isinstance(result[c].sf_type.datatype, NullType):
            ret.append(
                Attribute(
                    result[c].name if result[c].name else "NULL", StringType(), True
                )
            )
        else:
            data_type = result[c].sf_type.datatype
            if isinstance(data_type, (ByteType, ShortType, IntegerType)):
                data_type = LongType()
            elif isinstance(data_type, FloatType):
                data_type = DoubleType()
            elif (
                isinstance(data_type, DecimalType)
                and data_type.precision == 38
                and data_type.scale == 0
            ):
                data_type = LongType()
            elif isinstance(data_type, StringType):
                data_type.length = (
                    StringType._MAX_LENGTH
                    if data_type.length is None
                    else data_type.length
                )

            ret.append(
                Attribute(
                    quote_name(result[c].name.strip()),
                    data_type,
                    result[c].sf_type.nullable,
                )
            )
    return ret


def calculate_expression(
    exp: Expression,
    input_data: Union[TableEmulator, ColumnEmulator],
    analyzer: "MockAnalyzer",
    expr_to_alias: Dict[str, str],
    *,
    keep_literal: bool = False,
) -> ColumnEmulator:
    """
    Returns the calculated expression evaluated based on input table/column
    setting keep_literal to true returns Python datatype
    setting keep_literal to false returns a ColumnEmulator wrapping the Python datatype of a Literal
    """
    import numpy as np

    if isinstance(exp, Attribute):
        try:
            return input_data[expr_to_alias.get(exp.expr_id, exp.name)]
        except KeyError:
            # expr_id maps to the projected name, but input_data might still have the exp.name
            # dealing with the KeyError here, this happens in case df.union(df)
            # TODO: check SNOW-831880 for more context
            return input_data[exp.name]
    if isinstance(exp, (UnresolvedAttribute, Attribute)):
        if exp.is_sql_text:
            raise NotImplementedError(
                "[Local Testing] SQL Text Expression is not supported."
            )
        try:
            return input_data[exp.name]
        except KeyError:
            raise SnowparkSQLException(f"[Local Testing] invalid identifier {exp.name}")
    if isinstance(exp, (UnresolvedAlias, Alias)):
        return calculate_expression(exp.child, input_data, analyzer, expr_to_alias)
    if isinstance(exp, FunctionExpression):
        return handle_function_expression(exp, input_data, analyzer, expr_to_alias)
    if isinstance(exp, ListAgg):
        lhs = calculate_expression(exp.col, input_data, analyzer, expr_to_alias)
        lhs.sf_type = ColumnType(StringType(), exp.col.nullable)
        return _MOCK_FUNCTION_IMPLEMENTATION_MAP["listagg"](
            lhs,
            is_distinct=exp.is_distinct,
            delimiter=exp.delimiter,
        )
    if isinstance(exp, IsNull):
        child_column = calculate_expression(
            exp.child, input_data, analyzer, expr_to_alias
        )
        return ColumnEmulator(
            data=[bool(data is None) for data in child_column],
            sf_type=ColumnType(BooleanType(), True),
        )
    if isinstance(exp, IsNotNull):
        child_column = calculate_expression(
            exp.child, input_data, analyzer, expr_to_alias
        )
        return ColumnEmulator(
            data=[bool(data is not None) for data in child_column],
            sf_type=ColumnType(BooleanType(), True),
        )
    if isinstance(exp, IsNaN):
        child_column = calculate_expression(
            exp.child, input_data, analyzer, expr_to_alias
        )
        res = []
        for data in child_column:
            try:
                res.append(math.isnan(data))
            except TypeError:
                res.append(False)
        return ColumnEmulator(
            data=res, dtype=object, sf_type=ColumnType(BooleanType(), True)
        )
    if isinstance(exp, Not):
        child_column = calculate_expression(
            exp.child, input_data, analyzer, expr_to_alias
        ).astype(bool)
        return ~child_column
    if isinstance(exp, UnresolvedAttribute):
        return analyzer.analyze(exp, expr_to_alias)
    if isinstance(exp, Literal):
        if not keep_literal:
            if isinstance(exp.datatype, StringType):
                # in live session, literal of string type will have size auto inferred
                exp.datatype = StringType(len(exp.value))
            res = ColumnEmulator(
                data=[exp.value for _ in range(len(input_data))],
                sf_type=ColumnType(exp.datatype, False),
                dtype=object,
            )
            res.index = input_data.index
            return res
        return exp.value
    if isinstance(exp, BinaryExpression):
        left = fix_drift_between_column_sf_type_and_dtype(
            calculate_expression(exp.left, input_data, analyzer, expr_to_alias)
        )
        right = fix_drift_between_column_sf_type_and_dtype(
            calculate_expression(exp.right, input_data, analyzer, expr_to_alias)
        )
        # TODO: Address mixed type calculation here. For instance Snowflake allows to add a date to a number, but
        #  pandas doesn't allow. Type coercion will address it.
        if isinstance(exp, Multiply):
            new_column = left * right
        elif isinstance(exp, Divide):
            new_column = left / right
        elif isinstance(exp, Add):
            new_column = left + right
        elif isinstance(exp, Subtract):
            new_column = left - right
        elif isinstance(exp, Remainder):
            new_column = left % right
        elif isinstance(exp, Pow):
            new_column = left**right
        elif isinstance(exp, EqualTo):
            new_column = left == right
            if left.hasnans and right.hasnans:
                new_column[
                    left.apply(lambda x: x is None) & right.apply(lambda x: x is None)
                ] = True
                new_column[
                    left.apply(lambda x: x is not None and np.isnan(x))
                    & right.apply(lambda x: x is not None and np.isnan(x))
                ] = True
                # NaN == NaN evaluates to False in pandas, but True in Snowflake
                new_column[new_column.isna() | new_column.isnull()] = False
        elif isinstance(exp, NotEqualTo):
            new_column = left != right
        elif isinstance(exp, GreaterThanOrEqual):
            new_column = left >= right
        elif isinstance(exp, GreaterThan):
            new_column = left > right
        elif isinstance(exp, LessThanOrEqual):
            new_column = left <= right
        elif isinstance(exp, LessThan):
            new_column = left < right
        elif isinstance(exp, And):
            new_column = (
                (left & right)
                if isinstance(input_data, TableEmulator) or not input_data
                else (left & right) & input_data
            )
        elif isinstance(exp, Or):
            new_column = (
                (left | right)
                if isinstance(input_data, TableEmulator) or not input_data
                else (left | right) & input_data
            )
        elif isinstance(exp, EqualNullSafe):
            either_isna = left.isna() | right.isna() | left.isnull() | right.isnull()
            both_isna = (left.isna() & right.isna()) | (left.isnull() & right.isnull())
            new_column = ColumnEmulator(
                [False] * len(left),
                dtype=bool,
                sf_type=ColumnType(BooleanType(), False),
            )
            new_column[either_isna] = False
            new_column[~either_isna] = left[~either_isna] == right[~either_isna]
            new_column[both_isna] = True
        elif isinstance(exp, BitwiseOr):
            new_column = left | right
        elif isinstance(exp, BitwiseXor):
            new_column = left ^ right
        elif isinstance(exp, BitwiseAnd):
            new_column = left & right
        else:
            raise NotImplementedError(
                f"[Local Testing] Binary expression {type(exp)} is not implemented."
            )
        return new_column
    if isinstance(exp, UnaryMinus):
        res = calculate_expression(exp.child, input_data, analyzer, expr_to_alias)
        return -res
    if isinstance(exp, RegExp):
        lhs = calculate_expression(exp.expr, input_data, analyzer, expr_to_alias)
        raw_pattern = calculate_expression(
            exp.pattern, input_data, analyzer, expr_to_alias
        )[0]
        pattern = f"^{raw_pattern}" if not raw_pattern.startswith("^") else raw_pattern
        pattern = f"{pattern}$" if not pattern.endswith("$") else pattern
        try:
            re.compile(pattern)
        except re.error:
            raise SnowparkSQLException(f"Invalid regular expression {raw_pattern}")
        result = lhs.str.match(pattern)
        result.sf_type = ColumnType(BooleanType(), True)
        return result
    if isinstance(exp, Like):
        lhs = calculate_expression(exp.expr, input_data, analyzer, expr_to_alias)
        pattern = convert_wildcard_to_regex(
            str(
                calculate_expression(exp.pattern, input_data, analyzer, expr_to_alias)[
                    0
                ]
            )
        )
        result = lhs.str.match(pattern)
        result.sf_type = ColumnType(BooleanType(), True)
        return result
    if isinstance(exp, InExpression):
        lhs = calculate_expression(exp.columns, input_data, analyzer, expr_to_alias)
        res = ColumnEmulator([False] * len(lhs), dtype=object)
        res.sf_type = ColumnType(BooleanType(), True)
        for val in exp.values:
            rhs = calculate_expression(val, input_data, analyzer, expr_to_alias)
            if isinstance(lhs, ColumnEmulator):
                if isinstance(rhs, ColumnEmulator):
                    res = res | lhs.isin(rhs)
                elif isinstance(rhs, TableEmulator):
                    res = res | lhs.isin(rhs.iloc[:, 0])
                else:
                    raise NotImplementedError(
                        f"[Local Testing] IN expression does not support {type(rhs)} type on the right"
                    )
            else:
                exists = lhs.apply(tuple, 1).isin(rhs.apply(tuple, 1))
                exists.sf_type = ColumnType(BooleanType(), False)
                res = res | exists
        return res
    if isinstance(exp, ScalarSubquery):
        return execute_mock_plan(exp.plan, expr_to_alias)
    if isinstance(exp, MultipleExpression):
        res = TableEmulator()
        for e in exp.expressions:
            res[analyzer.analyze(e, expr_to_alias)] = calculate_expression(
                e, input_data, analyzer, expr_to_alias
            )
        return res
    if isinstance(exp, Cast):
        column = calculate_expression(exp.child, input_data, analyzer, expr_to_alias)
        if isinstance(exp.to, DateType):
            return _MOCK_FUNCTION_IMPLEMENTATION_MAP["to_date"](
                column, try_cast=exp.try_
            )
        elif isinstance(exp.to, TimeType):
            return _MOCK_FUNCTION_IMPLEMENTATION_MAP["to_time"](
                column, try_cast=exp.try_
            )
        elif isinstance(exp.to, TimestampType):
            return _MOCK_FUNCTION_IMPLEMENTATION_MAP["to_timestamp"](
                column, try_cast=exp.try_
            )
        elif isinstance(exp.to, DecimalType):
            return _MOCK_FUNCTION_IMPLEMENTATION_MAP["to_decimal"](
                column,
                precision=exp.to.precision,
                scale=exp.to.scale,
                try_cast=exp.try_,
            )
        elif isinstance(exp.to, IntegerType):
            res = _MOCK_FUNCTION_IMPLEMENTATION_MAP["to_decimal"](
                column, try_cast=exp.try_
            )
            res.set_sf_type(ColumnType(IntegerType(), nullable=column.sf_type.nullable))
            return res
        elif isinstance(exp.to, BinaryType):
            return _MOCK_FUNCTION_IMPLEMENTATION_MAP["to_binary"](
                column, try_cast=exp.try_
            )
        elif isinstance(exp.to, StringType):
            return _MOCK_FUNCTION_IMPLEMENTATION_MAP["to_char"](
                column, try_cast=exp.try_
            )
        elif isinstance(exp.to, DoubleType):
            return _MOCK_FUNCTION_IMPLEMENTATION_MAP["to_double"](
                column, try_cast=exp.try_
            )
        elif isinstance(exp.to, MapType):
            return _MOCK_FUNCTION_IMPLEMENTATION_MAP["to_object"](column)
        elif isinstance(exp.to, ArrayType):
            return _MOCK_FUNCTION_IMPLEMENTATION_MAP["to_array"](column)
        elif isinstance(exp.to, VariantType):
            return _MOCK_FUNCTION_IMPLEMENTATION_MAP["to_variant"](column)
        else:
            raise NotImplementedError(
                f"[Local Testing] Cast to {exp.to} is not supported yet"
            )
    if isinstance(exp, CaseWhen):
        remaining = input_data
        output_data = ColumnEmulator([None] * len(input_data))
        for case in exp.branches:
            if len(remaining) == 0:
                break
            condition = calculate_expression(
                case[0], input_data, analyzer, expr_to_alias
            )
            value = calculate_expression(case[1], input_data, analyzer, expr_to_alias)

            true_index = remaining[condition].index
            output_data[true_index] = value[true_index]
            remaining = remaining[~remaining.index.isin(true_index)]

            if output_data.sf_type:
                if (
                    not isinstance(output_data.sf_type.datatype, NullType)
                    and output_data.sf_type != value.sf_type
                ):
                    raise SnowparkSQLException(
                        f"CaseWhen expressions have conflicting data types: {output_data.sf_type} != {value.sf_type}"
                    )
            else:
                output_data.sf_type = value.sf_type

        if len(remaining) > 0 and exp.else_value:
            value = calculate_expression(
                exp.else_value, remaining, analyzer, expr_to_alias
            )
            output_data[remaining.index] = value[remaining.index]
            if output_data.sf_type:
                if (
                    not isinstance(output_data.sf_type.datatype, NullType)
                    and output_data.sf_type.datatype != value.sf_type.datatype
                ):
                    raise SnowparkSQLException(
                        f"CaseWhen expressions have conflicting data types: {output_data.sf_type.datatype} != {value.sf_type.datatype}"
                    )
            else:
                output_data.sf_type = value.sf_type
        return output_data
    if isinstance(exp, WindowExpression):
        window_function = exp.window_function
        window_spec = exp.window_spec

        # Process order by clause
        if window_spec.order_spec:
            res = handle_order_by_clause(
                window_spec.order_spec, input_data, analyzer, expr_to_alias
            )
        elif is_rank_related_window_function(window_function):
            raise SnowparkSQLException(
                f"Window function type [{str(window_function)}] requires ORDER BY in window specification"
            )
        else:
            res = input_data

        res_index = res.index  # List of row indexes of the result

        # Process partition_by clause
        if window_spec.partition_spec:
            res = res.groupby(
                [exp.name for exp in window_spec.partition_spec],
                sort=False,
                as_index=False,
            )
            res_index = []
            for r in res:
                res_index += list(r[1].index)

        # Process window frame specification
        # Reference: https://docs.snowflake.com/en/sql-reference/functions-analytic#window-frame-usage-notes
        if not window_spec.frame_spec or not isinstance(
            window_spec.frame_spec, SpecifiedWindowFrame
        ):
            if not is_rank_related_window_function(window_function):
                windows = handle_range_frame_indexing(
                    window_spec.order_spec,
                    res_index,
                    res,
                    analyzer,
                    expr_to_alias,
                    True,
                    False,
                )
            else:
                indexer = EntireWindowIndexer()
                res = res.rolling(indexer)
                windows = [input_data.loc[w.index] for w in res]

        elif isinstance(window_spec.frame_spec.frame_type, RowFrame):
            indexer = RowFrameIndexer(frame_spec=window_spec.frame_spec)
            res = res.rolling(indexer)
            windows = [w for w in res]

        elif isinstance(window_spec.frame_spec.frame_type, RangeFrame):
            upper = window_spec.frame_spec.upper
            lower = window_spec.frame_spec.lower

            if isinstance(upper, Literal) or isinstance(lower, Literal):
                raise SnowparkSQLException(
                    "Range is not supported for sliding window frames."
                )

            windows = handle_range_frame_indexing(
                window_spec.order_spec,
                res_index,
                res,
                analyzer,
                expr_to_alias,
                isinstance(lower, UnboundedPreceding),
                isinstance(upper, UnboundedFollowing),
            )
        # compute window function:
        if isinstance(window_function, (FunctionExpression,)):
            res_cols = []
            for current_row, w in zip(res_index, windows):
                res_cols.append(
                    handle_function_expression(
                        window_function, w, analyzer, expr_to_alias, current_row
                    )
                )
            res_col = pd.concat(res_cols)
            res_col.index = res_index
            if res_cols:
                res_col.set_sf_type(res_cols[0].sf_type)
            else:
                res_col.set_sf_type(ColumnType(NullType(), True))
            return res_col.sort_index()
        elif isinstance(window_function, (Lead, Lag)):
            calculated_sf_type = None
            offset = window_function.offset * (
                1 if isinstance(window_function, Lead) else -1
            )
            ignore_nulls = window_function.ignore_nulls
            res_cols = []
            for current_row, w in zip(res_index, windows):
                row_idx = list(w.index).index(
                    current_row
                )  # the row's 0-base index in the window
                offset_idx = row_idx + offset
                if offset_idx < 0 or offset_idx >= len(w):
                    sub_window_res = calculate_expression(
                        window_function.default,
                        w,
                        analyzer,
                        expr_to_alias,
                    )
                    if not calculated_sf_type:
                        calculated_sf_type = sub_window_res.sf_type
                    elif calculated_sf_type.datatype != sub_window_res.sf_type.datatype:
                        if isinstance(calculated_sf_type.datatype, NullType):
                            calculated_sf_type = sub_window_res.sf_type
                        # the result calculated upon a windows can be None, this is still valid and we can keep
                        # the calculation
                        elif not isinstance(sub_window_res.sf_type.datatype, NullType):
                            raise SnowparkSQLException(
                                f"[Local Testing] Detected type {type(calculated_sf_type.datatype)} and type {type(sub_window_res.sf_type.datatype)}"
                                f" in column, coercion is not currently supported"
                            )
                    res_cols.append(sub_window_res.iloc[0])
                elif not ignore_nulls or offset == 0:
                    sub_window_res = calculate_expression(
                        window_function.expr,
                        w.iloc[[offset_idx]],
                        analyzer,
                        expr_to_alias,
                    )
                    # we use the whole frame to calculate the type
                    cur_windows_sf_type = calculate_expression(
                        window_function.expr,
                        w,
                        analyzer,
                        expr_to_alias,
                    ).sf_type
                    if not calculated_sf_type:
                        calculated_sf_type = cur_windows_sf_type
                    elif calculated_sf_type != cur_windows_sf_type and (
                        not (
                            isinstance(calculated_sf_type.datatype, StringType)
                            and isinstance(cur_windows_sf_type.datatype, StringType)
                        )
                    ):
                        if isinstance(calculated_sf_type.datatype, NullType):
                            calculated_sf_type = sub_window_res.sf_type
                        # the result calculated upon a windows can be None, this is still valid and we can keep
                        # the calculation
                        elif not isinstance(sub_window_res.sf_type.datatype, NullType):
                            raise SnowparkSQLException(
                                f"[Local Testing] Detected type {type(calculated_sf_type.datatype)} and type {type(cur_windows_sf_type.datatype)}"
                                f" in column, coercion is not currently supported"
                            )
                    res_cols.append(sub_window_res.iloc[0])
                else:
                    # skip rows where expr is NULL
                    delta = 1 if offset > 0 else -1
                    cur_idx = row_idx + delta
                    cur_count = 0
                    while 0 <= cur_idx < len(w):
                        target_expr = calculate_expression(
                            window_function.expr,
                            w.iloc[[cur_idx]],
                            analyzer,
                            expr_to_alias,
                        ).iloc[0]
                        if target_expr is not None:
                            cur_count += 1
                            if cur_count == abs(offset):
                                break
                        cur_idx += delta
                    if cur_idx < 0 or cur_idx >= len(w):
                        res_cols.append(
                            calculate_expression(
                                window_function.default,
                                w,
                                analyzer,
                                expr_to_alias,
                            ).iloc[0]
                        )
                    else:
                        res_cols.append(target_expr)
            res_col = ColumnEmulator(
                data=res_cols, dtype=object
            )  # dtype=object prevents implicit converting None to Nan
            res_col.index = res_index
            res_col.sf_type = (
                calculated_sf_type
                if calculated_sf_type
                else ColumnType(NullType(), True)
            )
            return res_col.sort_index()
        elif isinstance(window_function, FirstValue):
            ignore_nulls = window_function.ignore_nulls
            res_cols = []
            for w in windows:
                if not ignore_nulls:
                    res_cols.append(
                        calculate_expression(
                            window_function.expr,
                            w.iloc[[0]],
                            analyzer,
                            expr_to_alias,
                        ).iloc[0]
                    )
                else:
                    for cur_idx in range(len(w)):
                        target_expr = calculate_expression(
                            window_function.expr,
                            w.iloc[[cur_idx]],
                            analyzer,
                            expr_to_alias,
                        ).iloc[0]
                        if target_expr is not None:
                            res_cols.append(target_expr)
                            break
                    else:
                        res_cols.append(None)
            res_col = ColumnEmulator(
                data=res_cols,
                dtype=object,
                sf_type=calculate_expression(
                    window_function.expr,
                    input_data,
                    analyzer,
                    expr_to_alias,
                ).sf_type,
            )  # dtype=object prevents implicit converting None to Nan
            res_col.index = res_index
            return res_col.sort_index()
        elif isinstance(window_function, LastValue):
            ignore_nulls = window_function.ignore_nulls
            res_cols = []
            for w in windows:
                if not ignore_nulls:
                    res_cols.append(
                        calculate_expression(
                            window_function.expr,
                            w.iloc[[len(w) - 1]],
                            analyzer,
                            expr_to_alias,
                        ).iloc[0]
                    )
                else:
                    for cur_idx in range(len(w) - 1, -1, -1):
                        target_expr = calculate_expression(
                            window_function.expr,
                            w.iloc[[cur_idx]],
                            analyzer,
                            expr_to_alias,
                        ).iloc[0]
                        if target_expr is not None:
                            res_cols.append(target_expr)
                            break
                    else:
                        res_cols.append(None)
            res_col = ColumnEmulator(
                data=res_cols,
                dtype=object,
                sf_type=calculate_expression(
                    window_function.expr,
                    windows[0],
                    analyzer,
                    expr_to_alias,
                ).sf_type,
            )  # dtype=object prevents implicit converting None to Nan
            res_col.index = res_index
            return res_col.sort_index()
        else:
            raise NotImplementedError(
                f"[Local Testing] Window Function {window_function} is not implemented."
            )
    elif isinstance(exp, SubfieldString):
        col = calculate_expression(exp.child, input_data, analyzer, expr_to_alias)
        field = str(exp.field)
        # in snowflake, two consecutive single quotes means escaping single quote
        field = field.replace("''", "'")
        col._null_rows_idxs = [
            index
            for index in range(len(col))
            if col[index] is not None
            and field in col[index]
            and col[index][field] is None
        ]
        res = col.apply(lambda x: None if x is None or field not in x else x[field])
        res.set_sf_type(ColumnType(VariantType(), col.sf_type.nullable))
        return res
    elif isinstance(exp, SubfieldInt):
        col = calculate_expression(exp.child, input_data, analyzer, expr_to_alias)
        res = col.apply(lambda x: None if x is None else x[exp.field])
        res.set_sf_type(ColumnType(VariantType(), col.sf_type.nullable))
        return res
    raise NotImplementedError(
        f"[Local Testing] Mocking Expression {type(exp).__name__} is not implemented."
    )


def execute_file_operation(source_plan: MockFileOperation, analyzer: "MockAnalyzer"):
    if source_plan.operator == MockFileOperation.Operator.PUT:
        return mock_file_operation.put(
            source_plan.local_file_name, source_plan.stage_location
        )
    if source_plan.operator == MockFileOperation.Operator.READ_FILE:
        return mock_file_operation.read_file(
            source_plan.stage_location,
            source_plan.format,
            source_plan.schema,
            analyzer,
            source_plan.options,
        )
    raise NotImplementedError(
        f"[Local Testing] File operation {source_plan.operator.value} is not implemented."
    )
