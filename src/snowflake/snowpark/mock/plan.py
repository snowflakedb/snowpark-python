#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import importlib
import inspect
import math
import re
import uuid
from enum import Enum
from functools import cached_property, partial
from typing import TYPE_CHECKING, Dict, List, NoReturn, Optional, Union
from unittest.mock import MagicMock

if TYPE_CHECKING:
    from snowflake.snowpark.mock.analyzer import MockAnalyzer

import numpy as np
import pandas as pd

import snowflake.snowpark.mock.file_operation as mock_file_operation
from snowflake.snowpark import Column, Row
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
    RegExp,
    Star,
    UnresolvedAttribute,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    Range,
    SnowflakeCreateTable,
    SnowflakeValues,
    UnresolvedRelation,
)
from snowflake.snowpark._internal.analyzer.sort_expression import Ascending, NullsFirst
from snowflake.snowpark._internal.analyzer.unary_expression import (
    Alias,
    Cast,
    IsNaN,
    IsNotNull,
    IsNull,
    Not,
    UnresolvedAlias,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import Aggregate, Sample
from snowflake.snowpark._internal.type_utils import infer_type
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.mock.functions import _MOCK_FUNCTION_IMPLEMENTATION_MAP
from snowflake.snowpark.mock.select_statement import (
    MockSelectable,
    MockSelectableEntity,
    MockSelectExecutionPlan,
    MockSelectStatement,
    MockSetStatement,
)
from snowflake.snowpark.mock.snowflake_data_type import (
    ColumnEmulator,
    ColumnType,
    TableEmulator,
)
from snowflake.snowpark.mock.util import convert_wildcard_to_regex, custom_comparator
from snowflake.snowpark.types import (
    BooleanType,
    ByteType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NullType,
    ShortType,
    StringType,
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
    ) -> NoReturn:
        super().__init__()
        self.source_plan = source_plan
        self.session = session
        mock_query = MagicMock()
        mock_query.sql = "SELECT MOCK_TEST_FAKE_QUERY()"
        self.queries = [mock_query]
        self.child = child
        self.expr_to_alias = expr_to_alias if expr_to_alias is not None else {}
        self.api_calls = []

    # @cached_property
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


def execute_mock_plan(
    plan: MockExecutionPlan,
    expr_to_alias: Optional[Dict[str, str]] = None,
) -> Union[TableEmulator, List[Row]]:
    if expr_to_alias is None:
        expr_to_alias = {}
    if isinstance(plan, (MockExecutionPlan, SnowflakePlan)):
        source_plan = plan.source_plan
        analyzer = plan.session._analyzer
    else:
        source_plan = plan
        analyzer = plan.analyzer
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
            table[column_name].set_sf_type(table.sf_types[column_name])
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

        sort_columns_array = []
        sort_orders_array = []
        null_first_last_array = []
        if order_by:
            for exp in order_by:
                sort_columns_array.append(analyzer.analyze(exp.child, expr_to_alias))
                sort_orders_array.append(isinstance(exp.direction, Ascending))
                null_first_last_array.append(
                    isinstance(exp.null_ordering, NullsFirst)
                    or exp.null_ordering == NullsFirst
                )

        if sort_columns_array:
            kk = reversed(
                list(zip(sort_columns_array, sort_orders_array, null_first_last_array))
            )
            for column, ascending, null_first in kk:
                comparator = partial(custom_comparator, ascending, null_first)
                result_df = result_df.sort_values(by=column, key=comparator)

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
        # TODO: supports other entities, e.g. view
        table_registry = analyzer.session._conn.table_registry
        return table_registry.read_table(source_plan.entity_name)
    if isinstance(source_plan, Aggregate):
        child_rf = execute_mock_plan(source_plan.child)
        if (
            not source_plan.aggregate_expressions
            and not source_plan.grouping_expressions
        ):
            return (
                child_rf.iloc[0].to_frame().T
                if len(child_rf)
                else TableEmulator(data=None, dtype=object, columns=child_rf.columns)
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
                elif isinstance(agg_expr.child, (ListAgg, FunctionExpression)):
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
        column_exps = [
            (
                plan.session._analyzer.analyze(exp),
                bool(isinstance(exp, Literal)),
                ColumnType(exp.datatype, exp.nullable),
            )
            for exp in source_plan.grouping_expressions
        ]
        for column_name, _, column_type in column_exps:
            result_df_sf_Types[column_name] = column_type
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
                    ] = cal_exp_res.sf_type
                else:
                    values.append(cal_exp_res)
                    result_df_sf_Types[columns[idx] + len(column_exps)] = ColumnType(
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
                for left_column in right.columns.values:
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
        table_registry = analyzer.session._conn.table_registry
        res_df = execute_mock_plan(source_plan.query)
        return table_registry.write_table(
            source_plan.table_name, res_df, source_plan.mode
        )
    if isinstance(source_plan, UnresolvedRelation):
        table_registry = analyzer.session._conn.table_registry
        return table_registry.read_table(source_plan.name)
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

    raise NotImplementedError(
        f"[Local Testing] Mocking SnowflakePlan {type(source_plan).__name__} is not implemented."
    )


def describe(plan: MockExecutionPlan) -> List[Attribute]:
    result = execute_mock_plan(plan)
    ret = []
    for c in result.columns:
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
    analyzer,
    expr_to_alias: Dict[str, str],
    *,
    keep_literal: bool = False,
) -> ColumnEmulator:
    """
    Returns the calculated expression evaluated based on input table/column
    setting keep_literal to true returns Python datatype
    setting keep_literal to false returns a ColumnEmulator wrapping the Python datatype of a Literal
    """
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
        # evaluated_children maps to parameters passed to the function call
        evaluated_children = [
            calculate_expression(
                c, input_data, analyzer, expr_to_alias, keep_literal=True
            )
            for c in exp.children
        ]
        try:
            original_func = getattr(
                importlib.import_module("snowflake.snowpark.functions"),
                exp.name.lower(),
            )
        except Attribute:
            raise NotImplementedError(
                f"[Local Testing] Mocking function {exp.name.lower()} is not supported."
            )

        signatures = inspect.signature(original_func)
        spec = inspect.getfullargspec(original_func)
        if exp.name not in _MOCK_FUNCTION_IMPLEMENTATION_MAP:
            raise NotImplementedError(
                f"[Local Testing] Mocking function {exp.name} is not implemented."
            )
        to_pass_args = []
        for idx, key in enumerate(signatures.parameters):
            if key == spec.varargs:
                to_pass_args.extend(evaluated_children[idx:])
            else:
                try:
                    to_pass_args.append(evaluated_children[idx])
                except IndexError:
                    to_pass_args.append(None)

        if exp.name == "count" and exp.is_distinct:
            if "count_distinct" not in _MOCK_FUNCTION_IMPLEMENTATION_MAP:
                raise NotImplementedError(
                    f"[Local Testing] Mocking function {exp.name}  is not implemented."
                )
            return _MOCK_FUNCTION_IMPLEMENTATION_MAP["count_distinct"](
                *evaluated_children
            )
        if (
            exp.name == "count"
            and isinstance(exp.children[0], Literal)
            and exp.children[0].sql == "LITERAL()"
        ):
            to_pass_args[0] = input_data
        if exp.name == "array_agg":
            to_pass_args[-1] = exp.is_distinct
        if exp.name == "sum" and exp.is_distinct:
            to_pass_args[0] = ColumnEmulator(
                data=to_pass_args[0].unique(), sf_type=to_pass_args[0].sf_type
            )
        return _MOCK_FUNCTION_IMPLEMENTATION_MAP[exp.name](*to_pass_args)
    if isinstance(exp, ListAgg):
        column = calculate_expression(exp.col, input_data, analyzer, expr_to_alias)
        column.sf_type = ColumnType(StringType(), exp.col.nullable)
        return _MOCK_FUNCTION_IMPLEMENTATION_MAP["listagg"](
            column,
            is_distinct=exp.is_distinct,
            delimiter=exp.delimiter,
        )
    if isinstance(exp, IsNull):
        child_column = calculate_expression(
            exp.child, input_data, analyzer, expr_to_alias
        )
        return ColumnEmulator(
            data=[bool(data is None) for data in child_column],
            sf_type=ColumnType(BooleanType(), False),
        )
    if isinstance(exp, IsNotNull):
        child_column = calculate_expression(
            exp.child, input_data, analyzer, expr_to_alias
        )
        return ColumnEmulator(
            data=[bool(data is not None) for data in child_column],
            sf_type=ColumnType(BooleanType(), False),
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
            data=res, dtype=object, sf_type=ColumnType(BooleanType(), False)
        )
    if isinstance(exp, Not):
        child_column = calculate_expression(
            exp.child, input_data, analyzer, expr_to_alias
        )
        return ~child_column
    if isinstance(exp, UnresolvedAttribute):
        return analyzer.analyze(exp, expr_to_alias)
    if isinstance(exp, Literal):
        if not keep_literal:
            res = ColumnEmulator(
                data=[exp.value for _ in range(len(input_data))],
                sf_type=ColumnType(exp.datatype, False),
                dtype=object,
            )
            res.index = input_data.index
            return res
        return exp.value
    if isinstance(exp, BinaryExpression):
        left = calculate_expression(exp.left, input_data, analyzer, expr_to_alias)
        right = calculate_expression(exp.right, input_data, analyzer, expr_to_alias)
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
            new_column = (
                (left == right)
                | (left.isna() & right.isna())
                | (left.isnull() & right.isnull())
            )
        else:
            raise NotImplementedError(
                f"[Local Testing] Binary expression {type(exp)} is not implemented."
            )
        return new_column
    if isinstance(exp, RegExp):
        column = calculate_expression(exp.expr, input_data, analyzer, expr_to_alias)
        raw_pattern = calculate_expression(
            exp.pattern, input_data, analyzer, expr_to_alias
        )[0]
        pattern = f"^{raw_pattern}" if not raw_pattern.startswith("^") else raw_pattern
        pattern = f"{pattern}$" if not pattern.endswith("$") else pattern
        try:
            re.compile(pattern)
        except re.error:
            raise SnowparkSQLException(f"Invalid regular expression {raw_pattern}")
        result = column.str.match(pattern)
        result.sf_type = ColumnType(BooleanType(), exp.nullable)
        return result
    if isinstance(exp, Like):
        column = calculate_expression(exp.expr, input_data, analyzer, expr_to_alias)
        pattern = convert_wildcard_to_regex(
            str(
                calculate_expression(exp.pattern, input_data, analyzer, expr_to_alias)[
                    0
                ]
            )
        )
        result = column.str.match(pattern)
        result.sf_type = ColumnType(BooleanType(), exp.nullable)
        return result
    if isinstance(exp, InExpression):
        column = calculate_expression(exp.columns, input_data, analyzer, expr_to_alias)
        values = [
            calculate_expression(
                expression, input_data, analyzer, expr_to_alias, keep_literal=True
            )
            for expression in exp.values
        ]
        result = column.isin(values)
        result.sf_type = ColumnType(BooleanType(), True)
        return result
    if isinstance(exp, Cast):
        column = calculate_expression(exp.child, input_data, analyzer, expr_to_alias)
        column.sf_type = ColumnType(exp.to, exp.nullable)
        return column
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
                if output_data.sf_type != value.sf_type:
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
                if output_data.sf_type != value.sf_type:
                    raise SnowparkSQLException(
                        f"CaseWhen expressions have conflicting data types: {output_data.sf_type} != {value.sf_type}"
                    )
            else:
                output_data.sf_type = value.sf_type
        return output_data

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
