#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from functools import cached_property
from typing import Any, Dict, List, Optional

import snowflake.snowpark
from snowflake.snowpark.mock import TableEmulator
from snowflake.snowpark._internal.analyzer.analyzer_utils import unquote_if_quoted
from snowflake.snowpark._internal.analyzer.binary_plan_node import Join
from snowflake.snowpark._internal.analyzer.expression import (
    Attribute,
    FunctionExpression,
    SnowflakeUDF,
    UnresolvedAttribute,
)
from snowflake.snowpark._internal.analyzer.select_statement import SelectSQL
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    SnowflakeCreateTable,
    SnowflakeTable,
)
from snowflake.snowpark._internal.analyzer.table_function import (
    TableFunctionJoin,
    TableFunctionRelation,
)
from snowflake.snowpark._internal.analyzer.table_merge_expression import (
    TableDelete,
    TableMerge,
    TableUpdate,
)
from snowflake.snowpark._internal.analyzer.unary_expression import (
    Alias,
    Cast,
    UnresolvedAlias,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import (
    Aggregate,
    Pivot,
    Project,
)
from snowflake.snowpark.mock._plan import MockExecutionPlan
from snowflake.snowpark.mock._select_statement import MockSelectable

# from snowflake.snowpark.session import Session
from snowflake.snowpark.types import (
    IntegerType,
    MapType,
    PandasDataFrameType,
    _NumericType,
)


def resolve_attributes(
    plan: LogicalPlan,
    session: Optional["snowflake.snowpark.session.Session"] = None,
):
    if isinstance(plan, MockSelectable):
        attributes = plan.attributes

    elif isinstance(plan, Aggregate):
        attributes = plan.grouping_expressions + plan.aggregate_expressions

    elif isinstance(plan, Join):
        attributes = plan.left.attributes + plan.right.attributes

    elif isinstance(plan, SelectSQL):
        # Since we don't know the attributes, we'll just assume it's a single column.
        attributes = [Attribute("$1", _NumericType())]

    elif isinstance(plan, SnowflakeCreateTable):
        attributes = plan.query.attributes

    elif isinstance(plan, Project):
        project_attributes = {
            unquote_if_quoted(attr.name): attr for attr in plan.project_list
        }
        child_attributes = resolve_attributes(plan.children[0], session)
        if len(project_attributes) == 1 and (
            "*" in project_attributes or "STAR()" in project_attributes
        ):
            attributes = child_attributes
        else:
            source_attributes = {
                unquote_if_quoted(attr.name): attr for attr in child_attributes
            }
            attributes = [
                Attribute(
                    attr.name,
                    source_attributes[attr_name].datatype
                    if attr_name in source_attributes
                    else IntegerType(),
                    source_attributes[attr_name].nullable
                    if attr_name in source_attributes
                    else True,
                )
                if isinstance(attr, UnresolvedAttribute)
                else attr
                for attr_name, attr in project_attributes.items()
            ]

    elif isinstance(plan, Pivot):
        pivot_attrs = plan.children[0].attributes.copy()
        pivot_col_index = next(
            i for i, v in enumerate(pivot_attrs) if v.name == plan.pivot_column.name
        )
        pivot_attrs.pop(pivot_col_index)
        # TODO: This doesn't work for dynamic pivot cases.
        pivot_result_cols = (
            [
                Attribute(str(val.value), _NumericType, False)
                for val in plan.pivot_values
            ]
            if plan.pivot_values
            else []
        )
        pivot_attrs.extend(pivot_result_cols)
        attributes = pivot_attrs

    elif isinstance(plan, TableEmulator):
        attributes = [Attribute(name, _NumericType(), False) for name in plan.columns]

    elif isinstance(plan, TableUpdate):
        attributes = [
            Attribute(name, _NumericType(), False)
            for name in ["multi_joined_rows_updated", "rows_updated"]
        ]

    elif isinstance(plan, TableMerge):
        attributes = [
            Attribute(name, _NumericType(), False)
            for name in ["rows_deleted", "rows_inserted", "rows_updated"]
        ]

    elif isinstance(plan, TableDelete):
        attributes = [
            Attribute(name, _NumericType(), False) for name in ["rows_deleted"]
        ]

    elif isinstance(plan, SnowflakeTable):
        entity_plan = session._conn.entity_registry.read_table(plan.name)
        attributes = resolve_attributes(entity_plan, session)

    elif isinstance(plan, TableFunctionRelation):
        output_schema = session.udtf.get_udtf(
            plan.table_function.func_name
        )._output_schema
        attributes = [
            Attribute(col.name, col.datatype, col.nullable) for col in output_schema
        ]

    elif isinstance(plan, TableFunctionJoin):
        left_attributes = resolve_attributes(plan.children[0], session)
        try:
            output_schema = session.udtf.get_udtf(
                plan.table_function.func_name
            )._output_schema
        except KeyError:
            if session is not None and session._conn._suppress_not_implemented_error:
                return []
            else:
                raise
        if isinstance(output_schema, PandasDataFrameType):
            right_attributes = [
                Attribute(col_name, col_type, True)
                for col_name, col_type in zip(
                    output_schema.col_names, output_schema.col_types
                )
            ]
        else:
            right_attributes = [
                Attribute(col.name, col.datatype, col.nullable) for col in output_schema
            ]
        # TODO: This assumes left_cols=['*'] and right_cols=['*']
        attributes = left_attributes + right_attributes

    elif hasattr(plan, "output"):
        attributes = plan.output

    elif hasattr(plan, "execution_plan"):
        if hasattr(plan.execution_plan, "attributes"):
            attributes = plan.execution_plan.attributes

    elif hasattr(plan, "children"):
        if plan.children:
            attributes = resolve_attributes(plan.children[0], session)
        else:
            # If there's no attributes, it could be a SQL so will assume a single column response.
            attributes = [Attribute("$1", _NumericType())]
    else:
        raise NotImplementedError

    # Note this doesn't handle all unresolved attributes, just most common like Alias or UDF usage.
    resolved_attributes = []
    for i, attr in enumerate(attributes):
        if isinstance(attr, (Alias, UnresolvedAlias)):
            # Handle special case of parse_json which is silently inserted for some types in session.createDataFrame
            if (
                isinstance(attr.child, Cast)
                and isinstance(attr.child.child, FunctionExpression)
                and attr.child.child.name == "parse_json"
            ):
                attr = Attribute(attr.name, MapType(), attr.children[0].nullable)
            else:
                attr = Attribute(
                    attr.name, attr.children[0].datatype, attr.children[0].nullable
                )
        elif isinstance(attr, SnowflakeUDF):
            data_type = session.udaf.get_udaf(attr.udf_name)._return_type
            attr = Attribute(f"${i}", data_type, True)
        elif isinstance(attr, UnresolvedAttribute):
            attr = Attribute(attr.name, _NumericType(), False)
        elif not isinstance(attr, Attribute):
            raise NotImplementedError
        resolved_attributes.append(attr)

    return resolved_attributes


class NopExecutionPlan(MockExecutionPlan):
    @property
    def attributes(self) -> List[Attribute]:
        return self.output

    @cached_property
    def output(self) -> List[Attribute]:
        return resolve_attributes(self.source_plan, session=self.session)

    @property
    def output_dict(self) -> Dict[str, Any]:
        output_dict = {
            attr.name: (attr.datatype, attr.nullable) for attr in self.output
        }
        return output_dict
