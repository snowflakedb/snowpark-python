#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import copy
import sys
from unittest import mock

from typing import List, Dict, Set

import uuid
import pytest

from snowflake.snowpark import Session
from snowflake.snowpark._internal.analyzer.analyzer import Analyzer

from snowflake.snowpark._internal.analyzer.select_statement import (
    ColumnStateDict,
    Selectable,
    SelectableEntity,
    SelectSQL,
    SelectSnowflakePlan,
    SelectTableFunction,
    SetStatement,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import Query, Attribute, SnowflakePlan
from snowflake.snowpark.types import (
    IntegerType,
    StringType,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    SnowflakeTable
)

def init_snowflake_plan(session: Session) -> SnowflakePlan:
    return SnowflakePlan(
        queries=[Query("FAKE PLAN QUERY")],
        schema_query="FAKE SCHEMA QUERY",
        post_actions=[Query("FAKE POST ACTION")],
        expr_to_alias={uuid.uuid4(): "PLAN_EXPR1", uuid.uuid4(): "PLAN_EXPR2"},
        source_plan=SnowflakeTable("TEST_TABLE"),
        is_ddl_on_temp_object=False,
        api_calls=None,
        df_aliased_col_name_to_real_col_name = {"df_alias": {"A": "A", "B": "B1"}},
        placeholder_query=None,
        session=session
    )


def init_selectable_fields(node: Selectable, init_plan: bool) -> None:
    node.pre_actions = [Query("DUMMY PRE ACTION")]
    node.post_actions = [Query("DUMMY POST ACTION")]
    node.flatten_disabled = True
    dummy_column_dict = ColumnStateDict()
    dummy_column_dict.projection = [Attribute("A", IntegerType()), Attribute("B", StringType())]
    # The following are useful aggregate information of all columns. Used to quickly rule if a query can be flattened.
    dummy_column_dict.has_changed_columns = False
    dummy_column_dict.has_new_columns = True
    dummy_column_dict.dropped_columns = ["C"]
    dummy_column_dict.active_columns = ["A", "B"]
    dummy_column_dict.columns_referencing_all_columns = set()
    node._column_states = dummy_column_dict

    node.expr_to_alias = {uuid.uuid4(): "EXPR1", uuid.uuid4(): "EXPR2"}
    node.df_aliased_col_name_to_real_col_name = {
        "df_alias": {"A": "A", "B": "B1"}
    }
    if init_plan:
        session = mock.create_autospec(Session)
        node._snowflake_plan = init_snowflake_plan(session)


def verify_copied_selectable(copied_selectable: Selectable, original_selectable: Selectable, expect_plan_copied: bool = False) -> None:
    # verify _snowflake_plan is never copied
    if expect_plan_copied:
        assert copied_selectable._snowflake_plan is not None
    else:
        assert copied_selectable._snowflake_plan is None
    assert copied_selectable.pre_actions == original_selectable.pre_actions
    assert copied_selectable.post_actions == original_selectable.post_actions
    assert copied_selectable.flatten_disabled == original_selectable.flatten_disabled
    assert copied_selectable.expr_to_alias == original_selectable.expr_to_alias
    assert copied_selectable.df_aliased_col_name_to_real_col_name == copied_selectable.df_aliased_col_name_to_real_col_name

    copied_state = copied_selectable._column_states
    original_state = original_selectable._column_states
    assert copied_state.active_columns == original_state.active_columns
    assert copied_state.has_new_columns == original_state.has_new_columns
    # verify the column projection
    # direct compare should fail because the attribute objects are different
    assert copied_state.projection != original_state.projection
    for copied_attribute, original_attribute in zip(
        copied_state.projection, original_state.projection
    ):
        assert copied_attribute.name == original_attribute.name
        assert copied_attribute.datatype == original_attribute.datatype
        assert copied_attribute.nullable == original_attribute.nullable


def test_selectable_entity():
    analyzer = mock.create_autospec(Analyzer)
    selectable_entity = SelectableEntity("test_selectable", analyzer=analyzer)
    init_selectable_fields(selectable_entity, init_plan=False)
    copied_selectable = copy.deepcopy(selectable_entity)
    verify_copied_selectable(copied_selectable, selectable_entity)
    assert copied_selectable.entity_name == selectable_entity.entity_name


def test_select_sql():
    analyzer = mock.create_autospec(Analyzer)
    # none-select sql
    sql = "show tables limit 10"
    select_sql = SelectSQL(
        sql,
        convert_to_select=False,
        analyzer=analyzer,
        params=[1, "a", 2, "b"]
    )

    init_selectable_fields(select_sql, init_plan=True)
    assert select_sql.convert_to_select is False
    assert select_sql._snowflake_plan is not None
    copied_selectable = copy.deepcopy(select_sql)
    verify_copied_selectable(copied_selectable, select_sql)

    # set convert_to_select to True to test the copy
    select_sql.convert_to_select = True
    copied_selectable = copy.deepcopy(select_sql)
    verify_copied_selectable(copied_selectable, select_sql)
    assert copied_selectable.convert_to_select == select_sql.convert_to_select
    assert copied_selectable.original_sql == select_sql.original_sql


def test_select_snowflake_plan():
    analyzer = mock.create_autospec(Analyzer)
    session = mock.create_autospec(Session)
    snowflake_plan = init_snowflake_plan(session)
    select_snowflake_plan = SelectSnowflakePlan(
        snowflake_plan=snowflake_plan,
        analyzer=analyzer,
    )
    init_selectable_fields(select_snowflake_plan, init_plan=False)
    copied_selectable = copy.deepcopy(select_snowflake_plan)
    verify_copied_selectable(copied_selectable, select_snowflake_plan, expect_plan_copied=True)


def test_select_statement():
    analyzer = mock.create_autospec(Analyzer)

    """
    projection: Optional[List[Expression]] = None,
    from_: Selectable,
    where: Optional[Expression] = None,
    order_by: Optional[List[Expression]] = None,
    limit_: Optional[int] = None,
    offset: Optional[int] = None,
    analyzer: "Analyzer",
    schema_query: Optional[str] = None,
    """
