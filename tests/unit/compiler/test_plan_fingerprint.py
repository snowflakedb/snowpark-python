#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark._internal.analyzer.binary_plan_node import Join, Inner
from snowflake.snowpark._internal.analyzer.expression import (
    Attribute,
    FunctionExpression,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import SnowflakeTable
from snowflake.snowpark._internal.analyzer.unary_plan_node import (
    Aggregate,
    Filter,
    Project,
)
from snowflake.snowpark._internal.compiler.plan_fingerprint import (
    MAX_FINGERPRINT_ENTRIES,
    extract_plan_fingerprint_from_logical_plan,
)
from snowflake.snowpark.types import LongType


def test_extract_plan_fingerprint_filter_aggregate(mock_session):
    table = SnowflakeTable("t", session=mock_session)
    filtered = Filter(
        Attribute("a", LongType()),
        table,
    )
    aggregated = Aggregate(
        [],
        [
            FunctionExpression("sum", [Attribute("a", LongType())], False),
        ],
        filtered,
    )
    fingerprint = extract_plan_fingerprint_from_logical_plan(aggregated)
    assert fingerprint.plan_operators == ["read", "filter", "aggregate"]
    assert fingerprint.plan_functions_ordered == ["sum"]
    assert fingerprint.plan_is_redacted is False
    assert fingerprint.snowpark_fp_skeleton() == fingerprint.snowpark_fp_skeleton()


def test_extract_plan_fingerprint_join(mock_session):
    left = SnowflakeTable("l", session=mock_session)
    right = SnowflakeTable("r", session=mock_session)
    joined = Join(left, right, Inner(), None, None, False)
    fingerprint = extract_plan_fingerprint_from_logical_plan(joined)
    assert fingerprint.plan_operators == ["read", "read", "join"]


def test_extract_plan_fingerprint_truncation(mock_session):
    project_list = [
        FunctionExpression("count", [Attribute(f"c{i}", LongType())], False)
        for i in range(MAX_FINGERPRINT_ENTRIES + 5)
    ]
    plan = Project(project_list, SnowflakeTable("t", session=mock_session))
    fingerprint = extract_plan_fingerprint_from_logical_plan(plan)
    assert fingerprint.plan_is_redacted is True
    assert len(fingerprint.plan_functions_ordered) <= MAX_FINGERPRINT_ENTRIES
