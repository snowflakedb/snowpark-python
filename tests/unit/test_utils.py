#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from collections import defaultdict
from unittest import mock

import pytest
import uuid
import copy
import time
from snowflake.snowpark._internal.utils import (
    ExprAliasUpdateDict,
    str_contains_alphabet,
    get_sorted_key_for_version,
    ttl_cache,
    remove_comments,
    get_line_numbers,
    get_plan_from_line_numbers,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    SnowflakePlan,
    QueryLineInterval,
)


def test_expr_alias_update_dict():
    expr_dict = ExprAliasUpdateDict()
    key1, key2, key3 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()

    # Test setting and getting values
    expr_dict[key1] = "alias1"
    assert (
        expr_dict[key1] == "alias1"
        and expr_dict.was_updated_due_to_inheritance(key1) is False
    )
    expr_dict[key2] = ("alias2", True)
    assert (
        expr_dict[key2] == "alias2"
        and expr_dict.was_updated_due_to_inheritance(key2) is True
    )
    expr_dict[key3] = ("alias3", False)
    assert (
        expr_dict[key3] == "alias3"
        and expr_dict.was_updated_due_to_inheritance(key3) is False
    )

    # Test invalid value
    with pytest.raises(ValueError, match=r"Value must be a tuple of \(str, bool\)"):
        expr_dict[key1] = ("alias3", "not a bool")

    # Test get with default
    new_key = uuid.uuid4()
    assert expr_dict.get(new_key, "default_alias") == "default_alias"
    assert expr_dict.was_updated_due_to_inheritance(new_key) is False

    # Test items and values
    expr_dict[key1] = "alias4"
    items = dict(expr_dict.items())
    assert items[key1] == "alias4"
    assert set(expr_dict.values()) == {"alias3", "alias4", "alias2"}

    # Test update
    dict2 = ExprAliasUpdateDict()
    key3 = uuid.uuid4()
    dict2[key3] = ("alias5", True)
    expr_dict.update(dict2)
    assert expr_dict[key3] == "alias5"
    assert expr_dict.was_updated_due_to_inheritance(key3) is True

    # Test copy and deepcopy
    copied_dict = expr_dict.copy()
    deep_copied_dict = copy.deepcopy(expr_dict)
    assert (
        copied_dict[key3] == "alias5"
        and copied_dict.was_updated_due_to_inheritance(key3) is True
    )
    assert (
        deep_copied_dict[key3] == "alias5"
        and deep_copied_dict.was_updated_due_to_inheritance(key3) is True
    )
    assert copied_dict is not expr_dict
    assert deep_copied_dict is not expr_dict

    # Test defaultdict update behavior
    df_alias_dict1, df_alias_dict2 = defaultdict(ExprAliasUpdateDict), defaultdict(
        ExprAliasUpdateDict
    )
    df_alias_1, df_alias_2 = "df_1", "df_2"
    col_key_1, col_key_2 = "col_1", "col_2"

    df_alias_dict1[df_alias_1][col_key_1] = ("alias1", True)
    df_alias_dict1[df_alias_1][col_key_2] = ("alias2", True)

    # only 1 df alias with 2 col aliases in the df_alias_dict1
    assert len(df_alias_dict1) == 1 and len(df_alias_dict1[df_alias_1]) == 2

    df_alias_dict2[df_alias_2][col_key_1] = ("alias3", False)
    df_alias_dict2[df_alias_1][col_key_2] = ("alias4", False)

    df_alias_dict1.update(df_alias_dict2)

    # after update, there should be 2 df alias replacing the previous one
    assert (
        len(df_alias_dict1) == 2
        and len(df_alias_dict1[df_alias_1]) == 1
        and len(df_alias_dict1[df_alias_2]) == 1
    )
    assert (
        df_alias_dict1[df_alias_1][col_key_2] == "alias4"
        and df_alias_dict1[df_alias_1].was_updated_due_to_inheritance(col_key_2)
        is False
        and df_alias_dict1[df_alias_2][col_key_1] == "alias3"
        and df_alias_dict1[df_alias_2].was_updated_due_to_inheritance(col_key_1)
        is False
    )


def test_str_contains_alphabet():
    assert str_contains_alphabet("1a1") is True
    assert str_contains_alphabet("abc") is True
    assert str_contains_alphabet("123") is False
    assert str_contains_alphabet("12b34") is True
    assert str_contains_alphabet(".") is False
    assert str_contains_alphabet("1.2.3") is False


def test_get_sorted_key_for_version():
    assert get_sorted_key_for_version("1.11.1a1") == (1, 11, -1)
    assert get_sorted_key_for_version("1.2.3") == (1, 2, 3)
    assert get_sorted_key_for_version("2.0.0") == (2, 0, 0)
    assert get_sorted_key_for_version("3.4a.5") == (3, -1, 5)
    assert get_sorted_key_for_version("10.20.30") == (10, 20, 30)
    assert get_sorted_key_for_version("4.5.6b7") == (4, 5, -1)
    assert get_sorted_key_for_version("7.8.9c") == (7, 8, -1)


def test_ttl_cache():
    calls = {"long": 0, "no": 0}

    @ttl_cache(ttl_seconds=60 * 60 * 24)  # 24 hours
    def sum_two_long(a, b):
        calls["long"] += 1
        return a + b

    @ttl_cache(ttl_seconds=0)  # Effectively no cache
    def sum_two_short(a, b):
        calls["no"] += 1
        return a + b

    # After one call each should have executed once
    sum_two_long(1, 1)
    sum_two_short(1, 1)
    assert calls["long"] == 1
    assert calls["no"] == 1

    # Windows has a 16ms time resolution so wait at least a second to make sure
    # the short cache ages out.
    time.sleep(1)

    # After a second call the long cache should use the cached result
    # The no-cache result should have executed again
    sum_two_long(1, 1)
    sum_two_short(1, 1)
    assert calls["long"] == 1
    assert calls["no"] == 2

    # Each of the caches should have one item
    assert len(sum_two_long._cache) == 1
    assert len(sum_two_short._cache) == 1

    time.sleep(1)

    # The long cache should have a second item
    # The no-cache should have aged out the previous call when adding the new one
    sum_two_long(2, 2)
    sum_two_short(2, 2)
    assert len(sum_two_long._cache) == 2
    assert len(sum_two_short._cache) == 1

    @ttl_cache(60)
    def union_sets(a, b):
        return a | b

    # Even though the inputs are unhashable the result is still cached
    union_sets({1}, {2})
    assert len(union_sets._cache) == 1


def test_remove_comments():

    # Test with one uuid
    sql_query = """SELECT * FROM table1
-- uuid-1
WHERE column1 = 'value'
-- uuid-1
ORDER BY column2"""

    uuids = ["uuid-1"]
    expected = """SELECT * FROM table1
WHERE column1 = 'value'
ORDER BY column2"""

    result = remove_comments(sql_query, uuids)
    assert result == expected

    # Test when no comments match
    sql_query = """SELECT * FROM table1
-- uuid-3
WHERE column1 = 'value'
-- uuid-3"""

    uuids = ["uuid-1", "uuid-2"]

    result = remove_comments(sql_query, uuids)
    assert result == sql_query  # Should be unchanged

    # Test with empty SQL query
    sql_query = ""
    uuids = ["uuid-1"]

    result = remove_comments(sql_query, uuids)
    assert result == ""

    # Test with empty UUID list
    sql_query = """SELECT * FROM table1
-- uuid-1
WHERE column1 = 'value'
-- uuid-1"""

    uuids = []

    result = remove_comments(sql_query, uuids)
    assert result == sql_query

    # Test with multiple UUIDs
    sql_query = """(
-- uuid-1
 SELECT
    "_1" AS "ID",
    "_2" AS "NAME",
    "_3" AS "VALUE"
 FROM (
 SELECT $1 AS "_1", $2 AS "_2", $3 AS "_3" FROM  VALUES (1 :: INT, 'A' :: STRING, 100 :: INT), (2 :: INT, 'B' :: STRING, 200 :: INT)
)
-- uuid-1
) UNION (
-- uuid-2
 SELECT
    "_1" AS "ID",
    "_2" AS "NAME",
    "_3" AS "VALUE"
 FROM (
 SELECT $1 AS "_1", $2 AS "_2", $3 AS "_3" FROM  VALUES (3 :: INT, 'C' :: STRING, 300 :: INT), (4 :: INT, 'D' :: STRING, 400 :: INT)
)
-- uuid-2
)"""

    uuids = ["uuid-1", "uuid-2"]
    expected = """(
 SELECT
    "_1" AS "ID",
    "_2" AS "NAME",
    "_3" AS "VALUE"
 FROM (
 SELECT $1 AS "_1", $2 AS "_2", $3 AS "_3" FROM  VALUES (1 :: INT, 'A' :: STRING, 100 :: INT), (2 :: INT, 'B' :: STRING, 200 :: INT)
)
) UNION (
 SELECT
    "_1" AS "ID",
    "_2" AS "NAME",
    "_3" AS "VALUE"
 FROM (
 SELECT $1 AS "_1", $2 AS "_2", $3 AS "_3" FROM  VALUES (3 :: INT, 'C' :: STRING, 300 :: INT), (4 :: INT, 'D' :: STRING, 400 :: INT)
)
)"""

    result = remove_comments(sql_query, uuids)
    assert result == expected


def test_get_line_numbers():
    # Test basic line number mapping with child UUIDs
    sql_query = """SELECT col1
FROM
-- child-uuid-1
table1
-- child-uuid-1
WHERE col1 > 0
-- child-uuid-2
ORDER BY col1
-- child-uuid-2"""

    child_uuids = ["child-uuid-1", "child-uuid-2"]
    parent_uuid = "parent-uuid"

    result = get_line_numbers(sql_query, child_uuids, parent_uuid)

    expected = [
        QueryLineInterval(0, 1, "parent-uuid"),
        QueryLineInterval(2, 2, "child-uuid-1"),
        QueryLineInterval(3, 3, "parent-uuid"),
        QueryLineInterval(4, 4, "child-uuid-2"),
    ]

    assert len(result) == len(expected)
    for i, a in enumerate(result):
        assert repr(a) == repr(expected[i])

    # Test when there are no child UUIDs
    sql_query = """SELECT col1
FROM table1
WHERE col1 > 0"""

    child_uuids = []
    parent_uuid = "parent-uuid"

    result = get_line_numbers(sql_query, child_uuids, parent_uuid)

    expected = [QueryLineInterval(0, 2, "parent-uuid")]

    assert len(result) == 1
    assert repr(result[0]) == repr(expected[0])

    # Test with query containing multiple UUIDs
    sql_query = """(
-- uuid-1
 SELECT
    "_1" AS "ID",
    "_2" AS "NAME",
    "_3" AS "VALUE"
 FROM (
 SELECT $1 AS "_1", $2 AS "_2", $3 AS "_3" FROM  VALUES (1 :: INT, 'A' :: STRING, 100 :: INT), (2 :: INT, 'B' :: STRING, 200 :: INT)
)
-- uuid-1
) UNION (
-- uuid-2
 SELECT
    "_1" AS "ID",
    "_2" AS "NAME",
    "_3" AS "VALUE"
 FROM (
 SELECT $1 AS "_1", $2 AS "_2", $3 AS "_3" FROM  VALUES (3 :: INT, 'C' :: STRING, 300 :: INT), (4 :: INT, 'D' :: STRING, 400 :: INT)
)
-- uuid-2
)"""

    child_uuids = ["uuid-1", "uuid-2"]
    parent_uuid = "parent-uuid"

    result = get_line_numbers(sql_query, child_uuids, parent_uuid)

    expected = [
        QueryLineInterval(0, 0, "parent-uuid"),
        QueryLineInterval(1, 7, "uuid-1"),
        QueryLineInterval(8, 8, "parent-uuid"),
        QueryLineInterval(9, 15, "uuid-2"),
        QueryLineInterval(16, 16, "parent-uuid"),
    ]

    assert len(result) == len(expected)
    for i, a in enumerate(result):
        assert repr(a) == repr(expected[i])


def test_get_plan_from_line_numbers():
    # Test finding plan for a line number in simple case
    intervals = [QueryLineInterval(0, 5, "plan-uuid-123")]
    simple_query = """SELECT
    id,
    name
FROM users
WHERE active = true
ORDER BY name"""

    with mock.patch("uuid.uuid4", return_value=mock.Mock(hex="plan-uuid-123")):
        mock_query = mock.Mock()
        mock_query.sql = simple_query
        mock_query.query_line_intervals = intervals

        plan = mock.Mock(spec=SnowflakePlan)
        plan.uuid = "plan-uuid-123"
        plan.children_plan_nodes = []
        plan.queries = [mock_query]

    result = get_plan_from_line_numbers(plan, 3)
    assert result.uuid == "plan-uuid-123"

    # Test finding plan with child plans
    child_intervals = [QueryLineInterval(0, 2, "child-uuid-456")]
    child_query = """SELECT *
FROM orders
WHERE status = 'pending'"""

    with mock.patch("uuid.uuid4", return_value=mock.Mock(hex="child-uuid-456")):
        child_mock_query = mock.Mock()
        child_mock_query.sql = child_query
        child_mock_query.query_line_intervals = child_intervals

        child_plan = mock.Mock(spec=SnowflakePlan)
        child_plan.uuid = "child-uuid-456"
        child_plan.children_plan_nodes = []
        child_plan.queries = [child_mock_query]

    parent_intervals = [
        QueryLineInterval(0, 2, "parent-uuid-789"),
        QueryLineInterval(3, 5, "child-uuid-456"),
        QueryLineInterval(6, 8, "parent-uuid-789"),
    ]
    parent_query = """SELECT u.name, COUNT(pending_orders.total) as pending_count
FROM users u
LEFT JOIN (
SELECT *
FROM orders
WHERE status = 'pending'
) pending_orders ON u.id = pending_orders.user_id
GROUP BY u.name"""

    with mock.patch("uuid.uuid4", return_value=mock.Mock(hex="parent-uuid-789")):
        parent_mock_query = mock.Mock()
        parent_mock_query.sql = parent_query
        parent_mock_query.query_line_intervals = parent_intervals

        parent_plan = mock.Mock(spec=SnowflakePlan)
        parent_plan.uuid = "parent-uuid-789"
        parent_plan.children_plan_nodes = [child_plan]
        parent_plan.queries = [parent_mock_query]

    result = get_plan_from_line_numbers(parent_plan, 3)
    assert result.uuid == "child-uuid-456"

    # Test error when line number doesn't fall in any interval
    intervals = [QueryLineInterval(0, 3, "interval-plan-uuid-222")]
    interval_query = """SELECT
    category,
    AVG(price)
FROM products GROUP BY category"""

    with mock.patch("uuid.uuid4", return_value=mock.Mock(hex="interval-plan-uuid-222")):
        interval_mock_query = mock.Mock()
        interval_mock_query.sql = interval_query
        interval_mock_query.query_line_intervals = intervals

        interval_plan = mock.Mock(spec=SnowflakePlan)
        interval_plan.uuid = "interval-plan-uuid-222"
        interval_plan.children_plan_nodes = []
        interval_plan.queries = [interval_mock_query]

    with pytest.raises(
        ValueError, match="Line number 10 does not fall within any interval"
    ):
        get_plan_from_line_numbers(interval_plan, 10)

    # Test finding plan where children have descendants
    grandchild_intervals = [QueryLineInterval(0, 1, "grandchild-uuid-999")]
    grandchild_query = """SELECT product_id
FROM inventory"""

    with mock.patch("uuid.uuid4", return_value=mock.Mock(hex="grandchild-uuid-999")):
        grandchild_mock_query = mock.Mock()
        grandchild_mock_query.sql = grandchild_query
        grandchild_mock_query.query_line_intervals = grandchild_intervals

        grandchild_plan = mock.Mock(spec=SnowflakePlan)
        grandchild_plan.uuid = "grandchild-uuid-999"
        grandchild_plan.children_plan_nodes = []
        grandchild_plan.queries = [grandchild_mock_query]

    child_with_desc_intervals = [
        QueryLineInterval(0, 1, "child-desc-uuid-888"),
        QueryLineInterval(2, 3, "grandchild-uuid-999"),  # Points to grandchild
        QueryLineInterval(4, 5, "child-desc-uuid-888"),
    ]
    child_with_desc_query = """SELECT p.name, inv.product_id
FROM products p
JOIN (
SELECT product_id
FROM inventory
) inv ON p.id = inv.product_id"""

    with mock.patch("uuid.uuid4", return_value=mock.Mock(hex="child-desc-uuid-888")):
        child_with_desc_mock_query = mock.Mock()
        child_with_desc_mock_query.sql = child_with_desc_query
        child_with_desc_mock_query.query_line_intervals = child_with_desc_intervals

        child_with_desc_plan = mock.Mock(spec=SnowflakePlan)
        child_with_desc_plan.uuid = "child-desc-uuid-888"
        child_with_desc_plan.children_plan_nodes = [grandchild_plan]
        child_with_desc_plan.queries = [child_with_desc_mock_query]

    grandparent_intervals = [
        QueryLineInterval(0, 2, "grandparent-uuid-777"),
        QueryLineInterval(3, 8, "child-desc-uuid-888"),
        QueryLineInterval(9, 10, "grandparent-uuid-777"),
    ]
    grandparent_query = """SELECT c.name, product_info.name as product_name
FROM customers c
JOIN (
SELECT p.name, inv.product_id
FROM products p
JOIN (
SELECT product_id
FROM inventory
) inv ON p.id = inv.product_id
) product_info ON c.preferred_product = product_info.product_id
ORDER BY c.name"""

    with mock.patch("uuid.uuid4", return_value=mock.Mock(hex="grandparent-uuid-777")):
        grandparent_mock_query = mock.Mock()
        grandparent_mock_query.sql = grandparent_query
        grandparent_mock_query.query_line_intervals = grandparent_intervals

        grandparent_plan = mock.Mock(spec=SnowflakePlan)
        grandparent_plan.uuid = "grandparent-uuid-777"
        grandparent_plan.children_plan_nodes = [child_with_desc_plan]
        grandparent_plan.queries = [grandparent_mock_query]

    result = get_plan_from_line_numbers(grandparent_plan, 6)
    assert result.uuid == "grandchild-uuid-999"

    result = get_plan_from_line_numbers(grandparent_plan, 4)
    assert result.uuid == "child-desc-uuid-888"

    result = get_plan_from_line_numbers(grandparent_plan, 1)
    assert result.uuid == "grandparent-uuid-777"


def test_get_plan_from_line_numbers_no_matching_child():
    """Test that get_plan_from_line_numbers raises ValueError when interval points to non-existent child."""
    orphan_intervals = [
        QueryLineInterval(0, 2, "parent-uuid-123"),
        QueryLineInterval(3, 5, "nonexistent-child-uuid-999"),
        QueryLineInterval(6, 8, "parent-uuid-123"),
    ]

    orphan_query = """SELECT u.name, COUNT(orders.total) as order_count
FROM users u
LEFT JOIN (
SELECT *
FROM orders
WHERE status = 'completed'
) orders ON u.id = orders.user_id
GROUP BY u.name"""

    with mock.patch("uuid.uuid4", return_value=mock.Mock(hex="parent-uuid-123")):
        orphan_mock_query = mock.Mock()
        orphan_mock_query.sql = orphan_query
        orphan_mock_query.query_line_intervals = orphan_intervals

        orphan_plan = mock.Mock(spec=SnowflakePlan)
        orphan_plan.uuid = "parent-uuid-123"
        orphan_plan.children_plan_nodes = []
        orphan_plan.queries = [orphan_mock_query]

    with pytest.raises(
        ValueError, match="Line number 4 does not fall within any interval"
    ):
        get_plan_from_line_numbers(orphan_plan, 4)
