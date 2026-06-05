#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from unittest.mock import patch

import snowflake.snowpark._internal.analyzer.snowflake_plan as snowflake_plan
from snowflake.snowpark import context
from snowflake.snowpark._internal.analyzer.schema_utils import (
    _cached_analyze_attributes_cache_key,
    analyze_attributes,
    cached_analyze_attributes,
)


class _DummySession:
    def __init__(self, session_id: int) -> None:
        self._session_id = session_id


class _UnhashableParam:
    __hash__ = None

    def __repr__(self) -> str:
        return "UNHASHABLE_PARAM"


def test_cached_analyze_attributes_cache_key_normalizes_only_source_plan_id():
    session = _DummySession(session_id=101)
    sql_plan_a = 'SELECT "A-0000000a-0" AS "A-1a" FROM T'
    sql_plan_b = 'SELECT "A-0f0f0f0f-0" AS "A-1a" FROM T'
    sql_different_projection = 'SELECT "A-0f0f0f0f-1" AS "A-1a" FROM T'

    with patch.object(context, "_is_snowpark_connect_compatible_mode", True):
        assert _cached_analyze_attributes_cache_key(sql_plan_a, session) == (
            _cached_analyze_attributes_cache_key(sql_plan_b, session)
        )
        assert _cached_analyze_attributes_cache_key(sql_plan_a, session) != (
            _cached_analyze_attributes_cache_key(sql_different_projection, session)
        )


def test_cached_analyze_attributes_cache_key_keeps_alias_unique_suffix_distinct():
    session = _DummySession(session_id=101)
    sql_alias_a = 'SELECT "A-0000000a-0" AS "A-1a" FROM T'
    sql_alias_b = 'SELECT "A-0f0f0f0f-0" AS "A-2b" FROM T'

    with patch.object(context, "_is_snowpark_connect_compatible_mode", True):
        assert _cached_analyze_attributes_cache_key(sql_alias_a, session) != (
            _cached_analyze_attributes_cache_key(sql_alias_b, session)
        )


def test_cached_analyze_attributes_cache_key_isolated_by_session_and_params():
    sql = 'SELECT "A-0000000a-0" AS "A-1a" FROM T'
    session_1 = _DummySession(session_id=101)
    session_2 = _DummySession(session_id=202)

    with patch.object(context, "_is_snowpark_connect_compatible_mode", True):
        key_1 = _cached_analyze_attributes_cache_key(
            sql, session_1, query_params=[1, "x"]
        )
        key_2 = _cached_analyze_attributes_cache_key(
            sql, session_2, query_params=[1, "x"]
        )
        key_3 = _cached_analyze_attributes_cache_key(
            sql, session_1, query_params=[2, "x"]
        )

    assert key_1 != key_2
    assert key_1 != key_3


def test_cached_analyze_attributes_cache_key_handles_nested_query_params():
    session = _DummySession(session_id=101)
    sql = 'SELECT "A-0000000a-0" AS "A-1a" FROM T WHERE C = ?'
    query_params = [{"p": [1, {"k": {"b", "a"}}]}]

    with patch.object(context, "_is_snowpark_connect_compatible_mode", True):
        key = _cached_analyze_attributes_cache_key(
            sql, session, query_params=query_params
        )

    assert key == (
        101,
        'SELECT "A-PLANID-0" AS "A-1a" FROM T WHERE C = ?',
        ((("p", (1, (("k", ("a", "b")),))),),),
    )


def test_cached_analyze_attributes_cache_key_uses_repr_for_unhashable_leaf():
    session = _DummySession(session_id=101)
    sql = 'SELECT "A-0000000a-0" AS "A-1a" FROM T WHERE C = ?'
    query_params = [{"p": _UnhashableParam()}]

    with patch.object(context, "_is_snowpark_connect_compatible_mode", True):
        key = _cached_analyze_attributes_cache_key(
            sql, session, query_params=query_params
        )

    assert key == (
        101,
        'SELECT "A-PLANID-0" AS "A-1a" FROM T WHERE C = ?',
        ((("p", "UNHASHABLE_PARAM"),),),
    )


def test_cached_analyze_attributes_reuses_equivalent_generated_suffix_sql():
    cached_analyze_attributes.clear_cache()
    session = _DummySession(session_id=303)
    sql_plan_a = 'SELECT "A-0000000a-0" AS "A-1a" FROM T'
    sql_plan_b = 'SELECT "A-0f0f0f0f-0" AS "A-1a" FROM T'

    with patch.object(context, "_is_snowpark_connect_compatible_mode", True):
        with patch(
            "snowflake.snowpark._internal.analyzer.schema_utils.analyze_attributes",
            side_effect=lambda sql, *_: [sql],
        ) as mock_analyze:
            result_1 = cached_analyze_attributes(
                sql_plan_a,
                session,
                dataframe_uuid="uuid-a",
                query_params=None,
            )
            result_2 = cached_analyze_attributes(
                sql_plan_b,
                session,
                dataframe_uuid="uuid-b",
                query_params=None,
            )

        assert mock_analyze.call_count == 1
        assert result_1 == result_2

    cached_analyze_attributes.clear_cache()


def test_cached_analyze_attributes_legacy_mode_uses_sql_key():
    cached_analyze_attributes.clear_cache()
    session = _DummySession(session_id=303)
    sql_plan_a = 'SELECT "A-0000000a-0" FROM T'
    sql_plan_b = 'SELECT "A-0f0f0f0f-0" FROM T'

    with patch.object(context, "_is_snowpark_connect_compatible_mode", False):
        with patch(
            "snowflake.snowpark._internal.analyzer.schema_utils.analyze_attributes",
            side_effect=lambda sql, *_: [sql],
        ) as mock_analyze:
            result_1 = cached_analyze_attributes(
                sql_plan_a,
                session,
                dataframe_uuid="uuid-a",
                query_params=None,
            )
            result_2 = cached_analyze_attributes(
                sql_plan_b,
                session,
                dataframe_uuid="uuid-b",
                query_params=None,
            )

        assert mock_analyze.call_count == 2
        assert result_1 != result_2

    cached_analyze_attributes.clear_cache()


def test_cached_analyze_attributes_clear_cache_clears_both_internal_caches():
    cached_analyze_attributes.clear_cache()
    session = _DummySession(session_id=303)
    sql = 'SELECT "A-0000000a-0" FROM T'

    with patch(
        "snowflake.snowpark._internal.analyzer.schema_utils.analyze_attributes",
        side_effect=lambda sql, *_: [sql],
    ):
        with patch.object(context, "_is_snowpark_connect_compatible_mode", False):
            cached_analyze_attributes(
                sql, session, dataframe_uuid="uuid-a", query_params=[1]
            )
        with patch.object(context, "_is_snowpark_connect_compatible_mode", True):
            cached_analyze_attributes(
                sql, session, dataframe_uuid="uuid-b", query_params=[1]
            )

    assert len(cached_analyze_attributes._cache) == 2  # type: ignore[attr-defined]

    cached_analyze_attributes.clear_cache()

    assert len(cached_analyze_attributes._cache) == 0  # type: ignore[attr-defined]


def test_cached_analyze_attributes_signature_compatible_with_analyze_attributes_wraps():
    # Integration tests monkeypatch snowflake_plan.cached_analyze_attributes with
    # wraps=analyze_attributes to bypass cache and count DESCRIBE calls.
    class _DummyAnalyzeSession:
        pass

    with patch.object(
        snowflake_plan, "cached_analyze_attributes", wraps=analyze_attributes
    ) as wrapped:
        result = snowflake_plan.cached_analyze_attributes(
            "alter session set foo=bar", _DummyAnalyzeSession(), "df-uuid", None
        )

    assert wrapped.call_count == 1
    assert len(result) == 1
