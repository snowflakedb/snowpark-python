#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
import snowflake.snowpark.context as context
from copy import copy
from unittest.mock import patch, Mock


from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import col, lit, max


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="debug_mode not used in local testing mode",
)
@pytest.mark.parametrize("debug_mode", [True, False])
@pytest.mark.parametrize(
    "transform",
    [
        pytest.param(lambda x: copy(x), id="copy"),
        pytest.param(lambda x: x.to_df(["C", "D"]), id="to_df"),
        pytest.param(lambda x: x.distinct(), id="distinct"),
        pytest.param(lambda x: x.drop_duplicates(), id="drop_duplicates"),
        pytest.param(lambda x: x.limit(1), id="limit"),
        pytest.param(lambda x: x.union(x), id="union"),
        pytest.param(lambda x: x.union_all(x), id="union_all"),
        pytest.param(lambda x: x.union_by_name(x), id="union_by_name"),
        pytest.param(lambda x: x.union_all_by_name(x), id="union_all_by_name"),
        pytest.param(lambda x: x.intersect(x), id="intersect"),
        pytest.param(lambda x: x.natural_join(x), id="natural_join"),
        pytest.param(lambda x: x.cross_join(x), id="cross_join"),
        pytest.param(lambda x: x.sample(n=1), id="sample"),
        pytest.param(
            lambda x: x.with_column_renamed(col("A"), "B"), id="with_column_renamed"
        ),
        # Unpivot already validates names
        pytest.param(lambda x: x.unpivot("x", "y", ["A"]), id="unpivot"),
        # The following functions do not error early because their schema_query do not contain
        # information about the transformation being called.
        pytest.param(lambda x: x.drop(col("A")), id="drop"),
        pytest.param(lambda x: x.filter(col("A") == lit(1)), id="filter"),
        pytest.param(lambda x: x.sort(col("A").desc()), id="sort"),
    ],
)
def test_early_attributes(session, transform, debug_mode):
    with patch.object(context, "_debug_eager_schema_validation", debug_mode):
        df = session.create_dataframe([(1, "A"), (2, "B"), (3, "C")], ["A", "B"])

        transformed = transform(df)

        # When debug mode is enabled the dataframe plan attributes are populated early
        if debug_mode:
            assert transformed._plan._metadata.attributes is not None
        else:
            assert transformed._plan._metadata.attributes is None


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="debug_mode not used in local testing mode",
)
@pytest.mark.parametrize("debug_mode", [True, False])
@pytest.mark.parametrize(
    "transform",
    [
        pytest.param(lambda x: x.select("B"), id="select"),
        pytest.param(lambda x: x.select_expr("cast(b as str)"), id="select_expr"),
        pytest.param(lambda x: x.agg(max("B")), id="agg"),
        pytest.param(lambda x: x.join(copy(x), on=(col("A") == col("B"))), id="join"),
        pytest.param(
            lambda x: x.join_table_function("flatten", col("B")),
            id="join_table_function",
        ),
        pytest.param(lambda x: x.with_column("C", col("B")), id="with_column"),
        pytest.param(lambda x: x.with_columns(["C"], [col("B")]), id="with_columns"),
    ],
)
def test_early_error(session, transform, debug_mode):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    with patch.object(context, "_debug_eager_schema_validation", debug_mode):
        df = session.create_dataframe([1, 2, 3], ["A"])

        show_mock = Mock()
        show_mock.__qualname__ = "show"
        show_mock.__name__ = "show"

        with patch("snowflake.snowpark.dataframe.DataFrame.show", show_mock):
            try:
                transformed = transform(df)
                transformed.show()
            except Exception:
                pass
            # When debug mode is enabled the error is thrown before reaching show.
            # Without debug mode the error only shows up once show is called.
            if debug_mode:
                show_mock.assert_not_called()
                assert df._plan._metadata.attributes is not None
            else:
                show_mock.assert_called()
                assert df._plan._metadata.attributes is None


def _count_describes(fn):
    calls = []
    original = SnowflakePlan._analyze_attributes
    original_full = SnowflakePlan._describe_full_query

    def counting(self):
        calls.append(1)
        return original(self)

    def counting_full(self):
        calls.append(1)
        return original_full(self)

    with patch.object(SnowflakePlan, "_analyze_attributes", counting), patch.object(
        SnowflakePlan, "_describe_full_query", counting_full
    ):
        fn()
    return len(calls)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="debug_mode not used in local testing mode",
)
@pytest.mark.parametrize("reduce_describe", [True, False])
@pytest.mark.parametrize("simplifier", [True, False])
@pytest.mark.parametrize(
    "transform",
    [
        pytest.param(lambda x: x.filter(col("NOPE") > 1), id="filter"),
        pytest.param(lambda x: x.sort("NOPE"), id="sort"),
        pytest.param(lambda x: x.sort(col("NOPE").desc()), id="sort_col"),
        pytest.param(
            lambda x: x.select("A").filter(col("NOPE") > 1), id="select_filter"
        ),
        pytest.param(lambda x: x.filter(col("A") > 0).sort("NOPE"), id="filter_sort"),
        pytest.param(lambda x: x.sort("A").filter(col("NOPE") > 1), id="sort_filter"),
    ],
)
def test_eager_validation_catches_missing_column_in_filter_and_sort(
    session, transform, simplifier, reduce_describe
):
    original_simplifier = session.sql_simplifier_enabled
    original_reduce = session.reduce_describe_query_enabled
    session.sql_simplifier_enabled = simplifier
    session.reduce_describe_query_enabled = reduce_describe
    try:
        df = session.create_dataframe([(1,), (2,)], ["A"])
        with patch.object(context, "_debug_eager_schema_validation", True):
            with pytest.raises(SnowparkSQLException) as ex_info:
                transform(df)
        assert "invalid identifier" in str(ex_info.value)
        assert "There are existing quoted column identifiers" in str(ex_info.value)

        # Without eager validation the error only appears when the query runs.
        with patch.object(context, "_debug_eager_schema_validation", False):
            lazy = transform(df)
        with pytest.raises(SnowparkSQLException, match="invalid identifier"):
            lazy.collect()
    finally:
        session.sql_simplifier_enabled = original_simplifier
        session.reduce_describe_query_enabled = original_reduce


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="debug_mode not used in local testing mode",
)
@pytest.mark.parametrize("simplifier", [True, False])
def test_eager_validation_keeps_valid_filter_and_sort_working(session, simplifier):
    original_simplifier = session.sql_simplifier_enabled
    session.sql_simplifier_enabled = simplifier
    try:
        df = session.create_dataframe([(1, "x"), (2, "y")], ["A", "B"])
        with patch.object(context, "_debug_eager_schema_validation", True):
            result = df.filter(col("A") > 1).sort(col("A").desc()).select("A")
            if simplifier:
                # B is dropped by the select but stays visible to a WHERE that is
                # flattened into the same statement, so this must keep working.
                result = result.filter(col("B") == "y")
        assert [r[0] for r in result.collect()] == [2]
    finally:
        session.sql_simplifier_enabled = original_simplifier


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="debug_mode not used in local testing mode",
)
def test_eager_validation_adds_one_describe_per_filter_or_sort(session):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    original_reduce = session.reduce_describe_query_enabled
    session.reduce_describe_query_enabled = True
    try:
        df = session.create_dataframe([(1,), (2,)], ["A"])
        df.schema  # warm the base schema

        with patch.object(context, "_debug_eager_schema_validation", False):
            assert _count_describes(lambda: df.filter(col("A") > 0).sort("A")) == 0
        with patch.object(context, "_debug_eager_schema_validation", True):
            assert _count_describes(lambda: df.filter(col("A") > 0).sort("A")) == 2
            # operations without a WHERE or ORDER BY add no full-query describe
            assert _count_describes(lambda: df.select("A").limit(1)) == 0
    finally:
        session.reduce_describe_query_enabled = original_reduce


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="debug_mode not used in local testing mode",
)
def test_eager_validation_skips_full_query_describe_with_pre_actions(session):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    # cache_result() creates a temp table through a pre-action; a filter on top of a
    # plan that still carries pre-actions must not describe a query that depends on them.
    cached = session.create_dataframe([(1,), (2,)], ["A"]).cache_result()
    with patch.object(context, "_debug_eager_schema_validation", True):
        assert [r[0] for r in cached.filter(col("A") > 1).collect()] == [2]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="debug_mode not used in local testing mode",
)
def test_session_eager_schema_validation_overrides_the_global_setting(session):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    original_reduce = session.reduce_describe_query_enabled
    session.reduce_describe_query_enabled = True
    eager = session.eager_schema_validation
    try:
        df = session.create_dataframe([(1,), (2,)], ["A"])
        df.schema  # warm the base schema

        def build():
            return df.filter(col("A") > 0).sort("A")

        with patch.object(context, "_debug_eager_schema_validation", False):
            assert eager.enabled is False
            with eager:
                assert eager.enabled is True
                assert _count_describes(build) == 2
            assert eager.enabled is False
            assert _count_describes(build) == 0

        with patch.object(context, "_debug_eager_schema_validation", True):
            # the global setting is only the fallback
            assert eager.enabled is True
            eager.disable()
            assert _count_describes(build) == 0
    finally:
        eager._set(None)
        session.reduce_describe_query_enabled = original_reduce


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="debug_mode not used in local testing mode",
)
def test_session_debug_turns_all_debugging_features_on_and_off(session):
    try:
        session.debug.enable_all()
        assert session.eager_schema_validation.enabled is True
        assert context._enable_dataframe_trace_on_error is True
        assert context._enable_trace_sql_errors_to_dataframe is True
        with pytest.raises(SnowparkSQLException):
            session.create_dataframe([(1,)], ["A"]).select("NOPE").collect()

        session.debug.disable_all()
        assert session.eager_schema_validation.enabled is False
        assert context._enable_dataframe_trace_on_error is False
        assert context._enable_trace_sql_errors_to_dataframe is False
    finally:
        session.eager_schema_validation._set(None)
        context.configure_development_features()
