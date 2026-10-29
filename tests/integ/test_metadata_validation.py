#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from unittest.mock import patch

import pytest

import snowflake.snowpark.context as context
from snowflake.snowpark import Row
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark.exceptions import SnowparkPlanException, SnowparkSQLException
from snowflake.snowpark.functions import col
from tests.utils import Utils

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="Metadata validation is not used in local testing mode",
    ),
]


@pytest.fixture(autouse=True)
def metadata_validation_on():
    context.configure_development_features(enable_metadata_validation=True)
    yield
    context.configure_development_features(enable_metadata_validation=False)


@pytest.fixture
def base(session):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    return session.create_dataframe([(1, "x"), (2, "y")], schema=["a", "b"])


MISSING_COLUMN_OPS = [
    pytest.param(lambda df: df.select("NOPE"), id="select_str"),
    pytest.param(lambda df: df.select(col("NOPE")), id="select_col"),
    pytest.param(lambda df: df.select("a", "NOPE"), id="select_valid_and_missing"),
    pytest.param(lambda df: df.select(col("NOPE") + 1), id="select_expr"),
    pytest.param(lambda df: df.with_column("n", col("NOPE")), id="with_column"),
    pytest.param(lambda df: df.with_columns(["n"], [col("NOPE")]), id="with_columns"),
    pytest.param(lambda df: df.filter(col("NOPE") > 1), id="filter"),
    pytest.param(
        lambda df: df.where(col("a") > 0).filter(col("NOPE") > 1), id="filter_chain"
    ),
    pytest.param(lambda df: df.sort("NOPE"), id="sort_str"),
    pytest.param(lambda df: df.sort(col("NOPE").desc()), id="sort_col"),
]


@pytest.mark.parametrize("op", MISSING_COLUMN_OPS)
@pytest.mark.parametrize("derived", [False, True], ids=["fresh", "after_select"])
def test_missing_column_raises_at_construction(base, op, derived):
    df = base.select("a", "b") if derived else base
    with pytest.raises(SnowparkPlanException) as ex_info:
        op(df)
    assert ex_info.value.error_code == "1200"
    assert 'Invalid identifier "NOPE"' in str(ex_info.value)
    assert "There are existing quoted column identifiers: ['\"A\"', '\"B\"']" in str(
        ex_info.value
    )


def test_error_suggests_case_sensitive_match(session):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    df = session.sql('select 1 as "abc", 2 as "def"')
    with pytest.raises(SnowparkPlanException) as ex_info:
        df.select("ABC")
    assert "Do you mean '\"abc\"'?" in str(ex_info.value)


# one operation per kind: select, with_column, filter, sort
@pytest.mark.parametrize("op", [MISSING_COLUMN_OPS[i] for i in (0, 4, 6, 8)])
def test_disabled_flag_keeps_server_error(base, op):
    context.configure_development_features(enable_metadata_validation=False)
    df = op(base)
    with pytest.raises(SnowparkSQLException, match="invalid identifier"):
        df.collect()


@pytest.mark.parametrize(
    "op, expected",
    [
        pytest.param(
            lambda df: df.select("a", "b"), [Row(1, "x"), Row(2, "y")], id="select"
        ),
        pytest.param(lambda df: df.select("*"), [Row(1, "x"), Row(2, "y")], id="star"),
        pytest.param(
            lambda df: df.with_column("a", col("a") + 10).select("a"),
            [Row(11), Row(12)],
            id="with_column_replace",
        ),
        pytest.param(
            lambda df: df.filter(col("a") > 1), [Row(2, "y")], id="filter_valid"
        ),
        pytest.param(
            # the dropped column is still visible to the flattened WHERE
            lambda df: df.select("a").filter(col("b") == "y"),
            [Row(2)],
            id="filter_on_dropped_column",
        ),
        pytest.param(
            lambda df: df.select(col("a").alias("z")).sort("z"),
            [Row(1), Row(2)],
            id="sort_on_alias",
        ),
        pytest.param(
            lambda df: df.select_expr("a + 1 as c"),
            [Row(2), Row(3)],
            id="select_expr_not_validated",
        ),
    ],
)
def test_valid_operations_do_not_raise(base, op, expected):
    Utils.check_answer(op(base), expected)


def test_quoted_mixed_case_names_are_not_rejected(session):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    df = session.sql('select 1 as "Mixed", 2 as x')
    grouped = df.group_by('"Mixed"').count()
    Utils.check_answer(grouped.select('"Mixed"'), [Row(1)])
    Utils.check_answer(grouped.filter(col('"Mixed"') > 0).select("COUNT"), [Row(1)])
    with pytest.raises(SnowparkPlanException):
        df.select('"mixed"')


def test_join_and_aliased_dataframes_are_not_validated(session):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    df1 = session.create_dataframe([(1, "x")], schema=["id", "v1"])
    df2 = session.create_dataframe([(1, "y")], schema=["id", "v2"])
    joined = df1.join(df2, on="id")
    Utils.check_answer(joined.select("id", "v1", "v2"), [Row(1, "x", "y")])

    a1 = df1.alias("l")
    a2 = df2.alias("r")
    aliased = a1.join(a2, col("l", "id") == col("r", "id"))
    Utils.check_answer(aliased.select(col("l", "v1"), col("r", "v2")), [Row("x", "y")])


def test_simplifier_disabled_is_noop(session):
    original = session.sql_simplifier_enabled
    session.sql_simplifier_enabled = False
    try:
        df = session.create_dataframe([(1,)], schema=["a"])
        bad = df.select("NOPE")  # no early error without the simplifier
        with pytest.raises(SnowparkSQLException):
            bad.collect()
    finally:
        session.sql_simplifier_enabled = original


def _count_describes(fn):
    calls = []
    original = SnowflakePlan._analyze_attributes

    def counting(self):
        calls.append(1)
        return original(self)

    with patch.object(SnowflakePlan, "_analyze_attributes", counting):
        try:
            fn()
        except Exception:
            pass
    return len(calls)


@pytest.mark.parametrize("reduce_describe", [True, False])
def test_no_extra_describe_queries(session, reduce_describe):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    original = session.reduce_describe_query_enabled
    session.reduce_describe_query_enabled = reduce_describe
    try:

        def chain():
            df = session.create_dataframe([(1, "x")], schema=["a", "b"])
            return (
                df.select("a", "b")
                .filter(col("a") > 0)
                .with_column("c", col("a") + 1)
                .sort("c")
                .select("c", "b")
            )

        def failing():
            session.create_dataframe([(1, "x")], schema=["a", "b"]).filter(
                col("NOPE") > 0
            )

        context.configure_development_features(enable_metadata_validation=False)
        describes_off = _count_describes(chain)
        failing_off = _count_describes(failing)
        context.configure_development_features(enable_metadata_validation=True)
        describes_on = _count_describes(chain)
        failing_on = _count_describes(failing)
        assert describes_on == describes_off
        assert failing_on == failing_off
        if reduce_describe:
            assert describes_on == 0
    finally:
        session.reduce_describe_query_enabled = original


def test_table_frame_validates_without_extra_describe(session):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    table_name = Utils.random_table_name()
    try:
        session.create_dataframe([(1, "x")], schema=["a", "b"]).write.save_as_table(
            table_name, table_type="temp"
        )

        def build():
            return session.table(table_name).filter(col("NOPE") > 0)

        context.configure_development_features(enable_metadata_validation=False)
        describes_off = _count_describes(build)
        context.configure_development_features(enable_metadata_validation=True)
        describes_on = _count_describes(build)
        with pytest.raises(SnowparkPlanException):
            build()
        assert describes_on == describes_off
    finally:
        Utils.drop_table(session, table_name)


def test_combined_with_eager_schema_validation(base):
    context.configure_development_features(enable_eager_schema_validation=True)
    try:
        with pytest.raises(SnowparkPlanException):
            base.select("NOPE")
        Utils.check_answer(base.select("a"), [Row(1), Row(2)])
    finally:
        context.configure_development_features(enable_eager_schema_validation=False)
