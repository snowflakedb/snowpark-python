#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.functions import col
from tests.utils import Utils

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="semantic views are a SQL feature",
        run=False,
    )
]


@pytest.fixture(scope="module")
def semantic_view(session):
    customers = Utils.random_table_name()
    orders = Utils.random_table_name()
    view = Utils.random_view_name()

    # The tables are permanent, so teardown must run even if the DDL below fails.
    try:
        Utils.create_table(
            session, customers, "customer_id INT, customer_name STRING, region STRING"
        )
        session._run_query(
            f"INSERT INTO {customers} VALUES "
            "(1,'Ada','EMEA'),(2,'Blake','AMER'),(3,'Chen','APAC'),(4,'Dara','EMEA')"
        )
        Utils.create_table(
            session,
            orders,
            "order_id INT, customer_id INT, order_date DATE, amount INT",
        )
        session._run_query(
            f"INSERT INTO {orders} VALUES "
            "(10,1,'2026-01-05',200),(11,1,'2026-02-11',150),(12,2,'2026-01-20',350),"
            "(13,3,'2026-03-02',300),(14,3,'2026-03-09',350),(15,4,'2026-03-15',450)"
        )
        session._run_query(
            f"""
            CREATE OR REPLACE SEMANTIC VIEW {view}
              TABLES (
                customers AS {customers} PRIMARY KEY (customer_id),
                orders    AS {orders}    PRIMARY KEY (order_id)
              )
              RELATIONSHIPS (
                orders (customer_id) REFERENCES customers
              )
              FACTS (
                orders.order_amount      AS amount,
                customers.customer_total AS SUM(orders.order_amount)
              )
              DIMENSIONS (
                customers.customer AS customer_name,
                customers.region   AS region
              )
              METRICS (
                orders.revenue           AS SUM(amount),
                customers.customer_count AS COUNT(customer_id)
              )
            """
        )

        yield view
    finally:
        # Each drop is independent: a failing one must not strand the others.
        for drop in (
            lambda: session._run_query(f"DROP SEMANTIC VIEW IF EXISTS {view}"),
            lambda: Utils.drop_table(session, orders),
            lambda: Utils.drop_table(session, customers),
        ):
            try:
                drop()
            except Exception:  # noqa: BLE001 - teardown is best effort
                pass


def test_metrics_only(session, semantic_view):
    assert session.semantic_view(semantic_view, metrics="orders.revenue").collect() == [
        Row(REVENUE=1800)
    ]


def test_dimensions_and_metrics(session, semantic_view):
    assert session.semantic_view(
        semantic_view,
        dimensions=["customers.region"],
        metrics=["orders.revenue"],
    ).sort(col("REGION")).collect() == [
        Row(REGION="AMER", REVENUE=350),
        Row(REGION="APAC", REVENUE=650),
        Row(REGION="EMEA", REVENUE=800),
    ]


def test_where_is_applied_before_aggregation(session, semantic_view):
    assert session.semantic_view(
        semantic_view,
        dimensions="customers.region",
        metrics="orders.revenue",
        where="customers.region = 'EMEA'",
    ).collect() == [Row(REGION="EMEA", REVENUE=800)]


def test_tuple_member_is_table_dot_attribute(session, semantic_view):
    assert session.semantic_view(
        semantic_view,
        dimensions=[("customers", "region")],
        metrics=["orders.revenue"],
    ).sort(col("REGION")).collect() == [
        Row(REGION="AMER", REVENUE=350),
        Row(REGION="APAC", REVENUE=650),
        Row(REGION="EMEA", REVENUE=800),
    ]


def test_filter_after_the_call_composes(session, semantic_view):
    """Filtering on a metric happens outside the parens, on the DataFrame."""
    assert session.semantic_view(
        semantic_view,
        dimensions="customers.region",
        metrics="orders.revenue",
    ).filter(col("REVENUE") > 400).sort(col("REGION")).collect() == [
        Row(REGION="APAC", REVENUE=650),
        Row(REGION="EMEA", REVENUE=800),
    ]


def test_inline_alias_renames_the_column(session, semantic_view):
    df = session.semantic_view(semantic_view, metrics="orders.revenue AS REV")
    assert df.columns == ["REV"]


def test_facts_are_row_level(session, semantic_view):
    rows = session.semantic_view(semantic_view, facts="customers.customer_total")
    assert sorted(r[0] for r in rows.collect()) == [350, 350, 450, 650]


def test_where_alone_raises_before_any_query(session, semantic_view):
    with pytest.raises(ValueError, match="at least one"):
        session.semantic_view(semantic_view, where="customers.region = 'EMEA'")
