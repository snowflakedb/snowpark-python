#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Demo: build, test, deploy and call a Snowflake-managed MCP server with Snowpark.

The demo creates a small ORDERS table in a schema of its own, then builds an MCP server
with every kind of tool the API supports:

- get_account_orders  a Python stored procedure that returns a DataFrame
- region_summary      a Python stored procedure using bound SQL parameters and ToolError
- order_band          a Python UDF (no session)
- top_accounts        an existing Python stored procedure, added with Tool.from_procedure
- run_sql             the server's built-in read-only SQL tool, added with Tool.builtin

It checks the server with a dry run of deploy, prints its spec, runs a tool locally with invoke(), deploys a dev
copy, calls the deployed tools with call_tool(), shows the grants a role would need,
and optionally calls the MCP endpoint over HTTPS. Everything is dropped at the end
unless --keep is given.

Usage:
    python demo/mcp/snowpark_mcp_demo.py --connection <name> --database <db> [--keep]
        [--pat-env SNOWFLAKE_PAT]

The connection must be in ~/.snowflake/connections.toml. The role needs CREATE SCHEMA
on the database. To call the MCP endpoint, put a programmatic access token in the
environment variable named by --pat-env.
"""

import argparse
import datetime
import json
import os
from typing import Literal, Optional

from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.functions import col, sum as sum_
from snowflake.snowpark.mcp import MCPServer, Tool, ToolError, mcp_tool
from snowflake.snowpark.types import IntegerType, VariantType

SCHEMA = "SNOWPARK_MCP_DEMO"


def show(*values, **kwargs) -> None:
    print(*values, **kwargs)  # noqa: T201


# ---------------------------------------------------------------------------
# Tools from Python functions
# ---------------------------------------------------------------------------


@mcp_tool(read_only=True)
def get_account_orders(
    session: Session,
    account_id: str,
    since: Optional[datetime.date] = None,
    limit: int = 20,
) -> DataFrame:
    """Orders of one account, newest first.

    Args:
        account_id: The account ID, such as "ACME".
        since: Only orders on or after this date. All orders when omitted.
        limit: Maximum number of orders to return.
    """
    orders = session.table("ORDERS").filter(col("ACCOUNT_ID") == account_id)
    if since is not None:
        orders = orders.filter(col("ORDER_DATE") >= since)
    return orders.sort(col("ORDER_DATE").desc()).limit(limit)


@mcp_tool(read_only=True)
def region_summary(session: Session, region: Literal["AMER", "EMEA", "APAC"]) -> dict:
    """Order count, revenue and average order value for one region.

    Args:
        region: The sales region.
    """
    # Values are bound as parameters, never formatted into the SQL text.
    row = session.sql(
        "SELECT COUNT(*) AS ORDERS, SUM(AMOUNT) AS REVENUE, AVG(AMOUNT) AS AVG_ORDER "
        "FROM ORDERS WHERE REGION = ?",
        params=[region],
    ).collect()[0]
    if row["ORDERS"] == 0:
        raise ToolError(
            f"There are no orders in {region}.",
            hint="Try another region, or call top_accounts to see where orders are.",
        )
    return row.as_dict()


@mcp_tool(read_only=True, idempotent=True)
def order_band(amount: float) -> str:
    """Classify an order amount as small, medium or large.

    Args:
        amount: The order amount in USD.
    """
    if amount < 100:
        return "small"
    return "medium" if amount < 1000 else "large"


# ---------------------------------------------------------------------------
# An existing stored procedure, registered without the MCP API
# ---------------------------------------------------------------------------


def top_accounts(session: Session, n: int = 3) -> list:
    rows = (
        session.table("ORDERS")
        .group_by("ACCOUNT_ID")
        .agg(sum_("AMOUNT").alias("REVENUE"))
        .sort(col("REVENUE").desc())
        .limit(n)
        .collect()
    )
    # Whole dollars: a VARIANT shows a Decimal as a string, and a float as 2.9e+03.
    return [{"account": r["ACCOUNT_ID"], "revenue": round(r["REVENUE"])} for r in rows]


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------


def section(title: str) -> None:
    show(f"\n=== {title} " + "=" * max(0, 70 - len(title)))


def setup_data(session: Session, database: str) -> None:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {database}.{SCHEMA}").collect()
    session.use_schema(f"{database}.{SCHEMA}")
    session.sql(
        """CREATE OR REPLACE TABLE ORDERS AS SELECT * FROM VALUES
            (1, 'ACME',    'AMER', 120.00, '2026-09-02'::DATE),
            (2, 'ACME',    'AMER', 980.50, '2026-10-11'::DATE),
            (3, 'ACME',    'AMER',  45.00, '2026-11-01'::DATE),
            (4, 'GLOBEX',  'EMEA', 2300.00, '2026-10-20'::DATE),
            (5, 'GLOBEX',  'EMEA',  610.25, '2026-11-03'::DATE),
            (6, 'INITECH', 'AMER',  75.10, '2026-10-29'::DATE)
        AS T(ORDER_ID, ACCOUNT_ID, REGION, AMOUNT, ORDER_DATE)"""
    ).collect()
    # A procedure registered the usual way, to show Tool.from_procedure. Its comment
    # becomes the tool's description.
    session.sql("CREATE STAGE IF NOT EXISTS DEMO_CODE").collect()
    session.sproc.register(
        top_accounts,
        name=f"{database}.{SCHEMA}.TOP_ACCOUNTS",
        return_type=VariantType(),
        input_types=[IntegerType()],
        is_permanent=True,
        stage_location=f"@{database}.{SCHEMA}.DEMO_CODE",
        replace=True,
        preserve_parameter_names=True,
        comment="Accounts with the most revenue, in whole US dollars.",
    )


def build_server(database: str) -> MCPServer:
    top_accounts = Tool.from_procedure(
        f"{database}.{SCHEMA}.TOP_ACCOUNTS",
        read_only=True,
        param_descriptions={"N": "How many accounts to return."},
    )
    run_sql = Tool.builtin(
        "SYSTEM_EXECUTE_SQL",
        name="run_sql",
        description="Run a read-only SQL query against the demo ORDERS table.",
        # read_only is the default for the SQL tool, made explicit here. The MCP
        # server accepts annotations (read_only=...) only on CORTEX_AGENT_RUN among
        # the built-in types.
        config={"read_only": True, "query_timeout": 60},
    )
    return MCPServer(
        "SALES_DEMO",
        tools=[get_account_orders, region_summary, order_band, top_accounts, run_sql],
    )


def call_endpoint(url: str, token: str) -> None:
    import requests

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }

    def rpc(method: str, params: dict) -> dict:
        response = requests.post(
            url,
            headers=headers,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            timeout=120,
        )
        body = response.text
        if "event-stream" in response.headers.get("content-type", ""):
            body = [ln[5:] for ln in body.splitlines() if ln.startswith("data:")][-1]
        return json.loads(body)

    tools = rpc("tools/list", {})["result"]["tools"]
    show("tools/list:", [t["name"] for t in tools])
    result = rpc(
        "tools/call", {"name": "region_summary", "arguments": {"region": "EMEA"}}
    )["result"]
    show("tools/call region_summary(EMEA):", result["content"][0]["text"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--connection", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--keep", action="store_true", help="Don't drop the demo.")
    parser.add_argument("--pat-env", default="SNOWFLAKE_PAT")
    args = parser.parse_args()

    session = Session.builder.config("connection_name", args.connection).create()
    # Procedures pin snowflake-snowpark-python to the client's version unless the
    # session already lists it; right after a release the warehouse may not have it.
    session.add_packages("snowflake-snowpark-python")
    target = dict(database=args.database, schema=SCHEMA, suffix="_DEV")
    server = build_server(args.database)
    try:
        section("Set up demo data")
        setup_data(session, args.database)
        show(f"Created {args.database}.{SCHEMA}.ORDERS and TOP_ACCOUNTS")

        section("Check with a dry run")
        check = server.deploy(session, dry_run=True, **target)
        for issue in check.issues:
            show(issue)
        show(
            f"{len(check.issues)} issue(s); errors would raise MCPLintError; "
            f"deploy would run {len(check.sql)} SQL statement(s)"
        )

        section("Spec the server will get")
        show(server.to_yaml(session, **target))

        section("Run tools locally with invoke()")
        show("order_band(1500):", order_band.invoke(amount=1500))
        show(
            "region_summary(APAC):",
            region_summary.invoke(session, region="APAC"),
        )
        show("region_summary(LATAM):", region_summary.invoke(session, region="LATAM"))

        section("Deploy a dev copy")
        result = server.deploy(session, **target)
        show("server:", result.server)
        show("objects:", *result.objects, sep="\n  ")
        show("endpoint:", result.endpoint_url)

        section("Call deployed tools with call_tool()")
        show(
            "get_account_orders(ACME, since 2026-10-01):",
            json.dumps(
                server.call_tool(
                    session,
                    "get_account_orders",
                    {"account_id": "ACME", "since": "2026-10-01"},
                    **target,
                ),
                indent=2,
            ),
        )
        show(
            "region_summary(EMEA):",
            server.call_tool(session, "region_summary", {"region": "EMEA"}, **target),
        )
        show(
            "order_band(42):",
            server.call_tool(session, "order_band", {"amount": 42}, **target),
        )
        show(
            "top_accounts(2):",
            server.call_tool(session, "top_accounts", {"n": 2}, **target),
        )

        section("Grants a role needs (dry run)")
        for statement in server.grant_usage(
            session, "SALES_MCP_USER", dry_run=True, **target
        ):
            show(statement)

        token = os.environ.get(args.pat_env)
        section("Call the MCP endpoint")
        if token:
            call_endpoint(result.endpoint_url, token)
        else:
            show(f"Skipped: set {args.pat_env} to a programmatic access token.")
    finally:
        if args.keep:
            show(f"\nKept {args.database}.{SCHEMA}; drop it with DROP SCHEMA.")
        else:
            section("Clean up")
            server.drop(session, include_tools=True, **target)
            session.sql(f"DROP SCHEMA IF EXISTS {args.database}.{SCHEMA}").collect()
            show(f"Dropped the server and {args.database}.{SCHEMA}")
        session.close()


if __name__ == "__main__":
    main()
