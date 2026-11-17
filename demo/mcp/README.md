# MCP tools and servers with Snowpark

`snowpark_mcp_demo.py` builds a Snowflake-managed MCP server with `snowflake.snowpark.mcp`
(private preview), using every kind of tool the API supports:

| Tool | Made with |
|---|---|
| `get_account_orders` | `@mcp_tool` on a function that takes a `Session` and returns a DataFrame (a stored procedure) |
| `region_summary` | `@mcp_tool` with bound SQL parameters, a `Literal` argument and `ToolError` (a stored procedure) |
| `order_band` | `@mcp_tool` on a function without a `Session` (a UDF) |
| `top_accounts` | `Tool.from_procedure` on an existing SQL procedure |
| `run_sql` | `Tool.builtin("SYSTEM_EXECUTE_SQL", ...)`, the server's own read-only SQL tool |

It creates a small `ORDERS` table in a schema of its own, then lints the server, prints
its spec, runs tools locally with `invoke`, deploys a `_DEV` copy, calls the deployed
tools with `call_tool`, prints the grants a role would need, and optionally calls the MCP
endpoint over HTTPS. Everything it created is dropped at the end unless you pass `--keep`.

## Run it

```bash
pip install "snowflake-snowpark-python" requests
python demo/mcp/snowpark_mcp_demo.py --connection <name> --database <database>
```

- `--connection` is a connection name from `~/.snowflake/connections.toml`.
- The connection's role needs `CREATE SCHEMA` on `--database`, and an account with MCP
  servers enabled.
- To call the MCP endpoint too, put a programmatic access token in `SNOWFLAKE_PAT`, or
  name another variable with `--pat-env`. Without one, that step is skipped.
- `--keep` leaves the schema and server in place so you can connect an MCP client to the
  printed endpoint URL.
