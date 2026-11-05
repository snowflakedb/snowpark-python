# `session.jobs` samples

Small scripts that show the experimental `session.jobs` API: write a program, submit it,
deploy it, schedule it. Each script creates a scratch schema `JOBS_DEMO_<random>` in your
current database, creates everything there, and drops the schema when it exits.

| Sample | Shows |
| --- | --- |
| `01_submit_function.py` | Submit a function that aggregates a table; `wait()`, `result()`, `logs()`, the failure path |
| `02_submit_project.py` | Submit a directory (`etl/`) that cleans a table, with a function entrypoint and packages |
| `03_deploy_stored_procedure.py` | Deploy a stored procedure (owner's and caller's rights), `run()`, `to_sql()` |
| `03b_submit_code_bundle.py` | One-off code bundle runs on a warehouse: a connection check, a report, `error()`, cancel |
| `04_deploy_code_bundle.py` | Deploy a code bundle: script and function (`tasks.py:run`) entry points, `overrides`, `add_version()`, `get_deployment()` |
| `05_schedule_stored_procedure.py` | Schedule a stored procedure that refreshes a table: `run_now()`, `runs()`, `suspend()`/`resume()` |
| `06_schedule_code_bundle.py` | Schedule a code bundle function entry point on the target's warehouse |
| `07_code_bundle_compute_pool.py` | A code bundle report on a compute pool |
| `08_find_runs.py` | `list()`, `get()`, `get_deployment()` |

The programs work on a small `SALES` table (and, for `02`, a messy `CUSTOMERS_RAW` table)
that `_common.py` creates in the scratch schema. They run in Snowflake, so they receive
fully qualified table names as string arguments and use the Snowpark session: passed in
as the first argument by function entry points, and taken from `get_active_session()` by
scripts. After a run, the samples read the result tables from the client.

## Prerequisites

- A Snowflake connection. The scripts use the connection named by
  `SNOWFLAKE_CONNECTION_NAME` (default: `default`).
- A current warehouse, and privileges to create schemas, stages, procedures, tasks and
  code bundles in the current database.
- `07` needs a compute pool you can use: set `SNOWPARK_DEMO_COMPUTE_POOL`. `04` also
  deploys to it when the variable is set.

## Run

```bash
export SNOWFLAKE_CONNECTION_NAME=my_connection
python demo/jobs/01_submit_function.py
```

`logs()` reads the account event table, which ingests with a delay, so the samples poll
for a while.
