# `session.jobs` samples

Small scripts that show the experimental `session.jobs` API: write a program, submit it,
deploy it, schedule it. Each script creates a scratch schema `JOBS_DEMO_<random>` in your
current database, creates everything there, and drops the schema when it exits.

| Sample | Shows |
| --- | --- |
| `01_submit_function.py` | Submit a function, `wait()`, `result()`, `logs()`, the failure path |
| `02_submit_project.py` | Submit a directory (`etl/`) with a function entrypoint and packages |
| `03_deploy_stored_procedure.py` | Deploy a stored procedure (owner's and caller's rights), `run()`, `to_sql()` |
| `03b_submit_code_bundle.py` | One-off code bundle runs on a warehouse: status, `error()`, cancel |
| `04_deploy_code_bundle.py` | Deploy a code bundle: entry points, `overrides`, `add_version()`, `get_deployment()` |
| `05_schedule_stored_procedure.py` | Schedule a stored procedure: `run_now()`, `runs()`, `suspend()`/`resume()` |
| `06_schedule_code_bundle.py` | Schedule a code bundle on the target's warehouse |
| `07_code_bundle_compute_pool.py` | A code bundle on a compute pool |
| `08_find_runs.py` | `list()`, `get()`, `get_deployment()` |

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
python demo/01_submit_function.py
```

`logs()` reads the account event table, which ingests with a delay, so the samples poll
for a while.
