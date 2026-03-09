# ADR: Snowpark Python — Universal Driver Integration

**Branch:** `universal-driver-compat`
**Date:** 2026-03-09
**Status:** In Progress

---

## Context

`snowflake-connector-python` (the traditional Python connector, v4.x) is being superseded by the **Universal Driver** (`snowflake-connector-python-ud`, v2026.0.0). The Universal Driver exposes the same `snowflake.connector` Python namespace but is built on a Rust core (`sf_core`) with Cython/ctypes Python bindings.

The goal of this work is to:
1. Validate that Snowpark Python works correctly with the Universal Driver.
2. Fix pre-existing test infrastructure issues that prevent `pytest` from collecting tests.
3. Establish a CI pipeline that builds the Universal Driver from source and runs the full Snowpark test suite against it.

---

## Decision Log

### 2026-03-09 — Submodule strategy

**Decision:** Add `universal-driver` as a git submodule with relative URL `../universal-driver` at path `universal-driver/`.

**Rationale:** The relative URL resolves to `snowflakedb/universal-driver` on GitHub (same org), so `actions/checkout` with `submodules: true` picks it up automatically in CI without requiring extra secrets or SSH keys. Locally, the submodule directory is replaced with a symlink to the existing checkout at `/home/fpawlowski/repo/universal-driver` to avoid a redundant clone. The symlink is added to `.gitignore` so git does not attempt to track it as a file; the submodule reference is tracked via `.gitmodules` and the commit SHA in the index.

---

### 2026-03-09 — Datasource collection errors — two-part fix

**Decision:** (a) Append stub empty-dict names to `tests/parameters.py`; (b) use `--ignore=tests/integ/datasource` for local runs.

**Rationale:** Several test files in `tests/integ/datasource/` do a top-level `from tests.parameters import MYSQL_CONNECTION_PARAMETERS` that fails at collection when the names are absent. Additionally, `tests/resources/test_data_source_dir/test_jdbc_data.py` subscripts the dicts at module load (`ORACLEDB_CONNECTION_PARAMETERS["host"]` etc.), which cannot be made safe by stubs alone. The real values are injected in CI by `decrypt_parameters.sh`. Locally: (a) stubs make the names importable for any non-datasource test that references them, and (b) `--ignore=tests/integ/datasource` prevents the `test_jdbc_data.py` module-level subscript from running. In CI the datasource directory is not ignored — the full real parameters are available.

**Stub parameters added to `tests/parameters.py`:**
- `DATABRICKS_CONNECTION_PARAMETERS = {}`
- `MYSQL_CONNECTION_PARAMETERS = {}`
- `POSTGRES_CONNECTION_PARAMETERS = {}`
- `ORACLEDB_CONNECTION_PARAMETERS = {}`
- `SQL_SERVER_CONNECTION_PARAMETERS = {}`

---

### 2026-03-09 — Fix OpenTelemetry collection errors

**Decision:** Guard OTel imports at module level in two test files.

**`tests/integ/test_open_telemetry.py`:** Had unconditional `from opentelemetry import trace` at module level — fails before any `pytestmark` skip markers can apply. Fixed by wrapping in `try/except ImportError` with `pytest.skip(..., allow_module_level=True)`.

**`tests/integ/test_external_telemetry.py`:** Had a `try/except` around the OTel imports that set `dependencies_missing = True` on failure, but class definitions `MockOTLPLogExporter(OTLPLogExporter)` and `MockOTLPSpanExporter(OTLPSpanExporter)` appeared unconditionally at module scope — causing `NameError` when the imports failed. Fixed by guarding the class definitions with `if not dependencies_missing:`.

---

### 2026-03-09 — Baseline test counts (pre-connector-swap)

Command: `uv run pytest tests/unit -q --tb=no`
**Unit tests:** 2601 passed, 3 failed (pre-existing), 21 skipped, 5 xfailed

Command: `uv run pytest tests/integ --ignore=tests/integ/modin --ignore=tests/integ/datasource -q --tb=no`
**Integration tests (excl. modin + datasource):** 3540 errors (all connection errors — no live Snowflake locally), 104 skipped, 39 xfailed

Note: 3 pre-existing unit failures in `test_packaging_utils.py` (`test_get_downloaded_packages_for_real_python_packages`, `test_pip_timeout`, `test_valid_pip_install`) are unrelated to connector version. Integ "errors" are all `SnowflakeLoginException`/connection failures, not test logic failures.

---

### 2026-03-09 — Connector swap strategy

**Decision:** Install Snowpark normally (which pulls in the old connector as a dependency), then:
```
uv pip uninstall snowflake-connector-python -y
uv pip install <universal-driver-wheel>
```

**Rationale:** The Universal Driver is published under a different PyPI package name (`snowflake-connector-python-ud`) but occupies the same `snowflake.connector` Python namespace. Snowpark's `setup.py` has the constraint `>=3.17.0, <5.0.0` targeting `snowflake-connector-python` by name. After the swap, pip metadata is technically inconsistent (snowpark "requires" a package that isn't installed), but the Python runtime works because `snowflake.connector` is present via the UD package. No `setup.py` change is needed at this stage; a follow-up PR should add an `OR` alternative once the UD is on PyPI.

---

### 2026-03-09 — GitHub Actions workflow

**Decision:** Created dedicated workflow `.github/workflows/test-with-universal-driver.yml`. Revised to `workflow_dispatch`-only (no nightly schedule), following the snowflake-cli `ud-manual-tests.yaml` pattern.

**Key aspects:**
- Workflow name: `Run UD tests (manual)`. Triggered exclusively via `workflow_dispatch` — no nightly cron.
- Inputs: `universal_driver_ref` (branch/tag/SHA, default `main`), `python-version` (choice: 3.10/3.11/3.12/3.13, default 3.10), `cloud-provider` (choice: aws/azure/gcp, default aws).
- `permissions: contents: read` and `persist-credentials: false` on all checkouts.
- Builds the UD wheel from source in CI using Rust 1.89.0 + Cython + hatchling.
- Caches Cargo registry and `universal-driver/target/` keyed on `Cargo.lock` hash for speed.
- Uses the same `decrypt_parameters.sh` script as the existing precommit workflow so datasource tests receive real credentials.
- Runs the same `tox -e py<ver>-notdoctest-ci` and `tox -e datasource` steps as the existing precommit.
- Two-job split (build wheel → test) is kept because Rust compilation is expensive and reusable; snowflake-cli's single-job approach works because it installs via `git+https://...` inline rather than pre-building.

---

## Open Items

- [ ] Identify and fix any API incompatibilities between Snowpark and the Universal Driver (Phase 5).
- [ ] Add `snowflake-connector-python-ud` as an alternative dependency in `setup.py` once UD is on PyPI.
- [ ] Validate modin tests with Universal Driver (Phase 7).
