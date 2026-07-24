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

---

### 2026-03-10 — Tox venv connector swap (root cause + fix)

**Observation:** The earlier connector swap (Phase 3 above) uninstalled the old connector and installed the UD wheel at the system-Python level. This had no effect on tox runs because tox creates its own isolated virtualenv and re-installs all deps from PyPI via `tox_install_cmd.sh`, overwriting the system-level change.

**Conclusion:** The connector swap must happen *inside* the tox venv, not before it.

**Fix — `scripts/tox_install_cmd.sh`:**
Added a new branch keyed on `ud_connector_path` env var:
```bash
if [[ -n "${ud_connector_path}" ]]; then
  uv pip install ${uv_options[@]}          # install all deps (pulls old connector)
  uv pip uninstall snowflake-connector-python
  uv pip install "${ud_connector_path}"    # replace with UD wheel or git+https URL
fi
```
`ud_connector_path` is passed in by the caller (either a local wheel path or a `git+https://` URL). When unset, behaviour is unchanged.

**Fix — `tox.ini`:**
Added `ud_connector_path` to `passenv` (line ~118) so tox forwards the env var into the venv install step.

**Verification (observation):** After the fix, the tox venv shows `snowflake-connector-python-ud==0.1.0` and the old `snowflake-connector-python` package is absent. Tests proceed until they hit UD API errors rather than silently using the old connector.

---

### 2026-03-10 — CI workflow hardening

**`test-with-universal-driver.yml` changes:**

1. **Dynamic wheel path resolution** — replaced hardcoded `ud-dist/snowflake_connector_python_ud-2026.0.0-py3-none-any.whl` with a step that resolves the path dynamically via `ls ud-dist/*.whl | head -1 → $GITHUB_OUTPUT`. Hardcoded name would break silently when the UD version string changes.

2. **UD ref checkout** — added step to `git -C universal-driver fetch origin && git -C universal-driver checkout origin/${{ inputs.universal_driver_ref }}` when the input is non-empty and non-`main`. Without this, the submodule was always pinned to the committed SHA regardless of the `universal_driver_ref` workflow input.

3. **Connector swap moved into tox** — removed standalone "Install snowpark + swap connector" step; the swap now happens inside tox via `ud_connector_path` passthrough.

**`ud-inline-tests.yml` changes:**

1. **`pull_request` trigger** — added as a secondary trigger (with a TODO comment noting it requires self-hosted runner access). Without a default-value mechanism, the `setup-python` action would receive an empty string for `python-version` on `pull_request` events (no `inputs.*` available), causing it to fall back to the system Python 3.12.3 (externally managed), and `uv pip install` would subsequently fail with `--system` conflicts.

2. **Workflow-level `env:` block** — added to provide defaults for all three matrix dimensions:
   ```yaml
   env:
     PYTHON_VERSION: ${{ inputs.python-version || '3.10' }}
     CLOUD_PROVIDER: ${{ inputs.cloud-provider || 'aws' }}
     UD_BRANCH: ${{ inputs.ud-branch || 'main' }}
   ```
   All steps reference `${{ env.PYTHON_VERSION }}` etc.

3. **Git credentials step** — replaced the "Install snowpark" + "Swap connector" steps with a "Configure git credentials" step that sets up `SNOWFLAKE_GITHUB_TOKEN` for `git+https://` installs. The UD is installed via `ud_connector_path: "git+https://github.com/snowflakedb/universal-driver@${{ env.UD_BRANCH }}#subdirectory=python"` passed into tox.

---

### 2026-03-10 — Compatibility branch selection

**Observation:** The UD `main` branch exposes the new Rust-native API. Several existing Snowflake ecosystem projects (snowflake-cli, snowfort) depend on legacy `snowflake.connector` symbols (`execute_string`, `DictCursor`, `SnowflakeConnection`, `fetchmany`, `config_manager`, `SnowflakeRestful`, `split_statements`, `compat.py`, `constants.py` enums, key-pair auth) that the UD `main` branch does not yet expose.

**Hypothesis (confirmed):** A compatibility-focused UD branch exists that backfills these symbols as thin wrappers over the Rust core.

**Finding:** Branch `turbaszek-ecosystem-compatibility-cli` on `snowflakedb/universal-driver` (24 commits ahead of `main` at time of writing) adds:
- `execute_string`, `execute_stream`
- `DictCursor`, `SnowflakeConnection` alias
- `fetchmany`, `config_manager`, `SnowflakeRestful`, `split_statements`
- `compat.py`, `constants.py` enums
- Key-pair auth

**Decision:** Created `snowpark-compatibility` branch in the `universal-driver` submodule, branching from `turbaszek-ecosystem-compatibility-cli`. This is the branch used for all local test runs and future CI targeting. Reason for a separate branch rather than tracking the upstream directly: lets us add snowpark-specific patches without polluting the upstream compat branch.

**Notable difference from `main`:** This branch uses `rustls = { version = "0.23.20", features = ["aws_lc_rs"] }` (same as main, no `"fips"` feature). Also carries `openssl = "0.10.73"` and `jwt = { version = "0.16.0", features = ["openssl"] }` as direct dependencies in `sf_core/Cargo.toml`, which link dynamically against the system's `libssl.so.3`.

---

### 2026-03-10 — Local wheel build: environment issues and fixes

#### Issue 1: FIPS delocator (resolved — not applicable to this branch)

**Hypothesis (wrong):** The `turbaszek-ecosystem-compatibility-cli` branch enables the `fips` feature on `rustls`, pulling in `aws-lc-fips-sys` which requires a Go-based delocator tool. On aarch64 Amazon/Rocky Linux with devtoolset-10 GCC 10.2.1, the delocator produces `parse error near WS` on the generated assembly.

**Correction:** The `snowpark-compatibility` branch (`turbaszek-ecosystem-compatibility-cli` base) does **not** have `"fips"` in `rustls` features and `aws-lc-fips-sys` is absent from `Cargo.lock`. This issue does not apply. The hypothesis was formed while investigating an earlier branch state.

#### Issue 2: `cmake` not found inside `uv run` subprocess

**Observation:** Running `uv run --with hatch ... hatch build` failed with `Missing dependency: cmake`. `/usr/bin/cmake` exists on the system but is not on the `PATH` visible to the `cargo` subprocess spawned from within `uv`'s isolated environment on Rocky Linux 9.7.

**Fix:** Pass `CMAKE=/usr/bin/cmake` as an explicit env var (the `aws-lc-sys` build script checks `$CMAKE` before searching `PATH`). Added to `scripts/build_ud_connector.sh`:
```bash
CMAKE="${CMAKE:-$(command -v cmake 2>/dev/null)}" \
  uv run --with hatch --with cython --with setuptools hatch build
```
On systems where cmake is on PATH (e.g. Ubuntu CI), `command -v cmake` resolves it and the env var is a no-op.

**Outcome (observation):** Wheel built successfully:
```
dist/snowflake_connector_python_ud-0.1.0-py3-none-any.whl
```
Contains `libsf_core.so` (55 MB unstripped release build) and `arrow_stream_iterator.cpython-310-aarch64-linux-gnu.so`. The `py3-none-any` tag is cosmetic — hatchling doesn't auto-tag platform wheels when binaries are injected via build hooks.

#### Issue 3: `libssl.so.3` not found at runtime (OPEN)

**Observation:** After installing the wheel into the tox venv, importing `snowflake.connector` fails with `RuntimeError: Couldn't load core driver dependency`. Root cause: `libsf_core.so` was compiled against system OpenSSL 3.5.1 (`/lib64/libssl.so.3`) because `openssl-sys` detects it via pkg-config. The tox venv uses the nix-managed Python interpreter (`/opt/sfc/python3.10`), which runs with `LD_LIBRARY_PATH` pointing exclusively to nix store paths. Those paths contain only OpenSSL 1.1.x (`libssl.so.1.1`), not 3.x.

**Hypothesis A (tested, failed):** Add `/lib64` to `LD_LIBRARY_PATH`. Result: nix bash (glibc 2.34) collides with system `libc.so.6` (requires GLIBC_2.35) — the two glibc stacks cannot be mixed.

**Hypothesis B (tested, failed):** `OPENSSL_STATIC=1` during cargo build. Result: linker still emits `-Wl,-Bdynamic -lssl -lcrypto`. `OPENSSL_STATIC=1` did not propagate into the `openssl-sys` build script subprocess. Static `.a` files may also be absent.

**Hypothesis C (not yet tested):** `RUSTFLAGS="-C link-arg=-Wl,-rpath,/lib64"` embeds `/lib64` into the `.so` RPATH. Since RPATH is consulted before `LD_LIBRARY_PATH`, this would let nix Python find `libssl.so.3` without mixing glibc stacks.

**Hypothesis D (not yet tested):** Copy `libssl.so.3` and `libcrypto.so.3` from `/lib64` into the wheel's `_core/` directory next to `libsf_core.so`. The existing `$ORIGIN` RPATH would find them. Avoids any rebuild. Requires bundling ~5 MB of system libs in the local wheel; CI wheels are built on Ubuntu where this issue does not arise.

**Open question:** Which of C or D is the right fix? D is simpler for local dev but potentially violates OpenSSL licensing/distribution expectations if the wheel were ever published. C is cleaner but requires a rebuild.

---

---

### 2026-03-11 — Env fixes: libssl.so.3 (resolved), GLIBCXX_3.4.29 (resolved), missing symbols (resolved)

#### libssl.so.3 — Hypothesis D confirmed

**Conclusion:** Copying `/lib64/libssl.so.3` and `/lib64/libcrypto.so.3` into the wheel's `_core/` directory resolves the `RuntimeError: Couldn't load core driver dependency` failure. The `$ORIGIN` RPATH on `libsf_core.so` causes the dynamic linker to find the bundled copies before consulting `LD_LIBRARY_PATH`. Implemented in `scripts/build_ud_connector.sh` as a conditional post-build step (no-op on Ubuntu CI where `libssl.so.3` is on the default search path).

**Note:** `libssl.so.3` and `libcrypto.so.3` require GLIBC up to 2.34; the nix Python uses GLIBC_2.34, so no collision.

#### GLIBCXX_3.4.29 — new blocker (resolved via LD_PRELOAD)

**Observation:** After libssl was fixed, the next error was:
```
ImportError: /nix/store/.../gcc-9.3.0-lib/lib/libstdc++.so.6: version 'GLIBCXX_3.4.29' not found
(required by snowflake/connector/_internal/arrow_stream_iterator.cpython-310-aarch64-linux-gnu.so)
```

**Root cause (observation):** `arrow_stream_iterator.so` was compiled against the system GCC (GLIBCXX_3.4.29). The nix Python environment loads the nix gcc-9.3.0 libstdc++ first (via pyarrow importing it), which only has GLIBCXX up to 3.4.28. The `$ORIGIN` trick is ineffective here because `libstdc++.so.6` is already loaded in the process image before `arrow_stream_iterator.so` is imported.

**$ORIGIN approach (hypothesis E, tested, failed):** Copying `/usr/lib64/libstdc++.so.6` into `_internal/` alongside the .so did not help — the already-loaded nix version wins the soname race.

**Fix (hypothesis F — confirmed):** `LD_PRELOAD=/usr/lib64/libstdc++.so.6` forces the system libstdc++ to be loaded before the nix Python environment loads its own. The system `libstdc++.so.6` requires GLIBC_2.34 (not 2.35), so no glibc collision occurs. Implemented in `scripts/run_tests_with_ud.sh` (conditional, no-op where `/usr/lib64/libstdc++.so.6` is absent) and `LD_PRELOAD` added to `passenv` in `tox.ini`.

#### Missing Python symbols — resolved

**Observation:** After env fixes, Snowpark could not be imported due to missing symbols in the UD connector. Full list resolved in the `snowpark-compatibility` branch:

| Symbol | File | Action |
|--------|------|--------|
| `ASYNC_RETRY_PATTERN` | `cursor.py` | Added constant `[1, 1, 2, 3, 4, 8, 10]` |
| `ResultMetadata` | `cursor.py` | Added NamedTuple |
| `ResultMetadataV2` | `cursor.py` | Added as alias for `ResultMetadata` |
| `FIELD_ID_TO_NAME`, `ENV_VAR_PARTNER`, `UTF8` | `constants.py` | Added |
| `OK` | `compat.py` | Added (`http.client.OK`) |
| `MissingDependencyError` | `errors.py` | Added |
| `options.py` | new | `installed_pandas`, `pandas`, `pyarrow`, `MissingOptionalDependency`, `MissingPandas`, `ModuleLikeObject` |
| `write_pandas`, `_create_temp_stage`, `_create_temp_file_format`, `build_location_helper` | `pandas_tools.py` | New file — stubs (raise NotImplementedError except `build_location_helper`) |
| `ReauthenticationRequest` | `network.py` | New file |
| `SecretDetector` | `secret_detector.py` | New file — minimal masking formatter |
| `TelemetryService` | `telemetry_oob.py` | New file — singleton stub (all calls no-op) |
| `VERSION` | `version.py` | New file — `(0, 1, 0, None)` tuple |

**Turbaszek check:** `turbaszek-ecosystem-compatibility-cli` did not have `options.py`, `pandas_tools.py`, `network.py`, `secret_detector.py`, `telemetry_oob.py`, or `version.py`. All written fresh. `turbaszek-python-missing-api` had `ResultMetadata` and `version.py` — `ResultMetadata` pattern was borrowed from there.

#### Test collection result — definition of done met

**Observation:**
```
LD_PRELOAD=/usr/lib64/libstdc++.so.6 pytest tests/unit --ignore=tests/unit/modin
→ 1767 tests collected in 0.80s
→ 499 passed, 1 xfailed, 1 error (first API failure)
```

**Conclusion:** Tests collect and execute. First real API failure:
```
AttributeError: Mock object has no attribute 'session_id'
  at snowflake.snowpark._internal.server_connection.ServerConnection.get_session_id
  at tests/unit/conftest.py::mock_server_connection
```
This is a real API incompatibility — Snowpark's unit test fixtures create `ServerConnection` with a `MagicMock` in place of a `SnowflakeConnection`. The UD's `Connection` object has a different attribute layout than the old connector (missing `session_id`).

---

### 2026-03-11 — FIPS feature in `snowpark-compatibility` branch — deferred item

**Observation:** The committed HEAD of the `snowpark-compatibility` submodule branch contains:
- `sf_core/Cargo.toml`: `rustls = { version = "0.23.20", features = ["aws_lc_rs", "fips"] }`
- `Cargo.lock`: `aws-lc-fips-sys v0.13.8` entry present

A working-tree modification (uncommitted) removed the `"fips"` feature from `sf_core/Cargo.toml` and removed the `aws-lc-fips-sys` package from `Cargo.lock`. This change was NOT committed.

**Hypothesis (unconfirmed — exact error not recorded):** The working-tree removal was applied to silence a local build failure. `aws-lc-fips-sys` requires `cmake` and a Go-based delocator at build time. The probable failure mode is category: **Build failure** — `cmake` or the delocator was unavailable during a local build attempt on the Rocky Linux / aarch64 build host. The same issue was documented for the `turbaszek-ecosystem-compatibility-cli` branch in "Issue 1" above, and `cmake` unavailability was separately documented as "Issue 2" in the same section. The connection between FIPS and cmake is well-supported by context, but the exact cargo/cmake error output was not captured at the time.

**Note on earlier ADR section ("Issue 1 — FIPS delocator — resolved — not applicable"):** That section stated the `snowpark-compatibility` branch does not have `"fips"` features. This is now contradicted by the committed branch state. The earlier statement was accurate when written (it described the `turbaszek-ecosystem-compatibility-cli` base), but the `snowpark-compatibility` branch diverged and the `"fips"` feature appears in its current commits (introduced by `63e04d74` or earlier snowpark-specific commits). The "resolved — not applicable" label in Issue 1 applies to the base branch as originally described, not to the current `snowpark-compatibility` HEAD. The working-tree removal is the correct local workaround; the committed state carries the FIPS dependency.

**Decision:** The working-tree modification (FIPS removal) is being reverted. The committed state is left as-is.

**Deferred — real fix required:** The `snowpark-compatibility` branch carries `"fips"` in rustls, which adds a build-time dependency on `cmake` and `aws-lc-fips-sys`. For local development on aarch64 / Rocky Linux where cmake is unavailable in the cargo subprocess PATH, this will continue to cause build failures. The real fix has two candidates:
1. Remove `"fips"` from `rustls` in `sf_core/Cargo.toml` in a proper commit on `snowpark-compatibility` with rationale — this is valid if FIPS is not required for Snowpark CI targets.
2. Ensure cmake is available on the PATH in the cargo subprocess during local builds (extend the `CMAKE=...` env var approach in `build_ud_connector.sh` to also cover the FIPS delocator).

**Scope ruling:** This fix is out of scope for the current task (Phase 5 — test collection). Deferred to a follow-up Cargo.toml commit on `snowpark-compatibility` pending user approval.

---

### 2026-03-11 — Symbol categorization: UD compat layer vs Snowpark-side fix — open question

**Background:** Phase 5 added ~12 symbol groups to the UD `snowpark-compatibility` branch to allow Snowpark's existing imports to compile and tests to collect (5468 tests). The question is which additions legitimately belong in the UD connector's compatibility layer vs. which represent Snowpark importing connector internals that should instead be fixed in Snowpark.

**Three-reviewer pipeline was run (2026-03-11).** Findings cross-referenced below.

**Orchestrator ranked split (hypothesis — pending user approval):**

#### Group 1 — Keep in UD compat layer (legitimate connector public API or protocol-level data)

| Symbol | File added to UD | Rationale |
|--------|-----------------|-----------|
| `ResultMetadata` | `cursor.py` | Protocol-level cursor metadata; connector owns this type |
| `ResultMetadataV2` | `cursor.py` | Same — alias for forward-compat |
| `ReauthenticationRequest` | `network.py` | Exception type raised by connector; Snowpark's server_connection.py production path catches it |
| `FIELD_ID_TO_NAME` | `constants.py` | Snowflake wire-protocol type ID mapping; connector owns this mapping |
| `ENV_VAR_PARTNER` | `constants.py` | Connector-owned environment variable name string constant |
| `MissingOptionalDependency` | `options.py` | Connector defines optional-dep infrastructure; Snowpark extends it |
| `MissingPandas` | `options.py` | Same |
| `ModuleLikeObject` | `options.py` | Same |
| `write_pandas` | `pandas_tools.py` | Connector owns the pandas → Snowflake write path |

**Reviewer confidence:** medium–high. These symbols are either used in Snowpark's production code path (`server_connection.py`, `utils.py`, `type_utils.py`) or represent connector-owned abstractions.

**Caveat (Reviewer B, Finding 3):** `write_pandas`, `_create_temp_stage`, `_create_temp_file_format` currently raise `NotImplementedError` in the UD stub. Tests that actually call these will fail at runtime, not at collection. The current definition-of-done (collection) is met; execution compatibility is a Phase 6+ concern.

#### Group 2 — Proposed for Snowpark-side fix (Snowpark importing connector internals)

| Symbol | File added to UD | Rationale for moving to Snowpark |
|--------|-----------------|----------------------------------|
| `_create_temp_stage` | `pandas_tools.py` | `_`-prefixed internal; Snowpark should own this helper or refactor the call site |
| `_create_temp_file_format` | `pandas_tools.py` | Same |
| `build_location_helper` | `pandas_tools.py` | Stage path helper — could live in Snowpark's `analyzer_utils.py` |
| `ASYNC_RETRY_PATTERN` | `cursor.py` | Internal retry backoff constant; Snowpark should own its own polling backoff values |
| `installed_pandas` | `options.py` | Snowpark can compute this itself: `try: import pandas; installed_pandas = True except ImportError: ...` |
| `pandas` (module ref) | `options.py` | Same — Snowpark already imports pandas directly elsewhere |
| `pyarrow` (module ref) | `options.py` | Same |
| `VERSION` | `version.py` | Snowpark can use `importlib.metadata.version("snowflake-connector-python-ud")` |
| `OK` | `compat.py` | Mock-layer only (`mock/_telemetry.py`); use `http.client.OK` directly |
| `SecretDetector` | `secret_detector.py` | Only used in `mock/_telemetry.py`; Snowpark's mock layer should define its own stub |
| `TelemetryService` | `telemetry_oob.py` | Only used in `mock/_telemetry.py`; same — mock-layer concern |

**Reviewer confidence:** medium. The `_`-prefix convention and mock-only usage (Reviewer C, Finding 2) are corroborating evidence but not conclusive. A grep of production vs test-only usage is needed before any Snowpark-side fix is implemented.

**Open questions (not resolved by this pipeline run):**
1. Are `SecretDetector` and `TelemetryService` ever imported outside Snowpark's mock layer? (hypothesis: no, but needs grep confirmation)
2. Does `ASYNC_RETRY_PATTERN` in the UD have the same semantic contract as the old connector's value? (the stub uses `[1, 1, 2, 3, 4, 8, 10]`)
3. Can `MissingDependencyError` (added to `errors.py` but not in the import audit table) remain or should it also be reviewed?

**Scope ruling:** No code changes in this pipeline run. The split is recorded as a hypothesis. Implementing the Snowpark-side fixes requires a separate task and explicit user approval.

**Decision (2026-03-11, user):** Keep all symbols in the UD compat layer for now — as stubs that fail at call time (`NotImplementedError`, no-op) but do not cause import errors. The UD/Snowpark split and any Snowpark-side refactors are deferred to a future task.

---

## Open Items

- [ ] **Phase 6 — API compatibility:** Fix `AttributeError: Mock object has no attribute 'session_id'` and subsequent API failures in `ServerConnection`. First step: check whether UD's `Connection` exposes `session_id` or what its equivalent is.
- [ ] Rebuild wheel before each test run (or automate reinstall in `run_tests_with_ud.sh`).
- [ ] Add `snowflake-connector-python-ud` as an alternative dependency in `setup.py` once UD is on PyPI.
- [ ] Validate modin tests with Universal Driver (Phase 7).
- [ ] Decide whether `snowpark-compatibility` branch should track `turbaszek-ecosystem-compatibility-cli` upstream or diverge with snowpark-specific patches.
- [ ] **FIPS deferred fix:** Remove `"fips"` from `rustls` features in `sf_core/Cargo.toml` on `snowpark-compatibility` (or ensure cmake/delocator availability) — pending user approval and confirmation that FIPS is not required for Snowpark CI targets.
- [ ] **Symbol cleanup (Snowpark-side):** Refactor Snowpark to not import `_create_temp_stage`, `_create_temp_file_format`, `build_location_helper`, `ASYNC_RETRY_PATTERN`, `installed_pandas`, `pandas`/`pyarrow` module refs, `VERSION`, `OK`, `SecretDetector`, `TelemetryService` from the connector — pending user approval. Grep production vs test-only usage first.
