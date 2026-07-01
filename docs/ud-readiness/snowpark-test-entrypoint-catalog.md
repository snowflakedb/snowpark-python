# Snowpark Python Test Entry-Point Catalog — Universal Driver (UD) Readiness

> **Purpose.** Decide, for every way Snowpark Python tests get run, whether it is meaningful to run it with the Universal Driver (UD) swapped in for the legacy `snowflake-connector-python`, and whether it should be *expected to pass*.
>
> **Scope note (per direction).** This catalog lists **every** entry point — including the ones that almost certainly tell us nothing about UD. Nothing is dropped; the classification is a recommendation, not a filter. You make the final call.
>
> **Out of scope (for now).** Prior attempts to run Snowpark on UD are deliberately not studied yet.
>
> **Honesty caveats.** Items marked *TBD* or *(unverified)* were not traced to source in this pass and need a deeper dig before being trusted. Where the original plan skeleton guessed wrong (notably `datasource`), this doc corrects it and says so.
>
> **Source revision.** Verified against `origin/main` @ `3f9d437` (2026-06-25). Connector-seam files (`server_connection.py`, `schema_utils.py`, `file_operation.py`) and `scripts/tox_install_cmd.sh` are unchanged from the prior local checkout; `setup.py` connector pin unchanged (`>=3.17.0,<5.0.0`). Deltas folded in below: **baseline Python moved 3.9 → 3.10**; modin workflow `daily_modin_precommit_py39_py310.yml` **renamed** to `daily_modin_precommit_py310.yml`; new `trigger_daily_tests_on_branch.yml` added.

---

## 0. The one fact everything hinges on

Snowpark never talks to Snowflake directly. It builds SQL/plans and hands them to `session._conn`. The real backend is **`ServerConnection`** (`src/snowflake/snowpark/_internal/server_connection.py`), a thin wrapper over a `snowflake.connector.SnowflakeConnection`. UD's Python wrapper **ships under the name `snowflake-connector-python`** (drop-in PEP 249). So:

> **"UD under the hood" = install the UD wheel instead of the PyPI connector, then run Snowpark's existing suites unchanged.**

An entry point only tells us something about UD **if it constructs a real `ServerConnection` (a live `snowflake.connector.connect`)**. That single question drives the classification.

### Classification legend

| Class | Meaning | What a run tells us about UD |
|---|---|---|
| **A** | UD-relevant, expected to pass | Traverses the connector seam on surfaces UD claims parity for. **Primary signal.** |
| **B** | UD-relevant, **at-risk** | Traverses the seam but touches a surface UD diverges on (`BehaviorDifferences.yaml`) or has not proven for Snowpark's usage (Arrow batches, PUT/`upload_stream`, async sfqid, `write_pandas`/`fetch_pandas`, auth). **Highest-information runs.** |
| **C** | Irrelevant — bypasses the connector | Mock/local-testing, AST/IR, pure plan-generation unit tests. Should pass unchanged; says nothing about UD. **Keep running as a control, not as signal.** |
| **D** | Server-side — out of scope for a *client* swap | Runs inside Snowflake using the bundled server-side Snowpark + stored-proc connector, not the client wheel. A client UD swap cannot move these. |

---

## 1. The connector API surface Snowpark depends on (the UD parity contract)

Derived from `server_connection.py` calls + `grep "from snowflake.connector"` across `src/`. This is exactly what UD's Python wrapper must satisfy for Snowpark.

**Connection/cursor object (`ServerConnection`):**
- `connect(...)`, `conn.cursor()`, `conn.close()`, `conn.is_closed()`, `conn.session_id`, `conn._session_parameters`
- `cursor.execute`, `cursor.execute_async`, `cursor.executemany`, `cursor.get_results_from_sfqid`, `cursor.upload_stream` (PUT)
- result attrs: `.description`, `.fetchall`, `.fetch_arrow_all`, `.fetch_arrow_batches`, `.fetch_pandas_all`, `.fetch_pandas_batches`, `.query`, `.sfqid`, `._request_id`, `.connection`

**Imported connector internals (each = a coupling UD must honor):**

| Connector module / symbol | Used by (file) | UD risk |
|---|---|---|
| `connection.SnowflakeConnection`, `connect` | server_connection.py, session.py | core — must exist & acce same kwargs |
| `cursor.ResultMetadata`, `ResultMetadataV2`, `SnowflakeCursor`, `ASYNC_RETRY_PATTERN` | server_connection, analyzer | result-metadata typing; **V2 shape must match** |
| `constants.FIELD_ID_TO_NAME`, `ENV_VAR_PARTNER` | analyzer_utils, server_connection | type-id→name map must be identical |
| `errors.{Error,ProgrammingError,NotSupportedError,DatabaseError,IntegrityError,OperationalError}` | error_message.py, session, analyzer | exception classes + hierarchy must match (BD#21 closed-cursor) |
| `options.{pandas,pyarrow,installed_pandas,MissingPandas,...}` | many | pandas/arrow optional-dep plumbing |
| `pandas_tools.write_pandas` | session.py | `write_pandas` parity (Arrow upload path) |
| `network.ReauthenticationRequest` | server_connection.py | reauth flow |
| `telemetry.*`, `telemetry_oob.TelemetryService`, `TelemetryField` | server_connection, telemetry | **telemetry classes UD may not expose (BD#20-ish)** |
| `secret_detector.SecretDetector` | utils.py | log masking |
| `wif_util.create_attestation` | session.py | workload-identity auth |
| `config_manager` | session.py | `connections.toml` implicit sessions |
| `version.VERSION` | server_connection.py | version tuple presence |
| `compat.OK`, `description.{OPERATING_SYSTEM,PLATFORM}`, `time_util.get_time_millis` | misc | low risk but must exist |

> **Critical-eye note:** Snowpark reaches into a lot of **private/internal** connector surface (`_session_parameters`, `_request_id`, `telemetry_oob`, `secret_detector`, `wif_util`, `config_manager`). Drop-in PEP 249 compliance is **not sufficient** — UD must replicate these internals or Snowpark import/runtime will break before a single query runs. This is the single biggest readiness risk and should be audited symbol-by-symbol against UD's `python/src/snowflake/connector/`.

---

## 1b. Private-API coupling & monkey-patching audit (verified)

Swept `src/snowflake/snowpark` for private-attribute/method access on the **real**
`SnowflakeConnection`/`SnowflakeCursor` (not Snowpark's own `ServerConnection`,
whose `_telemetry_client`/`_thread_safe_session_enabled`/`send_*` are *Snowpark's*
internals and irrelevant to UD), and for monkey-patching of the connector.

### Real-connector private coupling — UD must replicate or Snowpark breaks

| Private symbol | Site | Guarded? | Risk | Consequence if UD lacks it |
|---|---|---|---|---|
| `cursor._describe_internal(query, params=)` | `analyzer/schema_utils.py:177` (`run_new_describe`) | **No** — called unconditionally | **CRITICAL** | Describe-without-execute is how Snowpark resolves `df.schema` and every lazy type before running a query. Breaks pervasively. |
| `cursor._description_internal` | `schema_utils.py:158` | Yes — `hasattr` fallback to `.description` | Low | Graceful degrade to V1 metadata. |
| `cursor._upload` / `_download` / `_upload_stream` / `_download_stream` | `file_operation.py:131,206,274,336` | No | **HIGH** | `session.file.put/get` + stage I/O dead. Also underpins UDF/sproc artifact upload. |
| `conn._session_parameters` | `server_connection.py:172,175,992,993` | No | **HIGH** | Session-param plumbing (read at connect + per-query). Note: UD BD#15 changes session-param caching — *behavioral* divergence even if attr exists. |
| `conn._conn._paramstyle` | `analyzer/snowflake_plan.py:1028` | No | Medium | Bind-variable formatting decision; wrong/missing → bind errors. |
| `conn.get_query_status` / `is_still_running` / `is_an_error` / `result.expired` | async_job path (`_conn._conn.*`) | No | Medium | Async query polling (`collect_nowait`, `AsyncJob`). |
| `inspect.signature(cursor.execute)` probe for `_skip_upload_on_content_match` | `server_connection.py:198` | Yes — feature-flag bool | Low | If UD's `execute` uses generic `**kwargs`, the param won't be detected and the optimization silently disables. Acceptable, but means **UD's `execute` signature is introspected** — generic signatures lose feature detection. |

### Monkey-patching — essentially NONE on the connector (good news, verified)

- **No** assignments of the form `snowflake.connector.X = ...`.
- **No** method replacement (`cursor.execute = ...`, etc.).
- The single `setattr(self._conn, f"_active_{object_type}", object_name)` (`session.py:4352`) is in the **MockServerConnection branch** (guarded by `self._conn.get_lock()`, a mock-only method) — it does **not** touch the real connector, so there is **no `__slots__` risk** on UD from this.
- Connector optional-deps are *read*, not patched: `options.installed_pandas/pandas/pyarrow` gate code in `type_utils.py`/`session.py`. UD must expose `snowflake.connector.options` with the same truthiness or pandas/Arrow paths misbehave.

### Verdict
The danger is **not** monkey-patching — it's **deep private coupling**. The
unconditional `cursor._describe_internal` and the `cursor._upload/_download*`
family are the two that will break *before* test logic even runs. These belong at
the top of the import/connect pre-flight, ahead of any matrix run.

---

## 2. snowpark-python — tox factor entry points

All driven by `[testenv]` in `tox.ini`; install path is `scripts/tox_install_cmd.sh`. Markers from `tests/conftest.py` + `tox.ini`.

| Entry point (tox factor / command) | What it runs | Backend / seam? | Class | Notes & risk |
|---|---|---|---|---|
| `py3xx-notdoctest-ci` (`-m "(unit or integ or doctest) or udf"`, `tests`) | full unit+integ+doctest | **real connector** (integ/doctest parts) + no-conn (unit parts) | **B** | The headline PR/CI suite. integ DataFrame ops, results, types, PUT/GET, UDF registration → exercises most of §1. Highest-value UD run. |
| `py3xx-notdoctest-notudf` | same minus UDF tests | real connector | **B** | UDF reg/exec is a heavy connector+server path; splitting it out isolates risk. |
| `py3xx-udf...` | UDF/UDTF/UDAF suites | real connector + server compile | **B** | `cursor.execute` of CREATE FUNCTION + cloudpickle upload; PUT via `upload_stream`. |
| `py3xx-doctest-notudf-ci` (`src/snowflake/snowpark` + `tests`) | doctests in source | **real connector** (most doctests open a session) | **B (unverified per-doctest)** | Many functions.py/dataframe.py doctests `.collect()` live. Confirm none are mock-only. |
| `py3xx-dailynotdoctest` / `dailynotdoctestnotudf` | daily full integ at `-n 6` | real connector | **B** | Same as notdoctest, lower parallelism, broader matrix. |
| `py3xx-local` (`--local_testing_mode -m "integ or unit or mock"`) | local-testing engine | **`MockServerConnection`** — connector bypassed | **C** | Should pass identically on UD because UD is never loaded. Useful as a *control*, zero UD signal. |
| `py3xx-ast` (`-m ast`) | AST/IR encoding | **no live conn** (protobuf IR + Java unparser jar) | **C** | UD irrelevant. (Spot-check: confirm no ast test opens a session for round-trip validation.) |
| `datasource` (`tests/integ/datasource -n 8`) | DBAPI ingestion from MySQL/Postgres/Oracle/Databricks/ODBC **into Snowflake** | **source** = external drivers; **sink** = real connector (stage PUT + COPY) | **B** | **Correction vs plan skeleton (was guessed C).** The Snowflake write path goes through the connector → genuinely UD-relevant, and stresses `upload_stream`/`executemany`. External source drivers are unaffected by UD. |
| `snowparkpandasnotdoctest` / `snowparkpandasdailynotdoctest` (modin) | Snowpark pandas integ | real connector + **Arrow/`fetch_pandas`** | **B** | High risk: depends on `fetch_pandas_*`/`fetch_arrow_*` and `write_pandas`. Arrow result-format parity is exactly UD's least-proven Snowpark surface. |
| `snowparkpandasdoctest` (modin) | Snowpark pandas doctests | real connector + Arrow | **B** | Same Arrow risk. |
| `snowparkpandashybridcheck_*` | session-vs-plugin init order, AutoSwitchBackend | **real connector** (creates a Session) | **B (unverified)** | Creates a live Session in a one-liner; will load UD. Low surface but real. |
| `modin_previous_version-...`, `modin_pandas_version`, `pyarrowcap`/`pandascap`, `snowparkpandasjenkins` | dependency-pinned modin variants | real connector + Arrow | **B** | Same as modin above; pin combinations orthogonal to UD. |
| `nopandas` (`[testenv:nopandas]`) | tests with pandas uninstalled | real connector, no pandas | **B (unverified)** | Validates the no-pandas import path → UD must also degrade gracefully without pandas extra. |
| `coverage` | aggregates `.coverage.*` | no tests | **N/A** | Reporting only. |
| `flake8` / `fix_lint` / `pyright` / `protoc` / `docs` | lint, types, protobuf-gen, docs | no tests | **N/A** | Not test runs. (pyright *could* surface UD type-stub gaps, but only if UD is installed in the lint env — currently not.) |
| `dev` | editable dev env | n/a | **N/A** | Setup convenience. |

---

## 3. snowpark-python — GitHub Actions workflows (orchestrators)

These call the tox factors above. Trigger + matrix matter because UD must build on each OS/Python/cloud cell.

| Workflow | Trigger | Jobs (→ tox) | Matrix (OS × Py × cloud) | Class of the test jobs | UD notes |
|---|---|---|---|---|---|
| `precommit.yml` | push, PR, dispatch | lint, pyright, build; **Test** → `notdoctest`/`doctest`; **datasource**; **Local Testing** → `local`; **AST** → `ast` | ubuntu/win/macos × **py3.10–3.14** × aws/azure/gcp (sparse matrix; lint/pyright/build on 3.10) | Test=**B**, datasource=**B**, Local=**C**, AST=**C** | The PR gate. Inject UD here for the highest-fidelity readiness signal. Matrix breadth = UD must produce wheels for all cells. |
| `daily_precommit.yml` | cron 08:00 + dispatch | `dailynotdoctest(notudf)`, doctest, datasource | broader daily matrix | **B** (+C/N/A) | Nightly full sweep; best place for a non-blocking UD lane first. |
| `daily_modin_precommit_py310.yml` | cron 12:00 | modin doctest/notdoctest + import-error + old-np variants | py3.10 | **B** | Arrow-heavy; strongest test of UD's pandas/Arrow parity. (Renamed from `_py39_py310`; 3.9 dropped.) |
| `daily_modin_precommit_py311_py312.yml` | cron 15:00 | modin doctest/notdoctest | py3.11/3.12 | **B** | Same. |
| `ast-check.yml` | PR | AST encoding validation | py3.11 | **C** | No connector. |
| `daily_jupyter_nb_test.yml` | cron 11:00 | `protoc` then `pytest -ra` on notebooks | — | **A/B (unverified)** | Notebooks create live Sessions → loads UD. Confirm they hit Snowflake vs mock. |
| `trigger_daily_tests_on_branch.yml` | dispatch | fans the daily suites out against an arbitrary branch | — | **N/A (orchestration)** | New helper; runs the same underlying jobs, so inherits their classes. Useful to *trigger a UD-branch sweep on demand*. |
| `enforce_localtest.yml` | PR | guards local-testing coverage (meta-lint) | — | **N/A** | Policy check, not a connector test. |
| `threadsafe-check.yml` | PR | thread-safety lint/check | — | **N/A** | Not a connector test (but: UD's threading model differs — worth a dedicated future check). |
| `python-publish.yml` | release | build & publish wheel | — | **N/A** | Packaging. |
| `changelog.yml`, `cla_bot.yml`, `semgrep.yml`, `jira_*`, `label_*`, `changedoc_*`, `checkprs.yml`, `create-test-branch-from-release.yml` | various | repo hygiene/security | — | **N/A** | No tests. |

---

## 4. jenkins_utils — Jenkins jobs

> **Correction vs first pass:** there are far more snowpark Jenkins jobs than the
> initial scout found. Jenkins runs the *same Snowpark pytest suite* as GHA but via
> `scripts/jenkins_regress.sh` → `tox -e notdoctest-pandascap-pyarrowcap` (no `py3xx`
> prefix → uses the venv's Python, **3.8 or 3.10**), with credentials GPG-decrypted
> from `scripts/parameters.py.gpg`, on internal `regular-memory-node-snowos` (RHEL)
> nodes, **nightly cron only** (`H H(3-4) * * *`) — no PR gate.

| Job (file) | Trigger | What it runs | Connector source | Class / UD note |
|---|---|---|---|---|
| `SnowparkPythonClientRegress.groovy` | nightly cron | `jenkins_regress.sh` → `tox -e notdoctest-pandascap-pyarrowcap` (py3.8) | **PyPI** | **B** — same integ/unit/doctest suite as GHA, capped pandas/pyarrow. |
| **`SnowparkPythonSnowflakePythonClientRegress.groovy`** | nightly cron | checks out **both** snowpark-python **and** snowflake-connector-python, **builds the connector wheel from source**, sets `snowflake_path=.../dist/repaired_wheels/`, then runs the same tox suite (py3.10) | **source-built wheel via `snowflake_path`** | **★ THE UD-SWAP TEMPLATE.** This job *already* runs Snowpark against a locally-built connector via the exact seam UD needs. Swap the connector checkout/build for a UD wheel build and this job becomes the UD readiness lane essentially unchanged. |
| `SnowparkPythonReleaseValidation.groovy` | manual/release | orchestrates the above + stored-proc Snowfort build + connector regress across branches | mixed | release gate; out of day-to-day scope. |
| `SnowparkConnectAgainstSnowparkPython.groovy` | **PR gate** (`snowpark_python_merge_gate`) + cron | Snowpark **Connect** (Spark Connect) suites | TBD | **TBD** — connector involvement of the Spark-Connect client path still unverified. |
| `BuildAnaconda.groovy` / `BuildPipAnaconda.groovy` / `PushToTestPyPICustomRepo.groovy` | build/release | conda + pip packaging of snowpark-python | n/a | **N/A** packaging; future conda UD-variant concern. |
| `SnowparkClientRegress*.groovy` (incl. `Fips`, `GCP`, `LatestJdbc`) | nightly | **Java/Scala** Snowpark client regress (not Python) | JDBC | out of scope (different language client). |
| `RT.groovy` → `Local-Short-Snowpark-Group` | RT | regression-test grouping | — | TBD what it executes. |
| `SparkCompilerExtension/*`, `Sparkle/*` | merge gates | Spark extension build/test | — | likely D / Spark-side. |

### GHA vs Jenkins — the essential difference
- **GHA** = the **PR merge gate** + public-CI nightly. Broad matrix (ubuntu/win/macos × py3.9–3.14 × aws/azure/gcp), granular per-factor jobs (notdoctest / local / ast / datasource / modin), connector from **PyPI**, GitHub-hosted runners.
- **Jenkins** = **internal nightly** regression + packaging/release + cross-repo integration. Narrower Python (3.8/3.10), RHEL/`snowos` internal nodes (VPN/credentialed), single coarse `notdoctest` factor with pandas/pyarrow caps, GPG'd credentials, and — uniquely — **the ability to test against a source-built connector** (`SnowparkPythonSnowflakePythonClientRegress`).
- **For UD:** GHA is where you'd want the eventual signal (PR-visible), but **Jenkins already has the mechanism** (`snowflake_path` source-build) and the internal network/credentials UD's preprod testing may need. Start the UD lane by cloning `SnowparkPythonSnowflakePythonClientRegress` with a UD wheel build.



---

## 5. snowflake monorepo — Buildkite / server-side

| Entry point | What it runs | Class | UD notes |
|---|---|---|---|
| `.buildkite/pipelines/snowpark-python-sp-integ-tests/pipeline.yml` → `t_python_sp`, `t_python_sp_integ(_scala/_udf/_sp)`, `t_python_sp_connector`, `t_python_sp_bundle`, `t_python_sp_udf_server`, `udf_file_access/sp_put_get` (X86 + ARM) | Snowpark **inside a stored proc / UDF**, using the **server-side bundled snowpark + stored-proc Python connector** | **D** | A *client* UD swap cannot affect these. Moving them to UD = a separate "bundle UD server-side" effort (replacing the version pinned in `SpVanillaPythonFrozenSolveData*.json`). Note `t_python_sp_connector` specifically may be the connector's own server-side suite — worth confirming. |
| `GlobalServices/modules/snowpark/python-udfs/.../SpVanillaPythonFrozenSolveData{3.8..3.13}.json` | frozen conda manifests pinning `snowflake-snowpark-python` + connector versions used server-side | **D (config)** | The server-side version-pinning surface; the eventual server-side UD swap target. |
| credential `universal_driver_test_suite_api_token` (`.buildkite/fuse/secrets.yaml`) | — | **note** | Evidence UD has *some* monorepo CI footprint already; trace what consumes it (later iteration, not the "prior attempts" we're deferring). |

---

## 6. Step 5 — UD build/install substitution recipe & how the suite differs

### The injection seam already exists
`scripts/tox_install_cmd.sh` reads env var **`snowflake_path`**:
- unset → `uv pip install` the connector **from PyPI** (legacy, current behavior);
- set → `uv pip install ${snowflake_path}/snowflake_connector_python*${cpXY}*.whl` **before** the rest.

UD's wheel is named `snowflake_connector_python`. **Therefore: build the UD wheel, drop it in a dir, set `snowflake_path=<dir>`, and every tox factor above runs on UD with no test edits.** (`snowflake_path` is already in tox `passenv`.) This is the cleanest possible swap and should be the basis of the readiness lane.

### How UD is built (to confirm against `universal-driver/python/`)
- UD Python wheel builds via **hatch**; `hatch_build.py` compiles the Rust `sf_core` (**Rust 1.88+, edition 2024**).
- UD's own `pyproject.toml` already has a `reference` hatch env that installs the *legacy* connector for parity — we are doing the **mirror image**: build UD, feed it to Snowpark's tox.
- Need to confirm: exact build command, that the wheel filename matches `snowflake_connector_python*cpXY*.whl`, and per-(OS,Py) wheel availability for the precommit matrix cells.

### How the UD suite differs from the legacy-driver run
1. **Extra build step + toolchain**: Rust toolchain per runner; wheel build time; per-platform wheels (the legacy path just pulls a prebuilt PyPI wheel).
2. **Known-divergence skip/xfail list** derived from §1 × UD `BehaviorDifferences.yaml` (e.g. `private_key_file_pwd`→`private_key_password`, session-param caching, `ResultBatch` no longer exposing `session_manager`/`http_config`, closed-cursor `InterfaceError`, proxy-env opt-in). These should be **expected, annotated failures**, not surprises.
3. **Private-surface gaps** (§1 critical note): import-time breakage if UD lacks `telemetry_oob`, `secret_detector`, `wif_util`, `config_manager`, `_session_parameters`, `_request_id`. Audit these first — they gate *everything*, not individual tests.
4. **Arrow/pandas result format**: modin + `fetch_pandas`/`fetch_arrow` lanes are where format parity is most likely to bite.
5. **Telemetry**: Snowpark constructs `TelemetryClient(self._conn)` and OOB telemetry; UD's telemetry internals differ → may need shimming or disabling in the readiness lane.

### Recommended rollout (opinion, as SDET)
1. **Pre-flight**: a tiny script that just `import snowflake.snowpark` + `Session.builder...create()` against UD. If §1 private symbols are missing, you find out in seconds, not after a 90-minute matrix.
2. **Smoke**: one `py311-notdoctest-ci -k "test_filter or collect"` cell on aws/ubuntu with `snowflake_path` set.
3. **Class-B sweep, non-blocking**: add a UD lane to `daily_precommit.yml` (not the PR gate) so divergences surface without blocking merges.
4. **Modin/Arrow lane** separately — highest risk, worth isolating.
5. Keep **class C** running on the legacy driver only (no value under UD); keep **class D** untouched until a server-side bundling decision.

---

## 7. Open questions to resolve before trusting this catalog
- **Snowpark Connect (Jenkins)**: does the client path use `snowflake-connector-python`? (A/B vs D) — *blocking ambiguity.*
- **Doctests & notebooks**: confirm they open live Sessions (assumed B) vs mock.
- **`t_python_sp_connector`**: is this the connector's server-side suite (then partly UD-relevant server-side) or snowpark's?
- **AST round-trip**: any ast test that dispatches to the server?
- **UD wheel coverage**: which (OS, Python) cells in the precommit matrix have UD wheels today?
- **`universal_driver_test_suite_api_token`** consumer in the monorepo (defer if it leads into prior attempts).
