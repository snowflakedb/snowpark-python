#!/usr/bin/env bash
# Run Snowpark tests against the Universal Driver, mirroring ud-inline-tests.yml.
#
# Injection priority:
#   --ud-connector → ud_connector_path (direct pip-installable; no pre-build needed)
#   --ud-path DIR  → snowflake_path    (pre-built wheel dir; no pre-build needed)
#   (neither)      → auto-build via scripts/build-ud-wheel.sh
#
# Workflow parity:
#   --group integration  →  tox -e py<ver>-notdoctest-ci -- --ignore=tests/integ/scala
#   --group scala        →  tox -e py<ver>-notdoctest-ci -- -k scala
#   --group all          →  integration then scala (exact CI matrix)
#
# Extended groups (not in current workflow, but useful):
#   --group datasource   →  tox -e datasource
#   --group modin        →  tox -e py<ver>-snowparkpandasnotdoctest-ci
#   --group doctest      →  tox -e py<ver>-doctest-ci
#
# File mode (fast, avoids full-suite collection):
#   --file PATH          →  install deps via tox --notest, then pytest directly against
#                           just that file. Combine with -- -k name to pick specific tests.
#
# Usage examples:
#   scripts/run-ud-tests.sh --group integration --no-rust
#   scripts/run-ud-tests.sh --group scala --ud-path /tmp/ud-wheels
#   scripts/run-ud-tests.sh --file tests/integ/test_reduce_describe_query.py --no-rust
#   scripts/run-ud-tests.sh --file tests/integ/test_reduce_describe_query.py \
#     --ud-path /tmp/ud-wheels -- -k test_metadata_no_change -v
#   scripts/run-ud-tests.sh --group all \
#     --ud-connector "git+https://github.com/snowflakedb/universal-driver@snowpark-compatibility#subdirectory=python"
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Defaults.
GROUP="integration"
FILE=""
PYTHON_VER="3.13"
UD_PATH=""
UD_CONNECTOR=""
UD_REPO="/home/repo/universal-driver"
NO_RUST=0
OUT_DIR=""
DRY_RUN=0
EXTRA_PYTEST_ARGS=()

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS] [-- EXTRA_PYTEST_ARGS]

  --group GROUP        integration|scala|datasource|modin|doctest|all
                       (default: integration; ignored when --file is set)
  --file PATH          run pytest directly against one file only (fast; skips full
                       collection). Uses tox --notest to set up the env first.
  --python VER         Python version: 3.10|3.11|3.12|3.13  (default: 3.13)
  --ud-path DIR        pre-built wheel dir  → sets snowflake_path (skips build)
  --ud-connector URL   pip-installable URL/path → sets ud_connector_path (skips build)
  --ud-repo PATH       build wheel from this local clone (default: /home/repo/universal-driver)
  --no-rust            pass --no-rust to build-ud-wheel.sh  (SKIP_CORE_BUILD=1)
  --out-dir DIR        wheel output dir when auto-building
  --dry-run            print commands without running them
  -h, --help           show this message

Extra pytest args can be appended after -- :
  $(basename "$0") --file tests/integ/test_reduce_describe_query.py -- -k test_metadata_no_change -v
  $(basename "$0") --group integration --ud-path /tmp/ud-wheels -- -k test_dataframe -x
EOF
}

# Parse args.
while [[ $# -gt 0 ]]; do
  case "$1" in
    --group)       GROUP="$2";       shift 2 ;;
    --file)        FILE="$2";        shift 2 ;;
    --python)      PYTHON_VER="$2";  shift 2 ;;
    --ud-path)     UD_PATH="$2";     shift 2 ;;
    --ud-connector) UD_CONNECTOR="$2"; shift 2 ;;
    --ud-repo)     UD_REPO="$2";     shift 2 ;;
    --no-rust)     NO_RUST=1;        shift ;;
    --out-dir)     OUT_DIR="$2";     shift 2 ;;
    --dry-run)     DRY_RUN=1;        shift ;;
    -h|--help)     usage; exit 0 ;;
    --)            shift; EXTRA_PYTEST_ARGS=("$@"); break ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
  esac
done

# Validate group.
VALID_GROUPS="integration scala datasource modin doctest all"
if ! echo "$VALID_GROUPS" | tr ' ' '\n' | grep -qx "$GROUP"; then
  echo "ERROR: unknown group '$GROUP'. Valid: $VALID_GROUPS" >&2
  exit 1
fi

# Validate python version.
PYTHON_TAG="${PYTHON_VER/./}"  # "3.13" → "313"
case "$PYTHON_VER" in
  3.10|3.11|3.12|3.13) ;;
  *) echo "ERROR: unsupported Python version '$PYTHON_VER'. Use 3.10–3.13." >&2; exit 1 ;;
esac

# Check for parameters.py when running against real Snowflake.
needs_account() {
  case "$1" in
    integration|scala|datasource|all) return 0 ;;
    *) return 1 ;;
  esac
}
if [[ "$DRY_RUN" -eq 0 ]] && needs_account "$GROUP" && [[ ! -f "$REPO_DIR/tests/parameters.py" ]]; then
  echo "ERROR: tests/parameters.py not found." >&2
  echo "  Integration/scala/datasource tests require Snowflake credentials." >&2
  echo "  Decrypt it with .github/scripts/decrypt_parameters.sh or provide it manually." >&2
  exit 1
fi

# Resolve UD injection mechanism.
if [[ -n "$UD_CONNECTOR" ]]; then
  echo "==> Using ud_connector_path: $UD_CONNECTOR" >&2
  export ud_connector_path="$UD_CONNECTOR"
elif [[ -n "$UD_PATH" ]]; then
  echo "==> Using snowflake_path (wheel dir): $UD_PATH" >&2
  if [[ "$DRY_RUN" -eq 0 && ! -d "$UD_PATH" ]]; then
    echo "ERROR: wheel dir not found: $UD_PATH" >&2
    exit 1
  fi
  export snowflake_path="$UD_PATH"
else
  echo "==> No wheel dir supplied — building from $UD_REPO" >&2
  BUILD_ARGS=("--ud-repo" "$UD_REPO")
  [[ -n "$OUT_DIR" ]] && BUILD_ARGS+=("--out-dir" "$OUT_DIR")
  [[ "$NO_RUST" -eq 1 ]] && BUILD_ARGS+=("--no-rust")
  WHEEL_DIR=$("$SCRIPT_DIR/build-ud-wheel.sh" "${BUILD_ARGS[@]}")
  echo "==> Wheel built at: $WHEEL_DIR" >&2
  export snowflake_path="$WHEEL_DIR"
fi

# Build the tox command for a single group.
# Returns: tox command as an array (written to TOX_CMD global).
build_tox_cmd() {
  local grp="$1"
  TOX_CMD=()
  case "$grp" in
    integration)
      TOX_CMD=(tox -e "py${PYTHON_TAG}-notdoctest-ci"
               -- --ignore=tests/integ/scala "${EXTRA_PYTEST_ARGS[@]}")
      ;;
    scala)
      TOX_CMD=(tox -e "py${PYTHON_TAG}-notdoctest-ci"
               -- -k scala "${EXTRA_PYTEST_ARGS[@]}")
      ;;
    datasource)
      TOX_CMD=(tox -e datasource -- "${EXTRA_PYTEST_ARGS[@]}")
      ;;
    modin)
      TOX_CMD=(tox -e "py${PYTHON_TAG}-snowparkpandasnotdoctest-ci"
               -- "${EXTRA_PYTEST_ARGS[@]}")
      ;;
    doctest)
      # py<ver>-doctest-ci sets SNOWFLAKE_TEST_TYPE=doctest via the doctest factor.
      TOX_CMD=(tox -e "py${PYTHON_TAG}-doctest-ci" -- "${EXTRA_PYTEST_ARGS[@]}")
      ;;
  esac
}

run_group() {
  local grp="$1"
  build_tox_cmd "$grp"
  echo "" >&2
  echo "==> Group: $grp" >&2
  echo "    Command: ${TOX_CMD[*]}" >&2
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] Would run: ${TOX_CMD[*]}"
    return 0
  fi
  cd "$REPO_DIR"
  "${TOX_CMD[@]}"
}

# File mode: install deps via tox --notest, then run pytest directly against one file.
# This avoids collecting the full tests/ tree, so startup is much faster.
run_file() {
  local tox_env="py${PYTHON_TAG}-notdoctest-ci"
  local tox_venv="$REPO_DIR/.tox/${tox_env}"
  local setup_cmd=(tox -e "$tox_env" --notest)
  # LD_PRELOAD: expose system libssl/libcrypto/libz/libstdc++ so libsf_core.so can dlopen
  # them without pulling in the system libc (which would break Nix's glibc 2.34 loader).
  local LD_PRELOAD_LIBS="/lib64/libssl.so.3:/lib64/libcrypto.so.3:/lib64/libz.so.1:/lib64/libstdc++.so.6"
  local pytest_cmd=(env "LD_PRELOAD=${LD_PRELOAD_LIBS}" "${tox_venv}/bin/python" -m pytest "$FILE" "${EXTRA_PYTEST_ARGS[@]}")

  echo "" >&2
  echo "==> File mode: $FILE" >&2
  echo "    Setup  : ${setup_cmd[*]}" >&2
  echo "    Pytest : ${pytest_cmd[*]}" >&2

  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] Would run: ${setup_cmd[*]}"
    echo "[dry-run] Would run: ${pytest_cmd[*]}"
    return 0
  fi

  cd "$REPO_DIR"
  if [[ ! -x "${tox_venv}/bin/python" ]]; then
    echo "==> tox env not found — running tox --notest to create it" >&2
    "${setup_cmd[@]}"
  else
    echo "==> tox env exists; re-running tox --notest to ensure UD wheel is installed" >&2
    "${setup_cmd[@]}"
  fi

  echo "" >&2
  echo "==> Running pytest against $FILE" >&2
  "${pytest_cmd[@]}"
}

# Execute.
if [[ -n "$FILE" ]]; then
  if [[ "$DRY_RUN" -eq 0 && ! -f "$REPO_DIR/$FILE" && ! -f "$FILE" ]]; then
    echo "ERROR: test file not found: $FILE" >&2
    exit 1
  fi
  run_file
elif [[ "$GROUP" == "all" ]]; then
  run_group integration
  run_group scala
else
  run_group "$GROUP"
fi
