#!/usr/bin/env bash

set -e

# ── Standard install ─────────────────────────────────────────────────
# Install project dependencies using uv.  This section is shared by
# every tox environment and must remain provider-agnostic.
# ─────────────────────────────────────────────────────────────────────

# Check if uv is installed, and install it if not
if ! command -v uv &> /dev/null; then
    echo "uv not found, installing it..."
    python -m pip install uv
fi

# The options are passed to us with spaces and we need to get rid
# of those spaces when we pump it into uv pip install
declare -a uv_options=()
input_options=( "$@" )

for val in "${input_options[@]}"; do
  uv_options+=(${val// /})
done

echo "${uv_options[*]}"

# Default to empty, to ensure snowflake_path variable is defined.
snowflake_path=${snowflake_path:-""}
python_version=$(python -c 'import sys; print(f"cp{sys.version_info.major}{sys.version_info.minor}")')

if [[ -z "${snowflake_path}" ]]; then
  echo "Using Python Connector from PyPI"
  uv pip install ${uv_options[@]}
else
  echo "Installing locally built Python Connector"
  echo "Python Connector path: ${snowflake_path}"
  ls -al ${snowflake_path}
  uv pip install ${snowflake_path}/snowflake_connector_python*${python_version}*.whl
  uv pip install ${uv_options[@]}
fi

# ── Universal Driver connector swap ──────────────────────────────────
# When ud_connector_path is set, replace snowflake-connector-python
# with the Universal Driver build.  This is a no-op for normal CI.
# ─────────────────────────────────────────────────────────────────────

ud_connector_path=${ud_connector_path:-""}
if [[ -n "${ud_connector_path}" ]]; then
  echo "Swapping snowflake-connector-python → Universal Driver"
  echo "  UD connector path: ${ud_connector_path}"
  # --reinstall ensures wheel files are always extracted even if the
  # version matches a previous install (uv otherwise skips silently).
  uv pip uninstall snowflake-connector-python
  uv pip install --reinstall "${ud_connector_path}"
fi
