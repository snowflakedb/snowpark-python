#!/usr/bin/env bash
set -e

# Run normal install first (deps + project)
bash ./scripts/tox_install_cmd.sh "$@"

# Swap connector if ud_connector_path is set
ud_connector_path=${ud_connector_path:-""}
if [[ -n "${ud_connector_path}" ]]; then
  echo "Swapping snowflake-connector-python → Universal Driver"
  echo "UD connector path: ${ud_connector_path}"
  # Remove old connector and install UD in its place.
  # --reinstall ensures wheel files are always extracted even if the version
  # matches a previous install (uv otherwise skips re-extraction silently).
  uv pip uninstall snowflake-connector-python
  uv pip install --reinstall "${ud_connector_path}"
fi
