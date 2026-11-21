#!/usr/bin/env bash

set -e

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
# Python and ABI tags, e.g. cp314-cp314 or cp314-cp314t, so a free-threaded build never picks a GIL wheel.
python_version=$(python -c 'import sys; v = "cp%d%d" % sys.version_info[:2]; print(v + "-" + v + getattr(sys, "abiflags", ""))')

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
