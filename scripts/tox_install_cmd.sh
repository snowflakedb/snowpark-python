#!/usr/bin/env sh

set -e

# The options are passed to us with spaces and we need to get rid
# of those spaces when we pump it into pip install
declare -a pip_options=()
input_options=( "$@" )

for val in "${input_options[@]}"; do
  pip_options+=(${val// /})
done

echo "${pip_options[*]}"

if [[ -z "${snowflake_path}" ]]; then
  echo "Using Python Connector from PyPI"
  python -m pip install -U ${pip_options[@]}
else
  echo "Installing locally built Python Connector"
  echo "Python Connector path: ${snowflake_path}"
  ls -al ${snowflake_path}
  python -m pip install ${snowflake_path}/snowflake_connector_python*cp38*.whl
  python -m pip install -U ${pip_options[@]}
fi
