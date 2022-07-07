#!/usr/bin/env sh

set -e

# The options are passed to us with spaces and we need to get rid
# of those spaces when we pump it into pip install
declare -a pip_options=()
input_options=( "$@" )

for val in "${input_options[@]}"; do
  pip_options+=(${val// /})
done

echo "Upgrading pip and setuptools. Installing cffi"
python -m pip install -U pip setuptools wheel cffi --only-binary=:all:

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
