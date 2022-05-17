#!/usr/bin/env sh

set -e

# The options are passed to us with spaces and we need to get rid
# of those spaces when we pump it into pip install
declare -a pip_options=()
input_options=( "$@" )

for val in "${input_options[@]}"; do
  echo $val
  pip_options+=(${val// /})
done

echo "${pip_options[*]}"

python -m pip install -U ${pip_options[@]}
