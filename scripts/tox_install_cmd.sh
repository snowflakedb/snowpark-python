#!/usr/bin/env sh

set -e

declare -a pip_options=()
input_options=( "$@" )

for val in "${input_options[@]}"; do
  echo $val
  pip_options+=(${val// /})
done

echo "${pip_options[*]}"

python -m pip install -U ${pip_options[@]}
