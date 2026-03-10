#!/usr/bin/env bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UD_PYTHON_DIR="${SCRIPT_DIR}/../universal-driver/python"

echo "Building Universal Driver wheel from ${UD_PYTHON_DIR}..."
cd "${UD_PYTHON_DIR}"
uv run --with hatch --with cython --with setuptools --with "virtualenv<21.0.0" hatch build --target wheel
echo "Wheel written to: ${UD_PYTHON_DIR}/dist/"
ls -lh "${UD_PYTHON_DIR}/dist/"/*.whl
