#!/usr/bin/env bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UD_PYTHON_DIR="${SCRIPT_DIR}/../universal-driver/python"

echo "Building Universal Driver wheel from ${UD_PYTHON_DIR}..."
cd "${UD_PYTHON_DIR}"
# CMAKE must be set explicitly — cargo subprocesses don't inherit /usr/bin on PATH
# inside uv's isolated environment (Rocky/RHEL). No-op on systems where cmake
# is already on PATH.
CMAKE="${CMAKE:-$(command -v cmake 2>/dev/null)}" \
  uv run --with hatch --with cython --with setuptools hatch build -t wheel
echo "Wheel written to: ${UD_PYTHON_DIR}/dist/"
ls -lh "${UD_PYTHON_DIR}/dist/"/*.whl

# Local-dev workaround: bundle system OpenSSL 3.x libs alongside libsf_core.so
# so the $ORIGIN RPATH can find them under the nix Python environment, where
# LD_LIBRARY_PATH covers only nix-store OpenSSL 1.1.x paths.
# CI runs on Ubuntu where libssl.so.3 is on the default search path — this
# block is a no-op there.
if [[ -f /lib64/libssl.so.3 ]]; then
  WHL=$(ls "${UD_PYTHON_DIR}/dist/"*.whl | tail -1)
  echo "Bundling system libssl.so.3 + libcrypto.so.3 into ${WHL} (_core/)..."
  TMPDIR=$(mktemp -d)
  unzip -q "${WHL}" -d "${TMPDIR}"
  cp /lib64/libssl.so.3    "${TMPDIR}/snowflake/connector/_core/"
  cp /lib64/libcrypto.so.3 "${TMPDIR}/snowflake/connector/_core/"
  # Repack in place
  (cd "${TMPDIR}" && zip -qr "${WHL}" snowflake/connector/_core/libssl.so.3 \
                                       snowflake/connector/_core/libcrypto.so.3)
  rm -rf "${TMPDIR}"
  echo "Done bundling libs."
fi
