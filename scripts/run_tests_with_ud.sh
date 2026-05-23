#!/usr/bin/env bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SCRIPT_DIR}/.."
UD_DIST="${REPO_ROOT}/universal-driver/python/dist"
PYTHON_VERSION="${PYTHON_VERSION:-3.10}"
TOX_ENV="py${PYTHON_VERSION//./}-notdoctest"

# Build wheel if not present
if ! ls "${UD_DIST}"/snowflake_connector_python_ud-*.whl 2>/dev/null; then
  echo "No UD wheel found — building..."
  bash "${SCRIPT_DIR}/build_ud_connector.sh"
fi

WHEEL=$(ls "${UD_DIST}"/snowflake_connector_python_ud-*.whl | tail -1)
echo "Using UD wheel: ${WHEEL}"

cd "${REPO_ROOT}"

# Local-dev workaround: the UD's arrow_stream_iterator.so was compiled against
# system libstdc++ (GLIBCXX_3.4.29). Under nix Python, pyarrow loads the older
# nix libstdc++ (gcc 9.3.0) first, making $ORIGIN tricks ineffective. Preloading
# the system libstdc++ forces the newer version to win the soname race.
# No-op on Ubuntu CI where nix is absent.
if [[ -f /usr/lib64/libstdc++.so.6 ]]; then
  export LD_PRELOAD=/usr/lib64/libstdc++.so.6
fi

ud_connector_path="${WHEEL}" tox -e "${TOX_ENV}" -- \
  --ignore=tests/integ/modin \
  --ignore=tests/integ/datasource \
  "$@"
