#!/usr/bin/env bash
# Build the Universal Driver Python wheel from a local clone.
# Prints the output directory path on stdout so callers can capture it:
#   WHEEL_DIR=$(scripts/build-ud-wheel.sh --no-rust)
set -euo pipefail

UD_REPO="/home/repo/universal-driver"
BRANCH="snowpark-compatibility"
OUT_DIR=""
SKIP_RUST=0

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

  --ud-repo PATH    local UD clone          (default: /home/repo/universal-driver)
  --branch BRANCH   git branch to build     (default: snowpark-compatibility)
                    pass "" to stay on current branch
  --out-dir DIR     destination for .whl    (default: /tmp/ud-wheels-<sha>)
  --no-rust         set SKIP_CORE_BUILD=1 for a Python-only rebuild (fast)
  -h, --help        show this message
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ud-repo)  UD_REPO="$2"; shift 2 ;;
    --branch)   BRANCH="$2";  shift 2 ;;
    --out-dir)  OUT_DIR="$2"; shift 2 ;;
    --no-rust)  SKIP_RUST=1;  shift ;;
    -h|--help)  usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
  esac
done

if [[ ! -d "$UD_REPO" ]]; then
  echo "ERROR: UD repo not found: $UD_REPO" >&2
  echo "  Clone universal-driver or pass --ud-repo <path>" >&2
  exit 1
fi

# Ensure the expected branch is checked out.
# Auto-switch when the working tree is clean; abort if there are local changes.
if [[ -n "$BRANCH" ]]; then
  current_branch=$(git -C "$UD_REPO" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
  if [[ "$current_branch" != "$BRANCH" ]]; then
    dirty=$(git -C "$UD_REPO" status --porcelain 2>/dev/null)
    if [[ -n "$dirty" ]]; then
      echo "ERROR: $UD_REPO is on '$current_branch' and has uncommitted changes." >&2
      echo "  Commit or stash them, then re-run (or pass --branch \"\" to build as-is)." >&2
      exit 1
    fi
    echo "==> Switching $UD_REPO: '$current_branch' → '$BRANCH'" >&2
    git -C "$UD_REPO" checkout "$BRANCH" >&2
  fi
fi

COMMIT=$(git -C "$UD_REPO" rev-parse --short HEAD 2>/dev/null || echo "unknown")
if [[ -z "$OUT_DIR" ]]; then
  OUT_DIR="/tmp/ud-wheels-${COMMIT}"
fi

PYTHON_DIR="$UD_REPO/python"
if [[ ! -d "$PYTHON_DIR" ]]; then
  echo "ERROR: $PYTHON_DIR not found" >&2
  exit 1
fi

# Ensure hatch is available.
if ! command -v hatch &>/dev/null; then
  echo "==> hatch not found — installing via uv" >&2
  if ! command -v uv &>/dev/null; then
    echo "ERROR: uv not found; install it first: https://docs.astral.sh/uv/" >&2
    exit 1
  fi
  uv tool install hatch >&2
fi

echo "==> Building UD wheel (repo: $UD_REPO, commit: $COMMIT, SKIP_CORE_BUILD=$SKIP_RUST)" >&2
cd "$PYTHON_DIR"
# Clean previous build artefacts to avoid stale wheels.
rm -f dist/*.whl

if [[ "$SKIP_RUST" -eq 1 ]]; then
  SKIP_CORE_BUILD=1 hatch build -t wheel >&2
else
  hatch build -t wheel >&2
fi

# Copy wheel(s) to the output directory.
mkdir -p "$OUT_DIR"
cp dist/*.whl "$OUT_DIR/"
echo "==> Wheel(s) copied to $OUT_DIR" >&2
ls "$OUT_DIR"/*.whl >&2

# Print the resolved output dir — callers capture this.
echo "$OUT_DIR"
