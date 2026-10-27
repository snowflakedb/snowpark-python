#!/usr/bin/env bash
# Mirror this job's result from the previous push's run (used for CHANGELOG-only pushes).
# Sets skip=true (copy a success), exits 1 (copy a failure), or sets nothing (run for real).
set -euo pipefail
repo="$GITHUB_REPOSITORY"

# No previous push (opened/reopened PR whose whole diff is CHANGELOG-only): nothing to test.
if [[ -z "${BEFORE:-}" || "$BEFORE" =~ ^0+$ ]]; then
  echo "skip=true" >> "$GITHUB_OUTPUT"
  exit 0
fi

me=$(gh api --paginate "repos/$repo/actions/runs/$GITHUB_RUN_ID/jobs" \
  --jq ".jobs[] | select(.status==\"in_progress\" and .runner_name==\"$RUNNER_NAME\") | .name" | head -n1)
prev_run=$(gh api "repos/$repo/actions/workflows/precommit.yml/runs?head_sha=$BEFORE&event=pull_request&per_page=1" \
  --jq '.workflow_runs[0].id // empty')
result=""
if [[ -n "$me" && -n "$prev_run" ]]; then
  result=$(gh api --paginate "repos/$repo/actions/runs/$prev_run/jobs" \
    --jq ".jobs[] | select(.name==\"$me\") | .conclusion // empty" | head -n1)
fi
echo "job='$me' previous_run='$prev_run' previous_result='$result'"

case "$result" in
  success) echo "skip=true" >> "$GITHUB_OUTPUT" ;;
  failure) echo "::error::Previous run failed this job and this push only changes CHANGELOG.md. Re-run the job to test again."; exit 1 ;;
  *) echo "No usable previous result; running for real." ;;
esac
