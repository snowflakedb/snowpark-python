#!/usr/bin/env bash
# Logic for the "PR Gate Precheck" and "PR Gate" jobs in precommit.yml.
#
#   pr_gate.sh precheck   decide whether the latest push is CHANGELOG.md-only
#   pr_gate.sh report     report the gate state of the latest non-changelog-only commit
#
# Required env: GH_TOKEN, GITHUB_REPOSITORY, HEAD_SHA (PR head commit).
# precheck env: EVENT_NAME, EVENT_ACTION, BEFORE, SKIP_ENABLED
# report env:   PR_NUMBER, GITHUB_RUN_ID, optional PR_GATE_TIMEOUT_MIN (default 90),
#               PR_GATE_EXTERNAL_BLOCKING (space separated, default "snowpark-connect-tests")
set -euo pipefail

MODE="${1:?usage: pr_gate.sh precheck|report}"
repo="${GITHUB_REPOSITORY:?}"
head_sha="${HEAD_SHA:?}"
summary="${GITHUB_STEP_SUMMARY:-/dev/null}"
output="${GITHUB_OUTPUT:-/dev/null}"

# Jobs of the precommit workflow that never count as gates.
NON_GATE_JOBS=("PR Gate" "PR Gate Precheck" "Combine coverage")

# A commit is "changelog-only" when its diff against its parent is exactly CHANGELOG.md.
# Merge commits, empty diffs and API errors are treated as code.
commit_is_changelog_only() {
  local sha="$1" parents files
  parents=$(gh api "repos/$repo/commits/$sha" --jq '.parents | length' 2>/dev/null) || return 1
  [[ "$parents" == "1" ]] || return 1
  files=$(gh api "repos/$repo/commits/$sha" --jq '.files[].filename' 2>/dev/null) || return 1
  [[ "$files" == "CHANGELOG.md" ]]
}

precheck() {
  local changelog_only=false skip=false files=""
  if [[ "${EVENT_NAME:-}" == "pull_request" && "${EVENT_ACTION:-}" == "synchronize" \
        && -n "${BEFORE:-}" && ! "${BEFORE}" =~ ^0+$ ]]; then
    # Only a push that fast-forwards the PR is diffed; force-pushes fall through to "code".
    files=$(gh api "repos/$repo/compare/${BEFORE}...${head_sha}" \
      --jq 'select(.status=="ahead") | .files[].filename' 2>/dev/null || true)
    if [[ "$files" == "CHANGELOG.md" ]]; then
      changelog_only=true
    fi
  fi
  if [[ "$changelog_only" == "true" && "${SKIP_ENABLED:-}" == "true" ]]; then
    skip=true
  fi
  echo "changelog_only=$changelog_only skip_other_gates=$skip"
  {
    echo "changelog_only=$changelog_only"
    echo "skip_other_gates=$skip"
  } >> "$output"
  {
    echo "### PR Gate Precheck"
    echo "- latest push changelog-only: \`$changelog_only\`"
    echo "- skipping enabled (\`PR_GATE_SKIP_ENABLED\`): \`${SKIP_ENABLED:-unset}\`"
    echo "- skip other gates: \`$skip\`"
  } >> "$summary"
}

is_non_gate() {
  local n
  for n in "${NON_GATE_JOBS[@]}"; do
    [[ "$1" == "$n" ]] && return 0
  done
  return 1
}

# Newest PR commit that is not changelog-only; empty when every commit is.
find_tested_sha() {
  local sha
  while read -r sha; do
    if ! commit_is_changelog_only "$sha"; then
      echo "$sha"
      return 0
    fi
  done < <(gh api --paginate "repos/$repo/pulls/${PR_NUMBER:?}/commits?per_page=100" --jq '.[].sha' \
            | awk '{a[NR]=$0} END{for(i=NR;i>=1;i--) print a[i]}')
  return 0
}

# Writes TSV lines "kind<TAB>name<TAB>state<TAB>url" where state is pass|fail|pending|missing.
collect() {
  local tested="$1" run_id="$2" gates="$3" line name status conclusion url state ext
  : > "$gates"

  if [[ -n "$run_id" ]]; then
    while IFS=$'\t' read -r name status conclusion url; do
      is_non_gate "$name" && continue
      if [[ "$status" != "completed" ]]; then state=pending
      elif [[ "$conclusion" == "success" ]]; then state=pass
      else state="fail($conclusion)"; fi
      printf 'precommit\t%s\t%s\t%s\n' "$name" "$state" "$url" >> "$gates"
    done < <(gh api --paginate "repos/$repo/actions/runs/$run_id/jobs?per_page=100" \
              --jq '.jobs[] | [.name, .status, (.conclusion // ""), .html_url] | @tsv')
  else
    printf 'precommit\t%s\t%s\t%s\n' "precommit workflow run" "missing" "" >> "$gates"
  fi

  # External checks and commit statuses (Jenkins, other workflows, ...).
  local ext_all="$gates.ext"
  : > "$ext_all"
  gh api --paginate "repos/$repo/commits/$tested/check-runs?per_page=100" \
    --jq '.check_runs[] | [.name, .status, (.conclusion // ""), (.html_url // "")] | @tsv' \
    | while IFS=$'\t' read -r name status conclusion url; do
        if [[ "$status" != "completed" ]]; then state=pending
        elif [[ "$conclusion" == "success" || "$conclusion" == "neutral" ]]; then state=pass
        else state="fail($conclusion)"; fi
        printf 'external\t%s\t%s\t%s\n' "$name" "$state" "$url"
      done >> "$ext_all"
  gh api "repos/$repo/commits/$tested/status" \
    --jq '.statuses[] | [.context, .state, (.target_url // "")] | @tsv' \
    | while IFS=$'\t' read -r name status url; do
        case "$status" in
          success) state=pass ;;
          pending) state=pending ;;
          *) state="fail($status)" ;;
        esac
        printf 'external\t%s\t%s\t%s\n' "$name" "$state" "$url"
      done >> "$ext_all"
  # Skip external entries that merely repeat a precommit job, then add the rest.
  while IFS=$'\t' read -r line name state url; do
    if ! awk -F'\t' -v n="$name" '$1=="precommit" && $2==n {f=1} END{exit !f}' "$gates"; then
      printf '%s\t%s\t%s\t%s\n' "$line" "$name" "$state" "$url" >> "$gates"
    fi
  done < "$ext_all"
  rm -f "$ext_all"

  # Blocking external gates that never reported are "missing" (treated as pending while waiting).
  for ext in ${PR_GATE_EXTERNAL_BLOCKING:-snowpark-connect-tests}; do
    if ! awk -F'\t' -v n="$ext" '$1=="external" && $2==n {f=1} END{exit !f}' "$gates"; then
      printf 'external\t%s\tmissing\t\n' "$ext" >> "$gates"
    fi
  done
}

is_blocking() { # kind name
  local ext
  [[ "$1" == "precommit" ]] && return 0
  for ext in ${PR_GATE_EXTERNAL_BLOCKING:-snowpark-connect-tests}; do
    [[ "$2" == "$ext" ]] && return 0
  done
  return 1
}

report() {
  local tested run_id gates deadline timeout_min failed pending kind name state url
  timeout_min="${PR_GATE_TIMEOUT_MIN:-90}"
  gates=$(mktemp)
  tested=$(find_tested_sha)

  if [[ -z "$tested" ]]; then
    {
      echo "### PR Gate"
      echo "Every commit in this PR is changelog-only, so there is no code to gate."
    } | tee -a "$summary"
    echo "tested_sha=" >> "$output"
    return 0
  fi

  if [[ "$tested" == "$head_sha" ]]; then
    run_id="${GITHUB_RUN_ID:?}"
  else
    run_id=$(gh api "repos/$repo/actions/workflows/precommit.yml/runs?head_sha=$tested&event=pull_request&per_page=30" \
      --jq '.workflow_runs | sort_by(.created_at) | last | .id // empty')
  fi
  echo "tested_sha=$tested run_id=${run_id:-none}"
  echo "tested_sha=$tested" >> "$output"

  deadline=$(( $(date +%s) + timeout_min * 60 ))
  while true; do
    collect "$tested" "$run_id" "$gates"
    pending=0
    while IFS=$'\t' read -r kind name state url; do
      is_blocking "$kind" "$name" || continue
      if [[ "$state" == "pending" ]] || { [[ "$state" == "missing" ]] && [[ "$kind" == "external" ]]; }; then
        pending=$((pending + 1))
      fi
    done < "$gates"
    if [[ "$pending" -eq 0 || "$(date +%s)" -ge "$deadline" ]]; then
      break
    fi
    echo "$pending blocking gate(s) still pending at $tested, waiting..."
    sleep 60
  done

  failed=0
  {
    echo "### PR Gate"
    echo "Reporting gate state of the latest non-changelog-only commit: \`$tested\`"
    [[ "$tested" == "$head_sha" ]] || echo "(head \`$head_sha\` is changelog-only)"
    echo
    echo "| Gate | Blocking | State | Link |"
    echo "|---|---|---|---|"
  } >> "$summary"
  while IFS=$'\t' read -r kind name state url; do
    local blocking=no
    if is_blocking "$kind" "$name"; then
      blocking=yes
      [[ "$state" == "pass" ]] || failed=$((failed + 1))
    fi
    echo "| $name | $blocking | $state | ${url:+[link]($url)} |" >> "$summary"
  done < "$gates"

  rm -f "$gates"
  if [[ "$failed" -gt 0 ]]; then
    echo "::error::PR Gate: $failed blocking gate(s) of $tested are not green (see job summary)."
    echo "**Result: FAIL** ($failed blocking gate(s) not green)" >> "$summary"
    return 1
  fi
  echo "**Result: PASS**" >> "$summary"
}

case "$MODE" in
  precheck) precheck ;;
  report) report ;;
  *) echo "unknown mode: $MODE" >&2; exit 2 ;;
esac
