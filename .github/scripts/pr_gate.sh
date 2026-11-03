#!/usr/bin/env bash
# Logic for the "PR Gate Precheck" and "PR Gate" jobs in precommit.yml.
#
#   pr_gate.sh precheck   decide whether the latest push only touches skippable files; when it is not
#                         (and skipping is enabled) cancel the runs it supersedes
#   pr_gate.sh report     report the state of the required gates of the newest executed run whose
#                         code equals the PR head's code (ignoring skippable files)
#
# "Code state": two commits have the same code state when the diff between them touches only
# skippable files (added/modified). Both modes use this single definition.
#
# Skippable files: .github/pr_gate_skip_files.txt on the PR's base branch (so a PR cannot widen its
# own list), one repo-relative FILE path per line, compared as exact paths: a directory never
# matches. The list file itself is never skippable. No list or no base branch: CHANGELOG.md.
# API error reading the list: nothing is skippable.
#
# A run belongs to a PR when its title starts with "PR #<number>: " (run-name in precommit.yml);
# a commit can be in several PRs, and only this PR's runs are reused or cancelled.
#
# Required env: GH_TOKEN, GITHUB_REPOSITORY, HEAD_SHA (PR head commit).
# precheck env: EVENT_NAME, EVENT_ACTION, BEFORE, SKIP_ENABLED, GITHUB_RUN_ID, PR_NUMBER,
#               HEAD_REF, HEAD_REPO, BASE_REF
# report env:   PR_NUMBER, BASE_REF, GITHUB_RUN_ID, optional PR_GATE_REQUIRED_CHECKS (newline or
#               comma separated; overrides branch protection, needed once "PR Gate" is the only
#               required check), optional PR_GATE_TIMEOUT_MIN (default 90)
set -euo pipefail

MODE="${1:?usage: pr_gate.sh precheck|report}"
repo="${GITHUB_REPOSITORY:?}"
head_sha="${HEAD_SHA:?}"
summary="${GITHUB_STEP_SUMMARY:-/dev/null}"
output="${GITHUB_OUTPUT:-/dev/null}"

SKIP_FILE=".github/pr_gate_skip_files.txt"

# Writes the skippable paths, one per line, to the file $SKIP_LIST.
load_skip_list() {
  local raw
  SKIP_LIST=$(mktemp)
  if [[ -z "${BASE_REF:-}" ]]; then
    echo "CHANGELOG.md" > "$SKIP_LIST"
    return 0
  fi
  if raw=$(gh api -H "Accept: application/vnd.github.raw" \
      "repos/$repo/contents/$SKIP_FILE?ref=$BASE_REF" 2>&1); then
    printf '%s\n' "$raw" | sed 's/\r//; s/^[[:space:]]*//; s/[[:space:]]*$//' \
      | grep -v '^$' | grep -vxF -- "$SKIP_FILE" > "$SKIP_LIST" || true
  elif grep -q "HTTP 404" <<< "$raw"; then
    echo "CHANGELOG.md" > "$SKIP_LIST"
  else
    echo "::warning::could not read $SKIP_FILE from $BASE_REF; nothing is skippable"
    : > "$SKIP_LIST"
  fi
}

# Prints "<compare status> <changed files> <files that are not a plain edit of a skippable file>".
diff_summary() {
  gh api "repos/$repo/compare/$1...$2" | jq -r --rawfile l "$SKIP_LIST" '
    ($l | split("\n") | map(select(length > 0))) as $skip
    | [.status, ((.files // []) | length),
       ((.files // []) | map(select(
          (.filename as $f | $skip | any(. == $f) | not)
          or (.status != "added" and .status != "modified"))) | length)] | @tsv'
}

# 0: same code state as the head, 1: different, 2: API error.
same_code_state() {
  local out st nf nb
  out=$(diff_summary "$1" "$head_sha") || return 2
  IFS=$'\t' read -r st nf nb <<< "$out"
  if [[ ( "$st" == "ahead" || "$st" == "identical" ) && "${nb:-1}" -eq 0 ]]; then
    return 0
  fi
  return 1
}

cancel_superseded() {
  local id
  while read -r id; do
    [[ -n "$id" ]] || continue
    echo "cancelling superseded run $id"
    gh api -X POST "repos/$repo/actions/runs/$id/cancel" > /dev/null || echo "::warning::could not cancel run $id"
  done < <(gh api "repos/$repo/actions/workflows/precommit.yml/runs?event=pull_request&per_page=100" \
    --jq ".workflow_runs[] | select(.head_branch == \"${HEAD_REF:?}\"
      and .head_repository.full_name == \"${HEAD_REPO:?}\"
      and (.display_title | startswith(\"PR #${PR_NUMBER:?}: \"))
      and (.status == \"in_progress\" or .status == \"queued\" or .status == \"waiting\" or .status == \"pending\")
      and .id < ${GITHUB_RUN_ID:?}) | .id" || true)
}

precheck() {
  local skippable_only=false skip=false cancelled=false out st nf nb
  if [[ "${EVENT_NAME:-}" == "pull_request" && "${EVENT_ACTION:-}" == "synchronize" \
        && -n "${BEFORE:-}" && ! "${BEFORE}" =~ ^0+$ && "${GITHUB_RUN_ATTEMPT:-1}" == "1" ]]; then
    # Only a fast-forward push that changes skippable files and nothing else is skippable-only.
    # An empty push, a force-push, a re-run (same payload, must really test) or any API error is
    # treated as code.
    out=$(diff_summary "$BEFORE" "$head_sha" 2>/dev/null || true)
    IFS=$'\t' read -r st nf nb <<< "$out"
    if [[ "$st" == "ahead" && "${nf:-0}" -ge 1 && "${nb:-1}" -eq 0 ]]; then
      skippable_only=true
    fi
  fi
  if [[ "$skippable_only" == "true" && "${SKIP_ENABLED:-}" == "true" ]]; then
    skip=true
  fi
  # A push with code supersedes every older run of this PR; a skippable-only push supersedes none.
  if [[ "$skippable_only" == "false" && "${SKIP_ENABLED:-}" == "true" && "${EVENT_NAME:-}" == "pull_request" ]]; then
    cancel_superseded
    cancelled=true
  fi
  echo "skippable_only=$skippable_only skip_other_gates=$skip"
  {
    echo "skippable_only=$skippable_only"
    echo "skip_other_gates=$skip"
  } >> "$output"
  {
    echo "### PR Gate Precheck"
    echo "- latest push only touches skippable files: \`$skippable_only\`"
    echo "- skippable files (from \`$SKIP_FILE\` on \`${BASE_REF:-<no base>}\`): $(paste -sd, "$SKIP_LIST" | sed 's/,/, /g')"
    echo "- skipping enabled (\`PR_GATE_SKIP_ENABLED\`): \`${SKIP_ENABLED:-unset}\`"
    echo "- skip other gates: \`$skip\`"
    echo "- superseded runs cancelled by this job: \`$cancelled\`"
  } >> "$summary"
}

# Run id of this PR's executed precommit run for a commit. A run whose "Check linting" job was
# skipped (skipped by the precheck) does not count. 0: found (id printed), 1: none, 2: API error.
executed_run_for() {
  local sha="$1" id lint
  if [[ "$sha" == "$head_sha" ]]; then
    id="${GITHUB_RUN_ID:?}"
  else
    id=$(gh api --paginate "repos/$repo/actions/workflows/precommit.yml/runs?head_sha=$sha&event=pull_request&per_page=100" \
      --jq ".workflow_runs[] | select(.display_title | startswith(\"PR #${PR_NUMBER:?}: \")) | [.created_at, .id] | @tsv" \
      | sort | tail -n 1 | cut -f2) || return 2
    [[ -n "$id" ]] || return 1
  fi
  lint=$(gh api --paginate "repos/$repo/actions/runs/$id/jobs?per_page=100" \
    --jq '.jobs[] | select(.name == "Check linting") | .conclusion // "running"') || return 2
  [[ -n "$lint" && "$lint" != "skipped" ]] || return 1
  echo "$id"
}

# Walks first-parent from the PR head. Every commit with the same code state as the head is a
# candidate; the first one with an executed run wins. Fills RUN_ID (empty when none) and RUN_SHA.
# Returns non-zero on any API failure or inconsistent data so the gate fails instead of passing.
resolve_run() {
  local commits sha parent id rc
  RUN_ID=""
  RUN_SHA=""
  commits=$(gh api --paginate "repos/$repo/pulls/${PR_NUMBER:?}/commits?per_page=100" --jq '.[].sha') || return 1
  [[ -n "$commits" ]] || return 1
  # The PR head must be the newest commit in the list, otherwise the list is stale or wrong.
  [[ "$(printf '%s\n' "$commits" | tail -n 1)" == "$head_sha" ]] || return 1
  sha="$head_sha"
  while true; do
    if [[ "$sha" != "$head_sha" ]]; then
      if same_code_state "$sha"; then
        :
      else
        rc=$?
        [[ "$rc" -eq 2 ]] && return 1
        return 0 # different code state: no older commit can vouch for the head
      fi
    fi
    if id=$(executed_run_for "$sha"); then
      RUN_ID="$id"
      RUN_SHA="$sha"
      return 0
    else
      rc=$?
      [[ "$rc" -eq 2 ]] && return 1
    fi
    parent=$(gh api "repos/$repo/commits/$sha" --jq '.parents[0].sha // ""') || return 1
    # Left the PR: nothing older belongs to it.
    if [[ -z "$parent" ]] || ! grep -qx "$parent" <<< "$commits"; then
      return 0
    fi
    sha="$parent"
  done
}

# True when the whole PR only adds/modifies skippable files (nothing to gate).
pr_only_skippable() {
  local files
  files=$(gh api --paginate "repos/$repo/pulls/${PR_NUMBER:?}/files?per_page=100" \
    --jq '.[] | [.filename, .status] | @tsv') || return 1
  [[ -n "$files" ]] || return 1
  while IFS=$'\t' read -r name status; do
    grep -qxF -- "$name" "$SKIP_LIST" && [[ "$status" == "added" || "$status" == "modified" ]] || return 1
  done <<< "$files"
}

# One required check name per line. Excludes this gate's own jobs. Fails on API errors.
required_checks() {
  local raw
  if [[ -n "${PR_GATE_REQUIRED_CHECKS:-}" ]]; then
    raw=$(printf '%s\n' "$PR_GATE_REQUIRED_CHECKS" | tr ',' '\n')
  else
    raw=$(gh api "repos/$repo/branches/${BASE_REF:?}" \
      --jq '.protection.required_status_checks.contexts[]?') || return 1
  fi
  printf '%s\n' "$raw" | sed 's/^ *//;s/ *$//' | grep -v '^$' \
    | grep -vxF -e "PR Gate" -e "PR Gate Precheck" || true
}

# Writes TSV lines "name<TAB>state<TAB>url" for every check of RUN_SHA's run, plus a "missing"
# line for every required check that was not reported. state: pass|fail(...)|pending|missing.
collect() {
  local gates="$1" req="$2" jobs checks statuses name status conclusion url state
  jobs=$(gh api --paginate "repos/$repo/actions/runs/$RUN_ID/jobs?per_page=100" \
    --jq '.jobs[] | [.name, .status, (.conclusion // ""), .html_url] | @tsv') || return 1
  [[ -n "$jobs" ]] || return 1
  checks=$(gh api --paginate "repos/$repo/commits/$RUN_SHA/check-runs?per_page=100" \
    --jq '.check_runs[] | [.name, .status, (.conclusion // ""), (.html_url // "")] | @tsv') || return 1
  statuses=$(gh api "repos/$repo/commits/$RUN_SHA/status" \
    --jq '.statuses[] | [.context, .state, (.target_url // "")] | @tsv') || return 1

  : > "$gates"
  # Jobs of the chosen run win over same-named check-runs; both win over commit statuses.
  while IFS=$'\t' read -r name status conclusion url; do
    [[ "$name" == "PR Gate" || "$name" == "PR Gate Precheck" ]] && continue
    if [[ "$status" != "completed" ]]; then state=pending
    elif [[ "$conclusion" == "success" ]]; then state=pass
    else state="fail($conclusion)"; fi
    printf '%s\t%s\t%s\n' "$name" "$state" "$url" >> "$gates"
  done <<< "$jobs"
  while IFS=$'\t' read -r name status conclusion url; do
    [[ -n "$name" ]] || continue
    cut -f1 "$gates" | grep -qxF -- "$name" && continue
    if [[ "$status" != "completed" ]]; then state=pending
    elif [[ "$conclusion" == "success" || "$conclusion" == "neutral" ]]; then state=pass
    else state="fail($conclusion)"; fi
    printf '%s\t%s\t%s\n' "$name" "$state" "$url" >> "$gates"
  done <<< "$checks"
  while IFS=$'\t' read -r name status url; do
    [[ -n "$name" ]] || continue
    cut -f1 "$gates" | grep -qxF -- "$name" && continue
    case "$status" in
      success) state=pass ;;
      pending) state=pending ;;
      *) state="fail($status)" ;;
    esac
    printf '%s\t%s\t%s\n' "$name" "$state" "$url" >> "$gates"
  done <<< "$statuses"
  while read -r name; do
    cut -f1 "$gates" | grep -qxF -- "$name" && continue
    printf '%s\tmissing\t\n' "$name" >> "$gates"
  done < "$req"
}

fail_report() {
  echo "::error::PR Gate: $1"
  {
    echo "### PR Gate"
    echo "**Result: FAIL** ($1)"
  } >> "$summary"
  return 1
}

report() {
  local gates req timeout_min deadline pending failed name state url blocking
  timeout_min="${PR_GATE_TIMEOUT_MIN:-90}"
  gates=$(mktemp)
  req=$(mktemp)

  required_checks > "$req" || { fail_report "could not read the required checks (GitHub API failure)"; return 1; }
  [[ -s "$req" ]] || { fail_report "the list of required checks is empty; set the PR_GATE_REQUIRED_CHECKS repo variable"; return 1; }

  resolve_run || { fail_report "could not determine the tested commit (GitHub API failure or unexpected commit list)"; return 1; }

  if [[ -z "$RUN_ID" ]]; then
    if pr_only_skippable; then
      {
        echo "### PR Gate"
        echo "This PR only changes skippable files ($SKIP_FILE), so there is no code to gate."
      } | tee -a "$summary"
      echo "tested_sha=" >> "$output"
      return 0
    fi
    fail_report "no executed run covers the current code (every candidate run is missing or was skipped); re-run the workflow"
    return 1
  fi
  echo "tested_sha=$RUN_SHA run_id=$RUN_ID"
  echo "tested_sha=$RUN_SHA" >> "$output"

  deadline=$(( $(date +%s) + timeout_min * 60 ))
  while true; do
    collect "$gates" "$req" || { fail_report "could not read gate state from the GitHub API"; return 1; }
    pending=0
    while IFS=$'\t' read -r name state url; do
      grep -qxF -- "$name" "$req" || continue
      if [[ "$state" == "pending" || "$state" == "missing" ]]; then
        pending=$((pending + 1))
      fi
    done < "$gates"
    if [[ "$pending" -eq 0 || "$(date +%s)" -ge "$deadline" ]]; then
      break
    fi
    echo "$pending required gate(s) still pending at $RUN_SHA, waiting..."
    sleep 60
  done

  failed=0
  {
    echo "### PR Gate"
    echo "Reporting the newest executed run with the same code as the head: \`$RUN_SHA\` (run $RUN_ID)"
    [[ "$RUN_SHA" == "$head_sha" ]] || echo "(head \`$head_sha\` has the same code, ignoring skippable files)"
    echo
    echo "| Gate | Required | State | Link |"
    echo "|---|---|---|---|"
  } >> "$summary"
  while IFS=$'\t' read -r name state url; do
    blocking=no
    if grep -qxF -- "$name" "$req"; then
      blocking=yes
      [[ "$state" == "pass" ]] || failed=$((failed + 1))
    fi
    echo "| $name | $blocking | $state | ${url:+[link]($url)} |" >> "$summary"
  done < "$gates"

  rm -f "$gates" "$req"
  if [[ "$failed" -gt 0 ]]; then
    echo "::error::PR Gate: $failed required gate(s) of $RUN_SHA are not green (see job summary)."
    echo "**Result: FAIL** ($failed required gate(s) not green)" >> "$summary"
    return 1
  fi
  echo "**Result: PASS**" >> "$summary"
}

load_skip_list

case "$MODE" in
  precheck) precheck ;;
  report) report ;;
  *) echo "unknown mode: $MODE" >&2; exit 2 ;;
esac
