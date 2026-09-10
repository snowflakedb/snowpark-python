#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Security regression tests for .github/workflows/cla_bot.yml.

The CLA workflow runs on ``pull_request_target``, which executes in the base
repository's privileged security context and exposes both ``GITHUB_TOKEN`` and
the long-lived ``CLA_BOT_TOKEN`` PAT to every step. Two properties therefore
have to hold:

1. Every third-party action is pinned to an immutable full commit SHA. A mutable
   ref (``@master``, ``@v2``, a branch name) means an upstream compromise
   silently gains those tokens on the next ordinary pull request.
2. The job does not request ``actions: write``, which would permit deletion of
   workflow run records (anti-forensics) on top of the token theft.
"""

import re
from pathlib import Path

import pytest

import yaml

WORKFLOW_PATH = (
    Path(__file__).parent.parent.parent / ".github" / "workflows" / "cla_bot.yml"
)

# A full 40-character git object name, the only immutable way to reference an action.
FULL_SHA = re.compile(r"^[0-9a-f]{40}$")


def _load_workflow(path: Path = WORKFLOW_PATH) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def _iter_steps(workflow: dict):
    for job_name, job in (workflow.get("jobs") or {}).items():
        for step in job.get("steps") or []:
            yield job_name, step


def _iter_uses(workflow: dict):
    """Yield (job_name, uses) for every step that references an action."""
    for job_name, step in _iter_steps(workflow):
        uses = step.get("uses")
        if uses:
            yield job_name, uses


def collect_unpinned_actions(workflow: dict) -> list:
    """Return every ``uses:`` value that is not pinned to a full commit SHA.

    Local (``./path``) and container (``docker://``) references have no mutable
    upstream ref and are ignored.
    """
    unpinned = []
    for job_name, uses in _iter_uses(workflow):
        if uses.startswith("./") or uses.startswith("docker://"):
            continue
        ref = uses.rpartition("@")[2] if "@" in uses else ""
        if not FULL_SHA.match(ref):
            unpinned.append((job_name, uses))
    return unpinned


def collect_actions_write_jobs(workflow: dict) -> list:
    """Return jobs (and the workflow root) granting ``actions: write``."""
    offenders = []
    scopes = [("<workflow>", workflow.get("permissions"))]
    for job_name, job in (workflow.get("jobs") or {}).items():
        scopes.append((job_name, job.get("permissions")))
    for name, permissions in scopes:
        # A bare `permissions: write-all` grants actions: write implicitly.
        if permissions == "write-all":
            offenders.append(name)
        elif isinstance(permissions, dict) and permissions.get("actions") == "write":
            offenders.append(name)
    return offenders


def test_workflow_is_valid_yaml_and_has_steps():
    workflow = _load_workflow()
    assert workflow.get("jobs"), "cla_bot.yml defines no jobs"
    assert list(_iter_uses(workflow)), "expected at least one `uses:` step"


def test_all_actions_pinned_to_full_commit_sha():
    unpinned = collect_unpinned_actions(_load_workflow())
    assert not unpinned, (
        "cla_bot.yml runs in a privileged pull_request_target context; every "
        "action must be pinned to a full 40-character commit SHA (with a "
        f"trailing `# vX.Y.Z` comment). Unpinned: {unpinned}"
    )


def test_pinned_actions_carry_a_version_comment():
    """A bare SHA is unreviewable; require the `# vX.Y.Z` annotation."""
    text = WORKFLOW_PATH.read_text()
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("uses:") or stripped.startswith("- uses:"):
            assert (
                "#" in stripped
            ), f"`uses:` line lacks a version comment: {stripped!r}"


def test_actions_write_not_granted():
    offenders = collect_actions_write_jobs(_load_workflow())
    assert not offenders, (
        "`actions: write` allows deletion of workflow run records and must not "
        f"be granted in cla_bot.yml. Granted by: {offenders}"
    )


def test_privileged_trigger_still_guarded_by_permissions_block():
    """Any job reachable from pull_request_target must scope its token down."""
    workflow = _load_workflow()
    # `on` is parsed as the boolean True by PyYAML unless quoted in the file.
    triggers = workflow.get("on", workflow.get(True)) or {}
    if "pull_request_target" not in triggers:
        pytest.skip("cla_bot.yml no longer uses pull_request_target")
    for job_name, job in workflow["jobs"].items():
        assert (
            workflow.get("permissions") is not None
            or job.get("permissions") is not None
        ), f"job {job_name} inherits default token permissions"
