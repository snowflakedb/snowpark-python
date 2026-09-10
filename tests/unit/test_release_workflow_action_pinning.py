#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Guards the release workflow against mutable third-party action references.

``.github/workflows/python-publish.yml`` runs in a privileged job (``contents:
write``, ``id-token: write``, and the PyPI API token). Referencing an action by
a mutable tag or branch (for example ``@release/v1``) lets an upstream
maintainer retarget that ref at arbitrary code, which GitHub would then resolve
and execute at release time (CWE-829). Every ``uses:`` in that workflow must
therefore be pinned to a full 40-character commit SHA.
"""

import re
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

WORKFLOW_PATH = (
    Path(__file__).resolve().parents[2] / ".github" / "workflows" / "python-publish.yml"
)

FULL_SHA_REF = re.compile(r"^[^@\s]+@[0-9a-f]{40}$")


def _iter_uses(workflow):
    """Yield every ``uses:`` value in the workflow, with its step name."""
    for job_name, job in (workflow.get("jobs") or {}).items():
        for index, step in enumerate(job.get("steps") or []):
            uses = step.get("uses")
            if uses:
                label = step.get("name") or f"step #{index}"
                yield f"{job_name} / {label}", uses


def test_publish_workflow_actions_are_pinned_to_full_commit_shas():
    workflow = yaml.safe_load(WORKFLOW_PATH.read_text())
    references = list(_iter_uses(workflow))

    # Sanity check: if this workflow stops using any action the assertion below
    # would pass vacuously.
    assert references, f"no `uses:` references found in {WORKFLOW_PATH}"

    unpinned = [
        f"{label}: {uses}" for label, uses in references if not FULL_SHA_REF.match(uses)
    ]
    assert not unpinned, (
        "the following actions in .github/workflows/python-publish.yml are not "
        "pinned to a full 40-character commit SHA: " + ", ".join(unpinned)
    )


def test_publish_workflow_pins_carry_a_version_comment():
    """Each pin should document the human-readable version it corresponds to."""
    unannotated = []
    for line in WORKFLOW_PATH.read_text().splitlines():
        stripped = line.strip()
        if not re.match(r"^-?\s*(?:- )?uses:", stripped):
            continue
        if not re.search(r"@[0-9a-f]{40}\s+#\s*v?\d", stripped):
            unannotated.append(stripped)
    assert (
        not unannotated
    ), "pinned actions should carry a trailing `# vX.Y.Z` comment: " + ", ".join(
        unannotated
    )
