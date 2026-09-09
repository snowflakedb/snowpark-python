#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Security regression tests for CWE-494 in ``.github/scripts/install_protoc.sh``.

The installer downloads the ``protoc`` release archive over the network, extracts
it, puts the binary on ``PATH`` and executes it. ``python-publish.yml`` runs it on
``release: published`` with ``contents: write``, ``id-token: write`` and the PyPI
token in scope, and protoc executes during codegen *before* the artifacts are
signed, so a swapped release asset would yield a validly signed poisoned wheel.
TLS authenticates GitHub's endpoint but cannot detect an asset swapped through a
compromised upstream account, so the bytes must be pinned to a digest.

These tests are behavioural: they source the installer and exercise its
verification helper directly, asserting that it accepts bytes matching the
pinned digest and fails closed on anything else. They do not touch the network.
"""

import hashlib
import re
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
INSTALL_SCRIPT = REPO_ROOT / ".github" / "scripts" / "install_protoc.sh"

PLATFORMS = ["linux_x86_64", "osx_x86_64", "win64"]

# A shim ``curl`` that always fails. Sourcing the installer must only expose its
# functions; it must never run the installer, so this shim must never be hit.
# Against the pre-fix script (which called ``install`` at import time) the shim
# makes the download fail fast instead of reaching out to the network.
FAILING_CURL = (
    "#!/usr/bin/env bash\necho 'shim curl: unexpected download' >&2\nexit 1\n"
)


@pytest.fixture(scope="module")
def script_text():
    assert INSTALL_SCRIPT.is_file(), f"missing installer: {INSTALL_SCRIPT}"
    return INSTALL_SCRIPT.read_text()


@pytest.fixture
def run_sourced(tmp_path):
    """Source the installer in bash, run ``snippet``, and return the result."""
    shim = tmp_path / "shim"
    shim.mkdir()
    curl = shim / "curl"
    curl.write_text(FAILING_CURL)
    curl.chmod(0o755)

    home = tmp_path / "home"
    home.mkdir()

    def _run(snippet, env_overrides=None):
        env = {
            "PATH": f"{shim}:/usr/bin:/bin:/usr/sbin:/sbin",
            "HOME": str(home),
            "GITHUB_PATH": str(tmp_path / "github_path"),
        }
        if env_overrides:
            env.update(env_overrides)
        return subprocess.run(
            ["bash", "-c", f'source "{INSTALL_SCRIPT}"\n{snippet}'],
            capture_output=True,
            text=True,
            cwd=tmp_path,
            env=env,
            timeout=120,
        )

    return _run


@pytest.fixture
def payload(tmp_path):
    """A file plus the correct SHA-256 digest of its contents."""
    target = tmp_path / "payload.zip"
    target.write_bytes(b"authentic protoc release archive")
    return target, hashlib.sha256(target.read_bytes()).hexdigest()


def test_sourcing_installer_does_not_download_or_install(run_sourced):
    """Sourcing must expose the helpers without performing the install."""
    result = run_sourced("declare -f verifySha256 > /dev/null")
    assert result.returncode == 0, result.stderr
    assert "unexpected download" not in result.stderr


def test_verify_accepts_matching_digest(run_sourced, payload):
    target, digest = payload
    result = run_sourced(f'verifySha256 "{target}" "{digest}"')
    assert result.returncode == 0, result.stderr
    assert "integrity check passed" in result.stdout


def test_verify_rejects_tampered_bytes(run_sourced, payload):
    """The digest of the authentic bytes must not validate tampered bytes."""
    target, digest = payload
    # Simulate a swapped upstream asset: same name, different bytes.
    target.write_bytes(target.read_bytes() + b"\x00malicious payload")
    result = run_sourced(f'verifySha256 "{target}" "{digest}"')
    assert result.returncode != 0, "tampered bytes were accepted"
    assert "FAILED" in result.stderr


def test_verify_rejects_wrong_expected_digest(run_sourced, payload):
    target, _ = payload
    result = run_sourced(f'verifySha256 "{target}" "{"a" * 64}"')
    assert result.returncode != 0
    assert "FAILED" in result.stderr


def test_verify_rejects_empty_expected_digest(run_sourced, payload):
    """An unpinned platform must fail closed, not skip verification."""
    target, _ = payload
    result = run_sourced(f'verifySha256 "{target}" ""')
    assert result.returncode != 0
    assert "no expected SHA-256 digest" in result.stderr


def test_verify_rejects_missing_file(run_sourced, tmp_path, payload):
    _, digest = payload
    result = run_sourced(f'verifySha256 "{tmp_path / "absent.zip"}" "{digest}"')
    assert result.returncode != 0
    assert "not found" in result.stderr


def test_verify_fails_closed_without_digest_tooling(run_sourced, payload):
    """With no sha256sum/shasum/openssl, verification must abort, not pass."""
    target, digest = payload
    # A shell function shadows the ``command`` builtin, so this hides the digest
    # utilities from computeSha256 without disturbing the rest of PATH.
    hide = (
        "command() { "
        'case "${2:-}" in sha256sum|shasum|openssl) return 1;; esac; '
        'builtin command "$@"; }\n'
    )
    result = run_sourced(f'{hide}verifySha256 "{target}" "{digest}"')
    assert result.returncode != 0
    assert "no SHA-256 utility" in result.stderr


def test_verify_is_used_for_both_archive_and_executable(run_sourced, tmp_path):
    """downloadProtoc must verify the archive before unzip, and the binary too."""
    calls = tmp_path / "calls"
    snippet = (
        # Record verification targets and stub out everything with side effects.
        f'verifySha256() {{ echo "verify:$1" >> "{calls}"; }}\n'
        f'unzip() {{ echo "unzip" >> "{calls}"; }}\n'
        "curl() { : ; }\n"
        "getOSNameAndArch\nbuildProtocZIPName\ndownloadProtoc\n"
    )
    result = run_sourced(snippet)
    assert result.returncode == 0, result.stderr

    steps = calls.read_text().split()
    assert len(steps) == 3, steps
    # Archive verified first, then extracted, then the executable verified.
    assert steps[0].startswith("verify:") and steps[0].endswith(".zip")
    assert steps[1] == "unzip"
    assert steps[2].startswith("verify:") and "bin/protoc" in steps[2]


@pytest.mark.parametrize("platform", PLATFORMS)
def test_digests_pinned_for_every_supported_platform(script_text, platform):
    """Every platform the installer supports must pin a zip and binary digest."""
    for prefix in ("PROTOC_ZIP_SHA256", "PROTOC_BIN_SHA256"):
        match = re.search(rf'^{prefix}_{platform}="([0-9a-f]*)"$', script_text, re.M)
        assert match is not None, f"{prefix}_{platform} is not pinned"
        assert len(match.group(1)) == 64, f"{prefix}_{platform} is not a SHA-256"
