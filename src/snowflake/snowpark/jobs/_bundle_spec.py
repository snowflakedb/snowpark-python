#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import copy
import json
import logging
import re
import sys
from typing import Any, Dict, List, Optional

import yaml

from snowflake.snowpark.jobs._environment import Environment

MANIFEST_FILE = "code_bundle.yml"
REQUIREMENTS_FILE = "requirements.txt"
DEFAULT_POOL_RUNTIME_VERSION = "V2.9-CPU-PY3.12"

CODE_BUNDLE_ENV_FIELDS = [
    "packages",
    "requirements",
    "env_vars",
    "secrets",
    "external_access_integrations",
    "artifact_repositories",
    "stage_mounts",
]


def default_warehouse_runtime_version() -> str:
    major, minor = sys.version_info[:2]
    minor = max(10, min(13, minor)) if major == 3 else 12
    return f"3.{minor}"


def _mount_name(mount_path: str, index: int) -> str:
    base = re.sub(r"[^A-Za-z0-9_-]+", "_", mount_path.strip("/")) or "mount"
    return f"{base}_{index}"


def build_spec(
    *,
    compute_pool: Optional[str],
    query_warehouse: Optional[str],
    runtime_version: Optional[str],
    env: Environment,
    has_requirements: bool,
) -> Dict[str, Any]:
    """Builds the content of ``code_bundle.yml`` (a dict with a top-level ``bundle`` key)."""
    bundle: Dict[str, Any] = {"type": "custom", "language": "python"}
    if compute_pool is None:
        bundle["compute_type"] = "warehouse"
        bundle["compute_options"] = {
            "runtime_version": runtime_version or default_warehouse_runtime_version()
        }
    else:
        bundle["compute_type"] = "compute_pool"
        options: Dict[str, Any] = {"compute_pool": compute_pool}
        if query_warehouse:
            options["query_warehouse"] = query_warehouse
        options["runtime_version"] = runtime_version or DEFAULT_POOL_RUNTIME_VERSION
        bundle["compute_options"] = options
    if has_requirements:
        bundle["properties"] = {"requirements_file": REQUIREMENTS_FILE}
    if env.env_vars:
        bundle["env_vars"] = [{k: str(v)} for k, v in env.env_vars.items()]
    if env.secrets:
        bundle["secrets"] = list(env.secrets.values())
    if env.external_access_integrations:
        bundle["external_access_integrations"] = list(env.external_access_integrations)
    if env.artifact_repositories:
        bundle["artifact_repositories"] = list(env.artifact_repositories)
    if env.stage_mounts:
        bundle["stage_mounts"] = {
            _mount_name(path, i): {"stage_url": url, "mount_path": path}
            for i, (path, url) in enumerate(env.stage_mounts.items())
        }
    return {"bundle": bundle}


def dump_spec(spec: Dict[str, Any]) -> str:
    return yaml.safe_dump(spec, sort_keys=False, default_flow_style=False)


def merge_overrides(spec: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    """Deep-merges ``overrides`` into a copy of ``spec``.

    Overrides may be given either under the top-level ``bundle`` key or directly with the
    keys of the ``bundle`` section.
    """
    merged = copy.deepcopy(spec)
    if "bundle" not in overrides:
        overrides = {"bundle": overrides}
    _merge(merged, overrides)
    return merged


def _merge(base: Dict[str, Any], extra: Dict[str, Any]) -> None:
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _merge(base[key], value)
        else:
            base[key] = copy.deepcopy(value)


def requirements_text(packages: List[str]) -> Optional[str]:
    return "\n".join(packages) + "\n" if packages else None


def config_from_spec(spec: Optional[Dict[str, Any]]):
    """Rebuilds ``(CodeBundle, Environment, compute_type, requirements_file)`` from the
    manifest of a deployed code bundle. The packages are not in the manifest: they are in
    the requirements file it points to."""
    from snowflake.snowpark.jobs._targets import CodeBundle
    from snowflake.snowpark.jobs._utils import split_name, unquote_ident

    bundle = (spec or {}).get("bundle") or {}
    opts = bundle.get("compute_options") or {}
    compute_type = bundle.get("compute_type") or "warehouse"
    on_pool = compute_type == "compute_pool"
    target = CodeBundle(
        compute_pool=opts.get("compute_pool") if on_pool else None,
        # the query warehouse; a warehouse bundle runs on the session's warehouse
        warehouse=opts.get("query_warehouse") if on_pool else None,
        runtime_version=opts.get("runtime_version"),
    )
    secrets: Dict[str, str] = {}
    for fqn in bundle.get("secrets") or []:
        name = unquote_ident(split_name(str(fqn))[-1])
        secrets[name if name not in secrets else f"{name}_{len(secrets)}"] = str(fqn)
    env = Environment(
        env_vars={
            k: str(v) for item in bundle.get("env_vars") or [] for k, v in item.items()
        }
        or None,
        secrets=secrets or None,
        external_access_integrations=list(
            bundle.get("external_access_integrations") or []
        )
        or None,
        artifact_repositories=list(bundle.get("artifact_repositories") or []) or None,
        stage_mounts={
            str(m["mount_path"]): str(m["stage_url"])
            for m in (bundle.get("stage_mounts") or {}).values()
        }
        or None,
    )
    requirements_file = (bundle.get("properties") or {}).get("requirements_file")
    return target, env, compute_type, requirements_file


# The deployment metadata is also stored in the bundle's COMMENT, as a fallback for when
# the stored version of the bundle cannot be read back.
COMMENT_PREFIX = "snowpark.jobs:"
# far below what Snowflake accepts for a comment: bigger metadata is not recorded
MAX_COMMENT_CHARS = 4096

_logger = logging.getLogger(__name__)


def metadata_comment(
    spec: Dict[str, Any],
    has_launcher: bool,
    packages: List[str],
    warehouse: Optional[str] = None,
) -> Optional[str]:
    """The text for the COMMENT of a deployed bundle, or ``None`` if it is too big.

    The ``warehouse`` of the target is recorded here because a warehouse bundle's
    manifest has no field for it."""
    data = {
        "spec": spec,
        "has_launcher": bool(has_launcher),
        "packages": list(packages),
    }
    if warehouse:
        data["warehouse"] = warehouse
    text = COMMENT_PREFIX + json.dumps(data, separators=(",", ":"))
    if len(text) > MAX_COMMENT_CHARS:
        _logger.warning(
            "The deployment metadata (%d characters) does not fit into the bundle comment "
            "(limit %d) and is not recorded there.",
            len(text),
            MAX_COMMENT_CHARS,
        )
        return None
    return text


def parse_metadata_comment(comment: Optional[str]) -> Optional[Dict[str, Any]]:
    """Inverse of :func:`metadata_comment`; ``None`` for any other comment."""
    if not comment or not str(comment).startswith(COMMENT_PREFIX):
        return None
    try:
        data = json.loads(str(comment)[len(COMMENT_PREFIX) :])
    except ValueError:
        return None
    if not isinstance(data, dict) or not isinstance(data.get("spec"), dict):
        return None
    return data
