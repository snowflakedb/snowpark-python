#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import copy
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
