#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class Environment:
    """Runtime environment of a job. Each target validates the fields it supports and
    raises ``ValueError`` naming the unsupported ones.

    Args:
        packages: Package specifications such as ``"pandas==2.2.0"``.
        requirements: Path to a ``requirements.txt`` file.
        imports: Extra imports (StoredProcedure only).
        env_vars: Environment variables (CodeBundle only).
        secrets: Mapping of secret name to the fully qualified secret. Code bundles use
            only the FQNs. They support secrets on compute pools only (untested
            there), not on warehouses.
        external_access_integrations: Names of external access integrations.
        artifact_repositories: Names of artifact repositories (a single one for StoredProcedure).
        stage_mounts: Mapping of mount path to stage URL (CodeBundle only).
    """

    packages: Optional[List[str]] = None
    requirements: Optional[str] = None
    imports: Optional[List[str]] = None
    env_vars: Optional[Dict[str, str]] = None
    secrets: Optional[Dict[str, str]] = None
    external_access_integrations: Optional[List[str]] = None
    artifact_repositories: Optional[List[str]] = None
    stage_mounts: Optional[Dict[str, str]] = None

    def set_fields(self) -> List[str]:
        return [name for name, value in self.__dict__.items() if value]

    def check_supported(self, target_name: str, supported: List[str]) -> None:
        for name in self.set_fields():
            if name not in supported:
                raise ValueError(
                    f"Environment.{name} is not supported by the {target_name} target"
                )

    def resolved_packages(self) -> List[str]:
        """``packages`` plus the entries parsed from ``requirements``."""
        out = list(self.packages or [])
        if self.requirements:
            out.extend(parse_requirements_file(self.requirements))
        return out


def parse_requirements_text(text: str, source: str = "requirements") -> List[str]:
    packages = []
    for line in text.splitlines():
        line = line.split(" #", 1)[0].strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("-"):
            raise ValueError(f"Unsupported option in {source}: {line}")
        packages.append(line)
    return packages


def parse_requirements_file(path: str) -> List[str]:
    with open(path, encoding="utf-8") as f:
        return parse_requirements_text(f.read(), f"requirements file {path}")
