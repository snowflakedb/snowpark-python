#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import hashlib
import os
import tempfile
from dataclasses import dataclass
from typing import Dict, List, Optional

from snowflake.snowpark.jobs._bundle_spec import MANIFEST_FILE, REQUIREMENTS_FILE
from snowflake.snowpark.jobs._program import Program
from snowflake.snowpark.jobs._utils import sql_str

PACKAGE_PREFIX = "snowpark_jobs"


@dataclass
class PackagedBundle:
    stage_path: str  # e.g. @stage/snowpark_jobs/<hash>
    entrypoint: Optional[str]


def normalize_stage(stage: str) -> str:
    stage = stage.strip().rstrip("/")
    return stage if stage.startswith("@") else "@" + stage


def collect_files(program: Program) -> Dict[str, str]:
    """``{relative_path: absolute_path}`` of the program's files, minus generated ones."""
    files = {rel: full for full, rel in program.iter_files()}
    # the generated manifest and requirements always win over the user's own
    files.pop(MANIFEST_FILE, None)
    return files


def content_hash(
    files: Dict[str, str], manifest: str, requirements: Optional[str]
) -> str:
    h = hashlib.sha256()
    generated = {MANIFEST_FILE: manifest.encode()}
    if requirements:
        generated[REQUIREMENTS_FILE] = requirements.encode()
    entries = {rel: None for rel in files if rel not in generated}
    for rel in sorted(list(entries) + list(generated)):
        h.update(rel.encode() + b"\0")
        if rel in generated:
            h.update(generated[rel])
        else:
            with open(files[rel], "rb") as f:
                h.update(f.read())
        h.update(b"\0")
    return h.hexdigest()


def package_and_upload(
    session,
    program: Program,
    manifest: str,
    requirements: Optional[str],
    stage: Optional[str],
    entrypoint: Optional[str],
) -> PackagedBundle:
    """Uploads the program (plus the generated manifest) to a content-addressed stage path."""
    files = collect_files(program)
    if requirements:
        files.pop(REQUIREMENTS_FILE, None)
    if entrypoint and entrypoint not in files:
        raise ValueError(
            f"Entrypoint {entrypoint!r} is not part of the program files "
            f"(found: {sorted(files)[:10]}{'...' if len(files) > 10 else ''})"
        )
    digest = content_hash(files, manifest, requirements)
    base = normalize_stage(stage or session.get_session_stage())
    stage_path = f"{base}/{PACKAGE_PREFIX}/{digest}"

    exists = session.sql(f"LIST {sql_str(stage_path + '/' + MANIFEST_FILE)}").collect()
    if not exists:
        with tempfile.TemporaryDirectory() as tmp:
            generated: List[str] = []
            if requirements:
                req_path = os.path.join(tmp, REQUIREMENTS_FILE)
                with open(req_path, "w", encoding="utf-8") as f:
                    f.write(requirements)
                generated.append(req_path)
            by_dir: Dict[str, List[str]] = {}
            for rel, full in files.items():
                by_dir.setdefault(os.path.dirname(rel), []).append(full)
            for subdir, paths in sorted(by_dir.items()):
                dest = stage_path + (f"/{subdir}" if subdir else "")
                for path in paths:
                    session.file.put(path, dest, auto_compress=False, overwrite=True)
            for path in generated:
                session.file.put(path, stage_path, auto_compress=False, overwrite=True)
            # the manifest goes last: its presence marks a complete upload
            manifest_path = os.path.join(tmp, MANIFEST_FILE)
            with open(manifest_path, "w", encoding="utf-8") as f:
                f.write(manifest)
            session.file.put(
                manifest_path, stage_path, auto_compress=False, overwrite=True
            )
    return PackagedBundle(stage_path=stage_path, entrypoint=entrypoint)
