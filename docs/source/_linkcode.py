"""Resolve API source links without importing Sphinx or the Snowpark runtime."""

import inspect
import sys
from pathlib import Path


def resolve_linkcode(domain, info, release, repository_root):
    if domain != "py" or not info.get("module") or not info.get("fullname"):
        return None

    obj = sys.modules.get(info["module"])
    if obj is None:
        return None

    try:
        for part in info["fullname"].split("."):
            obj = getattr(obj, part)
        if isinstance(obj, property):
            obj = obj.fget
        obj = inspect.unwrap(obj)
        filename = inspect.getsourcefile(obj)
        if filename is None:
            return None
        # External dependencies have no source in the Snowpark repository.
        source_path = (
            Path(filename).resolve().relative_to(Path(repository_root).resolve())
        )
    except (AttributeError, OSError, TypeError, ValueError):
        return None

    try:
        source, first_line = inspect.getsourcelines(obj)
        linespec = f"#L{first_line}-L{first_line + len(source) - 1}"
    except (OSError, TypeError):
        linespec = ""

    return (
        "https://github.com/snowflakedb/snowpark-python/blob/"
        f"v{release}/{source_path.as_posix()}{linespec}"
    )
