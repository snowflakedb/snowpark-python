#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Server-side runner for ``StoredProcedure`` targets.

This module is shipped to Snowflake *by value* (see ``cloudpickle.register_pickle_by_value``
below), so it must only depend on the standard library: the Snowpark version available in
the stored procedure runtime can differ from the client's.
"""

import importlib
import json
import os
import runpy
import sys
import tempfile
import zipfile
from typing import Any, Dict, List, Optional

import cloudpickle


def _extract_archive(spec: Dict[str, Any]) -> str:
    import_dir = sys._xoptions.get("snowflake_import_directory")
    if not import_dir:
        raise RuntimeError("snowflake_import_directory is not available")
    archive = os.path.join(import_dir, spec["archive"])
    target = tempfile.mkdtemp(prefix="snowpark_job_")
    if zipfile.is_zipfile(archive):
        with zipfile.ZipFile(archive) as zf:
            zf.extractall(target)
    else:
        # a plain staged file, e.g. a single ``.py`` script
        with open(archive, "rb") as src, open(
            os.path.join(target, os.path.basename(archive)), "wb"
        ) as dst:
            dst.write(src.read())
    return os.path.join(target, spec.get("root", ""))


def _to_jsonable(value: Any) -> Any:
    if value is None:
        return None
    return json.loads(json.dumps(value, default=str))


def run_program(session: Any, spec: Dict[str, Any], args: Optional[List[Any]]) -> Any:
    args = list(args or [])
    kind = spec["kind"]
    if kind == "function":
        return _to_jsonable(spec["fn"](session, *args))

    root = _extract_archive(spec)
    if root not in sys.path:
        sys.path.insert(0, root)
    entry_file = spec["file"]
    entry_func = spec.get("func")
    if entry_func:
        module_name = os.path.splitext(entry_file)[0].replace("/", ".")
        module = importlib.import_module(module_name)
        return _to_jsonable(getattr(module, entry_func)(session, *args))

    old_argv = sys.argv
    sys.argv = [entry_file, *[str(a) for a in args]]
    try:
        globals_ = runpy.run_path(os.path.join(root, entry_file), run_name="__main__")
    finally:
        sys.argv = old_argv
    return _to_jsonable(globals_.get("__return__"))


def make_runner(spec: Dict[str, Any]):
    def runner(session, args):
        return run_program(session, spec, args)

    return runner


cloudpickle.register_pickle_by_value(sys.modules[__name__])
