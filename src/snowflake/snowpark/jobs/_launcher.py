#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Generic launcher for function entry points of code bundles.

``session.jobs`` uploads a verbatim copy of this file to every code bundle it creates
from a local file or directory, as ``__snowpark_jobs_launcher__.py``. A function entry
point ``pkg/daily.py:run`` becomes::

    ENTRYPOINT = '__snowpark_jobs_launcher__.py' ARGUMENTS = ('pkg/daily.py:run', arg, ...)

and this script then calls ``run(session, arg, ...)``. It must stay stdlib-only and free
of import-time side effects: it runs inside the code bundle, not on the client.
"""

import importlib
import os
import re
import sys

_TARGET = re.compile(r"^(?P<path>[^:]+\.py):(?P<func>[A-Za-z_][A-Za-z0-9_]*)$")


def parse_target(target):
    """Splits ``"pkg/daily.py:run"`` into ``("pkg.daily", "run")``."""
    match = _TARGET.match(target or "")
    if not match:
        raise ValueError(
            f"Invalid function entry point {target!r}: expected '<relative path>.py:<function>'"
        )
    path = match.group("path").replace("\\", "/")
    parts = path.split("/")
    if path.startswith("/") or any(p in ("", ".", "..") for p in parts):
        raise ValueError(
            f"Invalid function entry point {target!r}: the path must be relative to the "
            "bundle root and stay inside it"
        )
    return ".".join(parts)[: -len(".py")], match.group("func")


def bundle_root(file, script):
    """The directory of the bundle: where this launcher lives.

    On a compute pool the script runs as a notebook cell, where ``__file__`` is not
    defined. The working directory is the bundle root there, and ``sys.argv[0]`` is the
    script's relative name.
    """
    return os.path.dirname(os.path.abspath(file or script or "."))


def main(argv=None, session=None, root=None):
    """Calls ``func(session, *args)`` for ``argv = [launcher, "<file.py>:<func>", *args]``.

    The return value is ignored, as code bundles cannot return values; exceptions
    propagate so that the run ends as failed with the traceback.
    """
    argv = list(sys.argv if argv is None else argv)
    if len(argv) < 2:
        raise ValueError(
            "Missing function entry point argument ('<file.py>:<function>')"
        )
    module_name, func_name = parse_target(argv[1])
    if root is None:
        root = bundle_root(globals().get("__file__"), argv[0])
    if root not in sys.path:
        sys.path.insert(0, root)
    func = getattr(importlib.import_module(module_name), func_name)
    if session is None:
        from snowflake.snowpark.context import get_active_session

        session = get_active_session()
    func(session, *argv[2:])


if __name__ == "__main__":
    main()
