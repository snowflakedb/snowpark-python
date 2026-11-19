#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import fnmatch
import os
from typing import Callable, List, Optional, Tuple

ALWAYS_EXCLUDE = ["__pycache__", ".git", "*.pyc"]


class Program:
    """Describes the Snowpark Python code to run as a job.

    Create a program with one of the factory methods and pass it to
    :meth:`JobManager.submit <snowflake.snowpark.jobs.JobManager.submit>` or
    :meth:`JobManager.deploy <snowflake.snowpark.jobs.JobManager.deploy>`. A bare callable
    is accepted wherever a program is expected and is equivalent to
    :meth:`Program.function`.

    Example::

        >>> from snowflake.snowpark.jobs import Program
        >>> program = Program.function(lambda session, n: session.range(int(n)).count())  # doctest: +SKIP
        >>> job = session.jobs.submit(program, args=[10])  # doctest: +SKIP
    """

    def __init__(
        self,
        kind: str,
        *,
        fn: Optional[Callable] = None,
        path: Optional[str] = None,
        entrypoint: Optional[str] = None,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
    ) -> None:
        self.kind = kind
        self.fn = fn
        self.path = path
        self.entrypoint = entrypoint
        self.include = list(include) if include else None
        self.exclude = list(exclude) if exclude else []

    def __repr__(self) -> str:
        what = self.fn.__name__ if self.fn is not None else self.path
        return f"Program.{self.kind}({what!r}, entrypoint={self.entrypoint!r})"

    @classmethod
    def function(cls, fn: Callable) -> "Program":
        """A Python callable invoked as ``fn(session, *args)``."""
        if not callable(fn):
            raise TypeError("Program.function() requires a callable")
        return cls("function", fn=fn)

    @classmethod
    def file(cls, path: str, entrypoint_func: Optional[str] = None) -> "Program":
        """A local ``.py`` (or ``.ipynb``, code bundles on a compute pool only) file.

        With ``entrypoint_func`` the function is called as ``func(session, *args)``
        (code bundles: with string arguments, and the return value is ignored).
        Otherwise the file runs as ``__main__`` with ``sys.argv = [file, *args]`` and the
        script's global ``__return__``, if set, is the result.
        """
        ext = os.path.splitext(path)[1].lower()
        if ext not in (".py", ".ipynb"):
            raise ValueError(
                f"Program.file() supports .py and .ipynb files, got {path!r}"
            )
        if not os.path.isfile(path):
            raise FileNotFoundError(path)
        if entrypoint_func and ext != ".py":
            raise ValueError("entrypoint_func is only supported for .py files")
        entry = (
            f"{os.path.basename(path)}:{entrypoint_func}" if entrypoint_func else None
        )
        return cls("file", path=os.path.abspath(path), entrypoint=entry)

    @classmethod
    def directory(
        cls,
        path: str,
        entrypoint: Optional[str] = None,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
    ) -> "Program":
        """A local directory.

        ``entrypoint`` is either ``"pkg/main.py:func"`` (function form, called as
        ``func(session, *args)``) or ``"pkg/main.py"`` (script form), relative to ``path``.
        Code bundles support both forms; with the function form the arguments are
        strings and the return value is ignored.
        ``include`` and ``exclude`` are lists of glob patterns matched against relative
        paths; ``__pycache__``, ``.git`` and ``*.pyc`` are always excluded.
        """
        if not os.path.isdir(path):
            raise NotADirectoryError(path)
        return cls(
            "directory",
            path=os.path.abspath(path),
            entrypoint=entrypoint,
            include=include,
            exclude=exclude,
        )

    @classmethod
    def stage(cls, path: str, entrypoint: Optional[str] = None) -> "Program":
        """Code that is already uploaded to an internal stage, e.g. ``"@stage/dir"``."""
        if not path.startswith("@"):
            raise ValueError("Program.stage() requires a stage path starting with '@'")
        return cls("stage", path=path, entrypoint=entrypoint)

    @classmethod
    def workspace(cls, path: str, entrypoint: Optional[str] = None) -> "Program":
        """A workspace location such as ``"snow://workspace/..."`` (code bundles only)."""
        return cls("workspace", path=path, entrypoint=entrypoint)

    @classmethod
    def ref(cls, name: str) -> "Program":
        """An existing procedure or code bundle, identified by a (qualified) name."""
        return cls("ref", path=name)

    # ---- helpers shared by the targets ----

    def split_entrypoint(
        self, override: Optional[str] = None
    ) -> Tuple[Optional[str], Optional[str]]:
        """Returns ``(file, func)`` of the effective entrypoint."""
        ep = override or self.entrypoint
        if not ep:
            return None, None
        if ":" in ep:
            file_, func = ep.rsplit(":", 1)
            return file_, func
        return ep, None

    def iter_files(self) -> List[Tuple[str, str]]:
        """Returns ``[(absolute_path, relative_posix_path)]`` of the files to ship."""
        if self.kind == "file":
            return [(self.path, os.path.basename(self.path))]
        if self.kind != "directory":
            raise ValueError(f"Program.{self.kind}() has no local files")
        found = []
        for dirpath, dirnames, filenames in os.walk(self.path):
            dirnames[:] = sorted(
                d
                for d in dirnames
                if not _matches(
                    os.path.relpath(os.path.join(dirpath, d), self.path),
                    ALWAYS_EXCLUDE + self.exclude,
                )
            )
            for name in sorted(filenames):
                full = os.path.join(dirpath, name)
                rel = os.path.relpath(full, self.path).replace(os.sep, "/")
                if _matches(rel, ALWAYS_EXCLUDE + self.exclude):
                    continue
                if self.include is not None and not _matches(rel, self.include):
                    continue
                found.append((full, rel))
        return found


def _matches(rel_path: str, patterns: List[str]) -> bool:
    rel_path = rel_path.replace(os.sep, "/")
    parts = rel_path.split("/")
    for pattern in patterns:
        if fnmatch.fnmatch(rel_path, pattern) or fnmatch.fnmatch(parts[-1], pattern):
            return True
        # a bare directory name excludes everything below it
        if any(fnmatch.fnmatch(p, pattern) for p in parts[:-1]):
            return True
    return False


def as_program(program) -> Program:
    if isinstance(program, Program):
        return program
    if callable(program):
        return Program.function(program)
    raise TypeError(
        f"program must be a Program or a callable, got {type(program).__name__}"
    )
