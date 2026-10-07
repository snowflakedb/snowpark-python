#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Source-link tests that don't import Sphinx or its configuration."""

import functools
import importlib.util
import inspect
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "_linkcode", REPOSITORY_ROOT / "docs/source/_linkcode.py"
)
LINKCODE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LINKCODE)


def decorated(function):
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        return function(*args, **kwargs)

    return wrapper


class Example:
    @decorated
    def method(self):
        return "example"

    @property
    @decorated
    def value(self):
        return "example"


class LinkcodeTests(unittest.TestCase):
    def resolve(self, name, **kwargs):
        return LINKCODE.resolve_linkcode(
            "py",
            {"module": __name__, "fullname": name},
            "1.55.0",
            kwargs.get("repository_root", REPOSITORY_ROOT),
        )

    def expected(self, obj):
        source, line = inspect.getsourcelines(inspect.unwrap(obj))
        return (
            "https://github.com/snowflakedb/snowpark-python/blob/"
            f"v1.55.0/tests/unit/test_doc_linkcode.py#L{line}-L{line + len(source) - 1}"
        )

    def test_decorated_method(self):
        self.assertEqual(self.resolve("Example.method"), self.expected(Example.method))

    def test_decorated_property(self):
        self.assertEqual(
            self.resolve("Example.value"), self.expected(Example.value.fget)
        )

    def test_independent_of_working_directory(self):
        original = Path.cwd()
        try:
            with tempfile.TemporaryDirectory() as directory:
                for cwd in (
                    REPOSITORY_ROOT,
                    REPOSITORY_ROOT / "docs/source",
                    directory,
                ):
                    with self.subTest(cwd=str(cwd)):
                        os.chdir(cwd)
                        self.assertEqual(
                            self.resolve("Example.method"),
                            self.expected(Example.method),
                        )
        finally:
            os.chdir(original)

    def test_missing_object(self):
        self.assertIsNone(self.resolve("Example.missing"))

    def test_unsupported_domain_and_missing_metadata(self):
        for domain, info in (
            ("js", {"module": __name__, "fullname": "Example"}),
            ("py", {}),
            ("py", {"module": "not_a_loaded_module", "fullname": "Example"}),
        ):
            with self.subTest(domain=domain, info=info):
                self.assertIsNone(
                    LINKCODE.resolve_linkcode(domain, info, "1.55.0", REPOSITORY_ROOT)
                )

    def test_builtin(self):
        self.assertIsNone(
            LINKCODE.resolve_linkcode(
                "py",
                {"module": "builtins", "fullname": "len"},
                "1.55.0",
                REPOSITORY_ROOT,
            )
        )

    def test_source_outside_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertIsNone(self.resolve("Example.method", repository_root=directory))

    def test_missing_source_file(self):
        with patch.object(LINKCODE.inspect, "getsourcefile", return_value=None):
            self.assertIsNone(self.resolve("Example.method"))

    def test_missing_source_lines(self):
        with patch.object(LINKCODE.inspect, "getsourcelines", side_effect=OSError):
            self.assertEqual(
                self.resolve("Example.method"),
                "https://github.com/snowflakedb/snowpark-python/blob/"
                "v1.55.0/tests/unit/test_doc_linkcode.py",
            )


if __name__ == "__main__":
    unittest.main()
