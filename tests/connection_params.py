#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Resolve the Snowflake connection parameters used by the integration tests.

``tests/parameters.py`` (``CONNECTION_PARAMETERS``) takes precedence. When it does not
exist, fall back to a named connection in ``connections.toml`` (the file used by
snowflake-connector-python and the Snowflake CLI):

* location: ``$SNOWFLAKE_HOME/connections.toml``, defaulting to ``~/.snowflake/connections.toml``
* connection: ``$SNOWFLAKE_TEST_CONNECTION``, else ``$SNOWFLAKE_DEFAULT_CONNECTION_NAME``, else
  ``default_connection_name`` from ``connections.toml``, else ``default``
"""

import importlib
import os
from pathlib import Path
from typing import Any, Dict

try:
    import tomllib as _toml  # Python 3.11+
except ImportError:  # pragma: no cover
    try:
        import tomli as _toml
    except ImportError:
        import tomlkit

        class _toml:  # type: ignore
            @staticmethod
            def load(f):
                return tomlkit.load(f).unwrap()


def _connections_file() -> Path:
    home = os.getenv("SNOWFLAKE_HOME")
    base = Path(home) if home else Path.home() / ".snowflake"
    return base / "connections.toml"


def load_connections_toml_parameters() -> Dict[str, Any]:
    path = _connections_file()
    if not path.is_file():
        raise FileNotFoundError(
            f"Neither tests/parameters.py nor {path} exists. Create one of them, see tests/README.md."
        )
    with open(path, "rb") as f:
        content = _toml.load(f)
    name = (
        os.getenv("SNOWFLAKE_TEST_CONNECTION")
        or os.getenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME")
        or content.get("default_connection_name")
        or "default"
    )
    params = content.get(name)
    if not isinstance(params, dict):
        raise KeyError(
            f"Connection '{name}' not found in {path}. "
            "Set SNOWFLAKE_TEST_CONNECTION to the connection to use for tests."
        )
    return dict(params)


def load_connection_parameters() -> Dict[str, Any]:
    try:
        return importlib.import_module("tests.parameters").CONNECTION_PARAMETERS
    except ModuleNotFoundError as e:
        if e.name != "tests.parameters":
            raise
    return load_connections_toml_parameters()


CONNECTION_PARAMETERS = load_connection_parameters()
