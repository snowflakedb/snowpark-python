#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from pathlib import Path

import tomlkit


def load_connections_toml_parameters() -> dict:
    """Read the [SNOWPARK_TEST_CONNECTION] section of ~/.snowflake/connections.toml."""
    path = Path.home() / ".snowflake" / "connections.toml"
    return dict(tomlkit.parse(path.read_text())["SNOWPARK_TEST_CONNECTION"].unwrap())


try:
    from tests.parameters import CONNECTION_PARAMETERS
except ModuleNotFoundError:
    CONNECTION_PARAMETERS = load_connections_toml_parameters()
