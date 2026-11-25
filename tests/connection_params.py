#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import os
from pathlib import Path

import tomlkit


def load_connections_toml_parameters() -> dict:
    """Read a connection from ~/.snowflake/connections.toml.

    The connection name comes from $SNOWPARK_TEST_CONNECTION, defaulting to SNOWPARK_TEST_CONNECTION.
    """
    name = os.getenv("SNOWPARK_TEST_CONNECTION", "SNOWPARK_TEST_CONNECTION")
    path = Path.home() / ".snowflake" / "connections.toml"
    return dict(tomlkit.parse(path.read_text())[name].unwrap())


try:
    from tests.parameters import CONNECTION_PARAMETERS
except ModuleNotFoundError:
    CONNECTION_PARAMETERS = load_connections_toml_parameters()
