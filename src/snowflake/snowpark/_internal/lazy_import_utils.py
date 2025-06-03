#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import importlib
from typing import Any


# Helper functions for importing modules
def get_installed_pandas() -> Any:
    return importlib.import_module("snowflake.connector.options").installed_pandas


def get_pandas() -> Any:
    return importlib.import_module("snowflake.connector.options").pandas


def get_snowpark_types() -> Any:
    return importlib.import_module("snowflake.snowpark.types")


def get_write_pandas() -> Any:
    module = importlib.import_module("snowflake.connector.pandas_tools")
    return module.write_pandas


def get_pandas_tools() -> Any:
    return importlib.import_module("snowflake.connector.pandas_tools")
