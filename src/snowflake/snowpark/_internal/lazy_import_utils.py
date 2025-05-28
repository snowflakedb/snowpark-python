#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import importlib
from typing import Any


def lazy_import(module_name: str) -> Any:
    return importlib.import_module(module_name)


# Lazy import helper functions
def get_installed_pandas() -> Any:
    mod = lazy_import("snowflake.connector.options")
    return mod.installed_pandas


def get_pandas() -> Any:
    mod = lazy_import("snowflake.connector.options")
    return mod.pandas


def get_snowpark_types() -> Any:
    return lazy_import("snowflake.snowpark.types")


def get_numpy() -> Any:
    return lazy_import("numpy")


def get_pyarrow() -> Any:
    mod = lazy_import("snowflake.connector.options")
    return mod.pyarrow


def get_write_pandas() -> Any:
    return lazy_import("snowflake.connector.pandas_tools.write_pandas")


def get_pandas_tools() -> Any:
    return lazy_import("snowflake.connector.pandas_tools")
