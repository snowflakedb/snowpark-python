#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import importlib

from snowflake.connector.options import MissingOptionalDependency, MissingPandas


class MissingNumpy(MissingOptionalDependency):
    """The class is specifically for numpy optional dependency."""

    _dep_name = "numpy"


try:
    numpy = importlib.import_module("numpy")
    installed_numpy = True
except ImportError:
    numpy = MissingNumpy()
    installed_numpy = False


def installed_pandas():
    """Check if pandas is installed."""
    try:
        importlib.import_module("pandas")
        return True
    except ImportError:
        return False


def __getattr__(name):
    if name == "pandas":
        try:
            return importlib.import_module("pandas")
        except ImportError:
            return MissingPandas()

    elif name == "installed_pandas":
        return installed_pandas()

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
