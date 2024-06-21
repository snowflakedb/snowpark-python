#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import importlib

from snowflake.connector.options import MissingOptionalDependency, MissingPandas

try:
    import pandas

    installed_pandas = True
except ImportError:
    pandas = MissingPandas()
    installed_pandas = False


class MissingNumpy(MissingOptionalDependency):
    """The class is specifically for numpy optional dependency."""

    _dep_name = "numpy"


try:
    numpy = importlib.import_module("numpy")
    installed_numpy = True
except ImportError:
    numpy = MissingNumpy()
    installed_numpy = False
