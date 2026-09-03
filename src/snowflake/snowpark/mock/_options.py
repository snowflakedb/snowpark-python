#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import importlib

from snowflake.snowpark._internal.options import (
    MissingOptionalDependency,
    _missing_pandas,
)
from snowflake.snowpark._internal.utils import IS_V5_DRIVER

try:
    import pandas

    installed_pandas = True
except ImportError:
    pandas = _missing_pandas()
    installed_pandas = False


if IS_V5_DRIVER:
    from snowflake.connector._common.extras import numpy

    installed_numpy = not isinstance(numpy, MissingOptionalDependency)
else:

    class MissingNumpy(MissingOptionalDependency):
        """The class is specifically for numpy optional dependency."""

        _dep_name = "numpy"

    try:
        numpy = importlib.import_module("numpy")
        installed_numpy = True
    except ImportError:
        numpy = MissingNumpy()
        installed_numpy = False
