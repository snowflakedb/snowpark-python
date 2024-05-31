#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

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
    import numpy

    installed_numpy = True
except ImportError:
    pandas = MissingPandas()
    installed_numpy = False
