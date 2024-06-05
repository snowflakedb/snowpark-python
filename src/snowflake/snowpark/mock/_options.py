#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.connector.options import MissingPandas

try:
    import pandas

    installed_pandas = True
except ImportError:
    pandas = MissingPandas()
    installed_pandas = False
