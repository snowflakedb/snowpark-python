#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.connector.options import MissingOptionalDependency, MissingPandas


class MissingNumpy(MissingOptionalDependency):
    _dep_name = "numpy"

    @classmethod
    def get_pandas(cls):
        # This only runs when called
        if not hasattr(cls, "_pandas_checked"):
            try:
                import pandas

                cls.pandas = pandas
                cls.installed_pandas = True
            except ImportError:
                cls.pandas = MissingPandas()
                cls.installed_pandas = False
            cls._pandas_checked = True
        return cls.pandas


numpy = MissingNumpy()
