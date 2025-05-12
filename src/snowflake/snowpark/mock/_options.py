
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.


import importlib

from snowflake.connector.options import MissingOptionalDependency, MissingPandas


class MissingNumpy(MissingOptionalDependency):
    _dep_name = "numpy"

    @classmethod
    def get_pandas(cls):
        # This only runs when called
        if not hasattr(cls, '_pandas_checked'):
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


# class MissingNumpy(MissingOptionalDependency):
#     """The class is specifically for numpy optional dependency."""
#
#     _dep_name = "numpy"
#
#     try:
#         import pandas
#
#         installed_pandas = True
#     except ImportError:
#         pandas = MissingPandas()
#         installed_pandas = False
#
# numpy = MissingNumpy()
# try:
#     numpy = importlib.import_module("numpy")
#     installed_numpy = True
# except ImportError:
#     numpy = MissingNumpy()
#     installed_numpy = False
