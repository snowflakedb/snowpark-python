#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from json import JSONEncoder

from ._functions import patch
from ._snowflake_data_type import ColumnEmulator, ColumnType, TableEmulator


class NumpyEncoder(JSONEncoder):
    def default(self, obj):
        import numpy

        if isinstance(obj, numpy.integer):
            return int(obj)
        if isinstance(obj, numpy.floating):
            return float(obj)
        if isinstance(obj, numpy.ndarray):
            return obj.tolist()
        if isinstance(obj, numpy.bool_):
            return bool(obj)

        return super().default(obj)


CUSTOM_JSON_ENCODER = NumpyEncoder
CUSTOM_JSON_DECODER = None


__all__ = [
    "patch",
    "ColumnEmulator",
    "ColumnType",
    "TableEmulator",
]
