#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
# flake8: noqa

import base64
import itertools
import sys
import uuid
from decimal import Decimal
from inspect import signature
from typing import Callable, Tuple

from numpy import datetime64, float64, int32, int64
from pandas import Timestamp
from pandas.core.dtypes.inference import is_list_like

import snowflake.snowpark._internal.proto.ast_pb2 as proto


# TODO: currently unused.
def expr_to_dataframe_expr(expr):
    dfe = proto.SpDataframeExpr()
    variant = expr.WhichOneof("variant")
    getattr(dfe, variant).CopyFrom(getattr(expr, variant))
    return dfe


# Map from python type to its corresponding IR entity. The entities below all have the 'v' attribute.
TYPE_TO_IR_TYPE_NAME = {
    bytes: "binary_val",
    bool: "bool_val",
    datetime64: "date_val",
    Decimal: "big_decimal_val",
    float64: "float_64_val",
    int32: "int_32_val",
    int64: "int_64_val",
    str: "string_val",
    Timestamp: "timestamp_val",
}


def ast_expr_from_python_val(expr, val):
    """
    Converts a Python value to an IR expression.
    This IR expression is set to an attribute of `expr`.

    Parameters
    ----------
    expr : IR entity protobuf builder
    val : Python value that needs to be converted to IR expression.
    """
    if val is None:
        expr.none_val = val
    val_type = type(val)
    if val_type not in TYPE_TO_IR_TYPE_NAME:
        # Modin is imported here to prevent circular import issues.
        from snowflake.snowpark.modin.pandas import DataFrame, Series

        if isinstance(val, Callable):
            expr.fn_val.params = signature(val).parameters
            expr.fn_val.body = val
        if isinstance(val, slice):
            expr.slice_val.start = val.start
            expr.slice_val.stop = val.stop
            expr.slice_val.step = val.step
        elif not isinstance(val, Series) and is_list_like(val):
            # Checking that val is not a Series since Series objects are considered list-like.
            expr.list_val.vs = val
        elif isinstance(val, Series):
            expr.series_val.ref = val
        elif isinstance(val, DataFrame):
            expr.dataframe_val.ref = val
    else:
        ir_type_name = TYPE_TO_IR_TYPE_NAME[val_type]
        setattr(getattr(expr, ir_type_name), "v", val)


class AstBatch:
    def __init__(self, session):
        self._session = session
        self._id_gen = itertools.count(start=1)
        self._init_batch()
        # TODO: extended version from the branch snowpark-ir.

    def assign(self, symbol=None):
        stmt = self._request.body.add()
        stmt.assign.uid = next(self._id_gen)
        stmt.assign.var_id.bitfield1 = stmt.assign.uid
        stmt.assign.symbol = symbol if isinstance(symbol, str) else ""
        return stmt.assign

    def eval(self, target):
        stmt = self._request.body.add()
        stmt.eval.uid = next(self._id_gen)
        stmt.eval.var_id.CopyFrom(target.var_id)

    def flush(self) -> Tuple[str, str]:
        """Ties off a batch and starts a new one. Returns the tied-off batch."""
        batch = str(base64.b64encode(self._request.SerializeToString()), "utf-8")
        self._init_batch()

        print(f"encoded {batch}")
        d1 = base64.b64decode(batch)
        print(f"{len(d1)} bytes")
        p = proto.Request()
        p.ParseFromString(d1)
        print(f"parsed {p}")

        return (str(self._request_id), batch)

    def _init_batch(self):
        self._request_id = uuid.uuid4()  # Generate a new unique ID.
        self._request = proto.Request()
        self._request.client_version.major = 42
        self._request.client_version.minor = 0
        (major, minor, micro, releaselevel, serial) = sys.version_info
        self._request.client_language.python_language.version.major = major
        self._request.client_language.python_language.version.minor = minor
