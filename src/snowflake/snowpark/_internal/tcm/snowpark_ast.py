#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# Note: Do not call this file ast.py, because it clashes when using the python
#       interpreter as there are some bugs which require ast to point to the internal ast file.

import base64
import itertools
import sys
import uuid
from typing import Tuple


# TODO: currently unused.
def expr_to_dataframe_expr(expr):
    # Import locally, to enable debugger and pex as they run into
    # troubles resulting in a circular import.
    import snowflake.snowpark._internal.tcm.proto.ast_pb2 as proto

    dfe = proto.SpDataframeExpr()
    variant = expr.WhichOneof("variant")
    getattr(dfe, variant).CopyFrom(getattr(expr, variant))
    return dfe


class AstBatch:
    def __init__(self, session) -> None:
        self._session = session
        self._id_gen = itertools.count(start=1)
        self._init_batch()
        # TODO: extended version from the branch snowpark-ir.

    def assign(self, symbol=None) -> object:
        stmt = self._request.body.add()
        stmt.assign.uid = next(self._id_gen)
        stmt.assign.var_id.bitfield1 = stmt.assign.uid
        stmt.assign.symbol = symbol if isinstance(symbol, str) else ""
        return stmt.assign

    def eval(self, target) -> None:
        stmt = self._request.body.add()
        stmt.eval.uid = next(self._id_gen)
        stmt.eval.var_id.CopyFrom(target.var_id)

    def flush(self) -> Tuple[str, str]:
        """Ties off a batch and starts a new one. Returns the tied-off batch."""
        batch = str(base64.b64encode(self._request.SerializeToString()), "utf-8")
        return (str(self._request_id), batch)

    def _init_batch(self) -> None:
        # Import locally, to enable debugger and pex as they run into
        # troubles resulting in a circular import.
        import snowflake.snowpark._internal.tcm.proto.ast_pb2 as proto

        self._request_id = uuid.uuid4()  # Generate a new unique ID.
        self._request = proto.Request()
        self._request.client_version.major = 42
        self._request.client_version.minor = 0
        (major, minor, micro, releaselevel, serial) = sys.version_info
        self._request.client_language.python.version.major = major
        self._request.client_language.python.version.minor = minor
