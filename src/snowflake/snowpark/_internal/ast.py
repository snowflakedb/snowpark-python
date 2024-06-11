#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import base64
import itertools
import json
import sys
import uuid
from typing import Any, Sequence, Tuple

import snowflake.snowpark._internal.proto.ast_pb2 as proto
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark.exceptions import SnowparkSQLException


# TODO: currently unused.
def expr_to_dataframe_expr(expr):
    dfe = proto.SpDataframeExpr()
    variant = expr.WhichOneof("variant")
    getattr(dfe, variant).CopyFrom(getattr(expr, variant))
    return dfe


def check_response(response):
    # TODO
    pass


def decode_ast_response_from_snowpark(res: dict, session_parameters: Any) -> Any:
    """
    decodes Snowpark REST response to protobuf response
    Args:
        res: dictionary representing a snowpark REST response

    Returns:
        protobuf object
    """

    # check if response resulted in error code, if so decode
    data = res["data"]

    if "errorCode" in data.keys():
        is_internal_error = data["internalError"]
        error_code = data["errorCode"]
        sfqid = data["queryId"]
        error_message = res["message"]

        if is_internal_error:
            error_message = "INTERNAL ERROR: " + error_message

        raise SnowparkSQLException(error_message, error_code=error_code, sfqid=sfqid)

    # data is given as b64 encoded rowset result.
    # perform two step decode:
    # Regular Snowpark python decode, then protobuf decode:

    if data["queryResultFormat"] == "arrow" and "rowsetBase64" in data.keys():

        rowset_b64 = data["rowsetBase64"]
        from snowflake.connector.result_batch import ArrowResultBatch

        total_len: int = data.get("total", 0)
        first_chunk_len = total_len
        from snowflake.connector.arrow_context import ArrowConverterContext

        arrow_context = ArrowConverterContext(session_parameters)

        from snowflake.connector.cursor import ResultMetadataV2

        schema: Sequence[ResultMetadataV2] = [
            ResultMetadataV2.from_column(col) for col in data["rowtype"]
        ]

        if "chunks" in data:
            raise NotImplementedError("decoding chunks not yet supported")

        first_chunk = ArrowResultBatch.from_data(
            rowset_b64,
            first_chunk_len,
            arrow_context,
            True,
            True,  # does not matter
            schema,
            True,  # does not matter
        )

        assert (
            first_chunk.rowcount == 1
        ), "Result should consist of single row holding protobuf response"

        dict_result = first_chunk.to_arrow().to_pydict()

        # should be single key, value pair:
        response_as_json: str = list(dict_result.values())[0][0]

        response_as_dict = json.loads(response_as_json)

        if response_as_dict["status"] != 200:
            raise SnowparkClientExceptionMessages.IR_MESSAGE(
                f"Coprocessor returned status {response_as_dict['status']}"
            )

        # Should be also able to load the json directly into a python dict via json.loads(...),
        # however map here to protobuf to make sure contents align with protobuf message.
        from google.protobuf.json_format import ParseDict

        response = ParseDict(response_as_dict["data"], proto.Response())
        return response
    else:
        raise NotImplementedError("Only inline arrow result decode supported yet.")


class AstBatch:
    def __init__(self, session) -> None:
        self._session = session
        self._id_gen = itertools.count(start=1)
        self._init_batch()

    def assign(self, symbol=None):
        stmt = self._request.body.add()
        # TODO: extended BindingId spec from the branch snowpark-ir.
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
        return (str(self._request_id), batch)

    def _init_batch(self):
        self._request_id = uuid.uuid4()  # Generate a new unique ID.
        self._request = proto.Request()
        self._request.client_version.major = 42
        self._request.client_version.minor = 0
        (major, minor, micro, releaselevel, serial) = sys.version_info
        self._request.client_language.python_language.version.major = major
        self._request.client_language.python_language.version.minor = minor
