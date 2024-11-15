import json
import logging
import snowflake.snowpark
import snowflake.snowpark._internal.proto.generated.ast_pb2 as ast
import snowflake.snowpark._internal.proto.generated.DataframeProcessorMsg_pb2 as dp_proto
from snowflake.snowpark._internal.dp.sp_ast_decoder import SnowparkAstDecoder
import base64
from google.protobuf import message

logger = logging.getLogger(__name__)


def str2proto(b64_input: str, proto_output: message.Message) -> None:
    decoded = base64.b64decode(b64_input)
    proto_output.ParseFromString(decoded)


def proto2str(proto_input: message.Message) -> str:
    return str(base64.b64encode(proto_input.SerializeToString()), "utf-8")


class DataframeProcessorSession:
    def __init__(self, session: snowflake.snowpark.Session):
        """
        Initializes TCM with optional Snowpark session to connect to.
        Args:
            session: optional, if None automatically retrieves parameters.
        """
        self._session = session
        self._decoder = None

    def get_decoder(self, type):
        if type != dp_proto.SNOWPARK_API:
            raise NotImplementedError
        if self._decoder is None:
            self._decoder = SnowparkAstDecoder(self._session)
        return self._decoder

    def construct_dp_response_from_ast(self, ast_res_proto, df_type, rid):
        uuid = ast_res_proto.body[0].eval_ok.data.sf_query_result.uuid

        return dp_proto.DpResponse(payload=ast_res_proto.SerializeToString(), df_type=df_type,
                                              request_id=rid,
                                              # query_uuid=uuid
                                   )

    def request(self, tcm_req_base64: str) -> str:
        try:
            dp_req_proto = dp_proto.DpRequest()
            str2proto(tcm_req_base64, dp_req_proto)
            rid = dp_req_proto.request_id
            logging.debug(f"request id: {rid}")

            assert dp_req_proto.df_type == dp_proto.SNOWPARK_API

            decoder = self.get_decoder(dp_req_proto.df_type)

            ast_req_proto = ast.Request()
            ast_req_proto.ParseFromString(dp_req_proto.payload)

            ast_res_proto = decoder.request(ast_req_proto)

            tcm_res_proto = self.construct_dp_response_from_ast(ast_res_proto, dp_req_proto.df_type, rid)
            return proto2str(tcm_res_proto)
        except Exception:
            raise



