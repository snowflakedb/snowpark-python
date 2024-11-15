# Test in this file the TCM.
# If run on workspace/devvm with parameters configured, this should internally call a running GS/XP instance.
from snowflake.snowpark._internal.ast_utils import base64_lines_to_textproto
from snowflake.snowpark._internal.dp.session import DataframeProcessorSession, str2proto
import snowflake.snowpark._internal.proto.generated.ast_pb2 as ast_proto
import snowflake.snowpark._internal.proto.generated.DataframeProcessorMsg_pb2 as dp_proto
from google.protobuf.json_format import MessageToDict
import base64
import json

def test_dp_e2e(session):
    dp_session = DataframeProcessorSession(session)
    session.ast_enabled = True
    # Reset the entity ID generator.
    session._ast_batch.reset_id_gen()

    session._ast_batch.flush()  # Clear the AST.
    # Run the test.
    with session.ast_listener() as al:
        print(session.sql("select ln(3)").collect())

    # Retrieve the ASTs corresponding to the test.
    df_ast = al.base64_batches
    # if last_batch:
    #     result.append(last_batch)

    df_ast = "\n".join(df_ast)
    print(df_ast)
    print(f"len:{len(df_ast)}")

    # GS will receive df_ast

    # GS build tcm request
    ast_binary = base64.b64decode(df_ast)
    print(f"bin len: {len(ast_binary)}")

    print(base64_lines_to_textproto(df_ast.strip()))
    #
    #
    # req_proto = proto.Request()
    # req_proto.ParseFromString(req_binary)
    dp_request = dp_proto.DpRequest(payload=ast_binary, df_type=dp_proto.SNOWPARK_API, request_id=1)
    dp_req_str = str(base64.b64encode(dp_request.SerializeToString()), "utf-8")
    print("dp request:")

    print(json.dumps(MessageToDict(dp_request), indent=2))
    print("dp request str:")
    print(dp_req_str)
    print("dp request str end:")
    dp_res_str = dp_session.request(dp_req_str)
    print(dp_res_str)
    dp_res = dp_proto.DpResponse()

    str2proto(dp_res_str, dp_res)

    ans = MessageToDict(dp_res)

    print(json.dumps(ans, indent=2))

    ast_res = ast_proto.Response()
    ast_res.ParseFromString(dp_res.payload)
    ans = MessageToDict(ast_res)

    print(json.dumps(ans, indent=2))

    print(ast_res.body[0].eval_ok.data.sf_query_result.uuid)

