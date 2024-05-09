#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# evaluate AST given as base64 encoded protobuf
import argparse
import base64
import json
import sys


def process_ast(request_id: str, base64_encoded_ast: str) -> None:

    import snowflake.snowpark._internal.tcm.proto.ast_pb2 as proto

    decoded_ast_data = base64.b64decode(base64_encoded_ast)
    request = proto.Request().ParseFromString(decoded_ast_data)  # noqa: F841

    from snowflake.snowpark import Session

    session = Session.builder.getOrCreate()

    # TODO: process AST by translating it back to snowpark python calls.

    # TODO: do not ship result to this process directly, but get result file and then transport through.

    # return result as dict to further encode as json
    response = {"requestId": request_id, "warehouse": session.get_current_warehouse()}
    return response


# Example request Id: 942e040f-208d-4efe-9c08-910c044c56df
# Example b64 encoded AST: CgIIKhIICgYKBAgDEAkaGQoXCAESAggBGg/KCgwiCnRlc3RfdGFibGUaKwopCAISAggCGiHSBh4SB9oBBBICCAEaE6oCEBIOU1RSIExJS0UgJyVlJScaEQoPCAMSAggDGgeqCAQSAggCGggSBggEEgIIAw==
def main():
    """This is the main entry point used by XP for sending over a Snowpark IR AST."""

    # use https://grpc.io/docs/languages/python/basics/ for this?
    # i.e. use this as entry point together with timeout. Then start grpc server, wait for single query, execute and
    # then terminate process.
    # that should work.

    # use this as entry point in pex bundle.
    # in the future, use gRPC to communicate encoded ast, for now use argparse and command-line for a quick test.
    # TODO: shift to gRPC/protobuf
    parser = argparse.ArgumentParser(
        prog="Snowpark Coprocessor",
        description="Evaluates given AST in base64 encoded form.",
        epilog="(c) 2024 Snowflake, experimental feature.",
    )
    parser.add_argument("request_id", nargs=1)
    parser.add_argument("ast", nargs=1)  # positional argument
    args = parser.parse_args()

    response = process_ast(args.request_id[0], args.ast[0])
    sys.stdout.flush()
    sys.stdout.write("--- RESPONSE ---\n")
    sys.stdout.write(json.dumps(response, indent=2) + "\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
