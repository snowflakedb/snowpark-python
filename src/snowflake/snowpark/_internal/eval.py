#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# evaluate AST given as base64 encoded protobuf
import argparse
import base64
import json
import sys


def process_ast(base64_encoded_ast: str) -> None:
    ast = base64.b64decode(base64_encoded_ast)

    print("Hello world: " + base64_encoded_ast)

    from snowflake.snowpark import Session

    session = Session.builder.getOrCreate()
    print(f"Currently used warehouse is: {session.get_current_warehouse()}")

    # return result as dict to further encode as json
    response = {"warehouse": session.get_current_warehouse()}
    return response


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
    parser.add_argument("ast", nargs=1)  # positional argument
    args = parser.parse_args()

    response = process_ast(args.ast)
    sys.stdout.flush()
    sys.stdout("--- RESPONSE ---")
    sys.stdout.write(json.dumps(response, indent=2))
    sys.stdout.flush()


if __name__ == "__main__":
    main()
