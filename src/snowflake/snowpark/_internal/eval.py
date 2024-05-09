#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# evaluate AST given as base64 encoded protobuf


def process_ast(base64_encoded_ast: str) -> None:
    # use this as entry point in pex bundle.

    print("Hello world: " + base64_encoded_ast)
    pass


def main():
    """This is the main entry point used by XP for sending over a Snowpark IR AST."""

    # use https://grpc.io/docs/languages/python/basics/ for this?
    # i.e. use this as entry point together with timeout. Then start grpc server, wait for single query, execute and
    # then terminate process.
    # that should work.

    from snowflake.snowpark import Session

    session = Session.builder.getOrCreate()
    print(f'Currently used warehouse is: {session.get_current_warehouse()}')
    process_ast("test")


if __name__ == "__main__":
    main()
