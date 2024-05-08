#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# evaluate AST given as base64 encoded protobuf


def process_ast(base64_encoded_ast: str) -> None:
    # use this as entry point in pex bundle.

    print("Hello world: " + base64_encoded_ast)
    pass


def main():
    process_ast("test")


if __name__ == "__main__":
    main()
