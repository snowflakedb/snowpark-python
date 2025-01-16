#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#


def test_import_ast_proto():
    from snowflake.snowpark._internal.proto.generated import ast_pb2 as proto

    assert proto.MAX_VERSION
