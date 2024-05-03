# Builds the Python client library for the AST protobuf definition.
# TODO: make this a part of setup.py and ensure that protoc is a compile-time dependency.
protoc -I=. --python_out=_internal/proto ast.proto
