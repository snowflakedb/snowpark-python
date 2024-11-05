## AST Protobuf
The Snowpark AST protocol is defined in Protobuf format and stored in `ast.proto` here.
To get the AST definitions in Python, we can use `protoc` to compile them into Python files, and they will be stored
under `/generated` directory.

### Protoc
To generate the AST Python files, we can use either of the ways below:
1. `python -m tox -e protoc`
2. `pip install "."`

There will be two files generated so far:
- `ast_pb2.py`: the implementation of AST
- `ast_pb2.pyi`: the Python interface of AST
