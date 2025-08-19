#!/bin/bash

# Compute the source directory. We honor the MONOREPO_DIR environment variable if set,
# and default to the standard Cloud Workspace location otherwise.
DEFAULT_SRC_DIR=$HOME/Snowflake/trunk
MONOREPO_DIR="${MONOREPO_DIR:-$DEFAULT_SRC_DIR}"
if [ ! -d "$MONOREPO_DIR" ]; then
  echo "Source directory '${MONOREPO_DIR}' doesn't exist"
  exit 1
fi

# To allow this script to run from any subdirectory within snowpark-python, we use git rev-parse.
SNOWPARK_ROOT=$(git rev-parse --show-toplevel)

# Build the targets.
pushd $MONOREPO_DIR
bazel build @sparkle//snowpark/ast:ast_proto
bazel build //Snowpark/frontend/unparser
popd

# Copy the AST.
cp $MONOREPO_DIR/bazel-bin/external/sparkle/snowpark/ast/ast.proto $SNOWPARK_ROOT/src/snowflake/snowpark/_internal/proto/ast.proto

# Generate Python from ast.proto.
pushd $SNOWPARK_ROOT
python -m tox -e protoc
popd
