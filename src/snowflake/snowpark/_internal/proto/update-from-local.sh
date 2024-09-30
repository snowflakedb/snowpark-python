#!/usr/bin/env bash
# This script is a clone of update-from-devvm. It performs the same actions, except it uses the local clone of the Snowflake repo.
# Example usage:
#   ./src/snowflake/snowpark/_internal/proto/update-from-local.sh ~/src/Snowflake

# Check for args
SRC_ROOT=${SNOWFLAKE_SRC_HOME:-$HOME/src/Snowflake}
if [ -z "$1" ]
  then
    echo "No source directory provided, defaulting to \"$SRC_ROOT\""
elif [ -n "$1" ]
  then
    echo "Using source root \"$1\""
    SRC_ROOT=$1
fi
set -euxo pipefail

SCRIPT_DIR=$(dirname "$0")

# Step 1: Build the python proto file from scratch via bazel
pushd $SRC_ROOT/Snowpark
./make-ast.sh
popd

# Step 2: Copy the output file
cp $SRC_ROOT/Snowpark/proto/ast_pb2.py $SCRIPT_DIR/
cp $SRC_ROOT/Snowpark/proto/scalapb/scalapb_pb2.py $SCRIPT_DIR/

# Step 3: Fix up scalapb file import in ast_pb2.py
awk '{gsub("from scalapb import scalapb_pb2 as scalapb_dot_scalapb__pb2","import snowflake.snowpark._internal.proto.scalapb_pb2 as scalapb_dot_scalapb__pb2")}1' $SCRIPT_DIR/ast_pb2.py > $SCRIPT_DIR/ast_pb2.patched.py \
&& mv $SCRIPT_DIR/ast_pb2.patched.py $SCRIPT_DIR/ast_pb2.py
