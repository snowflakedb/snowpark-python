#!/usr/bin/env bash
# This is a little helper script to build the python protobuf file and copy it over to this directory.
# If not pulling from the devvm, please provide the SSH Host as an argument.
# Host devvm
#	  HostName sdp-devvm-<ldap user>
# Example usage from the repo root to pull from the devvm
#   ./src/snowflake/snowpark/_internal/proto/update-from-devvm.sh
# Exmaple usage from within this script's directory to pull from a cloud workspace
#   ./update-from-devvm.sh <workspace_id>


# Check for args
HOST="devvm"
if [ -z "$1" ]
  then
    echo "No SSH Host provided, defaulting to \"devvm\""
elif [ -n "$1" ]
  then
    echo "Using SSH Host \"$1\""
    HOST=$1
fi
set -euxo pipefail

SCRIPT_DIR=$(dirname "$0")

# Note: If changes are not reflected, run `bazel clean --expunge` first.

# Step 1: Build the python proto file from scratch via bazel
ssh $HOST "bash -c 'cd Snowflake/trunk;bazel build //Snowpark:ast && bazel build //Snowpark:py_proto'"

# Step 2: Fetch from source tree
REMOTE_HOME=$(ssh $HOST "bash -c 'echo \$HOME'")

# Step 3: Copy file from devvm to local machine
scp $HOST:$REMOTE_HOME/Snowflake/trunk/Snowpark/proto/ast_pb2.py $SCRIPT_DIR/
scp $HOST:$REMOTE_HOME/Snowflake/trunk/Snowpark/proto/scalapb_pb2.py $SCRIPT_DIR/

# Step 4: Fix up scalapb file import in ast_pb2.py
awk '{gsub("from scalapb import scalapb_pb2 as scalapb_dot_scalapb__pb2","import snowflake.snowpark._internal.proto.scalapb_pb2 as scalapb_dot_scalapb__pb2")}1' $SCRIPT_DIR/ast_pb2.py > $SCRIPT_DIR/ast_pb2.patched.py \
&& mv $SCRIPT_DIR/ast_pb2.patched.py $SCRIPT_DIR/ast_pb2.py
