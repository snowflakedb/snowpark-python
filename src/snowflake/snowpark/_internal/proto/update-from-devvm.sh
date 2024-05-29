#!/usr/bin/env bash
# This is a little helper script to build on the devvm the python protobuf file and copy it over to this directory.
# Run script from within this directory.

set -euxo pipefail

# have devvm as host configured

# Step 1: Built python proto file from scratch via bazel
ssh devvm "bash -c 'cd Snowflake/trunk;bazel build //Snowpark:py_proto'"

# Step 2: Fetch from source tree
REMOTE_HOME=$(ssh devvm "bash -c 'echo \$HOME'")

scp devvm:$REMOTE_HOME/Snowflake/trunk/Snowpark/python/src/snowflake/snowpark/_internal/tcm/proto/ast_pb2.py .
