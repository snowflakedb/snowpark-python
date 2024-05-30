#!/usr/bin/env bash
# This is a little helper script to build on the devvm the python protobuf file and copy it over to this directory.
# Run script from within this directory.
# For this script to succeed, need to configure SSH devvm host according to
# https://snowflakecomputing.atlassian.net/wiki/spaces/EN/pages/3019015180/DevVM+Troubleshooting#Symptom:-Need-to-ssh-into-the-certified-DevVM
# I.e., add the following lines to ~/.ssh/config
# Host devvm
#	  HostName sdp-devvm-<ldap user>

set -euxo pipefail

# Step 1: Built python proto file from scratch via bazel
ssh devvm "bash -c 'cd Snowflake/trunk;bazel build //Snowpark:py_proto'"

# Step 2: Fetch from source tree
REMOTE_HOME=$(ssh devvm "bash -c 'echo \$HOME'")

# Step 3: Copy file from devvm to local machine
scp devvm:$REMOTE_HOME/Snowflake/trunk/Snowpark/python/src/snowflake/snowpark/_internal/tcm/proto/ast_pb2.py .
