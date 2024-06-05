#!/usr/bin/env bash
# This is a little helper script to build the python protobuf file and copy it over to this directory.
# Run script from within this directory, and provide the Host as the argument.
# Cloud workspace ssh is set up via ~/.local/share/sfcli/ssh_config (or simply provide your workspace ID)
# For this script to succeed with the DevVM, you need to configure the SSH devvm host according to
# https://snowflakecomputing.atlassian.net/wiki/spaces/EN/pages/3019015180/DevVM+Troubleshooting#Symptom:-Need-to-ssh-into-the-certified-DevVM
# I.e., add the following lines to ~/.ssh/config
# Host devvm
#	  HostName sdp-devvm-<ldap user>

set -euxo pipefail

# Step 1: Build the python proto file from scratch via bazel
ssh $1 "bash -c 'cd Snowflake/trunk;bazel build //Snowpark:py_proto'"

# Step 2: Fetch from source tree
REMOTE_HOME=$(ssh $1 "bash -c 'echo \$HOME'")

# Step 3: Copy file from devvm to local machine
scp $1:$REMOTE_HOME/Snowflake/trunk/Snowpark/python/src/snowflake/snowpark/_internal/tcm/proto/ast_pb2.py .
