#!/usr/bin/env bash
# This is a little helper script to build the python protobuf file and copy it over to this directory.
# If not pulling from the devvm, please provide the SSH Host as an argument.
# Cloud workspace ssh is set up via ~/.local/share/sfcli/ssh_config (or simply provide your workspace ID)
# For this script to succeed with the DevVM, you need to configure the SSH devvm host according to
# https://snowflakecomputing.atlassian.net/wiki/spaces/EN/pages/3019015180/DevVM+Troubleshooting#Symptom:-Need-to-ssh-into-the-certified-DevVM
# I.e., add the following lines to ~/.ssh/config
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

# Step 1: Build the python proto file from scratch via bazel
ssh $HOST "bash -c 'cd Snowflake/trunk;bazel build //Snowpark:py_proto'"

# Step 2: Fetch from source tree
REMOTE_HOME=$(ssh $HOST "bash -c 'echo \$HOME'")

# Step 3: Copy file from devvm to local machine
scp $HOST:$REMOTE_HOME/Snowflake/trunk/Snowpark/python/src/snowflake/snowpark/_internal/tcm/proto/ast_pb2.py $SCRIPT_DIR/
