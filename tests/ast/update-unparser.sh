#!/usr/bin/env bash
# This helper script builds the Unparser JAR in the devvm or cloud ws and copies it over to ~/.snowflake
# If not pulling from the devvm, please provide the SSH Host as an argument.
# For cloud workspaces this will be your Cloud workspace ID (check ~/.local/share/sfcli/ssh_config)
# For the DevVM, you need to configure the SSH devvm host according to
# https://snowflakecomputing.atlassian.net/wiki/spaces/EN/pages/3019015180/DevVM+Troubleshooting#Symptom:-Need-to-ssh-into-the-certified-DevVM
# I.e., add the following lines to ~/.ssh/config
# Host devvm
#	  HostName sdp-devvm-<ldap user>
# Example usage from the repo root to pull from the devvm
#   ./tests/ast/update-unparser.sh
# Exmaple usage from within this script's directory to pull from a cloud workspace
#   ./update-unparser.sh <workspace_id>

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

# Step 0: Fetch remote home
REMOTE_HOME=$(ssh $HOST "bash -c 'echo \$HOME'")
UNPARSER_DIR="Snowflake/trunk/Snowpark/unparser"
JAR_HOME="$REMOTE_HOME/$UNPARSER_DIR/target/scala-2.13/unparser-assembly-0.1.jar"
LOCAL_TARGET_FILENAME="unparser-assembly-0.1.jar"


# Step 1: Build the Unparser JAR using sbt if it does not exist
ssh $HOST "bash -c 'cd Snowflake/trunk;bazel build //Snowpark/unparser:unparser;cd Snowpark/unparser;sbt assembly'"
# Bazel does not build a far JAR. Therefore above build again using sbt assembly.
# Commented should the path from bazel for the thin JAR, to be more portable we use the sbt assembly far jar.
# JAR_HOME=$(ssh $HOST "bash -c 'cd Snowflake/trunk;bazel cquery --output=files //Snowpark/unparser:unparser.jar'")
#JAR_HOME=$REMOTE_HOME/Snowflake/trunk/$JAR_HOME
echo "Remote JAR_HOME=$JAR_HOME"

# Step 2: Copy file to local
mkdir -p ~/.snowflake

# Remove file if exists.
LOCAL_TARGET_PATH=~/.snowflake/$LOCAL_TARGET_FILENAME
rm -f $LOCAL_TARGET_PATH

scp $HOST:$JAR_HOME $LOCAL_TARGET_PATH

# Step 3: Instruct user to export SNOWPARK_UNPARSER_JAR to be used in test cases
 echo "To use the pulled Snowpark Unparser JAR please set the environment variable"
 echo "export SNOWPARK_UNPARSER_JAR=$LOCAL_TARGET_PATH"
 echo "Or you may also provide it as an argument when running ast tests via --unparser-jar=$LOCAL_TARGET_PATH"
