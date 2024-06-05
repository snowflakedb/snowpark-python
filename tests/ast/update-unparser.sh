#!/usr/bin/env bash
# This helper script builds the Unparser JAR in the devvm or cloud ws and copies it over to ~/.snowflake
# Source this script (run with ". ") and provide the SSH Host as the only required argument
# For cloud workspaces this will be your Cloud workspace ID (check ~/.local/share/sfcli/ssh_config)
# For the DevVM, you need to configure the SSH devvm host according to
# https://snowflakecomputing.atlassian.net/wiki/spaces/EN/pages/3019015180/DevVM+Troubleshooting#Symptom:-Need-to-ssh-into-the-certified-DevVM
# I.e., add the following lines to ~/.ssh/config
# Host devvm
#	  HostName sdp-devvm-<ldap user>

set -euxo pipefail

# Step 0: Fetch remote home
REMOTE_HOME=$(ssh $1 "bash -c 'echo \$HOME'")
UNPARSER_DIR="Snowflake/trunk/Snowpark/unparser"
JAR_HOME="$REMOTE_HOME/$UNPARSER_DIR/target/scala-2.13/unparser-assembly-0.1.jar"

# Step 1: Build the Unparser JAR using sbt if it does not exist
ssh $1 "bash -c 'cd Snowflake/trunk/Snowpark/unparser;sbt assembly'"

# Step 2: Copy file to local
mkdir -p ~/.snowflake
scp $1:$JAR_HOME ~/.snowflake

# Step 3: Export SNOWPARK_UNPARSER_JAR to be used in test cases
# export SNOWPARK_UNPARSER_JAR=~/.snowflake/unparser-assembly-0.1.jar
