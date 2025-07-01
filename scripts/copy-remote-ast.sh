#!/usr/bin/env bash
# This script assumes the target Cloud Workspace specified as the command-line argument has the build target.
# To make sure this is the case, run bazel build //Snowpark/ast:ast_proto && bazel build //Snowpark/frontend/unparser && bazel run //Snowpark/frontend/unparser.
# The bazel build commands will create the proto and unparser.jar files, whereas bazel run will create the run-files directory.

# N.B. The calling environment further requires:
# export MONOREPO_DIR=$TMPDIR

set -euxo pipefail

if [ "$#" -ne 1 ]; then
  echo "Wrong number of parameters, usage: ./copy-remote-ast.sh <workspace id>"
  exit 1
fi

MONOREPO_DIR=${MONOREPO_DIR:-$TMPDIR}

# To allow this script to run from any subdirectory within snowpark-python, we use git rev-parse.
SNOWPARK_ROOT=$(git rev-parse --show-toplevel)

if [ ! -d "$MONOREPO_DIR" ]; then
  echo "MONOREPO_DIR not defined"
  exit 1
fi

# Quick way to determine what ~ is on the server, made explicit to avoid confusion.
REMOTE_HOME=$(ssh $1 'echo "$HOME"')

# Run bazel build remotely.
# Adding _deploy to a bazel JVM target builds a fat jar,
# For the unparser this target is //Snowpark/frontend/unparser:unparser_deploy.jar.
sf ws ssh $1 --command 'cd ~/Snowflake/trunk && bazel build @sparkle//snowpark/ast:ast_proto && bazel build //Snowpark/frontend/unparser:unparser_deploy.jar'

# (1) Copy over ast.proto file (required by python -x tox -e protoc).
scp $1:"$REMOTE_HOME/Snowflake/trunk/bazel-bin/external/sparkle/snowpark/ast/ast.proto" $SNOWPARK_ROOT/src/snowflake/snowpark/_internal/proto/ast.proto

# (2) Copy over fat unparser_deploy.jar and rename to unparser.jar.
mkdir -p $MONOREPO_DIR/bazel-bin/Snowpark/frontend/unparser/
scp $1:$REMOTE_HOME/Snowflake/trunk/bazel-bin/Snowpark/frontend/unparser/unparser_deploy.jar $MONOREPO_DIR/bazel-bin/Snowpark/frontend/unparser/unparser.jar

pushd $SNOWPARK_ROOT
python -m tox -e protoc
popd
