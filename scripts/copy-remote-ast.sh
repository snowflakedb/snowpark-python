#!/usr/bin/env bash
#
# This script assumes the target Cloud Workspace specified as the command-line argument has the build target.
# To make sure this is the case, run bazel build //Snowpark/ast:ast_proto && bazel build //Snowpark/unparser && bazel run //Snowpark/unparser.
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
ssh $1 'cd ~/Snowflake/trunk && bazel build //Snowpark/ast:ast_proto && bazel build //Snowpark/unparser && bazel run //Snowpark/unparser'

scp $1:"$REMOTE_HOME/Snowflake/trunk/bazel-bin/Snowpark/ast/ast.proto" $SNOWPARK_ROOT/src/snowflake/snowpark/_internal/proto/ast.proto

mkdir -p $MONOREPO_DIR/bazel-bin/Snowpark/unparser/unparser.runfiles

# The runfiles contain the scala libraries/jars.
scp -r $1:$REMOTE_HOME/Snowflake/trunk/bazel-bin/Snowpark/unparser/unparser.runfiles/ $MONOREPO_DIR/bazel-bin/Snowpark/unparser/unparser.runfiles/

scp $1:$REMOTE_HOME/Snowflake/trunk/bazel-bin/Snowpark/unparser/unparser-lib.jar $MONOREPO_DIR/bazel-bin/Snowpark/unparser/
scp $1:$REMOTE_HOME/Snowflake/trunk/bazel-bin/Snowpark/unparser/unparser.jar $MONOREPO_DIR/bazel-bin/Snowpark/unparser/

pushd $SNOWPARK_ROOT
python -m tox -e protoc
popd
