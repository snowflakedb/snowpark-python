#!/bin/bash

# This script assumes the target Cloud Workspace specified as the command-line argument has the build target.
# To make sure this is the case, run bazel build //Snowpark/ast:ast_proto and bazel build //Snowpark/unparser.

# N.B. The calling environment further requires:
# export MONOREPO_DIR=$TMPDIR

# To allow this script to run from any subdirectory within snowpark-python, we use git rev-parse.
SNOWPARK_ROOT=$(git rev-parse --show-toplevel)

if [ ! -d "$TMPDIR" ]; then
  echo "TMPDIR not defined"
  exit 1
fi

scp $1:~/Snowflake/trunk/bazel-bin/Snowpark/ast/ast.proto $SNOWPARK_ROOT/src/snowflake/snowpark/_internal/proto/ast.proto

mkdir -p $TMPDIR/bazel-bin/Snowpark/unparser/unparser.runfiles
scp -r $1:~/Snowflake/trunk/bazel-bin/Snowpark/unparser/unparser.runfiles/ $TMPDIR/bazel-bin/Snowpark/unparser/unparser.runfiles/

scp $1:~/Snowflake/trunk/bazel-bin/Snowpark/unparser/unparser-lib.jar $TMPDIR/bazel-bin/Snowpark/unparser/
scp $1:~/Snowflake/trunk/bazel-bin/Snowpark/unparser/unparser.jar $TMPDIR/bazel-bin/Snowpark/unparser/

pushd $SNOWPARK_ROOT
python -m tox -e protoc
popd
