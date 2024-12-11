#!/bin/bash

# This script assumes the target Cloud Workspace specified as the command-line argument has the build target.
# To make sure this is the case, run bazel build //Snowpark/ast:ast_proto and bazel build //Snowpark/unparser.

# N.B. The calling environment further requires:
# export MONOREPO_DIR=$TMPDIR

# To allow this script to run from any subdirectory within snowpark-python, we use git rev-parse.
SNOWPARK_ROOT=$(git rev-parse --show-toplevel)

scp $1:~/Snowflake/trunk/bazel-bin/Snowpark/ast/ast.proto $SNOWPARK_ROOT/src/snowflake/snowpark/_internal/proto/ast.proto

mkdir -p $TMPDIR/bazel-bin/Snowpark/unparser/unparser.runfiles/io_bazel_rules_scala_scala_library/
mkdir -p $TMPDIR/bazel-bin/Snowpark/unparser/unparser.runfiles/maven_future/com/github/scopt/scopt_2.12/4.1.0/
scp $1:~/Snowflake/trunk/bazel-bin/Snowpark/unparser/unparser.runfiles/io_bazel_rules_scala_scala_library/scala-library-2.12.18.jar $TMPDIR/bazel-bin/Snowpark/unparser/unparser.runfiles/io_bazel_rules_scala_scala_library/
scp $1:~/Snowflake/trunk/bazel-bin/Snowpark/unparser/unparser.runfiles/maven_future/com/github/scopt/scopt_2.12/4.1.0/scopt_2.12-4.1.0.jar $TMPDIR/bazel-bin/Snowpark/unparser/unparser.runfiles/maven_future/com/github/scopt/scopt_2.12/4.1.0/
scp $1:~/Snowflake/trunk/bazel-bin/Snowpark/unparser/unparser.jar $TMPDIR

pushd $SNOWPARK_ROOT
python -m tox -e protoc
popd
