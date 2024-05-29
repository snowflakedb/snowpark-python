#!/bin/bash

# Example usage:
# ./run.sh CgIIKhIICgYKBAgDEAgaGQoXCAESAggBGg/SCgwiCnRlc3RfdGFibGUaKwopCAISAggCGiHaBh4SB9oBBBICCAEaE7ICEBIOU1RSIExJS0UgJyVlJScaEQoPCAMSAggDGgeyCAQSAggCGggSBggEEgIIAw== --lang python

SCRIPT_DIR=$(dirname "$0")
JAR="$SCRIPT_DIR/target/scala-2.13/unparser-assembly-0.1.jar"

# Build the assembly if it doesn't exist.
[ ! -f $JAR ] && pushd $SCRIPT_DIR && sbt assembly && popd

java -cp $JAR com.snowflake.snowpark.experimental.unparser.Unparser $@
