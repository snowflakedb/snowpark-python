#!/bin/bash
SCRIPT_DIR=$(dirname "$0")
JAR="unparser-assembly-0.1.jar"
java -cp $SCRIPT_DIR/$JAR com.snowflake.snowpark.experimental.unparser.Unparser $@
