#!/bin/bash
java -cp $1 com.snowflake.snowpark.experimental.unparser.Unparser ${@:2}
