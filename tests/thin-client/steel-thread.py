#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import logging

from snowflake.snowpark import Session

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)


logger.info("Creating session")

# Local devvm connection parameters.
parameters = {
    "host": "snowflake.dev.local",
    "port": "8082",
    "protocol": "http",
    "account": "SNOWFLAKE",
    "user": "admin",
    "password": "test",
    "warehouse": "TESTWH_SNOWPANDAS",
    "database": "TESTDB_SNOWPANDAS",
    "schema": "public",
}

# SQL setup steps:
# CREATE OR REPLACE WAREHOUSE TESTWH_SNOWPANDAS;
# USE WAREHOUSE TESTWH_SNOWPANDAS;
# CREATE OR REPLACE DATABASE TESTDB_SNOWPANDAS;
# USE DATABASE TESTDB_SNOWPANDAS;
# CREATE OR REPLACE TABLE TEST_TABLE AS SELECT * FROM VALUES (1, 2), (3, 4) AS t(a,b);


session = Session.builder.configs(parameters).getOrCreate()

# create test table with data before invoking DataFrame API by running above SQL queries.

# Simple example, reads data and exeutes show() server-side when phase1 is enabled
# by setting environment variable SNOWPARK_PHASE_1=true
df = session.table("test_table")
df.show()
