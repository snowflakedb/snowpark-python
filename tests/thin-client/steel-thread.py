#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import logging

from parameters import CONNECTION_PARAMETERS

from snowflake.snowpark import Session

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)


logger.info("Creating session")

# Add a parameters.py file in this directory which defines a variable
# CONNECTION_PARAMETERS.
# For example, when using a local devvm this parameters should be:
# CONNECTION_PARAMETERS = {
#     "host": "snowflake.dev.local",
#     "port": "8082",
#     "protocol": "http",
#     "account": "SNOWFLAKE",
#     "user": "admin",
#     "password": "test",
#     "warehouse": "TESTWH_SNOWPANDAS",
#     "database": "TESTDB_SNOWPANDAS",
#     "schema": "public",
# }

# SQL setup steps:
# CREATE OR REPLACE WAREHOUSE TESTWH_SNOWPANDAS;
# USE WAREHOUSE TESTWH_SNOWPANDAS;
# CREATE OR REPLACE DATABASE TESTDB_SNOWPANDAS;
# USE DATABASE TESTDB_SNOWPANDAS;
# CREATE OR REPLACE TABLE TEST_TABLE AS SELECT * FROM VALUES (1, 2), (3, 4) AS t(a,b);


session = Session.builder.configs(CONNECTION_PARAMETERS).getOrCreate()

# Create test table with data before invoking DataFrame API by running above SQL queries.

# Simple example, reads data and executes show() server-side when phase1 is enabled
# by setting environment variable SNOWPARK_PHASE_1=true

# TODO: Once column expr PR is in, add filter. This requires to modify above SQL statements to include
# a VARCHAR column as well.

df = session.table("test_table")
df.show()
