#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark import Session
from parameters import CONNECTION_PARAMETERS


def run_smoke_test():
    """Run smoke test with or without Snowflake connection"""
    # Create session and run test query
    session = Session.builder.configs(CONNECTION_PARAMETERS).create()
    session.sql("select 1 as A").collect()
    session.close()


if __name__ == "__main__":
    run_smoke_test()
