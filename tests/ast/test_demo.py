#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import Session
from snowflake.snowpark._internal.utils import set_transmit_query_to_server
from snowflake.snowpark._internal.utils import set_ast_state, AstFlagSource

CONNECTION_PARAMETERS = {
    "account": "s3testaccount",
    "host": "snowflake.reg.local",
    "user": "snowman",
    "password": "test",
    "role": "sysadmin",
    "warehouse": "regress",
    "database": "testdb",
    "schema": "public",
    "port": "53200",
    "protocol": "http",
}


def test_basic_demo():
    demo_session = (
        Session.builder.configs(CONNECTION_PARAMETERS)
        .config("local_testing", False)
        .getOrCreate()
    )

    print("\n\n")
    # Please make sure ENABLE_DATAFRAME is set to True in the account level
    demo_session.sql("show parameters like 'ENABLE_DATAFRAME' in account").show()
    demo_session.sql("select current_version()").show()
    demo_session.sql("show notebooks;").show()
    print("\nThe notebook engine version is:")
    demo_session.sql("show parameters like 'NOTEBOOK_ENGINE_VERSION'").show()
    print("\n\n")

    AST_ENABLED = True
    set_ast_state(AstFlagSource.TEST, AST_ENABLED)

    # This transmits "SELECT 1;" to the server if False.
    set_transmit_query_to_server(False)

    with demo_session.ast_listener() as al:
        result = demo_session.create_dataframe([1, 2, 3, 4]).collect()
        print(result)

    assert len(al.base64_batches) == 1, "With ast_enabled=True expected 1 batches."


def test_diamonds_demo():
    demo_session = (
        Session.builder.configs(CONNECTION_PARAMETERS)
        .config("local_testing", False)
        .getOrCreate()
    )

    AST_ENABLED = True
    set_ast_state(AstFlagSource.TEST, AST_ENABLED)

    # This transmits "SELECT 1;" to the server
    set_transmit_query_to_server(False)

    with demo_session.ast_listener() as al:
        # Create a table
        print("\n\nCreating the table...")
        print(
            demo_session.sql(
                "create or replace temp table t_diamonds (x int, y int, z int, cut string)"
            ).collect()
        )

        # Insert data into the table
        demo_session.sql("insert into t_diamonds values (1, 2, 3, 'Ideal')").collect()
        demo_session.sql("insert into t_diamonds values (4, 5, 6, 'Premium')").collect()
        demo_session.sql("insert into t_diamonds values (7, 8, 9, 'Good')").collect()
        demo_session.sql(
            "insert into t_diamonds values (10, 11, 12, 'Very Good')"
        ).collect()
        demo_session.sql("insert into t_diamonds values (13, 14, 15, 'Fair')").collect()
        demo_session.sql(
            "insert into t_diamonds values (16, 17, 18, 'Ideal')"
        ).collect()
        demo_session.sql(
            "insert into t_diamonds values (19, 20, 21, 'Premium')"
        ).collect()
        demo_session.sql("insert into t_diamonds values (22, 23, 24, 'Good')").collect()

        # Create a DataFrame from the table
        df = demo_session.table("t_diamonds")

        # Show the DataFrame
        print("\n\nThe diamonds table is:")
        print(df.collect())
        print("\n\n")

        # Perform a simple transformation
        df1 = df.filter(df["x"] > 10)
        df1 = df1.select(df["x"], df["y"], df["z"], df["cut"])
        print("\nThe diamonds table after filtering is:")
        print(df1.collect())
        print("\n\n")

        # Perform a simple aggregation
        print("\nThe diamonds table after aggregation is:")
        df2 = df.group_by(df["cut"]).agg({"x": "sum", "y": "avg", "z": "max"})
        print(df2.collect())
        print("\n\n")

        # Perform a simple join
        print("\nThe diamonds table after joining is:")
        df2 = demo_session.table("t_diamonds")
        df3 = df.join(df2, df["x"] == df2["x"], "inner")
        print(df3.collect())
        print("\n\n")

    assert len(al.base64_batches) == 13
