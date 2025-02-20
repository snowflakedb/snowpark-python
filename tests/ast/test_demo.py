#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import Session
from snowflake.snowpark._internal.utils import set_transmit_query_to_server
from snowflake.snowpark._internal.utils import set_ast_state, AstFlagSource
from snowflake.snowpark.functions import udaf
from snowflake.snowpark.types import FloatType

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

        # Create a stored procedure.
        print("\n\nCreating the stored procedure...")

        # Example 1: Use stored procedure to copy data from one table to another.
        def my_copy(
            session: Session, from_table: str, to_table: str, count: int
        ) -> str:
            session.table(from_table).limit(count).write.save_as_table(
                to_table, mode="overwrite"
            )
            return "SUCCESS"

        set_transmit_query_to_server(True)
        _ = demo_session.sproc.register(
            my_copy, name="my_copy_sp", replace=True, comment="This is a comment"
        )
        set_transmit_query_to_server(False)

        _ = demo_session.sql(
            "create or replace temp table test_from(test_str varchar) as select randstr(20, random()) from table (generator(rowCount => 100))"
        ).collect()

        # Display test_from table.
        print("\n\nThe test_from table is:")
        print(demo_session.table("test_from").collect())

        # call using sql
        to_df = demo_session.create_dataframe([-1, -2], schema=["a"])
        to_df.write.save_as_table("test_to", mode="overwrite")
        # demo_session.sql("call my_copy_sp('test_from', 'test_to', 10)")
        # print("\n\nThe test_to table should have 10 elements:")
        # print("count: ", demo_session.table("test_to").count())
        # print("\n\nThe test_to table is:")
        # print(demo_session.table("test_to").collect())
        #
        # # call using session.call API
        # _ = demo_session.sql("drop table if exists test_to").collect()
        demo_session.call("my_copy_sp", "test_from", "test_to", 10)
        print("\n\nThe test_to table should have 10 elements:")
        print("count: ", demo_session.table("test_to").count())
        print("\n\nThe test_to table is:")
        print(demo_session.table("test_to").collect())

        # Create a UDF.
        print("\n\nCreating the UDF...")
        from snowflake.snowpark.functions import col, udf
        from snowflake.snowpark.types import IntegerType

        set_transmit_query_to_server(True)
        add_one = udf(
            lambda x: x + 1, return_type=IntegerType(), input_types=[IntegerType()]
        )
        set_transmit_query_to_server(False)
        df = demo_session.create_dataframe([1, 2, 3], schema=["a"])
        result = df.select(add_one(col("a")).as_("ans")).collect()
        print(result)

        # Create a UDAF.
        print("\n\nCreating the UDAF...")
        from snowflake.snowpark.functions import udaf
        from snowflake.snowpark.types import IntegerType

        class PythonSumUDAF:
            def __init__(self) -> None:
                self._sum = 0

            @property
            def aggregate_state(self):
                return self._sum

            def accumulate(self, input_value):
                self._sum += input_value

            def merge(self, other_sum):
                self._sum += other_sum

            def finish(self):
                return self._sum

        sum_udaf = udaf(
            PythonSumUDAF,
            name="sum_int",
            replace=True,
            return_type=IntegerType(),
            input_types=[IntegerType()],
        )

        df = demo_session.create_dataframe([[1, 3], [1, 4], [2, 5], [2, 6]]).to_df(
            "a", "b"
        )
        result = df.agg(sum_udaf("a").alias("sum_a")).collect(block=False)
        print(result)

        # Create a UDTF.
        print("\n\nCreating the UDTF...")
        from snowflake.snowpark.functions import lit, udtf
        from snowflake.snowpark.types import IntegerType, StructField, StructType

        class PrimeSieve:
            def process(self, n):
                is_prime = [True] * (n + 1)
                is_prime[0] = False
                is_prime[1] = False
                p = 2
                while p * p <= n:
                    if is_prime[p]:
                        # set all multiples of p to False
                        for i in range(p * p, n + 1, p):
                            is_prime[i] = False
                    p += 1
                # yield all prime numbers
                for p in range(2, n + 1):
                    if is_prime[p]:
                        yield (p,)

        set_transmit_query_to_server(True)
        prime_udtf = udtf(
            PrimeSieve,
            output_schema=StructType([StructField("number", IntegerType())]),
            input_types=[IntegerType()],
        )
        set_transmit_query_to_server(False)

        result = demo_session.table_function(prime_udtf(lit(20))).collect()
        print(result)

    assert len(al.base64_batches) == 20


def test_demo_udf():
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
        # Create a UDF.
        print("\n\nCreating the UDF...")
        from snowflake.snowpark.functions import col, udf
        from snowflake.snowpark.types import IntegerType

        set_transmit_query_to_server(True)
        add_one = udf(
            lambda x: x + 1, return_type=IntegerType(), input_types=[IntegerType()]
        )
        set_transmit_query_to_server(False)
        df = demo_session.create_dataframe([1, 2, 3], schema=["a"])
        result = df.select(add_one(col("a")).as_("ans")).collect()
        print(result)

    assert len(al.base64_batches) == 1


def test_demo_udaf():
    demo_session = (
        Session.builder.configs(CONNECTION_PARAMETERS)
        .config("local_testing", False)
        .getOrCreate()
    )

    AST_ENABLED = True
    set_ast_state(AstFlagSource.TEST, AST_ENABLED)

    # This transmits "SELECT 1;" to the server
    set_transmit_query_to_server(False)

    # Define the UDAF class
    class AveragePricePerCarat:
        def __init__(self) -> None:
            self.total_price = 0.0
            self.total_carat = 0.0

        @property
        def aggregate_state(self):
            return self.total_price / self.total_carat if self.total_carat > 0 else 0.0

        def accumulate(self, price, carat):
            if carat > 0:
                self.total_price += price
                self.total_carat += carat

        def merge(self, other):
            self.total_price += other.total_price
            self.total_carat += other.total_carat

        def finish(self):
            return self.total_price / self.total_carat if self.total_carat > 0 else 0.0

    # Register the UDAF
    set_transmit_query_to_server(True)
    avg_price_per_carat_udaf = udaf(
        AveragePricePerCarat,
        return_type=FloatType(),
        input_types=[FloatType(), FloatType()],
    )
    df = demo_session.table("t_diamonds")
    df_avg_price_per_carat = df.agg(avg_price_per_carat_udaf(df["price"], df["carat"]))
    print(df_avg_price_per_carat.collect())
