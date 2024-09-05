#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
#
#
# from typing import *
#
# from snowflake.snowpark.functions import udtf
# from snowflake.snowpark.session import Session
#
# session = Session.builder.config("local_testing", True).getOrCreate()
#
# from snowflake.snowpark.types import IntegerType, StructField, StructType
# from snowflake.snowpark.functions import udtf, lit
# class GeneratorUDTF:
#     def process(self, n):
#         for i in range(n):
#             yield (i, )
# generator_udtf = udtf(GeneratorUDTF, output_schema=StructType([StructField("number", IntegerType())]), input_types=[IntegerType()])
# session.table_function(generator_udtf(lit(3))).collect()  # Query it by calling it
#
# session.table_function(generator_udtf.name, lit(3)).collect()  # Query it by using the name
# # Or you can lateral-join a UDTF like any other table functions
# df = session.create_dataframe([2, 3], schema=["c"])
# df.join_table_function(generator_udtf(df["c"])).sort("c", "number").show()

#
# @udtf(output_schema=["number"])
# class sum_udtf:
#     def process(self, a: int, b: int) -> Iterable[Tuple[int]]:
#         yield (a + b,)
#
#
# df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
# df.with_column("total", sum_udtf(df.a, df.b)).sort(df.a).show()
