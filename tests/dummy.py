#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# #
# # Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
# #
#
# from typing import *
#
# from snowflake.snowpark.functions import udtf
# from snowflake.snowpark.session import Session
#
# session = Session.builder.config("local_testing", True).getOrCreate()
#
#
# @udtf(output_schema=["number"])
# class sum_udtf:
#     def process(self, a: int, b: int) -> Iterable[Tuple[int]]:
#         yield (a + b,)
#
#
# df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
# df.with_column("total", sum_udtf(df.a, df.b)).sort(df.a).show()
