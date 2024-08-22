#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

#
# # Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
#
# #
# # Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
# #
#
# from snowflake.snowpark import Session
#
# session = Session.builder.getOrCreate()
#
# from snowflake.snowpark.functions import *
# from snowflake.snowpark.functions import seq1, seq8, uniform
#
# df = session.sql("select 1, 'a'")
# df.filter(in_([col("col1"), col("col2")], df)).show()
#
# # # df = session.generator(seq1(1).as_("sequence one"), uniform(1, 10, 2).as_("uniform"), rowcount=3)
# # # df.show()
# #
# # df = session.create_dataframe(
# #     [
# #         [1, [1, 2, 3], {"Ashi Garami": "Single Leg X"}, "Kimura"],
# #         [2, [11, 22], {"Sankaku": "Triangle"}, "Coffee"],
# #     ],
# #     schema=["idx", "lists", "maps", "strs"],
# # )
# # df.select(df.idx, explode(df.lists)).sort(col("idx")).show()
