#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import Session

session = Session.builder.getOrCreate()

# src/snowflake/snowpark/dataframe.py::snowpark.dataframe.DataFrame.group_by

# from snowflake.snowpark.functions import col, lit, sum as sum_, max as max_
#
# df = session.create_dataframe([(1, 1),(1, 2),(2, 1),(2, 2),(3, 1),(3, 2)], schema=["a", "b"])
#
# df.group_by("a").median("b").collect()

df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
df2 = session.create_dataframe([[1, 2], [5, 6]], schema=["c", "d"])
df1.intersect(df2).show()
