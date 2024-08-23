#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import Session
from snowflake.snowpark.functions import to_timestamp

session = Session.builder.getOrCreate()


# src/snowflake/snowpark/dataframe.py::snowpark.dataframe.DataFrame.group_by

# from snowflake.snowpark.functions import col, lit, sum as sum_, max as max_
#
# df = session.create_dataframe([(1, 1),(1, 2),(2, 1),(2, 2),(3, 1),(3, 2)], schema=["a", "b"])
#
# df.group_by("a").median("b").collect()

# df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
# df2 = session.create_dataframe([[1, 2], [5, 6]], schema=["c", "d"])
# df1.intersect(df2).show()

sample_data = [
    ["2023-01-01", 101, 200],
    ["2023-01-02", 101, 100],
    ["2023-01-03", 101, 300],
    ["2023-01-04", 102, 250],
]
df = session.create_dataframe(sample_data).to_df(
    "ORDERDATE", "PRODUCTKEY", "SALESAMOUNT"
)
df = df.with_column("ORDERDATE", to_timestamp(df["ORDERDATE"]))


def custom_formatter(input_col, agg, window):
    return f"{agg}_{input_col}_{window}"


res = df.analytics.time_series_agg(
    time_col="ORDERDATE",
    group_by=["PRODUCTKEY"],
    aggs={"SALESAMOUNT": ["SUM", "MAX"]},
    windows=["1D", "-1D"],
    sliding_interval="12H",
    col_formatter=custom_formatter,
)
res.show()
