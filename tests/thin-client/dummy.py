#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import Session
from snowflake.snowpark.functions import (  # noqa: F401
    col,
    lit,
    max as max_,
    sum as sum_,
)
from snowflake.snowpark.types import FloatType, StringType, StructField, StructType

session = Session.builder.getOrCreate()
# session = Session.builder.config("local_testing", True).create()

df = session.create_dataframe(
    [(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)], schema=["a", "b"]
)
df.group_by().agg(sum_("b")).collect()


def convert(pandas_df):
    return pandas_df.assign(TEMP_F=lambda x: x.TEMP_C * 9 / 5 + 32)


df = session.createDataFrame(
    [("SF", 21.0), ("SF", 17.5), ("SF", 24.0), ("NY", 30.9), ("NY", 33.6)],
    schema=["location", "temp_c"],
)

ans = (
    df.group_by("location")
    .apply_in_pandas(
        convert,
        output_schema=StructType(
            [
                StructField("location", StringType()),
                StructField("temp_c", FloatType()),
                StructField("temp_f", FloatType()),
            ]
        ),
    )
    .order_by("temp_c")
    .collect()
)
print(ans)
