#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, udf
from snowflake.snowpark.types import IntegerType

session = Session.builder.getOrCreate()
add_one = udf(lambda x: x + 1, return_type=IntegerType(), input_types=[IntegerType()])
df = session.create_dataframe([1, 2, 3], schema=["a"])
ans = df.select(add_one(col("a")).as_("ans")).collect()

print(ans)
