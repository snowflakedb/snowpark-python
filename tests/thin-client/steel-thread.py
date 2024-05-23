from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, contains

session = Session.builder.create()
df = session.table('test_table').filter("STR LIKE '%e%'")
# col = df.col("STR") # TODO: determine whether this should create and assign statement or not
df2 = df.select("A", df.col("STR"), col("B"), df.STR, col("A") + col("B"))
print(session._ast_batch._request)
