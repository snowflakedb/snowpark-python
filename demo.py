from snowflake.snowpark import Session
from snowflake.snowpark.context import configure_development_features
from snowflake.snowpark.functions import col, lit, sum as sum_, avg, count, when

session = Session.builder.config("connection_name", "zyao_ent_preprod9").create()

configure_development_features(
    enable_dataframe_trace_on_error=False,
    enable_trace_sql_errors_to_dataframe=True,
)


# Create a test DataFrame using SQL literals (no database/schema needed)
df = session.sql(
    """
    SELECT column1 AS NAME, column2 AS AGE, column3 AS DEPT FROM VALUES
    ('Alice', 30, 'Engineering'),
    ('Bob', 25, 'Marketing'),
    ('Carol', 35, 'Engineering'),
    ('Dave', 28, 'Marketing'),
    ('Eve', 32, 'Engineering')
"""
)

# This should work fine — valid columns
df_valid = df.select(col("NAME"), col("AGE"), col("DEPT"))
df_valid.show()


# Pipeline 2: independent age statistics (not chained from df2/df3/df4)
df_stats = df.group_by(col("DEPT")).agg(
    avg("AGE").alias("AVG_AGE"), count("*").alias("CNT")
)
df_senior = df_stats.filter(col("AVG_AGE") > 30)

# Pipeline 1: department-level analysis
df2_eng = df.select(col("NAME"), col("DEPT"))
df2_extra = df.filter(col("AGE") > 30).select(col("NAME"), col("DEPT"))
df2 = df2_eng.union(df2_extra)

# Pipeline 3: lookup-style independent DataFrame
df_threshold = session.sql("SELECT 28 AS MIN_AGE, 'Marketing' AS TARGET_DEPT")

# Continue on Pipeline 1.
df3 = df2.filter(10 / (col("AGE_NON_EXISTENT_COLUMN") % 5) >= 1)
df4 = df3.filter(col("DEPT") == "Engineering")


df_filtered = df.join(
    df_threshold, (col("AGE") >= col("MIN_AGE")) & (col("DEPT") == col("TARGET_DEPT"))
)

df4.show()
df_senior.show()
df_filtered.show()
