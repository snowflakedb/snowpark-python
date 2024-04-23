import snowflake.snowpark
from snowflake.snowpark.functions import col


def mod5(session: snowflake.snowpark.Session, x: int) -> int:
    return (
        session.create_dataframe([[x]], schema=["a"])
        .select(col("a") % 5)
        .collect()[0][0]
    )
