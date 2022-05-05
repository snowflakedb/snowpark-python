import snowflake.snowpark


def mod5(session: snowflake.snowpark.Session, x: int) -> int:
    return session.sql(f"SELECT {x} % 5").collect()[0][0]
