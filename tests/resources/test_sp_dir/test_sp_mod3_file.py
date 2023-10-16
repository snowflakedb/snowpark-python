import snowflake.snowpark


def mod3(session: snowflake.snowpark.Session, x: int) -> int:
    return session.sql(f"SELECT {x} % 3").collect()[0][0]
