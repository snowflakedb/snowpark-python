from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.session import Session


def range5_sproc(session: Session) -> DataFrame:
    return session.range(5)
