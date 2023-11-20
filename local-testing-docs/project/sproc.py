from project.local import get_env_var_config
from project.transformers import *

from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.session import Session


def create_fact_tables(sess: Session, target_schema: str) -> int:
    """
    This job applies the transformations in transformers.py to the built-in Citibike dataset
    and saves two tables, month_summary and bike_summary, under CITIBIKE.PUBLIC. Returns the
    total number of rows created in both tables
    """

    SOURCE_DB = "CITIBIKE"
    SOURCE_SCHEMA = "PUBLIC"
    SOURCE_TABLE = "TRIPS"

    sess.use_database(SOURCE_DB)
    sess.use_schema(SOURCE_SCHEMA)

    df = sess.table(SOURCE_TABLE)
    df = add_rider_age(df)

    month_facts = calc_month_facts(df)
    bike_facts = calc_bike_facts(df)

    sess.use_schema(target_schema)

    month_facts.write.save_as_table("month_summary", table_type="", mode="overwite")
    bike_facts.write.save_as_table("bike_summary", table_type="", mode="overwrite")

    return month_facts.count() + bike_facts.count()


if __name__ == "__main__":
    print("Creating session")
    session = Session.builder.configs(get_env_var_config()).create()

    print("Running job...")
    rows = create_fact_tables(session, "PUBLIC")

    print("Job complete. Number of rows created:")
    print(rows)
