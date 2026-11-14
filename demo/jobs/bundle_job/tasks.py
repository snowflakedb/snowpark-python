import logging

from snowflake.snowpark import functions as F


def run(session, source_table, target_table, *rest):
    # Called as run(session, *args) through the launcher that session.jobs adds to the
    # bundle. Arguments are strings and the return value is ignored.
    since = rest[0] if rest else None  # optional: only days from this date on
    sales = session.table(source_table)
    if since:
        sales = sales.filter(F.col("SOLD_ON") >= F.to_date(F.lit(since)))
    daily = sales.group_by("SOLD_ON").agg(
        F.sum("AMOUNT").alias("TOTAL"), F.count("*").alias("ORDERS")
    )
    daily.write.save_as_table(target_table, mode="overwrite")
    logging.warning(
        "tasks.run: %s daily rows in %s (since %s)",
        session.table(target_table).count(),
        target_table,
        since or "the beginning",
    )
