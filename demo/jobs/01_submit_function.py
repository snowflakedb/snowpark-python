"""Submit a Python function as a one-off job. The default target is a stored procedure."""

import logging

from _common import demo_session, say, seed_sales, show_logs

from snowflake.snowpark import functions as F


def top_region(session, sales_table):
    # Runs in Snowflake; the session is passed in as the first argument.
    logging.warning("aggregating %s", sales_table)
    sales = session.table(sales_table)
    top = (
        sales.group_by("REGION")
        .agg(F.sum("AMOUNT").alias("TOTAL"))
        .sort(F.col("TOTAL").desc())
        .limit(1)
        .collect()[0]
    )
    return {"region": top["REGION"], "total": int(top["TOTAL"]), "rows": sales.count()}


def missing_column(session, sales_table):
    return session.table(sales_table).select("NO_SUCH_COLUMN").collect()


def main():
    with demo_session() as (session, _stage):
        sales = seed_sales(session)

        # submit() returns immediately with a handle; the program runs in Snowflake.
        job = session.jobs.submit(top_region, args=[sales])
        say("submitted", job.id, job.status())
        say("finished:", job.wait(), "result:", job.result())
        say("--- logs")
        show_logs(job)

        # A failing program ends as FAILED. error() has the message instead of raising.
        bad = session.jobs.submit(missing_column, args=[sales])
        say("failing job:", bad.wait())
        say([ln for ln in bad.error().splitlines() if "invalid identifier" in ln][0])


if __name__ == "__main__":
    main()
