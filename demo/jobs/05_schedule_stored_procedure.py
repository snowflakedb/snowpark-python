"""Schedule a stored procedure deployment with a cron expression."""

import time

from _common import demo_session, qualified, say, seed_sales, show_table

from snowflake.snowpark import functions as F
from snowflake.snowpark.jobs import StoredProcedure


def refresh_summary(session, sales_table, summary_table):
    summary = (
        session.table(sales_table)
        .group_by("PRODUCT")
        .agg(F.sum("AMOUNT").alias("TOTAL"))
    )
    summary.write.save_as_table(summary_table, mode="overwrite")
    return summary.count()


def main():
    with demo_session() as (session, stage):
        # Caller's rights, because the result of a scheduled run can only be read back
        # for those.
        sales = seed_sales(session)
        summary = qualified(session, "SALES_BY_PRODUCT")
        dep = session.jobs.deploy(
            refresh_summary,
            StoredProcedure(execute_as="caller"),
            name="DEMO_REFRESH",
            stage=stage,
        )

        # Jan 1 at 03:00 UTC: it never fires during the demo, so only run_now() runs it.
        # The task runs on the session's current warehouse unless warehouse=... says
        # otherwise; serverless=True creates a serverless task instead (it needs the
        # EXECUTE MANAGED TASK privilege).
        schedule = dep.schedule("0 3 1 1 * UTC", args=[sales, summary])
        say("scheduled as", schedule.name)

        schedule.run_now()
        runs = []
        while not runs or not runs[0].is_done():
            time.sleep(3)
            runs = schedule.runs()
        say("run:", runs[0].status(), "- summary rows:", runs[0].child().result())
        show_table(session, summary, "PRODUCT")

        schedule.suspend()  # stop future runs ...
        schedule.resume()  # ... and start them again
        schedule.drop()
        dep.drop()


if __name__ == "__main__":
    main()
