"""Schedule a code bundle deployment (Preview)."""

import os
import time

from _common import demo_session, qualified, say, seed_sales, show_table

from snowflake.snowpark.jobs import CodeBundle, Program

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bundle_job")


def main():
    with demo_session() as (session, _stage):
        sales = seed_sales(session)
        daily = qualified(session, "SALES_DAILY")
        wh = session.get_current_warehouse()
        dep = session.jobs.deploy(
            Program.directory(SRC), CodeBundle(warehouse=wh), name="DEMO_BUNDLE"
        )

        # The task uses the target's warehouse unless schedule(warehouse=...) says
        # otherwise (without either, the session's current warehouse; serverless=True
        # creates a serverless task). Jan 1 at 03:00 UTC never fires during the demo.
        schedule = dep.schedule(
            "0 3 1 1 * UTC", entrypoint="tasks.py:run", args=[sales, daily]
        )
        task = session.sql("SHOW TASKS LIKE 'DEMO_BUNDLE_SCHEDULE'").collect()[0]
        say("scheduled as", schedule.name, "on warehouse", task["warehouse"])

        schedule.run_now()
        runs = []
        while not runs or not runs[0].is_done():
            time.sleep(3)
            runs = schedule.runs()
        say("run:", runs[0].status(), runs[0].child().id)
        show_table(session, daily, "SOLD_ON", limit=3)

        schedule.suspend()
        schedule.resume()

        # The statement the task runs, for putting into a task you manage yourself.
        say("sql:", dep.to_sql(entrypoint="tasks.py:run", args=[sales, daily]))

        schedule.drop()
        dep.drop()


if __name__ == "__main__":
    main()
