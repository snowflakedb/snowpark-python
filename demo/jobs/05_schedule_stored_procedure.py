"""Schedule a stored procedure deployment with a cron expression."""

import time

from _common import demo_session, say

from snowflake.snowpark.jobs import StoredProcedure


def greet(session, name):
    return f"hello {name}"


def main():
    with demo_session() as (session, stage):
        # Caller's rights, because the result of a scheduled run can only be read back
        # for those.
        dep = session.jobs.deploy(
            greet, StoredProcedure(execute_as="caller"), name="DEMO_GREET", stage=stage
        )

        # Jan 1 at 03:00 UTC: it never fires during the demo, so only run_now() runs it.
        # Without warehouse=, the task is serverless instead.
        schedule = dep.schedule(
            "0 3 1 1 * UTC", warehouse=session.get_current_warehouse(), args=["Ada"]
        )
        say("scheduled as", schedule.name)

        schedule.run_now()
        runs = []
        while not runs or not runs[0].is_done():
            time.sleep(3)
            runs = schedule.runs()
        say("run:", runs[0].status(), "-", runs[0].child().result())

        schedule.suspend()  # stop future runs ...
        schedule.resume()  # ... and start them again
        schedule.drop()
        dep.drop()


if __name__ == "__main__":
    main()
