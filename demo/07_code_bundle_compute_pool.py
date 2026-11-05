"""Run a script as a code bundle on a compute pool."""

import os
import sys

from _common import demo_session, say

from snowflake.snowpark.jobs import CodeBundle, JobNotSupportedError, Program

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    pool = os.environ.get("SNOWPARK_DEMO_COMPUTE_POOL")
    if not pool:
        sys.exit(
            "Set SNOWPARK_DEMO_COMPUTE_POOL to the name of a compute pool you can use."
        )
    with demo_session() as (session, _stage):
        program = Program.directory(os.path.join(HERE, "bundle_job"))
        # The warehouse (default: the current one) is the query warehouse of the program.
        job = session.jobs.submit(
            program, CodeBundle(compute_pool=pool), entrypoint="hello.py", args=["pool"]
        )
        say("submitted", job.id, "- a cold pool can take a few minutes to start")
        say("finished:", job.wait(timeout=1200))
        # Output of pool runs cannot be retrieved.
        try:
            job.logs()
        except JobNotSupportedError as ex:
            say("logs:", ex)
        # The temporary bundle of the run was dropped when the run finished.
        left = session.sql("SHOW CODE BUNDLES LIKE 'SNOWPARK_JOB%'").collect()
        say("temporary bundles left:", len(left))


if __name__ == "__main__":
    main()
