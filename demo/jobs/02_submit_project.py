"""Submit a small project (a directory) with a function entrypoint and extra packages."""

import os

from _common import demo_session, say

from snowflake.snowpark.jobs import Environment, Program

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    with demo_session() as (session, _stage):
        # "main.py:run" means: call run(session, *args) from etl/main.py.
        program = Program.directory(os.path.join(HERE, "etl"), "main.py:run")
        job = session.jobs.submit(
            program,
            args=["  Ada ", "GRACE"],
            env=Environment(packages=["pandas"]),
        )
        say("finished:", job.wait(), "result:", job.result())


if __name__ == "__main__":
    main()
