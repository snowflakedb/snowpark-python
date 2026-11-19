"""Submit a small project (a directory) with a function entrypoint and extra packages."""

import os

from _common import demo_session, qualified, say, seed_customers, show_table

from snowflake.snowpark.jobs import Environment, Program

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    with demo_session() as (session, _stage):
        source = seed_customers(session)
        target = qualified(session, "CUSTOMERS_CLEAN")

        # "main.py:run" means: call run(session, *args) from etl/main.py. The table
        # names are fully qualified, because the program must not rely on the schema.
        program = Program.directory(os.path.join(HERE, "etl"), "main.py:run")
        job = session.jobs.submit(
            program, args=[source, target], env=Environment(packages=["pandas"])
        )
        say("finished:", job.wait())
        say("result:", job.result())
        say("cleaned customers:")
        show_table(session, target, "ID")


if __name__ == "__main__":
    main()
