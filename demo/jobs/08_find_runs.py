"""Find and re-open jobs: list(), get() and get_deployment()."""

import os

from _common import demo_session, say, seed_sales

from snowflake.snowpark.jobs import CodeBundle, Program

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bundle_job")


def row_count(session, table):
    return session.table(table).count()


def main():
    with demo_session() as (session, stage):
        sales = seed_sales(session)
        sproc_job = session.jobs.submit(row_count, args=[sales])
        bundle_job = session.jobs.submit(
            Program.directory(SRC), CodeBundle(), entrypoint="hello.py"
        )
        sproc_job.wait()
        bundle_job.wait()

        # Job ids are stable strings: keep them and come back later, even from another
        # session. list() only covers recent history (the last 1000 rows).
        for job in session.jobs.list(limit=5):
            say("listed:", job.id, job.target, job.status())
        say("reopened:", session.jobs.get(sproc_job.id).result())
        say("reopened:", session.jobs.get(bundle_job.id).status())

        session.jobs.deploy(row_count, name="DEMO_ROW_COUNT", stage=stage)
        found = session.jobs.get_deployment("DEMO_ROW_COUNT")
        say("deployment:", found.name, "->", found.run(args=[sales]).result())


if __name__ == "__main__":
    main()
