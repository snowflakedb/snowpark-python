"""Deploy a directory as a code bundle (Preview) with several entry points."""

import os

from _common import demo_session, say, show_logs

from snowflake.snowpark.jobs import CodeBundle, Program

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bundle_job")


def main():
    with demo_session() as (session, _stage):
        # Version 1 leaves out summary.py, which arrives later as version 2.
        v1 = Program.directory(SRC, exclude=["summary.py"])
        dep = session.jobs.deploy(v1, CodeBundle(), name="DEMO_BUNDLE")
        # replace=True overwrites a bundle of the same name.
        dep = session.jobs.deploy(v1, CodeBundle(), name="DEMO_BUNDLE", replace=True)
        say("deployed", dep.name)

        # One bundle, many entry points; arguments must be strings.
        say("hello:", dep.run(entrypoint="hello.py", args=["x"]).wait())
        # overrides patch the specification for this run only.
        run = dep.run(
            entrypoint="report.py", overrides={"env_vars": [{"REPORT_NAME": "Q3"}]}
        )
        say("report:", run.wait())
        show_logs(run)

        say("sql:", dep.to_sql(entrypoint="hello.py", args=["x"]))

        # Runs always use the latest version, so the new entry point works right away.
        dep.add_version(Program.directory(SRC))
        say("summary (v2):", dep.run(entrypoint="summary.py", args=["a", "b"]).wait())

        # Deployments can be found again by name, e.g. from another session.
        found = session.jobs.get_deployment("DEMO_BUNDLE")
        say("found:", found.name, found.run(entrypoint="hello.py").wait())

        pool = os.environ.get("SNOWPARK_DEMO_COMPUTE_POOL")
        if pool:
            # Same program on a compute pool; a cold pool can take minutes to start.
            on_pool = session.jobs.deploy(
                v1, CodeBundle(compute_pool=pool), name="DEMO_BUNDLE_POOL"
            )
            say("on the pool:", on_pool.run(entrypoint="hello.py").wait(timeout=1200))
            on_pool.drop()

        dep.drop()


if __name__ == "__main__":
    main()
