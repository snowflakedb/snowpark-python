"""Deploy a function as a permanent stored procedure and run it."""

from _common import demo_session, say

from snowflake.snowpark.jobs import StoredProcedure


def greet(session, name, times=1):
    return " ".join(f"hello {name}" for _ in range(int(times)))


def main():
    with demo_session() as (session, stage):
        # deploy() needs an internal stage for the procedure's code. The default is
        # owner's rights, like CREATE PROCEDURE; ask for caller's rights explicitly.
        owner = session.jobs.deploy(greet, name="DEMO_GREET", stage=stage)
        caller = session.jobs.deploy(
            greet,
            StoredProcedure(execute_as="caller"),
            name="DEMO_GREET_CALLER",
            stage=stage,
        )
        say("deployed", owner.name, "and", caller.name)

        say("run:", owner.run(args=["Ada", 2]).result())

        # The SQL that runs the deployment, e.g. to paste into your own task.
        say("sql:", owner.to_sql(args=["it's", 2]))

        # replace=True overwrites a deployment of the same name; without it, deploying
        # an existing name fails.
        session.jobs.deploy(greet, name="DEMO_GREET", stage=stage, replace=True)

        owner.drop()
        caller.drop()


if __name__ == "__main__":
    main()
