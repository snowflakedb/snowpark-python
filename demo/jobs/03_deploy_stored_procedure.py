"""Deploy a function as a permanent stored procedure and run it."""

from _common import demo_session, say, seed_sales

from snowflake.snowpark import functions as F
from snowflake.snowpark.jobs import StoredProcedure


def region_total(session, sales_table, region):
    row = (
        session.table(sales_table)
        .filter(F.col("REGION") == region)
        .agg(F.sum("AMOUNT").alias("TOTAL"))
        .collect()[0]
    )
    return {"region": region, "total": int(row["TOTAL"])}


def main():
    with demo_session() as (session, stage):
        sales = seed_sales(session)

        # deploy() needs an internal stage for the procedure's code. The default is
        # owner's rights, like CREATE PROCEDURE; ask for caller's rights explicitly.
        owner = session.jobs.deploy(region_total, name="DEMO_REGION_TOTAL", stage=stage)
        caller = session.jobs.deploy(
            region_total,
            StoredProcedure(execute_as="caller"),
            name="DEMO_REGION_TOTAL_CALLER",
            stage=stage,
        )
        say("deployed", owner.name, "and", caller.name)

        say("run:", owner.run(args=[sales, "EMEA"]).result())
        say("run (caller's rights):", caller.run(args=[sales, "APAC"]).result())

        # The SQL that runs the deployment, e.g. to paste into your own task.
        say("sql:", owner.to_sql(args=[sales, "AMER"]))

        # replace=True overwrites a deployment of the same name; without it, deploying
        # an existing name fails.
        session.jobs.deploy(
            region_total, name="DEMO_REGION_TOTAL", stage=stage, replace=True
        )

        owner.drop()
        caller.drop()


if __name__ == "__main__":
    main()
