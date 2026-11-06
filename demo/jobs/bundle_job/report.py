import logging
import os
import sys

from snowflake.snowpark import functions as F
from snowflake.snowpark.context import get_active_session


def main():
    source_table, target_table = sys.argv[1:3]
    session = get_active_session()
    sales = session.table(source_table)
    summary = sales.group_by("REGION").agg(
        F.sum("AMOUNT").alias("TOTAL"),
        F.count("*").alias("ORDERS"),
        F.avg("AMOUNT").alias("AVG_AMOUNT"),
    )
    summary.write.save_as_table(target_table, mode="overwrite")
    # REPORT_NAME is set by an override in the sample
    logging.warning(
        "report %s: %s sales rows -> %s regions in %s",
        os.environ.get("REPORT_NAME", "-"),
        sales.count(),
        summary.count(),
        target_table,
    )


if __name__ == "__main__":
    main()
