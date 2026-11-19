import logging
import sys

from snowflake.snowpark import functions as F
from snowflake.snowpark.context import get_active_session


def main():
    summary_table = sys.argv[1]
    session = get_active_session()
    top = session.table(summary_table).sort(F.col("TOTAL").desc()).limit(1).collect()[0]
    logging.warning("top region: %s with %s", top["REGION"], top["TOTAL"])


if __name__ == "__main__":
    main()
