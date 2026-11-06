import logging
import sys

from snowflake.snowpark.context import get_active_session


def main():
    # A connection check: the session is already open inside a code bundle.
    session = get_active_session()
    user, role, warehouse = session.sql(
        "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()"
    ).collect()[0]
    logging.warning(
        "hello user=%s role=%s warehouse=%s args=%s",
        user,
        role,
        warehouse,
        sys.argv[1:],
    )


if __name__ == "__main__":
    main()
