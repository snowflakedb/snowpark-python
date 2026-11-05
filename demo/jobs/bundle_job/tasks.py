import logging


def run(session, *args):
    # Called as run(session, *args) through the launcher that session.jobs adds to the
    # bundle. Arguments are strings and the return value is ignored.
    rows = session.sql("SELECT CURRENT_WAREHOUSE()").collect()[0][0]
    logging.warning("tasks.run args=%s warehouse=%s", list(args), rows)
