"""Submit a Python function as a one-off job. The default target is a stored procedure."""

import logging

from _common import demo_session, say, show_logs


def count_rows(session, n):
    logging.warning("counting %s rows", n)
    return {"n": n, "rows": session.range(n).count()}


def broken(session):
    return 1 / 0


def main():
    with demo_session() as (session, _stage):
        # submit() returns immediately with a handle; the program runs in Snowflake.
        job = session.jobs.submit(count_rows, args=[1000])
        say("submitted", job.id, job.status())
        say("finished:", job.wait(), "result:", job.result())
        say("--- logs")
        show_logs(job)

        # A failing program ends as FAILED. error() has the message instead of raising.
        bad = session.jobs.submit(broken)
        say("failing job:", bad.wait())
        say([ln for ln in bad.error().splitlines() if "Error" in ln][-1])


if __name__ == "__main__":
    main()
