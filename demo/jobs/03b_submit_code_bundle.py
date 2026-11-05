"""One-off code bundle runs on the session's warehouse (code bundles are in Preview)."""

import os

from _common import demo_session, say, show_logs

from snowflake.snowpark.jobs import CodeBundle, Program

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bundle_job")


def main():
    with demo_session() as (session, _stage):
        program = Program.directory(SRC)
        target = CodeBundle()  # a warehouse; the session's current one by default

        # A code bundle has many entry points: pick one per run.
        job = session.jobs.submit(
            program, target, entrypoint="hello.py", args=["a", "b"]
        )
        say("submitted", job.id, job.status(), "->", job.wait())
        show_logs(job)

        bad = session.jobs.submit(program, target, entrypoint="fail.py")
        say("failing run:", bad.wait())
        say([ln for ln in bad.error().splitlines() if "Error" in ln][-1])

        slow = session.jobs.submit(program, target, entrypoint="slow.py")
        slow.cancel()
        say("cancelled run:", slow.wait())


if __name__ == "__main__":
    main()
