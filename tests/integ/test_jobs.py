#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import time
import uuid

import pytest

from snowflake.snowpark.jobs import (
    BundleRun,
    CodeBundle,
    CodeBundleDeployment,
    Environment,
    JobNotSupportedError,
    JobStatus,
    ProcedureDeployment,
    Program,
    QueryJob,
    StoredProcedure,
    TaskRun,
)

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="session.jobs is not supported in local testing mode",
    ),
]

TEST_DATABASE = "TESTDB_YIXIE"
RUN_TIMEOUT = 300
TASK_TIMEOUT = 90
LOGS_TIMEOUT = 120
COMPUTE_POOL = os.environ.get("SNOWPARK_JOBS_TEST_COMPUTE_POOL")


@pytest.fixture(scope="module", autouse=True)
def jobs_schema(session):
    """A scratch schema that is dropped (CASCADE) even if tests fail."""
    previous = (session.get_current_database(), session.get_current_schema())
    schema = f"JOBS_TEST_{uuid.uuid4().hex[:10].upper()}"
    session.sql(f"CREATE SCHEMA {TEST_DATABASE}.{schema}").collect()
    try:
        session.sql(f"USE SCHEMA {TEST_DATABASE}.{schema}").collect()
        session.sql("CREATE STAGE JOBS_STG").collect()
        yield schema
    finally:
        try:
            if previous[0] and previous[1]:
                session.sql(f"USE SCHEMA {previous[0]}.{previous[1]}").collect()
        finally:
            session.sql(
                f"DROP SCHEMA IF EXISTS {TEST_DATABASE}.{schema} CASCADE"
            ).collect()


@pytest.fixture(scope="module")
def programs(tmp_path_factory):
    root = tmp_path_factory.mktemp("jobs_programs")
    script = root / "script.py"
    script.write_text(
        "import logging, sys\n"
        "logging.warning('JOBS_SCRIPT_MARKER %s', sys.argv[1:])\n"
        "__return__ = {'argv': sys.argv[1:], 'n': 3}\n"
    )
    proj = root / "proj"
    (proj / "pkg").mkdir(parents=True)
    (proj / "pkg" / "__init__.py").write_text("")
    (proj / "pkg" / "helper.py").write_text("def value():\n    return 'helped'\n")
    (proj / "pkg" / "main.py").write_text(
        "from pkg import helper\n"
        "def run(session, x):\n"
        "    return {'x': x, 'h': helper.value(), 'count': session.range(5).count()}\n"
    )
    bundle = root / "bundle"
    bundle.mkdir()
    (bundle / "ok.py").write_text(
        "import logging, sys\n"
        "logging.warning('JOBS_BUNDLE_MARKER %s', sys.argv[1:])\n"
    )
    (bundle / "fail.py").write_text("raise RuntimeError('boom from bundle')\n")
    (bundle / "sleep.py").write_text("import time\ntime.sleep(180)\n")
    return {
        "script": str(script),
        "proj": str(proj),
        "bundle": str(bundle),
        "root": root,
    }


def _poll(fn, timeout, interval=3.0):
    """Calls ``fn`` until it returns something truthy; returns that or the last value."""
    deadline = time.monotonic() + timeout
    value = fn()
    while not value and time.monotonic() < deadline:
        time.sleep(interval)
        value = fn()
    return value


def _wait_for_runs(schedule, timeout=TASK_TIMEOUT):
    """Waits for a finished task run to show up in the task history."""

    def finished():
        runs = schedule.runs()
        return runs if runs and runs[0].status().is_terminal else None

    runs = _poll(finished, timeout)
    assert runs, f"no finished run of {schedule.name} within {timeout}s"
    return runs


def _poll_logs(job, marker, timeout=LOGS_TIMEOUT):
    return _poll(lambda: marker in job.logs() and job.logs(), timeout, interval=10)


@pytest.fixture(scope="module")
def has_event_table(session):
    rows = session.sql("SHOW PARAMETERS LIKE 'EVENT_TABLE' IN ACCOUNT").collect()
    if not rows or not rows[0]["value"]:
        pytest.skip("no account event table")


# ---- StoredProcedure ----


@pytest.fixture(scope="module")
def function_job(session):
    def fn(session, a, b):
        import logging

        logging.warning("JOBS_FN_MARKER %s %s", a, b)
        return {"sum": a + b, "rows": session.range(3).count()}

    return session.jobs.submit(fn, args=[1, 2])


def test_sproc_submit_function(session, function_job):
    job = function_job
    assert isinstance(job, QueryJob)
    assert job.target == "stored_procedure"
    assert job.id == f"query:{job.query_id}"
    assert job.wait(RUN_TIMEOUT) == JobStatus.SUCCEEDED
    assert job.result() == {"sum": 3, "rows": 3}
    assert job.error() is None
    assert job.status() == JobStatus.SUCCEEDED


def test_sproc_submit_script_file(session, programs):
    job = session.jobs.submit(Program.file(programs["script"]), args=["a", "b'c", 7])
    assert job.result(RUN_TIMEOUT) == {"argv": ["a", "b'c", "7"], "n": 3}
    assert job.status() == JobStatus.SUCCEEDED


def test_sproc_submit_file_with_entrypoint_function(session, programs, tmp_path):
    path = tmp_path / "entry.py"
    path.write_text("def go(session, n):\n    return session.range(n).count()\n")
    job = session.jobs.submit(Program.file(str(path), "go"), args=[4])
    assert job.result(RUN_TIMEOUT) == 4


def test_sproc_submit_directory_with_function_entrypoint(session, programs):
    job = session.jobs.submit(
        Program.directory(programs["proj"], "pkg/main.py:run"), args=["hi"]
    )
    assert job.result(RUN_TIMEOUT) == {"x": "hi", "h": "helped", "count": 5}


def test_sproc_failing_program(session):
    def bad(session):
        raise ValueError("kaboom from sproc")

    job = session.jobs.submit(bad)
    assert job.wait(RUN_TIMEOUT) == JobStatus.FAILED
    assert job.status() == JobStatus.FAILED
    assert "kaboom from sproc" in job.error()
    with pytest.raises(Exception, match="kaboom from sproc"):
        job.result()


def test_sproc_cancel(session):
    def slow(session):
        import time

        time.sleep(120)

    job = session.jobs.submit(slow)
    _poll(lambda: job.status() == JobStatus.RUNNING, 60, interval=2)
    job.cancel()
    assert job.wait(120) in (JobStatus.CANCELLED, JobStatus.FAILED)
    assert job.status().is_terminal


def test_sproc_get_and_list(session, function_job):
    function_job.wait(RUN_TIMEOUT)
    got = session.jobs.get(function_job.id)
    assert isinstance(got, QueryJob)
    assert got.status() == JobStatus.SUCCEEDED
    assert got.result() == {"sum": 3, "rows": 3}

    listed = session.jobs.list(target="stored_procedure")
    assert function_job.id in [j.id for j in listed]
    assert all(isinstance(j, QueryJob) for j in listed)
    ok = session.jobs.list(target=StoredProcedure.kind, status="SUCCEEDED", limit=500)
    assert function_job.id in [j.id for j in ok]
    assert function_job.id not in [
        j.id for j in session.jobs.list(target="stored_procedure", status="FAILED")
    ]
    assert len(session.jobs.list(target="stored_procedure", limit=1)) == 1


def test_sproc_deploy_run_schedule(session):
    def deployed(session, name):
        return {"hello": name}

    name = f"SP_DEP_{uuid.uuid4().hex[:6].upper()}"
    dep = session.jobs.deploy(
        deployed,
        StoredProcedure(execute_as="caller"),
        name=name,
        stage="@JOBS_STG",
    )
    try:
        assert isinstance(dep, ProcedureDeployment)
        assert dep.target == "stored_procedure"
        assert dep.to_sql(args=["x'y"]).startswith(
            f"CALL {session.get_current_database()}.{session.get_current_schema()}.{name}(ARRAY_CONSTRUCT("
        )
        proc = session.sql(f"SHOW PROCEDURES LIKE '{name}'").collect()[0]
        assert proc["arguments"].split(" RETURN ")[0].endswith("(ARRAY)")
        desc = {
            r["property"]: r["value"]
            for r in session.sql(f"DESCRIBE PROCEDURE {name}(ARRAY)").collect()
        }
        assert desc["execute as"] == "CALLER"

        run = dep.run(args=["world"])
        assert run.result(RUN_TIMEOUT) == {"hello": "world"}

        # Program.ref and get_deployment resolve the target from the object type
        via_ref = session.jobs.submit(Program.ref(name), args=["ref"])
        assert isinstance(via_ref, QueryJob)
        assert via_ref.result(RUN_TIMEOUT) == {"hello": "ref"}
        got = session.jobs.get_deployment(name)
        assert isinstance(got, ProcedureDeployment)
        assert got.run(args=["got"]).result(RUN_TIMEOUT) == {"hello": "got"}
        with pytest.raises(ValueError, match="conflicts with the given target"):
            session.jobs.submit(Program.ref(name), CodeBundle())

        # schedule, trigger, inspect the run, drop
        sched = dep.schedule(
            "0 0 1 1 * UTC",
            warehouse=session.get_current_warehouse(),
            args=["task"],
        )
        try:
            assert sched.name.endswith(f"{name}_SCHEDULE")
            assert sched.runs() == []
            sched.run_now()
            runs = _wait_for_runs(sched)
            assert isinstance(runs[0], TaskRun)
            assert runs[0].id == f"task:{sched.name}/{runs[0].query_id}"
            assert runs[0].target == "stored_procedure"
            assert runs[0].status() == JobStatus.SUCCEEDED
            assert runs[0].child().result(RUN_TIMEOUT) == {"hello": "task"}
            assert runs[0].result() == {"hello": "task"}
            assert session.jobs.get(runs[0].id).status() == JobStatus.SUCCEEDED
            sched.suspend()
            sched.resume()
        finally:
            sched.drop()
        assert (
            session.sql(f"SHOW TASKS LIKE '{name}_SCHEDULE'").collect() == []
        ), "schedule.drop() should remove the task"
    finally:
        dep.drop()
    assert session.sql(f"SHOW PROCEDURES LIKE '{name}'").collect() == []


def test_sproc_deploy_defaults_to_owner(session):
    name = f"SP_OWN_{uuid.uuid4().hex[:6].upper()}"
    dep = session.jobs.deploy(lambda session: 1, name=name, stage="@JOBS_STG")
    try:
        desc = {
            r["property"]: r["value"]
            for r in session.sql(f"DESCRIBE PROCEDURE {name}(ARRAY)").collect()
        }
        assert desc["execute as"] == "OWNER"
        assert dep.run().result(RUN_TIMEOUT) == 1
    finally:
        dep.drop()


def test_sproc_deploy_replace(session):
    name = f"SP_REP_{uuid.uuid4().hex[:6].upper()}"
    dep = session.jobs.deploy(lambda session: 1, name=name, stage="@JOBS_STG")
    try:
        with pytest.raises(Exception, match="already exists"):
            session.jobs.deploy(lambda session: 2, name=name, stage="@JOBS_STG")
        dep2 = session.jobs.deploy(
            lambda session: 2, name=name, stage="@JOBS_STG", replace=True
        )
        assert dep2.run().result(RUN_TIMEOUT) == 2
    finally:
        dep.drop()


def test_sproc_submit_validation_errors(session, programs):
    with pytest.raises(ValueError, match="caller's rights"):
        session.jobs.submit(lambda session: 1, StoredProcedure(execute_as="owner"))
    with pytest.raises(ValueError, match="execution_name"):
        session.jobs.submit(lambda session: 1, execution_name="x")
    with pytest.raises(ValueError, match="stage directories"):
        session.jobs.submit(Program.stage("@JOBS_STG/dir", "main.py"))


def test_sproc_logs(session, function_job, has_event_table):
    function_job.wait(RUN_TIMEOUT)
    logs = _poll_logs(function_job, "JOBS_FN_MARKER 1 2")
    assert logs and "JOBS_FN_MARKER 1 2" in logs
    assert "WARN" in logs.upper()
    last = function_job.logs(limit=1)
    assert len(last.splitlines()) == 1


# ---- CodeBundle on a warehouse ----


def test_bundle_submit_success_and_logs_flow(session, programs, has_event_table):
    run = session.jobs.submit(
        Program.directory(programs["bundle"]),
        CodeBundle(),
        entrypoint="ok.py",
        args=["x", "it's"],
    )
    assert isinstance(run, BundleRun)
    assert run.target == "code_bundle" and run.id == f"bundle:{run.query_id}"
    assert run.execution_name.startswith("SNOWPARK_JOB_OK_")
    assert run.wait(RUN_TIMEOUT) == JobStatus.SUCCEEDED
    assert run.error() is None
    with pytest.raises(JobNotSupportedError):
        run.result()

    got = session.jobs.get(run.id)
    assert isinstance(got, BundleRun) and got.status() == JobStatus.SUCCEEDED

    listed = session.jobs.list(target="code_bundle")
    assert run.id in [j.id for j in listed]
    assert run.id in [
        j.id for j in session.jobs.list(target=CodeBundle(), status="SUCCEEDED")
    ]

    logs = _poll_logs(run, "JOBS_BUNDLE_MARKER")
    assert logs and "JOBS_BUNDLE_MARKER" in logs


def test_bundle_submit_single_file(session, programs):
    run = session.jobs.submit(
        Program.file(programs["script"]), CodeBundle(), args=["z"]
    )
    assert run.wait(RUN_TIMEOUT) == JobStatus.SUCCEEDED


def test_bundle_failing_script(session, programs):
    run = session.jobs.submit(
        Program.directory(programs["bundle"]), CodeBundle(), entrypoint="fail.py"
    )
    assert run.wait(RUN_TIMEOUT) == JobStatus.FAILED
    assert "boom from bundle" in run.error()
    assert run.id in [
        j.id for j in session.jobs.list(target="code_bundle", status="FAILED")
    ]


def test_bundle_cancel(session, programs):
    run = session.jobs.submit(
        Program.directory(programs["bundle"]), CodeBundle(), entrypoint="sleep.py"
    )
    assert _poll(lambda: run.status() == JobStatus.RUNNING, 90, interval=2)
    run.cancel()
    assert run.wait(120) == JobStatus.CANCELLED


def test_bundle_rejects_non_string_args(session, programs):
    with pytest.raises(ValueError, match="must be strings"):
        session.jobs.submit(
            Program.directory(programs["bundle"]),
            CodeBundle(),
            entrypoint="ok.py",
            args=[1],
        )


def test_bundle_rejects_unsupported_programs(session, programs):
    with pytest.raises(ValueError, match="Function programs are not supported"):
        session.jobs.submit(lambda session: 1, CodeBundle())
    with pytest.raises(ValueError, match="secrets.*warehouse"):
        session.jobs.submit(
            Program.file(programs["script"]),
            CodeBundle(),
            env=Environment(secrets={"s": "a.b.c"}),
        )


def test_bundle_deploy_run_version_schedule(session, programs):
    name = f"BUNDLE_DEP_{uuid.uuid4().hex[:6].upper()}"
    program = Program.directory(programs["bundle"])
    dep = session.jobs.deploy(program, CodeBundle(), name=name)
    try:
        assert isinstance(dep, CodeBundleDeployment)
        assert dep.target == "code_bundle"
        assert dep.to_sql(args=["a'b"], entrypoint="ok.py") == (
            f"EXECUTE CODE BUNDLE {session.get_current_database()}.{session.get_current_schema()}.{name} "
            "ENTRYPOINT = 'ok.py' ARGUMENTS = ('a''b')"
        )
        with pytest.raises(ValueError, match="no default entrypoint"):
            dep.to_sql()

        run = dep.run(entrypoint="ok.py", args=["q"])
        assert isinstance(run, BundleRun)
        assert run.wait(RUN_TIMEOUT) == JobStatus.SUCCEEDED

        # overrides are merged into the deployment's specification
        over = dep.run(entrypoint="ok.py", overrides={"env_vars": [{"Z": "1"}]})
        assert over.wait(RUN_TIMEOUT) == JobStatus.SUCCEEDED, over.error()

        # a new version with a new entrypoint
        new_dir = programs["root"] / "bundle_v2"
        new_dir.mkdir(exist_ok=True)
        (new_dir / "v2.py").write_text("print('v2')\n")
        dep.add_version(Program.directory(str(new_dir)))
        v2 = dep.run(entrypoint="v2.py")
        assert v2.wait(RUN_TIMEOUT) == JobStatus.SUCCEEDED, v2.error()

        # get_deployment returns the same kind and can run
        got = session.jobs.get_deployment(name)
        assert isinstance(got, CodeBundleDeployment)
        ref_run = session.jobs.submit(Program.ref(name), entrypoint="v2.py")
        assert isinstance(ref_run, BundleRun)
        assert ref_run.wait(RUN_TIMEOUT) == JobStatus.SUCCEEDED
        with pytest.raises(ValueError, match="conflicts with the given target"):
            session.jobs.submit(Program.ref(name), StoredProcedure())

        # schedule
        sched = dep.schedule(
            "0 0 1 1 * UTC",
            warehouse=session.get_current_warehouse(),
            args=["task"],
            entrypoint="v2.py",
        )
        try:
            sched.run_now()
            runs = _wait_for_runs(sched)
            assert runs[0].target == "code_bundle"
            assert runs[0].status() == JobStatus.SUCCEEDED
            child = runs[0].child()
            assert isinstance(child, BundleRun)
            assert child.wait(RUN_TIMEOUT) == JobStatus.SUCCEEDED
            with pytest.raises(JobNotSupportedError):
                runs[0].result()
        finally:
            sched.drop()
    finally:
        dep.drop()
    assert session.sql(f"SHOW CODE BUNDLES LIKE '{name}'").collect() == []


# ---- CodeBundle on a compute pool ----


@pytest.mark.skipif(
    not COMPUTE_POOL, reason="SNOWPARK_JOBS_TEST_COMPUTE_POOL is not set"
)
def test_bundle_compute_pool_submit(session, programs):
    run = session.jobs.submit(
        Program.directory(programs["bundle"]),
        CodeBundle(compute_pool=COMPUTE_POOL),
        entrypoint="ok.py",
        args=["pool"],
    )
    assert run.compute_type == "compute_pool"
    temp = run._temp_bundle
    assert temp
    assert run.wait(900) == JobStatus.SUCCEEDED, run.error()
    assert run._temp_bundle is None
    short = temp.split(".")[-1]
    assert session.sql(f"SHOW CODE BUNDLES LIKE '{short}'").collect() == []
    with pytest.raises(JobNotSupportedError):
        run.logs()
    with pytest.raises(JobNotSupportedError):
        run.result()
