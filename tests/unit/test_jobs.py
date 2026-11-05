#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import re
import threading
from unittest import mock

import pytest
import yaml

from snowflake.snowpark import Session
from snowflake.snowpark.jobs import (
    BundleRun,
    CodeBundle,
    CodeBundleDeployment,
    Environment,
    JobManager,
    JobNotSupportedError,
    JobStatus,
    ProcedureDeployment,
    Program,
    QueryJob,
    StoredProcedure,
    TaskRun,
    _bundle,
    _handles,
    _packaging,
    _sproc,
)
from snowflake.snowpark.jobs._bundle_spec import (
    DEFAULT_POOL_RUNTIME_VERSION,
    build_spec,
    default_warehouse_runtime_version,
    dump_spec,
    merge_overrides,
)
from snowflake.snowpark.jobs._utils import (
    identifier_safe,
    qualify,
    split_fqn,
    sql_str,
    unquote_ident,
    validate_name,
)


class FakeRow(dict):
    def as_dict(self):
        return dict(self)


class FakeResult:
    def __init__(self, rows, query_id="QID_1") -> None:
        self._rows = rows
        self.query_id = query_id

    def collect(self):
        return self._rows

    def collect_nowait(self):
        return FakeAsyncJob(self.query_id)


class FakeAsyncJob:
    def __init__(self, query_id) -> None:
        self.query_id = query_id


class FakeFile:
    def __init__(self) -> None:
        self.puts = []

    def put(self, path, dest, **kwargs):
        with open(path, "rb") as f:
            self.puts.append((os.path.basename(path), dest, f.read()))


class FakeSession:
    """Records the SQL statements; ``responses`` maps a regex to the rows to return."""

    def __init__(self, responses=None, warehouse="WH") -> None:
        self.statements = []
        self.responses = responses or {}
        self.file = FakeFile()
        self._warehouse = warehouse
        self._lock = threading.RLock()

    def use_warehouse(self, warehouse):
        self.statements.append(f"USE WAREHOUSE {warehouse}")
        self._warehouse = warehouse

    def sql(self, statement):
        self.statements.append(statement)
        for pattern, rows in self.responses.items():
            if re.search(pattern, statement):
                return FakeResult([FakeRow(r) for r in rows])
        return FakeResult([])

    def get_current_database(self):
        return "DB"

    def get_current_schema(self):
        return "SCH"

    def get_current_warehouse(self):
        return self._warehouse

    def get_session_stage(self):
        return "@SESS_STAGE"

    def create_async_job(self, query_id):
        return FakeAsyncJob(query_id)


@pytest.fixture
def fake():
    return FakeSession()


@pytest.fixture
def jobs(fake):
    return JobManager(fake)


@pytest.fixture
def script(tmp_path):
    path = tmp_path / "script.py"
    path.write_text("__return__ = 1\n")
    return str(path)


@pytest.fixture
def pkg_dir(tmp_path):
    root = tmp_path / "proj"
    (root / "pkg").mkdir(parents=True)
    (root / "pkg" / "__init__.py").write_text("")
    (root / "pkg" / "main.py").write_text("def run(session, x):\n    return x\n")
    (root / "pkg" / "data.csv").write_text("a,b\n")
    (root / "pkg" / "__pycache__").mkdir()
    (root / "pkg" / "__pycache__" / "main.cpython-310.pyc").write_text("x")
    (root / "top.py").write_text("print(1)\n")
    (root / "tests").mkdir()
    (root / "tests" / "test_a.py").write_text("")
    return str(root)


def last_statement(fake):
    return fake.statements[-1]


# ---- Program ----


def test_program_function_requires_callable():
    with pytest.raises(TypeError):
        Program.function("not callable")
    p = Program.function(lambda session: 1)
    assert p.kind == "function"
    assert "function" in repr(p)


def test_program_file_entrypoint(script):
    assert Program.file(script).entrypoint is None
    p = Program.file(script, "main")
    assert p.entrypoint == "script.py:main"
    assert p.split_entrypoint() == ("script.py", "main")
    assert Program.file(script).split_entrypoint() == (None, None)


def test_program_file_validation(tmp_path):
    with pytest.raises(ValueError, match="supports .py and .ipynb"):
        Program.file(str(tmp_path / "a.txt"))
    with pytest.raises(FileNotFoundError):
        Program.file(str(tmp_path / "missing.py"))
    nb = tmp_path / "nb.ipynb"
    nb.write_text("{}")
    assert Program.file(str(nb)).kind == "file"
    with pytest.raises(ValueError, match="only supported for .py"):
        Program.file(str(nb), "func")


def test_program_directory_and_stage_validation(tmp_path):
    with pytest.raises(NotADirectoryError):
        Program.directory(str(tmp_path / "nope"))
    with pytest.raises(ValueError, match="starting with '@'"):
        Program.stage("stage/dir")
    assert Program.stage("@s/x.zip", "a.py").kind == "stage"
    assert Program.workspace("snow://workspace/x").kind == "workspace"
    assert Program.ref("a.b.c").path == "a.b.c"


def test_split_entrypoint_forms_and_override():
    p = Program.directory(os.getcwd(), "pkg/main.py:run")
    assert p.split_entrypoint() == ("pkg/main.py", "run")
    assert p.split_entrypoint("other.py") == ("other.py", None)
    assert p.split_entrypoint("other.py:f") == ("other.py", "f")
    assert Program.directory(os.getcwd(), "pkg/main.py").split_entrypoint() == (
        "pkg/main.py",
        None,
    )


def test_directory_always_excludes_caches(pkg_dir):
    rels = [rel for _, rel in Program.directory(pkg_dir).iter_files()]
    assert "pkg/main.py" in rels and "top.py" in rels
    assert not any("__pycache__" in r or r.endswith(".pyc") for r in rels)


def test_directory_include_and_exclude(pkg_dir):
    rels = lambda p: sorted(rel for _, rel in p.iter_files())  # noqa: E731
    assert rels(Program.directory(pkg_dir, include=["*.py"])) == [
        "pkg/__init__.py",
        "pkg/main.py",
        "tests/test_a.py",
        "top.py",
    ]
    assert "tests/test_a.py" not in rels(Program.directory(pkg_dir, exclude=["tests"]))
    assert "pkg/data.csv" not in rels(Program.directory(pkg_dir, exclude=["*.csv"]))
    both = rels(Program.directory(pkg_dir, include=["*.py"], exclude=["tests"]))
    assert both == ["pkg/__init__.py", "pkg/main.py", "top.py"]


def test_iter_files_of_non_local_program():
    with pytest.raises(ValueError, match="no local files"):
        Program.stage("@s/x.zip").iter_files()


# ---- Environment ----


def test_environment_check_supported_names_the_field():
    env = Environment(env_vars={"A": "1"})
    with pytest.raises(ValueError, match="env_vars.*StoredProcedure"):
        env.check_supported("StoredProcedure", _sproc.SPROC_ENV_FIELDS)
    env.check_supported("CodeBundle", ["env_vars"])
    Environment().check_supported("X", [])


def test_requirements_file_parsing(tmp_path):
    req = tmp_path / "requirements.txt"
    req.write_text("# comment\npandas==2.2.0  # trailing\n\nnumpy\n")
    env = Environment(packages=["scipy"], requirements=str(req))
    assert env.resolved_packages() == ["scipy", "pandas==2.2.0", "numpy"]
    req.write_text("-r other.txt\n")
    with pytest.raises(ValueError, match="Unsupported option"):
        env.resolved_packages()


@pytest.mark.parametrize(
    "field, value",
    [
        ("env_vars", {"A": "1"}),
        ("stage_mounts", {"/mnt": "@s"}),
    ],
)
def test_stored_procedure_rejects_code_bundle_only_fields(jobs, script, field, value):
    with pytest.raises(ValueError, match=field):
        jobs.submit(Program.file(script), env=Environment(**{field: value}))


def test_stored_procedure_rejects_multiple_artifact_repositories(jobs, script):
    env = Environment(artifact_repositories=["a", "b"])
    with pytest.raises(ValueError, match="only one artifact repository"):
        jobs.submit(Program.file(script), env=env)


def test_code_bundle_rejects_imports(jobs, script):
    with pytest.raises(ValueError, match="imports.*CodeBundle"):
        jobs.submit(
            Program.file(script), CodeBundle(), env=Environment(imports=["@s/x.py"])
        )


def test_code_bundle_warehouse_rejects_secrets(jobs, script):
    env = Environment(secrets={"s": "db.sch.sec"})
    with pytest.raises(ValueError, match="secrets.*warehouse"):
        jobs.submit(Program.file(script), CodeBundle(), env=env)


def test_code_bundle_pool_accepts_secrets(fake, jobs, script):
    env = Environment(secrets={"s": "db.sch.sec"})
    run = jobs.submit(Program.file(script), CodeBundle(compute_pool="P"), env=env)
    assert isinstance(run, BundleRun)
    # the pool path creates a temporary named bundle first
    assert any(
        s.startswith("CREATE CODE BUNDLE DB.SCH.SNOWPARK_JOB_") for s in fake.statements
    )


# ---- target resolution ----


def test_execute_as_validation():
    with pytest.raises(ValueError, match="execute_as"):
        StoredProcedure(execute_as="root")
    for mode in ("caller", "owner", "restricted caller"):
        assert StoredProcedure(execute_as=mode).execute_as == mode


@pytest.mark.parametrize("mode", ["owner", "restricted caller"])
def test_submit_rejects_non_caller_execute_as(jobs, mode):
    with pytest.raises(ValueError, match="caller's rights"):
        jobs.submit(lambda session: 1, StoredProcedure(execute_as=mode))


def test_submit_rejects_execution_name_for_stored_procedure(jobs):
    with pytest.raises(ValueError, match="execution_name"):
        jobs.submit(lambda session: 1, execution_name="x")


def test_invalid_target_type(jobs):
    with pytest.raises(TypeError, match="StoredProcedure"):
        jobs.submit(lambda session: 1, "stored_procedure")
    with pytest.raises(TypeError):
        jobs.submit(42)


def test_stored_procedure_rejects_stage_directory(jobs):
    with pytest.raises(ValueError, match="stage directories"):
        jobs.submit(Program.stage("@stg/dir", "main.py"))
    with pytest.raises(ValueError, match="requires an entrypoint"):
        jobs.submit(Program.stage("@stg/code.zip"))


def test_stored_procedure_rejects_notebook_and_workspace(jobs, tmp_path):
    nb = tmp_path / "nb.ipynb"
    nb.write_text("{}")
    with pytest.raises(ValueError, match="Notebooks are not supported"):
        jobs.submit(Program.file(str(nb)))
    with pytest.raises(ValueError, match="only supported by the CodeBundle"):
        jobs.submit(Program.workspace("snow://workspace/x", "a.py"))


def test_code_bundle_rejects_function_program(jobs):
    with pytest.raises(ValueError, match="Function programs are not supported"):
        jobs.submit(lambda session: 1, CodeBundle())
    with pytest.raises(ValueError, match="Function programs are not supported"):
        jobs.submit(Program.function(lambda session: 1), CodeBundle(compute_pool="P"))


def test_code_bundle_rejects_function_entrypoint(jobs, script):
    with pytest.raises(ValueError, match="Function entrypoints"):
        jobs.submit(Program.file(script, "main"), CodeBundle())


def test_code_bundle_notebook_only_on_pool(jobs, tmp_path):
    nb = tmp_path / "nb.ipynb"
    nb.write_text("{}")
    with pytest.raises(ValueError, match="only supported on a compute pool"):
        jobs.submit(Program.file(str(nb)), CodeBundle())


def test_directory_entrypoint_validation(jobs, pkg_dir):
    with pytest.raises(ValueError, match="requires an entrypoint"):
        jobs.submit(Program.directory(pkg_dir))
    with pytest.raises(ValueError, match="An entrypoint is required"):
        jobs.submit(Program.directory(pkg_dir), CodeBundle())
    with pytest.raises(ValueError, match="not part of the program files"):
        jobs.submit(Program.directory(pkg_dir, "missing.py:f"))
    with pytest.raises(ValueError, match="not part of the program files"):
        jobs.submit(Program.directory(pkg_dir), CodeBundle(), entrypoint="nope.py")


def test_file_entrypoint_must_match_file(jobs, script):
    with pytest.raises(ValueError, match="does not match the file"):
        jobs.submit(Program.file(script), entrypoint="other.py")


def test_no_warehouse_is_rejected(script):
    jobs = JobManager(FakeSession(warehouse=None))
    with pytest.raises(ValueError, match="no current warehouse"):
        jobs.submit(lambda session: 1)
    with pytest.raises(ValueError, match="no current warehouse"):
        jobs.submit(Program.file(script), CodeBundle())


def test_deploy_requires_stage_for_stored_procedure(jobs):
    with pytest.raises(ValueError, match="requires stage="):
        jobs.deploy(lambda session: 1, name="P")


def test_deploy_ref_is_rejected(jobs):
    with pytest.raises(ValueError, match="already deployed"):
        jobs.deploy(Program.ref("a.b.c"), name="P", stage="@s")


def test_deploy_defaults_to_owner_and_honors_explicit_mode(fake, jobs):
    calls = []

    with mock.patch.object(
        _sproc, "_register", side_effect=lambda s, p, e, **x: calls.append(x)
    ):
        d = jobs.deploy(lambda session: 1, name="P", stage="@s")
        jobs.deploy(
            lambda session: 1,
            StoredProcedure(execute_as="caller"),
            name="P2",
            stage="@s",
            replace=True,
        )
    assert isinstance(d, ProcedureDeployment) and d.name == "DB.SCH.P"
    assert calls[0]["execute_as"] == "owner"
    assert calls[0]["is_permanent"] is True and calls[0]["replace"] is False
    assert calls[0]["stage_location"] == "@s" and calls[0]["name"] == "DB.SCH.P"
    assert calls[1]["execute_as"] == "caller" and calls[1]["replace"] is True


# ---- submit: SQL rendering ----


def test_stored_procedure_submit_builds_call(fake, jobs):
    captured = {}
    quoted = "x'y"

    class FakeSP:
        _anonymous_sp_sql = "WITH SP AS PROCEDURE (...) "
        name = "SP"

    fake._execute_sproc_internal = lambda **kw: captured.update(kw) or FakeAsyncJob(
        "Q9"
    )
    with mock.patch.object(_sproc, "_register", return_value=FakeSP()) as reg:
        job = jobs.submit(lambda session, a, b: 1, args=[quoted, 2])
    assert reg.call_args.kwargs["anonymous"] is True
    assert captured["query"] == (
        f"WITH SP AS PROCEDURE (...) CALL SP(ARRAY_CONSTRUCT({sql_str(quoted)}, 2)) "
        "/* snowpark.jobs */"
    )
    assert captured["block"] is False
    assert isinstance(job, QueryJob) and job.id == "query:Q9"
    assert job.target == "stored_procedure"


def test_render_args():
    assert _sproc.render_args(None) == "ARRAY_CONSTRUCT()"
    assert _sproc.render_args([1, 2.5, True, False, None, "s"]) == (
        "ARRAY_CONSTRUCT(1, 2.5, TRUE, FALSE, NULL, 's')"
    )
    # bool must not be rendered as an int
    assert _sproc.render_args([True]) == "ARRAY_CONSTRUCT(TRUE)"
    with pytest.raises(ValueError, match="str, int, float, bool or None"):
        _sproc.render_args([[1]])
    with pytest.raises(ValueError):
        _sproc.render_args([{"a": 1}])


def test_sql_str_escapes_quotes_and_backslashes():
    assert sql_str("plain") == "'plain'"
    assert sql_str("it's") == "'it''s'"
    escaped = sql_str("a\\b")
    assert escaped == "'a\\\\b'"
    # a trailing backslash plus quote must not terminate the literal early
    tricky = sql_str("\\'; DROP TABLE t; --")
    assert tricky.startswith("'\\\\") and tricky.endswith("'")
    inner = tricky[1:-1]
    assert "'" not in inner.replace("''", "").replace("\\'", "")


def test_call_sql_uses_args():
    assert _sproc.call_sql("A.B.C", ["x", 1]) == "CALL A.B.C(ARRAY_CONSTRUCT('x', 1))"


def test_procedure_deployment_to_sql_and_run(fake):
    d = ProcedureDeployment(fake, "DB.SCH.P")
    assert d.to_sql(args=[1]) == "CALL DB.SCH.P(ARRAY_CONSTRUCT(1))"
    with pytest.raises(ValueError, match="entrypoint"):
        d.to_sql(entrypoint="x.py")
    job = d.run(args=["a"])
    assert (
        last_statement(fake)
        == "CALL DB.SCH.P(ARRAY_CONSTRUCT('a')) /* snowpark.jobs */"
    )
    assert job.id == "query:QID_1"
    for kwargs in (
        {"execution_name": "n"},
        {"overrides": {"a": 1}},
        {"entrypoint": "e"},
    ):
        with pytest.raises(ValueError):
            d.run(**kwargs)
    d.drop()
    assert last_statement(fake) == "DROP PROCEDURE IF EXISTS DB.SCH.P(ARRAY)"


def test_code_bundle_submit_on_warehouse(fake, jobs, script):
    run = jobs.submit(
        Program.file(script),
        CodeBundle(),
        args=["a", "it's", "back\\slash"],
        execution_name="MY_RUN",
    )
    stmt = last_statement(fake)
    assert re.match(
        r"EXECUTE CODE BUNDLE FROM '@SESS_STAGE/snowpark_jobs/[0-9a-f]{64}' "
        r"ENTRYPOINT = 'script.py' ARGUMENTS = \('a', ",
        stmt,
    )
    assert "EXECUTION_NAME = 'MY_RUN'" in stmt
    assert "WITH SPECIFICATION" not in stmt  # the manifest was uploaded
    assert sql_str("it's") in stmt and sql_str("back\\slash") in stmt
    assert isinstance(run, BundleRun)
    assert run.id == "bundle:QID_1" and run.execution_name == "MY_RUN"
    assert run.compute_type == "warehouse"
    assert run._temp_bundle is None
    names = [name for name, _, _ in fake.file.puts]
    assert names[-1] == "code_bundle.yml" and "script.py" in names


def test_code_bundle_default_execution_name(fake, jobs, script):
    run = jobs.submit(Program.file(script), CodeBundle())
    assert re.fullmatch(r"SNOWPARK_JOB_SCRIPT_[0-9A-F]{8}", run.execution_name)


def test_code_bundle_rejects_non_string_args(fake, jobs, script):
    with pytest.raises(ValueError, match="must be strings"):
        jobs.submit(Program.file(script), CodeBundle(), args=[1])
    assert fake.statements == []
    with pytest.raises(ValueError, match="must be strings"):
        _bundle.check_args(["a", None])
    assert _bundle.check_args(None) == []


def test_code_bundle_pool_submit_creates_temp_bundle(fake, jobs, script):
    run = jobs.submit(Program.file(script), CodeBundle(compute_pool="POOL"), args=["x"])
    create, execute = fake.statements[-2:]
    assert re.match(
        r"CREATE CODE BUNDLE DB\.SCH\.SNOWPARK_JOB_[0-9A-F]{12} FROM "
        r"'@SESS_STAGE/snowpark_jobs/",
        create,
    )
    temp = run._temp_bundle
    assert execute.startswith(f"EXECUTE CODE BUNDLE {temp} ENTRYPOINT")
    assert run.compute_type == "compute_pool"
    # the temporary bundle is dropped once the run is seen to finish
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [{"STATUS": "DONE"}]
    assert run.status() == JobStatus.SUCCEEDED
    assert f"DROP CODE BUNDLE IF EXISTS {temp}" in fake.statements
    assert run._temp_bundle is None


def test_pre_staged_program_sends_specification(fake, jobs):
    jobs.submit(Program.stage("@STG/bundle/", "main.py"), CodeBundle(), args=["a"])
    stmt = last_statement(fake)
    assert stmt.startswith(
        "EXECUTE CODE BUNDLE FROM '@STG/bundle' ENTRYPOINT = 'main.py'"
    )
    assert "WITH SPECIFICATION $$\nbundle:" in stmt and stmt.endswith("$$")
    assert fake.file.puts == []


def test_pre_staged_program_rejects_packages(jobs):
    with pytest.raises(ValueError, match="cannot be added to a pre-staged"):
        jobs.submit(
            Program.stage("@STG/bundle", "main.py"),
            CodeBundle(),
            env=Environment(packages=["pandas"]),
        )


def test_dollar_dollar_in_spec_is_rejected(jobs):
    with pytest.raises(ValueError, match=r"\$\$"):
        jobs.submit(
            Program.stage("@STG/bundle", "main.py"),
            CodeBundle(),
            env=Environment(env_vars={"X": "a$$b"}),
        )


def test_code_bundle_entry_required():
    with pytest.raises(ValueError, match="entrypoint is required"):
        _bundle.execute_sql("A.B.C", None)


# ---- ref resolution ----


def _bundle_rows():
    return {r"SHOW CODE BUNDLES": [{"name": "MYB"}]}


def test_ref_resolves_code_bundle_and_runs(fake, jobs):
    fake.responses.update(_bundle_rows())
    run = jobs.submit(Program.ref("myb"), entrypoint="a.py", args=["x"])
    assert isinstance(run, BundleRun)
    assert last_statement(fake).startswith(
        "EXECUTE CODE BUNDLE DB.SCH.myb ENTRYPOINT = 'a.py' ARGUMENTS = ('x')"
    )
    dep = jobs.get_deployment("myb")
    assert isinstance(dep, CodeBundleDeployment) and dep.target == "code_bundle"


def test_ref_resolves_procedure(fake, jobs):
    fake.responses[r"SHOW PROCEDURES"] = [
        {"name": "MYP", "is_builtin": "N", "arguments": "MYP (ARRAY) RETURN VARIANT"}
    ]
    job = jobs.submit(Program.ref("DB.SCH.MYP"), args=[1])
    assert isinstance(job, QueryJob)
    assert "CALL DB.SCH.MYP(ARRAY_CONSTRUCT(1))" in last_statement(fake)
    assert jobs.get_deployment("MYP").target == "stored_procedure"


def test_ref_conflicting_explicit_target(fake, jobs):
    fake.responses[r"SHOW PROCEDURES"] = [
        {"name": "MYP", "is_builtin": "N", "arguments": "MYP (ARRAY) RETURN VARIANT"}
    ]
    with pytest.raises(ValueError, match="conflicts with the given target"):
        jobs.submit(Program.ref("MYP"), CodeBundle())
    fake.responses.update(_bundle_rows())
    with pytest.raises(ValueError, match="conflicts with the given target"):
        jobs.submit(Program.ref("MYB"), StoredProcedure())
    # a matching explicit target is fine
    assert isinstance(
        jobs.submit(Program.ref("MYB"), CodeBundle(), entrypoint="a.py"), BundleRun
    )


def test_ref_rejects_env_and_stage(fake, jobs):
    with pytest.raises(ValueError, match="cannot be combined"):
        jobs.submit(Program.ref("X"), env=Environment())
    with pytest.raises(ValueError, match="cannot be combined"):
        jobs.submit(Program.ref("X"), stage="@s")


def test_get_deployment_errors(fake, jobs):
    with pytest.raises(ValueError, match="No code bundle or stored procedure"):
        jobs.get_deployment("nothing")
    fake.responses[r"SHOW PROCEDURES"] = [
        {"name": "ODD", "is_builtin": "N", "arguments": "ODD (NUMBER) RETURN VARIANT"}
    ]
    with pytest.raises(ValueError, match="only procedures with the"):
        jobs.get_deployment("ODD")


def test_get_deployment_uses_unquoted_pattern(fake, jobs):
    with pytest.raises(ValueError):
        jobs.get_deployment('"Mixed Case"')
    assert any("LIKE 'Mixed Case'" in s for s in fake.statements)
    assert any("IN SCHEMA DB.SCH" in s for s in fake.statements)


# ---- identifiers ----


@pytest.mark.parametrize(
    "name",
    [
        "a;drop table x",
        "a b",
        "a-b",
        'x"; drop table t; --',
        "a' or '1'='1",
        "",
        "  ",
        "a.b.c.d",
        "1abc",
    ],
)
def test_injection_like_names_are_rejected(jobs, fake, name):
    with pytest.raises(ValueError):
        validate_name(name)
    with pytest.raises(ValueError):
        qualify(fake, name)
    with pytest.raises(ValueError):
        jobs.deploy(lambda session: 1, name=name, stage="@s")
    with pytest.raises(ValueError):
        jobs.get_deployment(name)


def test_quoted_and_qualified_names_are_accepted(fake):
    assert qualify(fake, "p") == "DB.SCH.p"
    assert qualify(fake, "s.p") == "DB.s.p"
    assert qualify(fake, "d.s.p") == "d.s.p"
    assert qualify(fake, '"my proc"') == 'DB.SCH."my proc"'
    assert validate_name('"a b"') == '"a b"'
    assert split_fqn('d."s s".p') == ("d", '"s s"', "p")
    with pytest.raises(ValueError):
        split_fqn("p")
    assert unquote_ident('"Ab""c"') == 'Ab"c'
    assert unquote_ident("ab") == "AB"


def test_identifier_safe():
    assert identifier_safe("my-script.py") == "MY_SCRIPT_PY"
    assert identifier_safe("1abc") == "J_1ABC"
    assert identifier_safe("!!!") == "J_"
    assert len(identifier_safe("x" * 100, 10)) == 10


def test_schedule_validates_names(fake):
    d = ProcedureDeployment(fake, "DB.SCH.P")
    with pytest.raises(ValueError):
        d.schedule("0 * * * * UTC", name="t; drop")
    with pytest.raises(ValueError):
        d.schedule("0 * * * * UTC", warehouse="wh; drop")
    with pytest.raises(ValueError, match="Invalid warehouse"):
        d.schedule("0 * * * * UTC", warehouse="a.b")
    assert fake.statements == []


def test_schedule_sql(fake):
    d = ProcedureDeployment(fake, "DB.SCH.P")
    sched = d.schedule("0 * * * * UTC", warehouse="WH", args=["it's"])
    create, resume = fake.statements[0:2]
    assert create.startswith(
        "CREATE OR REPLACE TASK DB.SCH.P_SCHEDULE WAREHOUSE = WH "
        "SCHEDULE = 'USING CRON 0 * * * * UTC' AS CALL DB.SCH.P(ARRAY_CONSTRUCT("
    )
    assert resume == "ALTER TASK DB.SCH.P_SCHEDULE RESUME"
    assert sched.name == "DB.SCH.P_SCHEDULE"
    # serverless without a warehouse, explicit short name goes into the deployment schema
    d.schedule("0 * * * * UTC", name="T1")
    assert any(
        s.startswith("CREATE OR REPLACE TASK DB.SCH.T1 SCHEDULE =")
        for s in fake.statements
    )


def test_schedule_default_name_is_quoted_when_needed(fake):
    ProcedureDeployment(fake, 'DB.SCH."my proc"').schedule("0 * * * * UTC")
    assert fake.statements[0].startswith(
        'CREATE OR REPLACE TASK DB.SCH."my proc_SCHEDULE"'
    )


def test_schedule_methods(fake):
    sched = ProcedureDeployment(fake, "DB.SCH.P").schedule("0 * * * * UTC")
    sched.suspend()
    assert last_statement(fake) == "ALTER TASK DB.SCH.P_SCHEDULE SUSPEND"
    sched.resume()
    assert last_statement(fake) == "ALTER TASK DB.SCH.P_SCHEDULE RESUME"
    sched.run_now()
    assert last_statement(fake) == "EXECUTE TASK DB.SCH.P_SCHEDULE"
    sched.drop()
    assert last_statement(fake) == "DROP TASK IF EXISTS DB.SCH.P_SCHEDULE"


def test_schedule_runs_skip_runs_of_earlier_task_with_same_name(fake):
    from datetime import datetime

    created = datetime(2025, 1, 2)
    fake.responses[r"SHOW TASKS"] = [{"name": "P_SCHEDULE", "created_on": created}]
    fake.responses[r"TASK_HISTORY"] = [
        {
            "QUERY_ID": "new",
            "SCHEDULED_TIME": datetime(2025, 1, 3),
            "STATE": "SUCCEEDED",
        },
        {
            "QUERY_ID": "old",
            "SCHEDULED_TIME": datetime(2025, 1, 1),
            "STATE": "SUCCEEDED",
        },
    ]
    sched = ProcedureDeployment(fake, "DB.SCH.P").schedule("0 * * * * UTC")
    runs = sched.runs()
    assert [r.query_id for r in runs] == ["new"]
    assert runs[0].id == "task:DB.SCH.P_SCHEDULE/new"
    assert runs[0].target == "stored_procedure"
    assert (
        "DB.INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'P_SCHEDULE'"
        in [s for s in fake.statements if "TASK_HISTORY" in s][0]
    )


# ---- code bundle deployment ----


def test_code_bundle_deploy_and_run_sql(fake, jobs, pkg_dir):
    dep = jobs.deploy(
        Program.directory(pkg_dir, "top.py"), CodeBundle(), name="B", replace=True
    )
    assert isinstance(dep, CodeBundleDeployment) and dep.name == "DB.SCH.B"
    assert re.fullmatch(
        r"CREATE OR REPLACE CODE BUNDLE DB\.SCH\.B FROM '@SESS_STAGE/snowpark_jobs/[0-9a-f]{64}'",
        last_statement(fake),
    )
    jobs.deploy(Program.directory(pkg_dir, "top.py"), CodeBundle(), name="B2")
    assert last_statement(fake).startswith("CREATE CODE BUNDLE DB.SCH.B2 FROM ")
    assert dep.to_sql(args=["a"]) == (
        "EXECUTE CODE BUNDLE DB.SCH.B ENTRYPOINT = 'top.py' ARGUMENTS = ('a')"
    )
    assert dep.to_sql(entrypoint="pkg/main.py") == (
        "EXECUTE CODE BUNDLE DB.SCH.B ENTRYPOINT = 'pkg/main.py'"
    )
    with pytest.raises(ValueError, match="must be strings"):
        dep.to_sql(args=[1])
    dep.drop()
    assert last_statement(fake) == "DROP CODE BUNDLE IF EXISTS DB.SCH.B"


def test_code_bundle_deploy_without_entrypoint(fake, jobs, pkg_dir):
    dep = jobs.deploy(Program.directory(pkg_dir), CodeBundle(), name="B")
    with pytest.raises(ValueError, match="no default entrypoint"):
        dep.to_sql()
    run = dep.run(entrypoint="top.py", execution_name="E1")
    assert "EXECUTION_NAME = 'E1'" in last_statement(fake)
    assert run.execution_name == "E1"


def test_code_bundle_run_overrides(fake, jobs, pkg_dir):
    dep = jobs.deploy(
        Program.directory(pkg_dir, "top.py"),
        CodeBundle(),
        name="B",
        env=Environment(env_vars={"A": "1"}),
    )
    dep.run(overrides={"env_vars": [{"Z": "2"}]})
    stmt = last_statement(fake)
    spec = yaml.safe_load(stmt.split("WITH SPECIFICATION $$\n", 1)[1][:-2])
    assert spec["bundle"]["env_vars"] == [{"Z": "2"}]
    assert spec["bundle"]["compute_type"] == "warehouse"
    # no overrides: no specification clause
    dep.run()
    assert "WITH SPECIFICATION" not in last_statement(fake)
    # a deployment loaded without its spec cannot be overridden
    loaded = CodeBundleDeployment(fake, "DB.SCH.B", spec=None, entrypoint="top.py")
    with pytest.raises(
        ValueError, match="overrides need the deployment's specification"
    ):
        loaded.run(overrides={"a": 1})


def test_code_bundle_add_version(fake, jobs, pkg_dir, tmp_path):
    dep = jobs.deploy(Program.directory(pkg_dir, "top.py"), CodeBundle(), name="B")
    extra = tmp_path / "new.py"
    extra.write_text("print(2)\n")
    dep.add_version(Program.file(str(extra)))
    assert re.fullmatch(
        r"ALTER CODE BUNDLE DB\.SCH\.B ADD VERSION FROM '@SESS_STAGE/snowpark_jobs/[0-9a-f]{64}'",
        last_statement(fake),
    )
    assert dep.to_sql() == "EXECUTE CODE BUNDLE DB.SCH.B ENTRYPOINT = 'new.py'"


def test_code_bundle_schedule_sql(fake, jobs, pkg_dir):
    dep = jobs.deploy(Program.directory(pkg_dir, "top.py"), CodeBundle(), name="B")
    dep.schedule("0 0 * * * UTC", args=["x"])
    create = [s for s in fake.statements if s.startswith("CREATE OR REPLACE TASK")][0]
    assert create == (
        "CREATE OR REPLACE TASK DB.SCH.B_SCHEDULE SCHEDULE = 'USING CRON 0 0 * * * UTC' "
        "AS EXECUTE CODE BUNDLE DB.SCH.B ENTRYPOINT = 'top.py' ARGUMENTS = ('x')"
    )


# ---- manifest / spec ----


def test_warehouse_spec_defaults():
    spec = build_spec(
        compute_pool=None,
        query_warehouse=None,
        runtime_version=None,
        env=Environment(),
        has_requirements=False,
    )
    assert spec == {
        "bundle": {
            "type": "custom",
            "language": "python",
            "compute_type": "warehouse",
            "compute_options": {"runtime_version": default_warehouse_runtime_version()},
        }
    }
    assert re.fullmatch(r"3\.(10|11|12|13)", default_warehouse_runtime_version())


def test_warehouse_runtime_version_is_clamped():
    for version, expected in [((3, 8), "3.10"), ((3, 11), "3.11"), ((3, 15), "3.13")]:
        with mock.patch(
            "snowflake.snowpark.jobs._bundle_spec.sys.version_info", version + (0,)
        ):
            assert default_warehouse_runtime_version() == expected


def test_pool_spec_full():
    env = Environment(
        env_vars={"A": 1, "B": "x"},
        secrets={"tok": "db.sch.sec"},
        external_access_integrations=["EAI"],
        artifact_repositories=["REPO"],
        stage_mounts={"/mnt/data": "@stg/data", "/other": "@stg2"},
    )
    spec = build_spec(
        compute_pool="POOL",
        query_warehouse="QWH",
        runtime_version=None,
        env=env,
        has_requirements=True,
    )["bundle"]
    assert spec["compute_type"] == "compute_pool"
    assert spec["compute_options"] == {
        "compute_pool": "POOL",
        "query_warehouse": "QWH",
        "runtime_version": DEFAULT_POOL_RUNTIME_VERSION,
    }
    assert DEFAULT_POOL_RUNTIME_VERSION == "V2.9-CPU-PY3.12"
    assert spec["properties"] == {"requirements_file": "requirements.txt"}
    assert spec["env_vars"] == [{"A": "1"}, {"B": "x"}]
    assert spec["secrets"] == ["db.sch.sec"]
    assert spec["external_access_integrations"] == ["EAI"]
    assert spec["artifact_repositories"] == ["REPO"]
    assert spec["stage_mounts"] == {
        "mnt_data_0": {"stage_url": "@stg/data", "mount_path": "/mnt/data"},
        "other_1": {"stage_url": "@stg2", "mount_path": "/other"},
    }


def test_explicit_runtime_version_and_spec_dump():
    spec = build_spec(
        compute_pool=None,
        query_warehouse=None,
        runtime_version="3.11",
        env=Environment(),
        has_requirements=False,
    )
    assert spec["bundle"]["compute_options"]["runtime_version"] == "3.11"
    text = dump_spec(spec)
    assert text.startswith("bundle:\n  type: custom")
    assert yaml.safe_load(text) == spec


def test_pool_spec_uses_session_warehouse_as_query_warehouse(fake, jobs):
    jobs.submit(Program.stage("@STG/b", "m.py"), CodeBundle(compute_pool="POOL"))
    spec = yaml.safe_load(
        last_statement(fake).split("WITH SPECIFICATION $$\n", 1)[1][:-2]
    )
    assert spec["bundle"]["compute_options"]["query_warehouse"] == "WH"


def test_merge_overrides_forms():
    spec = {"bundle": {"compute_options": {"runtime_version": "3.12"}, "x": 1}}
    merged = merge_overrides(spec, {"compute_options": {"runtime_version": "3.11"}})
    assert merged["bundle"]["compute_options"]["runtime_version"] == "3.11"
    assert merged["bundle"]["x"] == 1
    merged = merge_overrides(spec, {"bundle": {"x": 2}})
    assert merged["bundle"]["x"] == 2
    assert (
        spec["bundle"]["x"] == 1
        and spec["bundle"]["compute_options"]["runtime_version"] == "3.12"
    )  # original untouched


def test_requirements_file_is_uploaded_with_packages(fake, jobs, script):
    jobs.submit(
        Program.file(script),
        CodeBundle(),
        env=Environment(packages=["pandas==2.2.0", "numpy"]),
    )
    uploads = {name: data for name, _, data in fake.file.puts}
    assert uploads["requirements.txt"] == b"pandas==2.2.0\nnumpy\n"
    manifest = yaml.safe_load(uploads["code_bundle.yml"])
    assert manifest["bundle"]["properties"] == {"requirements_file": "requirements.txt"}
    assert [n for n, _, _ in fake.file.puts][-1] == "code_bundle.yml"


def test_user_requirements_file_is_replaced_by_generated(fake, jobs, pkg_dir):
    open(os.path.join(pkg_dir, "requirements.txt"), "w").write("old\n")
    jobs.submit(
        Program.directory(pkg_dir, "top.py"),
        CodeBundle(),
        env=Environment(packages=["new"]),
    )
    reqs = [data for name, _, data in fake.file.puts if name == "requirements.txt"]
    assert reqs == [b"new\n"]


def test_user_manifest_is_replaced_by_generated(fake, jobs, pkg_dir):
    open(os.path.join(pkg_dir, "code_bundle.yml"), "w").write("bundle: {type: evil}\n")
    jobs.submit(Program.directory(pkg_dir, "top.py"), CodeBundle())
    manifests = [data for name, _, data in fake.file.puts if name == "code_bundle.yml"]
    assert len(manifests) == 1 and b"evil" not in manifests[0]


def test_subdirectories_are_uploaded_to_matching_stage_paths(fake, jobs, pkg_dir):
    jobs.submit(Program.directory(pkg_dir, "top.py"), CodeBundle())
    dests = {
        (name, dest.split("/", 3)[-1].split("/", 1)[-1])
        for name, dest, _ in fake.file.puts
    }
    assert ("main.py", "pkg") in dests
    assert ("test_a.py", "tests") in dests
    assert not any(name.endswith(".pyc") for name, _ in dests)


def test_upload_is_skipped_when_content_exists(fake, jobs, script):
    fake.responses[r"^LIST "] = [{"name": "x"}]
    jobs.submit(Program.file(script), CodeBundle())
    assert fake.file.puts == []
    assert last_statement(fake).startswith("EXECUTE CODE BUNDLE FROM")


def test_custom_stage_is_used(fake, jobs, script):
    jobs.submit(Program.file(script), CodeBundle(), stage="my_stage/")
    assert "FROM '@my_stage/snowpark_jobs/" in last_statement(fake)


# ---- content hash ----


def _hash(program, manifest="m", requirements=None):
    files = _packaging.collect_files(program)
    return _packaging.content_hash(files, manifest, requirements)


def test_content_hash_is_stable_and_content_addressed(pkg_dir):
    p = Program.directory(pkg_dir)
    first = _hash(p)
    assert first == _hash(Program.directory(pkg_dir))
    assert re.fullmatch(r"[0-9a-f]{64}", first)
    assert _hash(p, manifest="other") != first
    assert _hash(p, requirements="pandas\n") != first
    with open(os.path.join(pkg_dir, "top.py"), "a") as f:
        f.write("# changed\n")
    assert _hash(p) != first


def test_content_hash_ignores_user_generated_files_and_mtime(pkg_dir):
    base = _hash(Program.directory(pkg_dir), requirements="a\n")
    open(os.path.join(pkg_dir, "requirements.txt"), "w").write("user\n")
    open(os.path.join(pkg_dir, "code_bundle.yml"), "w").write("user: 1\n")
    assert _hash(Program.directory(pkg_dir), requirements="a\n") == base
    os.utime(os.path.join(pkg_dir, "top.py"), (1, 1))
    assert _hash(Program.directory(pkg_dir), requirements="a\n") == base


def test_content_hash_depends_on_file_path(tmp_path):
    a = tmp_path / "a"
    b = tmp_path / "b"
    for d, name in ((a, "x.py"), (b, "y.py")):
        d.mkdir()
        (d / name).write_text("same\n")
    assert _hash(Program.directory(str(a))) != _hash(Program.directory(str(b)))


def test_normalize_stage():
    assert _packaging.normalize_stage(" @s/ ") == "@s"
    assert _packaging.normalize_stage("s/p/") == "@s/p"


# ---- status mapping ----


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("PENDING", JobStatus.PENDING),
        ("RUNNING", JobStatus.RUNNING),
        ("DONE", JobStatus.SUCCEEDED),
        ("FAILED", JobStatus.FAILED),
        ("CANCELLED", JobStatus.CANCELLED),
    ],
)
def test_bundle_history_status_mapping(fake, raw, expected):
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [{"STATUS": raw, "ERROR_MESSAGE": "bad"}]
    run = BundleRun(fake, "Q1")
    assert run.raw_status == raw
    assert run.status() == expected
    assert run.status().is_terminal == (
        expected not in (JobStatus.PENDING, JobStatus.RUNNING)
    )
    if expected == JobStatus.FAILED:
        assert run.error() == "bad"
    else:
        assert run.error() is None


def test_unknown_bundle_status_is_running(fake):
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [{"STATUS": "WEIRD"}]
    assert BundleRun(fake, "Q1").status() == JobStatus.RUNNING


@pytest.mark.parametrize(
    "query_status, expected",
    [
        ("FAILED_WITH_ERROR", "FAILED"),
        ("FAILED_WITH_INCIDENT", "FAILED"),
        ("ABORTED", "CANCELLED"),
        ("ABORTING", "CANCELLED"),
        ("QUEUED", "PENDING"),
        ("RUNNING", "PENDING"),
    ],
)
def test_bundle_without_history_row_falls_back_to_query_status(
    fake, query_status, expected
):
    with mock.patch.object(_handles, "_query_status_name", return_value=query_status):
        assert BundleRun(fake, "Q1").raw_status == expected


@pytest.mark.parametrize(
    "state, expected",
    [
        ("SCHEDULED", JobStatus.PENDING),
        ("EXECUTING", JobStatus.RUNNING),
        ("SUCCEEDED", JobStatus.SUCCEEDED),
        ("FAILED", JobStatus.FAILED),
        ("FAILED_AND_AUTO_SUSPENDED", JobStatus.FAILED),
        ("CANCELLED", JobStatus.CANCELLED),
        ("SKIPPED", JobStatus.CANCELLED),
    ],
)
def test_task_state_mapping(fake, state, expected):
    fake.responses[r"TASK_HISTORY"] = [
        {"QUERY_ID": "Q1", "STATE": state, "ERROR_MESSAGE": "task broke"}
    ]
    run = TaskRun(fake, "DB.SCH.T", "Q1")
    assert run.status() == expected
    if expected == JobStatus.FAILED:
        assert run.error() == "task broke"


def test_task_run_without_history_is_pending(fake):
    assert TaskRun(fake, "DB.SCH.T", "Q1").status() == JobStatus.PENDING


def test_task_run_target_and_child(fake):
    fake.responses[r"TASK_HISTORY"] = [
        {
            "QUERY_ID": "Q1",
            "QUERY_TEXT": "EXECUTE CODE BUNDLE DB.SCH.B ENTRYPOINT = 'a'",
        }
    ]
    run = TaskRun(fake, "DB.SCH.T", "Q1")
    assert run.target == "code_bundle"
    assert isinstance(run.child(), BundleRun) and run.child().query_id == "Q1"
    sp = TaskRun(fake, "DB.SCH.T", "Q2", {"QUERY_TEXT": "CALL P(ARRAY_CONSTRUCT())"})
    assert sp.target == "stored_procedure" and isinstance(sp.child(), QueryJob)


@pytest.mark.parametrize(
    "name, expected",
    [
        ("SUCCESS", JobStatus.SUCCEEDED),
        ("FAILED_WITH_ERROR", JobStatus.FAILED),
        ("FAILED_WITH_INCIDENT", JobStatus.FAILED),
        ("ABORTED", JobStatus.CANCELLED),
        ("QUEUED", JobStatus.PENDING),
        ("RESUMING_WAREHOUSE", JobStatus.PENDING),
        ("RUNNING", JobStatus.RUNNING),
    ],
)
def test_query_status_mapping(name, expected):
    assert _handles._map_query_status(name) == expected


def test_bundle_result_is_not_supported(fake):
    with pytest.raises(JobNotSupportedError):
        BundleRun(fake, "Q1").result()


def test_bundle_logs_on_pool_not_supported(fake):
    with pytest.raises(JobNotSupportedError):
        BundleRun(fake, "Q1", "compute_pool").logs()
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [
        {"STATUS": "DONE", "COMPUTE_TYPE": "COMPUTE_POOL"}
    ]
    with pytest.raises(JobNotSupportedError):
        BundleRun(fake, "Q1").logs()


def test_logs_query_uses_event_table_and_escapes_query_id(fake):
    fake.responses[r"SHOW PARAMETERS"] = [{"value": "DB.SCH.EVENTS"}]
    BundleRun(fake, "Q'1", "warehouse").logs(limit=5)
    stmt = last_statement(fake)
    assert "FROM DB.SCH.EVENTS WHERE RECORD_TYPE = 'LOG'" in stmt
    assert sql_str("Q'1") in stmt and stmt.endswith("ORDER BY TIMESTAMP DESC LIMIT 5")


def test_logs_without_event_table(fake):
    with pytest.raises(RuntimeError, match="event table"):
        BundleRun(fake, "Q1", "warehouse").logs()


def test_cancel_issues_system_cancel(fake):
    qid = "Q'1"
    BundleRun(fake, qid).cancel()
    assert last_statement(fake) == f"SELECT SYSTEM$CANCEL_QUERY({sql_str(qid)})"


def test_wait_times_out(fake):
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [{"STATUS": "RUNNING"}]
    with pytest.raises(TimeoutError, match="did not finish"):
        BundleRun(fake, "Q1").wait(timeout=0.05, poll_interval=0.01)
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [{"STATUS": "DONE"}]
    assert BundleRun(fake, "Q1").wait(timeout=1) == JobStatus.SUCCEEDED


def test_owner_rights_hint_is_added_to_query_errors(fake):
    job = QueryJob(fake, FakeAsyncJob("Q1"))
    with mock.patch.object(
        _handles, "_query_status_name", return_value="FAILED_WITH_ERROR"
    ), mock.patch.object(
        _handles,
        "_query_error",
        return_value="not allowed in an owner's rights context",
    ):
        assert "Hint:" in job.error()
    with mock.patch.object(_handles, "_query_status_name", return_value="SUCCESS"):
        assert job.error() is None


# ---- get() ----


def test_get_parses_job_ids(fake, jobs):
    q = jobs.get("query:01abc")
    assert isinstance(q, QueryJob) and q.query_id == "01abc" and q.id == "query:01abc"
    b = jobs.get("bundle:01abc")
    assert isinstance(b, BundleRun) and b.id == "bundle:01abc"
    t = jobs.get("task:DB.SCH.MY_TASK/01abc")
    assert isinstance(t, TaskRun)
    assert (t.task_fqn, t.query_id) == ("DB.SCH.MY_TASK", "01abc")
    assert t.id == "task:DB.SCH.MY_TASK/01abc"
    quoted = jobs.get('task:DB."S/1".T/01abc')
    assert quoted.task_fqn == 'DB."S/1".T' and quoted.query_id == "01abc"


@pytest.mark.parametrize(
    "job_id",
    [
        "",
        "01abc",
        "query:",
        ":01abc",
        "other:01abc",
        "task:nosep",
        "task:/qid",
        "task:T/",
    ],
)
def test_get_rejects_malformed_ids(jobs, job_id):
    with pytest.raises(ValueError):
        jobs.get(job_id)


# ---- list() ----


def test_list_validates_arguments(jobs):
    with pytest.raises(ValueError, match="target must be"):
        jobs.list(target="task")
    with pytest.raises(ValueError):
        jobs.list(status="BOGUS")


def test_list_merges_and_filters(fake, jobs):
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [
        {
            "QUERY_ID": "B1",
            "START_TIME": "2025-01-02",
            "STATUS": "DONE",
            "EXECUTION_NAME": "SNOWPARK_JOB_X",
            "COMPUTE_TYPE": "WAREHOUSE",
        },
        {
            "QUERY_ID": "B2",
            "START_TIME": "2025-01-04",
            "STATUS": "FAILED",
            "EXECUTION_NAME": "SNOWPARK_JOB_Y",
            "COMPUTE_TYPE": "WAREHOUSE",
        },
    ]

    fake.responses[r"QUERY_HISTORY_BY_USER"] = []
    jobs_all = jobs.list(target=CodeBundle())
    assert [j.id for j in jobs_all] == ["bundle:B2", "bundle:B1"]
    assert jobs_all[0].execution_name == "SNOWPARK_JOB_Y"
    failed = jobs.list(target="code_bundle", status="FAILED")
    assert [j.id for j in failed] == ["bundle:B2"]
    assert [
        j.id for j in jobs.list(target="code_bundle", status=JobStatus.SUCCEEDED)
    ] == ["bundle:B1"]
    assert len(jobs.list(target="code_bundle", limit=1)) == 1
    stmts = " ".join(fake.statements)
    assert "STARTSWITH(EXECUTION_NAME, 'SNOWPARK_JOB_')" in stmts
    # a code bundle only listing does not look at the query history
    assert "QUERY_HISTORY_BY_USER" not in stmts


def test_list_stored_procedures_uses_marker_without_self_match(fake, jobs):
    jobs.list(target="stored_procedure")
    stmt = last_statement(fake)
    assert "QUERY_HISTORY_BY_USER" in stmt
    # the statement must not contain the marker itself, or it would list itself
    assert "/* snowpark.jobs */" not in stmt
    assert "CODE_BUNDLE_HISTORY" not in stmt


# ---- Session.jobs ----


def test_session_jobs_not_supported_in_local_testing():
    from snowflake.snowpark.mock._connection import MockServerConnection

    session = Session(
        MockServerConnection(options={"disable_local_testing_telemetry": True})
    )
    with pytest.raises(NotImplementedError):
        session.jobs


def test_session_jobs_is_cached():
    conn = mock.MagicMock()
    conn._conn = mock.MagicMock()
    conn._thread_safe_session_enabled = True
    session = Session(conn)
    assert isinstance(session.jobs, JobManager)
    assert session.jobs is session.jobs


# ---- warehouse setting ----


def _uses(fake):
    return [s for s in fake.statements if s.startswith("USE WAREHOUSE")]


def _around_execute(fake):
    """The statements from the first USE WAREHOUSE to the last, as a compact list."""
    out = []
    for s in fake.statements:
        if s.startswith("USE WAREHOUSE"):
            out.append(s)
        elif s.startswith("EXECUTE CODE BUNDLE"):
            out.append("EXECUTE")
    return out


def test_bundle_submit_switches_warehouse_and_restores(fake, jobs, script):
    jobs.submit(Program.file(script), CodeBundle(warehouse="OTHER"))
    assert _around_execute(fake) == [
        "USE WAREHOUSE OTHER",
        "EXECUTE",
        "USE WAREHOUSE WH",
    ]
    assert fake.get_current_warehouse() == "WH"
    # the EXECUTE is issued between the two switches
    stmts = fake.statements
    assert (
        stmts.index("USE WAREHOUSE OTHER")
        < [i for i, s in enumerate(stmts) if s.startswith("EXECUTE")][0]
        < stmts.index("USE WAREHOUSE WH")
    )


@pytest.mark.parametrize("warehouse", [None, "WH", '"WH"', "wh"])
def test_bundle_submit_does_not_switch_without_need(fake, jobs, script, warehouse):
    jobs.submit(Program.file(script), CodeBundle(warehouse=warehouse))
    assert _uses(fake) == []


def test_bundle_submit_restores_when_submission_raises(fake, jobs, script):
    original = fake.sql

    def failing(statement):
        if statement.startswith("EXECUTE CODE BUNDLE"):
            raise RuntimeError("submit failed")
        return original(statement)

    fake.sql = failing
    with pytest.raises(RuntimeError, match="submit failed"):
        jobs.submit(Program.file(script), CodeBundle(warehouse="OTHER"))
    assert _uses(fake) == ["USE WAREHOUSE OTHER", "USE WAREHOUSE WH"]
    assert fake.get_current_warehouse() == "WH"


def test_bundle_submit_without_session_warehouse_uses_target(script):
    fake = FakeSession(warehouse=None)
    JobManager(fake).submit(Program.file(script), CodeBundle(warehouse="OTHER"))
    # nothing to restore: the session stays on the target warehouse
    assert _uses(fake) == ["USE WAREHOUSE OTHER"]
    assert fake.get_current_warehouse() == "OTHER"
    with pytest.raises(ValueError, match="no current warehouse"):
        JobManager(FakeSession(warehouse=None)).submit(
            Program.file(script), CodeBundle()
        )


def test_switch_holds_the_session_lock(fake, jobs, script):
    held = []
    original = fake.use_warehouse

    def tracking(warehouse):
        # RLock: a second non-blocking acquire from another thread must fail
        result = []
        t = threading.Thread(target=lambda: result.append(fake._lock.acquire(False)))
        t.start()
        t.join()
        held.append(not result[0])
        original(warehouse)

    fake.use_warehouse = tracking
    jobs.submit(Program.file(script), CodeBundle(warehouse="OTHER"))
    assert held == [True, True]


def test_bundle_deployment_run_switches_warehouse(fake, jobs, pkg_dir):
    dep = jobs.deploy(
        Program.directory(pkg_dir, "top.py"), CodeBundle(warehouse="OTHER"), name="B"
    )
    assert _uses(fake) == []  # deploy does not run anything
    dep.run()
    assert _around_execute(fake) == [
        "USE WAREHOUSE OTHER",
        "EXECUTE",
        "USE WAREHOUSE WH",
    ]
    assert fake.get_current_warehouse() == "WH"
    # to_sql is a pure rendering
    n = len(fake.statements)
    dep.to_sql()
    assert len(fake.statements) == n


def test_bundle_deployment_run_without_target_warehouse(fake, jobs, pkg_dir):
    dep = jobs.deploy(Program.directory(pkg_dir, "top.py"), CodeBundle(), name="B")
    dep.run()
    assert _uses(fake) == []


def test_bundle_deployment_run_without_any_warehouse():
    fake = FakeSession(warehouse=None)
    dep = CodeBundleDeployment(
        fake, "DB.SCH.B", CodeBundle(), entrypoint="top.py", compute_type="warehouse"
    )
    with pytest.raises(ValueError, match="no current warehouse"):
        dep.run()


def test_ref_run_uses_target_warehouse_of_deployment(fake, jobs):
    fake.responses.update(_bundle_rows())
    # a deployment loaded by name has no target config, so it runs on the session's
    jobs.submit(Program.ref("MYB"), entrypoint="a.py")
    assert _uses(fake) == []


def test_pool_submit_does_not_switch_and_uses_warehouse_in_manifest(fake, jobs, script):
    jobs.submit(Program.file(script), CodeBundle(compute_pool="POOL", warehouse="QWH"))
    assert _uses(fake) == []
    manifest = _uploaded_manifest(fake)
    assert manifest["bundle"]["compute_options"]["query_warehouse"] == "QWH"


def test_pool_manifest_defaults_to_current_warehouse(fake, jobs, script):
    jobs.submit(Program.file(script), CodeBundle(compute_pool="POOL"))
    assert _uses(fake) == []
    manifest = _uploaded_manifest(fake)
    assert manifest["bundle"]["compute_options"]["query_warehouse"] == "WH"


def test_pool_deploy_and_run_do_not_switch(fake, jobs, pkg_dir):
    dep = jobs.deploy(
        Program.directory(pkg_dir, "top.py"),
        CodeBundle(compute_pool="POOL", warehouse="QWH"),
        name="B",
    )
    dep.run()
    assert _uses(fake) == []
    assert (
        _uploaded_manifest(fake)["bundle"]["compute_options"]["query_warehouse"]
        == "QWH"
    )


def test_pool_manifest_without_any_warehouse(script):
    fake = FakeSession(warehouse=None)
    JobManager(fake).submit(Program.file(script), CodeBundle(compute_pool="POOL"))
    options = _uploaded_manifest(fake)["bundle"]["compute_options"]
    assert "query_warehouse" not in options


def test_warehouse_warehouse_in_manifest_only_for_pool(fake, jobs, script):
    jobs.submit(Program.file(script), CodeBundle(warehouse="OTHER"))
    options = _uploaded_manifest(fake)["bundle"]["compute_options"]
    assert "query_warehouse" not in options and "compute_pool" not in options


def _uploaded_manifest(fake):
    manifests = [d for name, _, d in fake.file.puts if name == "code_bundle.yml"]
    return yaml.safe_load(manifests[-1])


def test_stored_procedure_has_no_warehouse():
    with pytest.raises(TypeError):
        StoredProcedure(warehouse="WH")
    assert not hasattr(StoredProcedure(), "warehouse")


def test_stored_procedure_submit_and_run_never_switch(fake, jobs):
    fake._execute_sproc_internal = lambda **kw: FakeAsyncJob("Q9")

    class FakeSP:
        _anonymous_sp_sql = "WITH SP AS PROCEDURE (...) "
        name = "SP"

    with mock.patch.object(_sproc, "_register", return_value=FakeSP()):
        jobs.submit(lambda session: 1)
    ProcedureDeployment(fake, "DB.SCH.P").run()
    assert _uses(fake) == []


@pytest.mark.parametrize(
    "bad",
    [
        "a.b",
        "wh; drop",
        "a b",
        "",
        "  ",
        "wh'x",
        'x"; drop table t; --',
        "db.sch.wh",
    ],
)
def test_invalid_warehouse_names_are_rejected(fake, bad):
    with pytest.raises(ValueError):
        CodeBundle(warehouse=bad)
    with pytest.raises(ValueError):
        ProcedureDeployment(fake, "DB.SCH.P").schedule("0 * * * * UTC", warehouse=bad)
    assert _uses(fake) == []


def test_quoted_warehouse_name_is_accepted(fake, jobs, script):
    CodeBundle(warehouse='"My WH"')
    jobs.submit(Program.file(script), CodeBundle(warehouse='"My WH"'))
    assert _uses(fake) == ['USE WAREHOUSE "My WH"', "USE WAREHOUSE WH"]


def test_schedule_warehouse_defaults(fake, jobs, pkg_dir):
    def task_sql():
        return [s for s in fake.statements if s.startswith("CREATE OR REPLACE TASK")][
            -1
        ]

    # code bundle: the target's warehouse
    dep = jobs.deploy(
        Program.directory(pkg_dir, "top.py"), CodeBundle(warehouse="TWH"), name="B"
    )
    dep.schedule("0 * * * * UTC")
    assert task_sql().startswith(
        "CREATE OR REPLACE TASK DB.SCH.B_SCHEDULE WAREHOUSE = TWH "
    )
    # an explicit schedule warehouse wins
    dep.schedule("0 * * * * UTC", warehouse="EXPLICIT")
    assert " WAREHOUSE = EXPLICIT " in task_sql()
    # code bundle without a warehouse: serverless
    plain = jobs.deploy(Program.directory(pkg_dir, "top.py"), CodeBundle(), name="B2")
    plain.schedule("0 * * * * UTC")
    assert "WAREHOUSE" not in task_sql()
    plain.schedule("0 * * * * UTC", warehouse="EXPLICIT")
    assert " WAREHOUSE = EXPLICIT " in task_sql()
    # stored procedure: serverless unless warehouse= is given
    sp = ProcedureDeployment(fake, "DB.SCH.P", StoredProcedure())
    sp.schedule("0 * * * * UTC")
    assert "WAREHOUSE" not in task_sql()
    sp.schedule("0 * * * * UTC", warehouse="EXPLICIT")
    assert " WAREHOUSE = EXPLICIT " in task_sql()
    # no session warehouse is switched by scheduling
    assert _uses(fake) == []


def test_schedule_on_pool_deployment_uses_target_warehouse(fake, jobs, pkg_dir):
    dep = jobs.deploy(
        Program.directory(pkg_dir, "top.py"),
        CodeBundle(compute_pool="POOL", warehouse="QWH"),
        name="B",
    )
    dep.schedule("0 * * * * UTC")
    task = [s for s in fake.statements if s.startswith("CREATE OR REPLACE TASK")][-1]
    assert " WAREHOUSE = QWH " in task
