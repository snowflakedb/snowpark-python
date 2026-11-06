#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import importlib
import io
import json
import logging
import os
import re
import sys
import threading
import time
from unittest import mock

import pytest
import yaml
from snowflake.connector.errors import ProgrammingError

from snowflake.snowpark import Session
from snowflake.snowpark.jobs import (
    BundleRun,
    CodeBundle,
    CodeBundleDeployment,
    Environment,
    JobManager,
    JobNotFoundError,
    JobNotSupportedError,
    JobStatus,
    ProcedureDeployment,
    Program,
    QueryJob,
    Schedule,
    StoredProcedure,
    TaskRun,
    _bundle,
    _deployments,
    _handles,
    _launcher,
    _manager,
    _packaging,
    _runner,
    _sproc,
)
from snowflake.snowpark.jobs._bundle_spec import (
    DEFAULT_POOL_RUNTIME_VERSION,
    MAX_COMMENT_CHARS,
    build_spec,
    default_warehouse_runtime_version,
    dump_spec,
    merge_overrides,
    metadata_comment,
    parse_metadata_comment,
)
from snowflake.snowpark.jobs._utils import (
    identifier_safe,
    qualify,
    split_fqn,
    sql_str,
    unquote_ident,
    validate_name,
)

Q1 = "01c83764-0a1c-3a24-000c-a9154c27d30f"
Q2 = "01c83764-0a1c-3a24-000c-a9154c27d310"


class FakeRow(dict):
    def __getitem__(self, key):
        if isinstance(key, int) and key not in self:
            return list(self.values())[key]  # positional access like a real Row
        return super().__getitem__(key)

    def as_dict(self):
        return dict(self)


class FakeResult:
    def __init__(self, rows, query_id="QID_1") -> None:
        self._rows = rows
        self.query_id = query_id

    def collect(self, **kwargs):
        return self._rows

    def collect_nowait(self, **kwargs):
        return FakeAsyncJob(self.query_id)


class FakeAsyncJob:
    def __init__(self, query_id) -> None:
        self.query_id = query_id


class FakeFile:
    def __init__(self) -> None:
        self.puts = []
        self.streams = {}  # path -> bytes readable with get_stream

    def get_stream(self, path, **kwargs):
        if path not in self.streams:
            raise FileNotFoundError(path)
        return io.BytesIO(self.streams[path])

    def put(self, path, dest, **kwargs):
        with open(path, "rb") as f:
            self.puts.append((os.path.basename(path), dest, f.read()))


class FakeSession:
    """Records the SQL statements; ``responses`` maps a regex to the rows to return."""

    def __init__(self, responses=None, warehouse="WH") -> None:
        self.statements = []
        self.sql_kwargs = []
        self.responses = responses or {}
        self.file = FakeFile()
        self._warehouse = warehouse
        self._lock = threading.RLock()

    def use_warehouse(self, warehouse):
        self.statements.append(f"USE WAREHOUSE {warehouse}")
        self._warehouse = warehouse

    def sql(self, statement, **kwargs):
        self.statements.append(statement)
        self.sql_kwargs.append(kwargs)
        for pattern, rows in self.responses.items():
            if re.search(pattern, statement):
                return FakeResult(
                    [
                        FakeRow(r if isinstance(r, dict) else dict(enumerate(r)))
                        for r in rows
                    ]
                )
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
    manager = JobManager(fake)
    fake._jobs = manager  # as Session.jobs does
    return manager


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
        s.startswith("CREATE CODE BUNDLE DB.SCH.SNOWPARK_JOB_TMP_")
        for s in fake.statements
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
    with pytest.raises(ValueError, match="Callables and Program.function"):
        jobs.submit(lambda session: 1, CodeBundle())
    with pytest.raises(ValueError, match="Program.file"):
        jobs.submit(Program.function(lambda session: 1), CodeBundle(compute_pool="P"))


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
        r"CREATE CODE BUNDLE DB\.SCH\.SNOWPARK_JOB_TMP_[0-9A-F]{12} FROM "
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


WAREHOUSE_MANIFEST = (
    b"bundle:\n  type: custom\n  language: python\n  compute_type: warehouse\n"
)
POOL_MANIFEST = (
    b"bundle:\n  type: custom\n  language: python\n  compute_type: compute_pool\n"
)


def test_pre_staged_program_runs_with_its_own_manifest(fake, jobs):
    fake.file.streams["@STG/bundle/code_bundle.yml"] = WAREHOUSE_MANIFEST
    run = jobs.submit(
        Program.stage("@STG/bundle/", "main.py"), CodeBundle(), args=["a"]
    )
    stmt = last_statement(fake)
    assert stmt == (
        "EXECUTE CODE BUNDLE FROM '@STG/bundle' ENTRYPOINT = 'main.py' "
        "ARGUMENTS = ('a') EXECUTION_NAME = '" + run.execution_name + "'"
    )
    assert "WITH SPECIFICATION" not in stmt
    assert run.compute_type == "warehouse" and fake.file.puts == []


def test_pre_staged_pool_bundle_uses_a_temporary_bundle_without_a_spec(fake, jobs):
    fake.file.streams["@STG/bundle/code_bundle.yml"] = POOL_MANIFEST
    run = jobs.submit(Program.stage("@STG/bundle", "main.py"), CodeBundle())
    create, execute = fake.statements[-2:]
    assert create.startswith("CREATE CODE BUNDLE DB.SCH.SNOWPARK_JOB_TMP_")
    assert create.endswith("FROM '@STG/bundle'")
    assert "WITH SPECIFICATION" not in execute
    assert run.compute_type == "compute_pool" and run._temp_bundle


def test_pre_staged_program_with_unreadable_manifest_runs_as_warehouse(fake, jobs):
    run = jobs.submit(Program.workspace("snow://workspace/x", "main.py"), CodeBundle())
    assert "FROM 'snow://workspace/x'" in last_statement(fake)
    assert run.compute_type == "warehouse"


@pytest.mark.parametrize(
    "target, env, named",
    [
        (CodeBundle(compute_pool="POOL"), Environment(), "compute_pool"),
        (CodeBundle(warehouse="OTHER"), Environment(), "warehouse"),
        (CodeBundle(runtime_version="3.11"), Environment(), "runtime_version"),
        (CodeBundle(), Environment(packages=["pandas"]), "Environment.packages"),
        (CodeBundle(), Environment(env_vars={"A": "1"}), "Environment.env_vars"),
        (
            CodeBundle(compute_pool="P", warehouse="W"),
            Environment(secrets={"a": "d.s.x"}),
            "compute_pool, warehouse, Environment.secrets",
        ),
    ],
)
@pytest.mark.parametrize("kind", ["stage", "workspace"])
def test_pre_staged_program_rejects_settings(fake, jobs, kind, target, env, named):
    program = (
        Program.stage("@STG/bundle", "main.py")
        if kind == "stage"
        else Program.workspace("snow://workspace/x", "main.py")
    )
    for call in (jobs.submit, lambda *a, **k: jobs.deploy(*a, name="B", **k)):
        with pytest.raises(ValueError, match="code_bundle.yml in the staged code") as e:
            call(program, target, env=env)
        assert named in str(e.value)
    assert fake.statements == []


def test_deploying_a_staged_program_describes_the_created_bundle(fake, jobs):
    fake.file.streams["@STG/b/code_bundle.yml"] = POOL_MANIFEST + (
        b"  compute_options:\n    compute_pool: STAGED_POOL\n"
        b"    runtime_version: V2.9-CPU-PY3.12\n"
    )
    version = "snow://code bundle/DB.SCH.B/versions/version$1/"
    fake.file.streams[version + "code_bundle.yml"] = fake.file.streams[
        "@STG/b/code_bundle.yml"
    ]
    fake.responses[r"^DESCRIBE CODE BUNDLE"] = [
        {"default_version_location_uri": version}
    ]
    fake.responses[r"^SHOW CODE BUNDLES"] = [{"name": "B"}]
    dep = jobs.deploy(Program.stage("@STG/b", "main.py"), CodeBundle(), name="B")
    create = [s for s in fake.statements if s.startswith("CREATE CODE BUNDLE")][0]
    assert create == "CREATE CODE BUNDLE DB.SCH.B FROM '@STG/b'"  # no COMMENT
    assert dep._compute_type == "compute_pool"
    assert dep._target_config.compute_pool == "STAGED_POOL"
    assert dep._entrypoint == "main.py"
    # the same as a re-attached handle sees
    again = jobs.get_deployment("B")
    assert (again._compute_type, again._target_config, again._spec) == (
        dep._compute_type,
        dep._target_config,
        dep._spec,
    )
    # overrides merge into the stored manifest
    dep.run(overrides={"env_vars": [{"A": "1"}]})
    spec = yaml.safe_load(last_statement(fake).split("$$\n", 1)[1][:-2])
    assert spec["bundle"]["compute_options"]["compute_pool"] == "STAGED_POOL"
    assert spec["bundle"]["env_vars"] == [{"A": "1"}]


def test_dollar_dollar_in_spec_is_rejected(fake, jobs, pkg_dir):
    dep = jobs.deploy(Program.directory(pkg_dir, "top.py"), CodeBundle(), name="B")
    with pytest.raises(ValueError, match=r"\$\$"):
        dep.run(overrides={"env_vars": [{"X": "a$$b"}]})


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
        "CREATE TASK DB.SCH.P_SCHEDULE WAREHOUSE = WH "
        "SCHEDULE = 'USING CRON 0 * * * * UTC' AS CALL DB.SCH.P(ARRAY_CONSTRUCT("
    )
    assert resume == "ALTER TASK DB.SCH.P_SCHEDULE RESUME"
    assert sched.name == "DB.SCH.P_SCHEDULE"
    # no warehouse given: the session's current one; an explicit short name goes into
    # the deployment's schema
    d.schedule("0 * * * * UTC", name="T1")
    assert any(
        s.startswith("CREATE TASK DB.SCH.T1 WAREHOUSE = WH SCHEDULE =")
        for s in fake.statements
    )
    d.schedule("0 * * * * UTC", name="T2", serverless=True)
    assert any(
        s.startswith("CREATE TASK DB.SCH.T2 SCHEDULE =") for s in fake.statements
    )


def test_schedule_default_name_is_quoted_when_needed(fake):
    ProcedureDeployment(fake, 'DB.SCH."my proc"').schedule("0 * * * * UTC")
    assert fake.statements[0].startswith('CREATE TASK DB.SCH."my proc_SCHEDULE"')


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
            "QUERY_ID": Q1,
            "SCHEDULED_TIME": datetime(2025, 1, 3),
            "STATE": "SUCCEEDED",
        },
        {
            "QUERY_ID": Q2,
            "SCHEDULED_TIME": datetime(2025, 1, 1),
            "STATE": "SUCCEEDED",
        },
    ]
    sched = ProcedureDeployment(fake, "DB.SCH.P").schedule("0 * * * * UTC")
    runs = sched.runs()
    assert [r.query_id for r in runs] == [Q1]
    assert runs[0].id == f"task:DB.SCH.P_SCHEDULE/{Q1}"
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
        r"CREATE OR REPLACE CODE BUNDLE DB\.SCH\.B FROM '@SESS_STAGE/snowpark_jobs/[0-9a-f]{64}'"
        r" COMMENT = 'snowpark\.jobs:.*'",
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
    add, comment = fake.statements[-2:]
    assert re.fullmatch(
        r"ALTER CODE BUNDLE DB\.SCH\.B ADD VERSION FROM '@SESS_STAGE/snowpark_jobs/[0-9a-f]{64}'",
        add,
    )
    assert comment.startswith(
        "ALTER CODE BUNDLE DB.SCH.B SET COMMENT = 'snowpark.jobs:"
    )
    assert dep.to_sql() == "EXECUTE CODE BUNDLE DB.SCH.B ENTRYPOINT = 'new.py'"


def test_code_bundle_schedule_sql(fake, jobs, pkg_dir):
    dep = jobs.deploy(Program.directory(pkg_dir, "top.py"), CodeBundle(), name="B")
    dep.schedule("0 0 * * * UTC", args=["x"])
    create = [s for s in fake.statements if s.startswith("CREATE TASK")][0]
    assert create == (
        "CREATE TASK DB.SCH.B_SCHEDULE WAREHOUSE = WH "
        "SCHEDULE = 'USING CRON 0 0 * * * UTC' "
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


def test_pool_spec_uses_session_warehouse_as_query_warehouse(fake, jobs, script):
    jobs.submit(Program.file(script), CodeBundle(compute_pool="POOL"))
    manifests = [d for name, _, d in fake.file.puts if name == "code_bundle.yml"]
    spec = yaml.safe_load(manifests[-1])
    assert spec["bundle"]["compute_options"]["query_warehouse"] == "WH"
    assert "WITH SPECIFICATION" not in last_statement(fake)


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
    run = BundleRun(fake, Q1)
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
    assert BundleRun(fake, Q1).status() == JobStatus.RUNNING


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
        assert BundleRun(fake, Q1).raw_status == expected


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
        {"QUERY_ID": Q1, "STATE": state, "ERROR_MESSAGE": "task broke"}
    ]
    run = TaskRun(fake, "DB.SCH.T", Q1)
    assert run.status() == expected
    if expected == JobStatus.FAILED:
        assert run.error() == "task broke"


def test_task_run_without_history_is_pending(fake):
    assert TaskRun(fake, "DB.SCH.T", Q1).status() == JobStatus.PENDING


def test_task_run_target_and_child(fake):
    fake.responses[r"TASK_HISTORY"] = [
        {
            "QUERY_ID": Q1,
            "QUERY_TEXT": "EXECUTE CODE BUNDLE DB.SCH.B ENTRYPOINT = 'a'",
        }
    ]
    run = TaskRun(fake, "DB.SCH.T", Q1)
    assert run.target == "code_bundle"
    assert isinstance(run.child(), BundleRun) and run.child().query_id == Q1
    sp = TaskRun(fake, "DB.SCH.T", Q2, {"QUERY_TEXT": "CALL P(ARRAY_CONSTRUCT())"})
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
        BundleRun(fake, Q1).result()


def test_bundle_logs_on_pool_not_supported(fake):
    with pytest.raises(JobNotSupportedError):
        BundleRun(fake, Q1, "compute_pool").logs()
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [
        {"STATUS": "DONE", "COMPUTE_TYPE": "COMPUTE_POOL"}
    ]
    with pytest.raises(JobNotSupportedError):
        BundleRun(fake, Q1).logs()


def test_logs_query_uses_event_table_and_escapes_query_id(fake):
    fake.responses[r"SHOW PARAMETERS"] = [{"value": "DB.SCH.EVENTS"}]
    BundleRun(fake, "Q'1", "warehouse").logs(limit=5)
    stmt = last_statement(fake)
    assert "FROM DB.SCH.EVENTS WHERE RECORD_TYPE = 'LOG'" in stmt
    assert sql_str("Q'1") in stmt and stmt.endswith("ORDER BY TIMESTAMP DESC LIMIT 5")


def test_logs_without_event_table(fake):
    with pytest.raises(RuntimeError, match="event table"):
        BundleRun(fake, Q1, "warehouse").logs()


def test_cancel_issues_system_cancel(fake):
    qid = "Q'1"
    BundleRun(fake, qid).cancel()
    assert last_statement(fake) == f"SELECT SYSTEM$CANCEL_QUERY({sql_str(qid)})"


def test_wait_times_out(fake):
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [{"STATUS": "RUNNING"}]
    with pytest.raises(TimeoutError, match="did not finish"):
        BundleRun(fake, Q1).wait(timeout=0.05, poll_interval=0.01)
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [{"STATUS": "DONE"}]
    assert BundleRun(fake, Q1).wait(timeout=1) == JobStatus.SUCCEEDED


def test_owner_rights_hint_is_added_to_query_errors(fake):
    job = QueryJob(fake, FakeAsyncJob(Q1))
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
    q = jobs.get(f"query:{Q1}")
    assert isinstance(q, QueryJob) and q.query_id == Q1 and q.id == f"query:{Q1}"
    # ids are case-insensitive and normalized to lower case
    assert jobs.get(f"query:{Q1.upper()}").query_id == Q1
    b = jobs.get(f"bundle:{Q1}")
    assert isinstance(b, BundleRun) and b.id == f"bundle:{Q1}"
    t = jobs.get(f"task:DB.SCH.MY_TASK/{Q1}")
    assert isinstance(t, TaskRun)
    assert (t.task_fqn, t.query_id) == ("DB.SCH.MY_TASK", Q1)
    assert t.id == f"task:DB.SCH.MY_TASK/{Q1}"
    quoted = jobs.get(f'task:DB."S/1".T/{Q1}')
    assert quoted.task_fqn == 'DB."S/1".T' and quoted.query_id == Q1
    assert fake.statements == []


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
        "query:01abc",
        "query:bogus",
        f"query:{Q1}'; DROP TABLE T; --",
        f"query:{Q1} ",
        "query:" + Q1[:-1],
        f"bundle:{Q1}x",
        "bundle:' OR 1=1 --",
        f"task:T/{Q1}",
        f"task:SCH.T/{Q1}",
        f"task:DB.SCH.T.X/{Q1}",
        f"task:DB.SCH.T; DROP TABLE X/{Q1}",
        "task:DB.SCH.T/01abc",
        f"task:DB.SCH.T/{Q1}'",
    ],
)
def test_get_rejects_malformed_ids(fake, jobs, job_id):
    with pytest.raises(ValueError):
        jobs.get(job_id)
    assert fake.statements == []


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
            "START_TIME": datetime.datetime(2025, 1, 2),
            "STATUS": "DONE",
            "EXECUTION_NAME": "SNOWPARK_JOB_X",
            "COMPUTE_TYPE": "WAREHOUSE",
        },
        {
            "QUERY_ID": "B2",
            "START_TIME": datetime.datetime(2025, 1, 4),
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
    conn._telemetry_client = mock.MagicMock()
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

    def failing(statement, **kwargs):
        if statement.startswith("EXECUTE CODE BUNDLE"):
            raise RuntimeError("submit failed")
        return original(statement, **kwargs)

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


def test_ref_run_uses_the_saved_warehouse_of_the_deployment(fake, jobs):
    meta = {"spec": {"bundle": {"compute_type": "warehouse"}}, "warehouse": "SAVED"}
    fake.responses.update(_bundle_rows())
    fake.responses[r"^DESCRIBE CODE BUNDLE"] = [
        {"comment": "snowpark.jobs:" + json.dumps(meta)}
    ]
    jobs.submit(Program.ref("MYB"), entrypoint="a.py")
    assert _uses(fake) == ["USE WAREHOUSE SAVED", "USE WAREHOUSE WH"]


def test_ref_run_without_a_saved_warehouse_uses_the_sessions(fake, jobs):
    fake.responses.update(_bundle_rows())
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
        return [s for s in fake.statements if s.startswith("CREATE TASK")][-1]

    # code bundle: the target's warehouse
    dep = jobs.deploy(
        Program.directory(pkg_dir, "top.py"), CodeBundle(warehouse="TWH"), name="B"
    )
    dep.schedule("0 * * * * UTC")
    assert task_sql().startswith("CREATE TASK DB.SCH.B_SCHEDULE WAREHOUSE = TWH ")
    # an explicit schedule warehouse wins
    dep.schedule("0 * * * * UTC", warehouse="EXPLICIT")
    assert " WAREHOUSE = EXPLICIT " in task_sql()
    # code bundle without a warehouse: the session's current warehouse at schedule time
    plain = jobs.deploy(Program.directory(pkg_dir, "top.py"), CodeBundle(), name="B2")
    plain.schedule("0 * * * * UTC")
    assert " WAREHOUSE = WH " in task_sql()
    fake._warehouse = "LATER"
    plain.schedule("0 * * * * UTC")
    assert " WAREHOUSE = LATER " in task_sql()
    fake._warehouse = "WH"
    plain.schedule("0 * * * * UTC", warehouse="EXPLICIT")
    assert " WAREHOUSE = EXPLICIT " in task_sql()
    # stored procedure: the same
    sp = ProcedureDeployment(fake, "DB.SCH.P", StoredProcedure())
    sp.schedule("0 * * * * UTC")
    assert " WAREHOUSE = WH " in task_sql()
    sp.schedule("0 * * * * UTC", warehouse="EXPLICIT")
    assert " WAREHOUSE = EXPLICIT " in task_sql()
    # serverless only when asked for
    sp.schedule("0 * * * * UTC", serverless=True)
    plain.schedule("0 * * * * UTC", serverless=True)
    assert "WAREHOUSE" not in task_sql()
    # no session warehouse is switched by scheduling
    assert _uses(fake) == []


def test_schedule_on_pool_deployment_uses_target_warehouse(fake, jobs, pkg_dir):
    dep = jobs.deploy(
        Program.directory(pkg_dir, "top.py"),
        CodeBundle(compute_pool="POOL", warehouse="QWH"),
        name="B",
    )
    dep.schedule("0 * * * * UTC")
    task = [s for s in fake.statements if s.startswith("CREATE TASK")][-1]
    assert " WAREHOUSE = QWH " in task


# ---- reading results of scheduled runs ----


class ResultJob(FakeAsyncJob):
    def __init__(self, query_id, rows=None, exc=None) -> None:
        super().__init__(query_id)
        self._rows = rows
        self._exc = exc

    def result(self, kind):
        if self._exc:
            raise self._exc
        return self._rows


def _scheduled_run(fake, async_job):
    fake.responses[r"TASK_HISTORY"] = [
        {
            "QUERY_ID": Q1,
            "STATE": "SUCCEEDED",
            "QUERY_TEXT": "CALL DB.SCH.P(ARRAY_CONSTRUCT())",
        }
    ]
    fake.create_async_job = lambda qid: async_job
    return TaskRun(fake, "DB.SCH.T", Q1)


def test_task_run_result_statement_not_found_is_not_supported(fake):
    original = ProgrammingError(msg="Statement Q1 not found", errno=709)
    run = _scheduled_run(fake, ResultJob(Q1, exc=original))
    with mock.patch.object(_handles, "_query_status_name", return_value="SUCCESS"):
        with pytest.raises(JobNotSupportedError, match="cannot be read") as info:
            run.result()
    assert "execute_as='caller'" in str(info.value)
    assert "usual cause" in str(info.value)
    assert info.value.__cause__ is original


def test_task_run_result_other_errors_pass_through(fake):
    other = ProgrammingError(msg="boom", errno=1304)
    run = _scheduled_run(fake, ResultJob(Q1, exc=other))
    with mock.patch.object(_handles, "_query_status_name", return_value="SUCCESS"):
        with pytest.raises(ProgrammingError) as info:
            run.result()
    assert info.value is other
    plain = RuntimeError("not a sql error")
    run = _scheduled_run(fake, ResultJob(Q1, exc=plain))
    with mock.patch.object(_handles, "_query_status_name", return_value="SUCCESS"):
        with pytest.raises(RuntimeError) as info:
            run.result()
    assert info.value is plain


def test_task_run_result_success_is_unchanged(fake):
    run = _scheduled_run(fake, ResultJob(Q1, rows=[('{"a": 1}',)]))
    with mock.patch.object(_handles, "_query_status_name", return_value="SUCCESS"):
        assert run.result() == {"a": 1}
        assert run.child().result() == {"a": 1}


def test_direct_query_job_result_is_unaffected_by_statement_not_found(fake):
    original = ProgrammingError(msg="Statement Q1 not found", errno=709)
    job = QueryJob(fake, ResultJob(Q1, exc=original))
    with mock.patch.object(_handles, "_query_status_name", return_value="SUCCESS"):
        with pytest.raises(ProgrammingError) as info:
            job.result()
    assert info.value is original


# ---- function entry points (launcher) ----

LAUNCHER = "__snowpark_jobs_launcher__.py"


def test_launcher_call_translation():
    assert _bundle.launcher_call("a/b.py", ["x"]) == ("a/b.py", ["x"])
    assert _bundle.launcher_call("a/b.py:run", ["x", "y"]) == (
        LAUNCHER,
        ["a/b.py:run", "x", "y"],
    )
    assert _packaging.LAUNCHER_FILE == LAUNCHER


def test_execute_sql_renders_function_entrypoint():
    assert _bundle.execute_sql("DB.S.B", "jobs/daily.py:run", ["1", "it's"]) == (
        f"EXECUTE CODE BUNDLE DB.S.B ENTRYPOINT = '{LAUNCHER}' "
        "ARGUMENTS = ('jobs/daily.py:run', '1', 'it''s')"
    )
    assert _bundle.execute_sql("DB.S.B", "jobs/daily.py:run") == (
        f"EXECUTE CODE BUNDLE DB.S.B ENTRYPOINT = '{LAUNCHER}' "
        "ARGUMENTS = ('jobs/daily.py:run')"
    )
    with pytest.raises(ValueError, match="must be strings"):
        _bundle.execute_sql("DB.S.B", "jobs/daily.py:run", [1])


def test_default_execution_name_includes_function(jobs):
    name = _bundle.default_execution_name("pkg/tasks.py:run")
    assert re.fullmatch(r"SNOWPARK_JOB_TASKS_RUN_[0-9A-F]{8}", name)


def test_submit_function_entrypoint_uses_launcher(fake, jobs, pkg_dir):
    jobs.submit(Program.directory(pkg_dir, "pkg/main.py:run"), CodeBundle(), args=["a"])
    sql = last_statement(fake)
    assert f"ENTRYPOINT = '{LAUNCHER}'" in sql
    assert "ARGUMENTS = ('pkg/main.py:run', 'a')" in sql
    assert "SNOWPARK_JOB_MAIN_RUN_" in sql
    uploads = {name: data for name, _, data in fake.file.puts}
    assert uploads[LAUNCHER].decode() == _packaging.launcher_source()


def test_submit_function_entrypoint_override_and_program_file(fake, jobs, script):
    jobs.submit(Program.file(script, "go"), CodeBundle(), args=["x"])
    assert "ARGUMENTS = ('script.py:go', 'x')" in last_statement(fake)
    jobs.submit(Program.file(script), CodeBundle(), entrypoint="script.py:other")
    assert "ARGUMENTS = ('script.py:other')" in last_statement(fake)


def test_launcher_is_uploaded_for_script_programs_and_hashed(fake, jobs, script):
    jobs.submit(Program.file(script), CodeBundle())
    assert LAUNCHER in {name for name, _, _ in fake.file.puts}
    files = {"script.py": script}
    with mock.patch.object(_packaging, "launcher_source", return_value="# other\n"):
        other = _packaging.content_hash(files, "m", None)
    assert other != _packaging.content_hash(files, "m", None)


def test_user_launcher_file_is_replaced_by_generated(fake, jobs, pkg_dir):
    open(os.path.join(pkg_dir, LAUNCHER), "w").write("evil = 1\n")
    jobs.submit(Program.directory(pkg_dir, "top.py"), CodeBundle())
    launchers = [d for name, _, d in fake.file.puts if name == LAUNCHER]
    assert launchers == [_packaging.launcher_source().encode()]


def test_function_entrypoint_must_be_part_of_the_files(jobs, pkg_dir):
    with pytest.raises(ValueError, match="not part of the program files"):
        jobs.submit(Program.directory(pkg_dir, "nope.py:f"), CodeBundle())


def test_function_entrypoint_rejected_for_stage_and_workspace(jobs):
    with pytest.raises(ValueError, match="launcher"):
        jobs.submit(Program.stage("@STG/b", "m.py:run"), CodeBundle())
    with pytest.raises(ValueError, match="launcher"):
        jobs.submit(Program.workspace("snow://workspace/x", "m.py:run"), CodeBundle())


def test_function_entrypoint_rejected_for_notebooks(jobs, tmp_path):
    root = tmp_path / "nbs"
    root.mkdir()
    (root / "nb.ipynb").write_text("{}")
    with pytest.raises(ValueError, match="only supported for .py"):
        jobs.submit(
            Program.directory(str(root), "nb.ipynb:run"),
            CodeBundle(compute_pool="P"),
        )


def test_function_entrypoint_args_must_be_strings(jobs, pkg_dir):
    with pytest.raises(ValueError, match="must be strings"):
        jobs.submit(
            Program.directory(pkg_dir, "pkg/main.py:run"), CodeBundle(), args=[1]
        )


def test_deploy_run_schedule_and_to_sql_with_function_entrypoint(fake, jobs, pkg_dir):
    dep = jobs.deploy(Program.directory(pkg_dir), CodeBundle(), name="B")
    assert LAUNCHER in {name for name, _, _ in fake.file.puts}
    expected = (
        f"EXECUTE CODE BUNDLE DB.SCH.B ENTRYPOINT = '{LAUNCHER}' "
        "ARGUMENTS = ('pkg/main.py:run', 'x')"
    )
    assert dep.to_sql(entrypoint="pkg/main.py:run", args=["x"]) == expected
    dep.run(entrypoint="pkg/main.py:run", args=["x"])
    assert last_statement(fake).startswith(expected)
    dep.schedule("0 0 * * * UTC", entrypoint="pkg/main.py:run", args=["x"])
    create = [s for s in fake.statements if s.startswith("CREATE TASK")][-1]
    assert create.endswith(f"AS {expected}")


def test_deployment_default_function_entrypoint(fake, jobs, pkg_dir):
    dep = jobs.deploy(
        Program.directory(pkg_dir, "pkg/main.py:run"), CodeBundle(), name="B"
    )
    assert f"ENTRYPOINT = '{LAUNCHER}'" in dep.to_sql()


def test_deployment_without_launcher_rejects_function_entrypoint(fake, jobs):
    fake.responses[r"^SHOW CODE BUNDLES"] = [{"name": "B"}]
    dep = jobs.get_deployment("B")
    assert dep.to_sql(entrypoint="a.py").endswith("ENTRYPOINT = 'a.py'")
    with pytest.raises(ValueError, match="does not contain the session.jobs launcher"):
        dep.to_sql(entrypoint="a.py:run")
    with pytest.raises(ValueError, match="does not contain the session.jobs launcher"):
        dep.run(entrypoint="a.py:run")
    with pytest.raises(ValueError, match="does not contain the session.jobs launcher"):
        jobs.submit(Program.ref("B"), entrypoint="a.py:run")


def test_get_deployment_detects_the_launcher(fake, jobs):
    fake.responses[r"^SHOW CODE BUNDLES"] = [{"name": "B"}]
    fake.responses[r"^DESCRIBE CODE BUNDLE"] = [
        {"last_version_source_location_uri": "@STG/snowpark_jobs/abc/"}
    ]
    fake.responses[r"^LIST "] = [{"name": "launcher"}]
    dep = jobs.get_deployment("B")
    assert "ARGUMENTS = ('a.py:run')" in dep.to_sql(entrypoint="a.py:run")
    assert any(
        s.startswith("LIST '@STG/snowpark_jobs/abc/" + LAUNCHER)
        for s in fake.statements
    )


def test_add_version_updates_launcher_availability(fake, jobs, pkg_dir):
    dep = jobs.deploy(Program.directory(pkg_dir), CodeBundle(), name="B")
    # the staged version's manifest is read back from the bundle; it has no launcher
    fake.responses[r"^DESCRIBE CODE BUNDLE"] = [
        {"default_version_location_uri": VERSION_URI}
    ]
    fake.file.streams[VERSION_URI + "code_bundle.yml"] = WAREHOUSE_MANIFEST
    dep.add_version(Program.stage("@STG/v2"))
    with pytest.raises(ValueError, match="does not contain the session.jobs launcher"):
        dep.to_sql(entrypoint="a.py:run")
    dep.add_version(Program.directory(pkg_dir))
    assert "a.py:run" in dep.to_sql(entrypoint="a.py:run")


# ---- the launcher itself ----


def _load_launcher():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "launcher_under_test", _packaging.Path(_launcher.__file__)
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def bundle_root(tmp_path):
    root = tmp_path / "bundle"
    (root / "jobs").mkdir(parents=True)
    (root / "helper_mod.py").write_text("def twice(x):\n    return x * 2\n")
    (root / "jobs" / "__init__.py").write_text("")
    (root / "jobs" / "daily.py").write_text(
        "import helper_mod\n"
        "CALLS = []\n"
        "def run(session, *args):\n"
        "    CALLS.append((session, args, helper_mod.twice(2)))\n"
        "    return 'ignored'\n"
        "def boom(session):\n"
        "    raise RuntimeError('boom')\n"
    )
    yield str(root)
    for name in ("jobs", "jobs.daily", "helper_mod"):
        sys.modules.pop(name, None)
    if str(root) in sys.path:
        sys.path.remove(str(root))


def test_launcher_main_calls_function_with_session_and_args(bundle_root):
    launcher = _load_launcher()
    session = object()
    assert (
        launcher.main(
            [LAUNCHER, "jobs/daily.py:run", "a", "b"], session=session, root=bundle_root
        )
        is None
    )
    daily = importlib.import_module("jobs.daily")
    assert daily.CALLS == [(session, ("a", "b"), 4)]


def test_launcher_main_uses_the_active_session_by_default(bundle_root):
    launcher = _load_launcher()
    active = object()
    with mock.patch(
        "snowflake.snowpark.context.get_active_session", return_value=active
    ):
        launcher.main([LAUNCHER, "jobs/daily.py:run"], root=bundle_root)
    assert importlib.import_module("jobs.daily").CALLS[0][0] is active


@pytest.mark.parametrize(
    "target",
    [
        None,
        "daily",
        "jobs/daily.py",
        "jobs/daily.py:",
        "../x.py:f",
        "/abs/x.py:f",
        "a//b.py:f",
    ],
)
def test_launcher_main_rejects_bad_targets(bundle_root, target):
    launcher = _load_launcher()
    argv = [LAUNCHER] if target is None else [LAUNCHER, target]
    with pytest.raises(ValueError):
        launcher.main(argv, session=object(), root=bundle_root)


def test_launcher_main_propagates_exceptions(bundle_root):
    launcher = _load_launcher()
    with pytest.raises(RuntimeError, match="boom"):
        launcher.main(
            [LAUNCHER, "jobs/daily.py:boom"], session=object(), root=bundle_root
        )


def test_launcher_is_import_safe_and_stdlib_only():
    source = _packaging.launcher_source()
    assert "snowflake" not in source.split("def main")[0]
    assert _load_launcher().parse_target("a/b.py:f") == ("a.b", "f")


def test_launcher_bundle_root_falls_back_to_argv0_without_file(tmp_path, monkeypatch):
    launcher = _load_launcher()
    monkeypatch.chdir(tmp_path)
    assert launcher.bundle_root("/b/x/launcher.py", "ignored.py") == "/b/x"
    # a notebook cell on a compute pool: no __file__, relative script name, cwd is the root
    assert launcher.bundle_root(None, LAUNCHER) == str(tmp_path)


# ---- review fixes ----

# F1: validation of names and ids


def test_task_run_and_schedule_validate_names_and_ids(fake):
    with pytest.raises(ValueError, match="Invalid task name"):
        TaskRun(fake, "DB.SCH.T; DROP TABLE X", Q1)
    with pytest.raises(ValueError, match="db.schema.name"):
        TaskRun(fake, "SCH.T", Q1)
    with pytest.raises(ValueError, match="Invalid query id"):
        TaskRun(fake, "DB.SCH.T", "Q1' OR 1=1")
    with pytest.raises(ValueError, match="db.schema.name"):
        Schedule(fake, "T")
    with pytest.raises(ValueError, match="Invalid task name"):
        _handles.task_history_sql("DB.SCH.T') --", 10)
    with pytest.raises(ValueError, match="Invalid query id"):
        _handles.task_history_sql("DB.SCH.T", 10, "x' OR '1'='1")
    assert fake.statements == []


# F2: status mapping and missing jobs


@pytest.mark.parametrize(
    "name, expected",
    [
        ("DISCONNECTED", JobStatus.FAILED),
        ("RESTARTED", JobStatus.RUNNING),
        ("INCIDENT", JobStatus.FAILED),
        ("NO_DATA", JobStatus.PENDING),
    ],
)
def test_query_status_mapping_is_explicit(name, expected):
    assert _handles._map_query_status(name) == expected


def test_bundle_history_deleted_is_cancelled(fake):
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [{"STATUS": "DELETED"}]
    assert BundleRun(fake, Q1).status() == JobStatus.CANCELLED


def test_unknown_status_names_are_running_and_logged_once(fake, caplog):
    _handles._warned_status_names.clear()
    with caplog.at_level(logging.WARNING, logger=_handles.__name__):
        assert _handles._map_query_status("SOMETHING_NEW") == JobStatus.RUNNING
        assert _handles._map_query_status("SOMETHING_NEW") == JobStatus.RUNNING
        fake.responses[r"CODE_BUNDLE_HISTORY"] = [{"STATUS": "NEW_STATE"}]
        assert BundleRun(fake, Q1).status() == JobStatus.RUNNING
        fake.responses[r"TASK_HISTORY"] = [{"QUERY_ID": Q1, "STATE": "NEW_TASK_STATE"}]
        assert TaskRun(fake, "DB.SCH.T", Q1).status() == JobStatus.RUNNING
    messages = [r.getMessage() for r in caplog.records]
    assert sum("SOMETHING_NEW" in m for m in messages) == 1
    assert sum("NEW_STATE" in m for m in messages) == 1
    assert sum("NEW_TASK_STATE" in m for m in messages) == 1


def _after_grace(monkeypatch):
    """Makes the second status check happen after the grace period."""
    ticks = iter([1000.0] + [1000.0 + _handles.MISSING_JOB_GRACE_SECONDS + 1] * 100)
    monkeypatch.setattr(_handles.time, "monotonic", lambda: next(ticks))


def test_query_job_unknown_id_raises_after_grace(fake, monkeypatch):
    job = QueryJob(fake, FakeAsyncJob(Q1))
    _after_grace(monkeypatch)
    with mock.patch.object(_handles, "_query_status_name", return_value="NO_DATA"):
        assert job.status() == JobStatus.PENDING  # within the grace period
        with pytest.raises(JobNotFoundError, match=Q1):
            job.status()
    assert issubclass(JobNotFoundError, ValueError)


def test_query_job_not_found_does_not_hang_wait(fake, monkeypatch):
    monkeypatch.setattr(_handles, "MISSING_JOB_GRACE_SECONDS", 0.05)
    job = QueryJob(fake, FakeAsyncJob(Q1))
    with mock.patch.object(_handles, "_query_status_name", return_value="NO_DATA"):
        with pytest.raises(JobNotFoundError):
            job.wait(poll_interval=0.02)  # no timeout given


def test_query_job_seen_once_never_raises_not_found(fake, monkeypatch):
    job = QueryJob(fake, FakeAsyncJob(Q1))
    _after_grace(monkeypatch)
    with mock.patch.object(
        _handles, "_query_status_name", side_effect=["RUNNING", "NO_DATA"]
    ):
        job.status()
        assert job.status() == JobStatus.PENDING


def test_bundle_run_without_history_row(fake, monkeypatch):
    run = BundleRun(fake, Q1)
    _after_grace(monkeypatch)
    with mock.patch.object(_handles, "_query_status_name", return_value="NO_DATA"):
        assert run.status() == JobStatus.PENDING
        with pytest.raises(JobNotFoundError):
            run.status()


def test_bundle_run_pending_is_not_missing(fake, monkeypatch):
    # a running EXECUTE without a history row yet, and a PENDING history row (slow
    # compute pool start) both wait without JobNotFoundError
    run = BundleRun(fake, Q1)
    _after_grace(monkeypatch)
    with mock.patch.object(_handles, "_query_status_name", return_value="RUNNING"):
        assert run.status() == JobStatus.PENDING
        assert run.status() == JobStatus.PENDING
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [{"STATUS": "PENDING"}]
    run = BundleRun(fake, Q2)
    _after_grace(monkeypatch)
    assert run.status() == JobStatus.PENDING
    assert run.status() == JobStatus.PENDING


def test_bundle_run_failed_query_without_history_row(fake):
    with mock.patch.object(
        _handles, "_query_status_name", return_value="FAILED_WITH_ERROR"
    ):
        assert BundleRun(fake, Q1).status() == JobStatus.FAILED
    with mock.patch.object(_handles, "_query_status_name", return_value="ABORTED"):
        assert BundleRun(fake, Q1).status() == JobStatus.CANCELLED


def test_task_run_without_history_raises_after_grace(fake, monkeypatch):
    run = TaskRun(fake, "DB.SCH.T", Q1)
    _after_grace(monkeypatch)
    assert run.status() == JobStatus.PENDING
    with pytest.raises(JobNotFoundError):
        run.status()
    # a run with a history row is fine
    fake.responses[r"TASK_HISTORY"] = [{"QUERY_ID": Q2, "STATE": "EXECUTING"}]
    run = TaskRun(fake, "DB.SCH.T", Q2)
    _after_grace(monkeypatch)
    assert run.status() == JobStatus.RUNNING
    assert run.status() == JobStatus.RUNNING


def test_wait_has_no_default_timeout():
    import inspect

    assert inspect.signature(_handles.Job.wait).parameters["timeout"].default is None
    assert _handles.MISSING_JOB_GRACE_SECONDS == 120


# F3: temporary bundles

TEMP = "SNOWPARK_JOB_TMP_0123456789AB"


def _temp_row(status="DONE", name=TEMP, **extra):
    row = {
        "STATUS": status,
        "CODE_BUNDLE_NAME": name,
        "DATABASE_NAME": "DB",
        "SCHEMA_NAME": "SCH",
        "COMPUTE_TYPE": "COMPUTE_POOL",
    }
    row.update(extra)
    return row


def _drops(fake):
    return [s for s in fake.statements if s.startswith("DROP CODE BUNDLE")]


def test_temp_bundle_names_use_the_reserved_prefix(fake, jobs, script):
    run = jobs.submit(Program.file(script), CodeBundle(compute_pool="POOL"))
    assert re.fullmatch(r"DB\.SCH\.SNOWPARK_JOB_TMP_[0-9A-F]{12}", run._temp_bundle)
    assert _handles.TEMP_BUNDLE_RE.match(run._temp_bundle.split(".")[-1])
    assert run._temp_bundle in jobs._temp_bundles
    # the execution name does not use the temporary prefix
    assert not run.execution_name.startswith("SNOWPARK_JOB_TMP_")
    temp = run._temp_bundle
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [{"STATUS": "DONE"}]
    run.status()
    assert _drops(fake) == [f"DROP CODE BUNDLE IF EXISTS {temp}"]
    assert not jobs._temp_bundles


def test_reattached_pool_run_drops_its_temporary_bundle(fake, jobs):
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [_temp_row("DONE")]
    run = jobs.get(f"bundle:{Q1}")
    assert run.status() == JobStatus.SUCCEEDED
    assert _drops(fake) == [f'DROP CODE BUNDLE IF EXISTS "DB"."SCH"."{TEMP}"']
    run.status()
    assert len(_drops(fake)) == 1  # only once


@pytest.mark.parametrize("status", ["PENDING", "RUNNING"])
def test_reattached_run_that_is_not_finished_keeps_the_bundle(fake, jobs, status):
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [_temp_row(status)]
    jobs.get(f"bundle:{Q1}").status()
    assert _drops(fake) == []


@pytest.mark.parametrize(
    "name",
    ["MY_DEPLOYMENT", "SNOWPARK_JOB_TMP_user", "SNOWPARK_JOB_TMP_0123456789ABCD", None],
)
def test_reattached_run_of_other_bundles_never_drops(fake, jobs, name):
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [_temp_row("DONE", CODE_BUNDLE_NAME=name)]
    jobs.get(f"bundle:{Q1}").status()
    assert _drops(fake) == []


def test_listed_pool_runs_drop_their_temporary_bundle(fake, jobs):
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [
        _temp_row(
            "CANCELLED",
            QUERY_ID=Q1,
            EXECUTION_NAME="SNOWPARK_JOB_X",
            START_TIME=datetime.datetime(2025, 1, 1),
        )
    ]
    run = jobs.list(target="code_bundle")[0]
    run.status()
    assert len(_drops(fake)) == 1


def test_task_run_child_bundle_run_drops_temp_bundle(fake):
    fake.responses[r"TASK_HISTORY"] = [
        {"QUERY_ID": Q1, "QUERY_TEXT": "EXECUTE CODE BUNDLE DB.SCH.B ENTRYPOINT = 'a'"}
    ]
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [_temp_row("DONE")]
    TaskRun(fake, "DB.SCH.T", Q1).child().status()
    assert len(_drops(fake)) == 1


def _cleanup_fake(shows, histories):
    """``histories`` maps a bundle name to the statuses of its runs."""
    responses = {r"^SHOW CODE BUNDLES": shows}
    for name, statuses in histories.items():
        responses[f"BUNDLE_NAME => '{name}'"] = [{"STATUS": st} for st in statuses]
    return FakeSession(responses)


def _bundle_names(count):
    return [f"SNOWPARK_JOB_TMP_{i:012X}" for i in range(count)]


def test_cleanup_drops_only_idle_temporary_bundles():
    old = datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)
    now = datetime.datetime.now(datetime.timezone.utc)
    done, failed, deleted, running, pending, unused_new, unused_old = _bundle_names(7)
    fake = _cleanup_fake(
        [
            {"name": n, "created_on": old}
            for n in (done, failed, deleted, running, pending, unused_old)
        ]
        + [
            {"name": unused_new, "created_on": now},
            {"name": "SNOWPARK_JOB_TMP_user", "created_on": old},  # not our shape
            {"name": "SNOWPARK_JOB_TMP_0123456789ABCD", "created_on": old},
        ],
        {
            done: ["DONE", "DONE"],
            failed: ["FAILED"],
            deleted: ["DELETED", "CANCELLED"],
            running: ["DONE", "RUNNING"],
            pending: ["PENDING"],
        },
    )
    dropped = JobManager(fake).cleanup()
    expected = [done, failed, deleted, unused_old]
    assert dropped == [f'"DB"."SCH"."{n}"' for n in expected]
    assert [s.split()[-1] for s in _drops(fake)] == dropped
    assert "SHOW CODE BUNDLES LIKE 'SNOWPARK_JOB_TMP_%' IN SCHEMA DB.SCH" in (
        fake.statements[0]
    )


@pytest.mark.parametrize(
    "schema, expected",
    [("OTHER", "IN SCHEMA DB.OTHER"), ("D2.OTHER", "IN SCHEMA D2.OTHER")],
)
def test_cleanup_schema_argument(schema, expected):
    fake = FakeSession()
    assert JobManager(fake).cleanup(schema) == []
    assert expected in fake.statements[0]


@pytest.mark.parametrize("schema", ["a b", "x; DROP", "d.s.t", ""])
def test_cleanup_rejects_bad_schema(fake, jobs, schema):
    with pytest.raises(ValueError):
        jobs.cleanup(schema)
    assert fake.statements == []


def test_cleanup_on_close_drops_idle_bundles_and_never_raises():
    done, running = _bundle_names(2)
    fake = _cleanup_fake([], {done: ["DONE"], running: ["RUNNING"]})
    manager = JobManager(fake)
    manager._temp_bundles = {f'"DB"."SCH"."{done}"', f'"DB"."SCH"."{running}"'}
    manager._cleanup_on_close()
    assert [s.split()[-1] for s in _drops(fake)] == [f'"DB"."SCH"."{done}"']
    assert manager._temp_bundles == {f'"DB"."SCH"."{running}"'}

    def broken(statement, **kwargs):
        raise RuntimeError("connection closed")

    fake.sql = broken
    manager._cleanup_on_close()  # must not raise


def test_session_close_runs_the_jobs_cleanup():
    conn = mock.MagicMock()
    conn.is_closed.return_value = False
    session = Session(conn)
    session._jobs = mock.MagicMock()
    session.close()
    session._jobs._cleanup_on_close.assert_called_once_with()
    # a session that never used jobs closes as before
    session = Session(conn)
    session.close()


# F4: get_deployment rebuilds the configuration

POOL_SPEC = """
bundle:
  type: custom
  language: python
  compute_type: compute_pool
  compute_options:
    compute_pool: MY_POOL
    query_warehouse: QWH
    runtime_version: V2.9-CPU-PY3.12
  properties:
    requirements_file: requirements.txt
  env_vars:
  - A: '1'
  - B: x
  secrets:
  - DB.SCH.SEC
  external_access_integrations:
  - EAI
  artifact_repositories:
  - REPO
  stage_mounts:
    data_0:
      stage_url: '@stg/data'
      mount_path: /mnt/data
"""


def _bundle_fake(
    spec=POOL_SPEC, requirements=b"pandas==2.2.0\n# c\nnumpy\n", launcher=True
):
    fake = FakeSession(
        {
            r"^SHOW CODE BUNDLES": [{"name": "B"}],
            r"^DESCRIBE CODE BUNDLE": [
                {"last_version_source_location_uri": "@STG/snowpark_jobs/abc/"}
            ],
        }
    )
    if spec is not None:
        fake.file.streams["@STG/snowpark_jobs/abc/code_bundle.yml"] = spec.encode()
    if requirements is not None:
        fake.file.streams["@STG/snowpark_jobs/abc/requirements.txt"] = requirements
    if launcher:
        fake.responses[r"^LIST '@STG/snowpark_jobs/abc/"] = [{"name": LAUNCHER}]
    return fake


def test_get_deployment_rebuilds_target_and_environment():
    dep = JobManager(_bundle_fake()).get_deployment("B")
    assert isinstance(dep, CodeBundleDeployment)
    target = dep._target_config
    assert (target.compute_pool, target.warehouse, target.runtime_version) == (
        "MY_POOL",
        "QWH",
        "V2.9-CPU-PY3.12",
    )
    assert dep._compute_type == "compute_pool"
    env = dep._env
    assert env.env_vars == {"A": "1", "B": "x"}
    assert env.secrets == {"SEC": "DB.SCH.SEC"}
    assert env.external_access_integrations == ["EAI"]
    assert env.artifact_repositories == ["REPO"]
    assert env.stage_mounts == {"/mnt/data": "@stg/data"}
    assert env.packages == ["pandas==2.2.0", "numpy"]
    assert dep._spec["bundle"]["compute_type"] == "compute_pool"


def test_add_version_after_get_deployment_keeps_the_configuration(pkg_dir):
    fake = _bundle_fake()
    dep = JobManager(fake).get_deployment("B")
    dep.add_version(Program.directory(pkg_dir))
    uploads = {name: data for name, _, data in fake.file.puts}
    manifest = yaml.safe_load(uploads["code_bundle.yml"])["bundle"]
    assert manifest["compute_type"] == "compute_pool"
    assert manifest["compute_options"] == {
        "compute_pool": "MY_POOL",
        "query_warehouse": "QWH",
        "runtime_version": "V2.9-CPU-PY3.12",
    }
    assert manifest["env_vars"] == [{"A": "1"}, {"B": "x"}]
    assert manifest["secrets"] == ["DB.SCH.SEC"]
    assert manifest["stage_mounts"]["mnt_data_0"]["mount_path"] == "/mnt/data"
    assert uploads["requirements.txt"] == b"pandas==2.2.0\nnumpy\n"
    assert any(s.startswith("ALTER CODE BUNDLE") for s in fake.statements)


def test_warehouse_bundle_is_rebuilt_without_pool():
    spec = (
        "bundle:\n  type: custom\n  language: python\n  compute_type: warehouse\n"
        "  compute_options:\n    runtime_version: '3.11'\n"
    )
    dep = JobManager(_bundle_fake(spec, requirements=None)).get_deployment("B")
    target = dep._target_config
    assert (target.compute_pool, target.warehouse, target.runtime_version) == (
        None,
        None,
        "3.11",
    )
    assert dep._compute_type == "warehouse"
    assert dep._env.set_fields() == []


def test_unreadable_manifest_blocks_add_version_and_overrides_only(pkg_dir):
    fake = _bundle_fake(spec=None)
    dep = JobManager(fake).get_deployment("B")
    with pytest.raises(ValueError, match="configuration is not known.*code_bundle.yml"):
        dep.add_version(Program.directory(pkg_dir))
    with pytest.raises(ValueError, match="specification.*could not be loaded"):
        dep.run(entrypoint="a.py", overrides={"env_vars": []})
    assert fake.file.puts == []
    assert not any(s.startswith("ALTER CODE BUNDLE") for s in fake.statements)
    # plain runs still work
    dep.run(entrypoint="a.py")
    assert last_statement(fake).startswith("EXECUTE CODE BUNDLE")


def test_invalid_manifest_is_a_problem_not_a_default(pkg_dir):
    for text in ("- a list\n", ":::not yaml"):
        fake = _bundle_fake(spec=text)
        dep = JobManager(fake).get_deployment("B")
        with pytest.raises(ValueError, match="configuration is not known"):
            dep.add_version(Program.directory(pkg_dir))


def test_unreadable_requirements_block_add_version_but_not_overrides(pkg_dir):
    fake = _bundle_fake(requirements=None)
    dep = JobManager(fake).get_deployment("B")
    with pytest.raises(ValueError, match="packages in requirements.txt"):
        dep.add_version(Program.directory(pkg_dir))
    dep.run(entrypoint="a.py", overrides={"env_vars": [{"Z": "1"}]})
    assert "WITH SPECIFICATION" in last_statement(fake)


def test_loaded_deployment_without_default_entrypoint_gives_a_clear_error():
    fake = _bundle_fake()
    jobs_ = JobManager(fake)
    with pytest.raises(ValueError, match="not recorded in the bundle"):
        jobs_.get_deployment("B").run()
    with pytest.raises(ValueError, match="not recorded in the bundle"):
        jobs_.submit(Program.ref("B"))
    jobs_.submit(Program.ref("B"), entrypoint="a.py")
    assert "ENTRYPOINT = 'a.py'" in last_statement(fake)


# F6: schedule(replace=...)


def _sched_dep(fake):
    return ProcedureDeployment(fake, "DB.SCH.P")


def test_schedule_creates_without_replace_by_default(fake):
    _sched_dep(fake).schedule("0 * * * * UTC")
    create = [s for s in fake.statements if "TASK" in s and s.startswith("CREATE")][0]
    assert create.startswith("CREATE TASK DB.SCH.P_SCHEDULE")
    assert "OR REPLACE" not in create


def test_schedule_replace_true(fake):
    _sched_dep(fake).schedule("0 * * * * UTC", replace=True)
    assert fake.statements[0].startswith("CREATE OR REPLACE TASK DB.SCH.P_SCHEDULE")


def test_schedule_existing_task_hints_at_replace(fake):
    original = fake.sql

    def exists(statement, **kwargs):
        if statement.startswith("CREATE TASK"):
            raise ProgrammingError(
                msg="Object 'P_SCHEDULE' already exists.", errno=2002
            )
        return original(statement, **kwargs)

    fake.sql = exists
    with pytest.raises(ValueError, match="replace=True") as info:
        _sched_dep(fake).schedule("0 * * * * UTC")
    assert "already exists" in str(info.value)
    assert isinstance(info.value.__cause__, ProgrammingError)

    def other_error(statement, **kwargs):
        raise ProgrammingError(msg="insufficient privileges", errno=3001)

    fake.sql = other_error
    with pytest.raises(ProgrammingError, match="insufficient"):
        _sched_dep(fake).schedule("0 * * * * UTC")


def test_schedule_resume_failure_says_the_task_exists_suspended(fake):
    original = fake.sql

    def no_resume(statement, **kwargs):
        if statement.startswith("ALTER TASK") and "RESUME" in statement:
            raise ProgrammingError(msg="Cannot resume", errno=3042)
        return original(statement, **kwargs)

    fake.sql = no_resume
    with pytest.raises(RuntimeError, match="created but is suspended") as info:
        _sched_dep(fake).schedule("0 * * * * UTC")
    assert "DB.SCH.P_SCHEDULE" in str(info.value)
    assert isinstance(info.value.__cause__, ProgrammingError)


# minor fixes


def test_stored_procedure_submit_rejects_stage(fake, jobs):
    with pytest.raises(ValueError, match="stage is only used by deploy"):
        jobs.submit(lambda session: 1, stage="@STG")
    assert fake.statements == []


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_render_args_rejects_non_finite_floats(value):
    with pytest.raises(ValueError, match="cannot be"):
        _sproc.render_args([value])
    assert _sproc.render_args([1.5, 2, True, None]) == (
        "ARRAY_CONSTRUCT(1.5, 2, TRUE, NULL)"
    )


def _procedures(*signatures):
    return [
        {"name": "P", "is_builtin": "N", "arguments": f"P({sig}) RETURN VARIANT"}
        for sig in signatures
    ]


def test_get_deployment_skips_non_array_overloads(fake, jobs):
    fake.responses[r"^SHOW PROCEDURES"] = _procedures(
        "VARCHAR", "ARRAY", "NUMBER, NUMBER"
    )
    assert isinstance(jobs.get_deployment("P"), ProcedureDeployment)


def test_get_deployment_without_array_overload_fails(fake, jobs):
    fake.responses[r"^SHOW PROCEDURES"] = _procedures("VARCHAR", "NUMBER")
    with pytest.raises(ValueError, match=r"no \(ARRAY\) overload.*VARCHAR"):
        jobs.get_deployment("P")


def test_function_registration_holds_the_pickle_lock(fake):
    seen = []

    class Registry:
        def register(self, *args, **kwargs):
            seen.append(_sproc._PICKLE_LOCK.locked())
            return mock.MagicMock()

    fake.sproc = Registry()
    prepared = _sproc.prepare_program(
        Program.function(lambda session: 1), None, Environment()
    )
    _sproc._register(fake, prepared, Environment())
    assert seen == [True]
    assert not _sproc._PICKLE_LOCK.locked()


def test_list_sorts_by_real_timestamps(fake, jobs):
    utc = datetime.timezone.utc
    pst = datetime.timezone(datetime.timedelta(hours=-8))
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [
        {
            "QUERY_ID": "B1",
            "START_TIME": datetime.datetime(2025, 1, 9, 17, 0, tzinfo=pst),
            "STATUS": "DONE",
            "EXECUTION_NAME": "SNOWPARK_JOB_X",
        }
    ]
    fake.responses[r"QUERY_HISTORY_BY_USER"] = [
        # naive timestamps are UTC: 00:30 is older than the bundle run (01:00 UTC)
        [Q1, datetime.datetime(2025, 1, 10, 0, 30), "SUCCESS"],
        [Q2, datetime.datetime(2025, 1, 10, 2, 0, tzinfo=utc), "SUCCESS"],
    ]
    listed = [j.id for j in jobs.list()]
    assert listed == [f"query:{Q2}", "bundle:B1", f"query:{Q1}"]


def test_list_needs_a_current_database_for_procedure_jobs(fake, jobs):
    fake.get_current_database = lambda: None
    with pytest.raises(ValueError, match="current database"):
        jobs.list()
    with pytest.raises(ValueError, match="current database"):
        jobs.list(target="stored_procedure")
    jobs.list(target="code_bundle")  # does not read the query history


def _event_fake():
    return FakeSession({r"SHOW PARAMETERS": [{"value": "DB.EV.EVENTS"}]})


def test_logs_scan_is_bounded_by_the_job_start_time():
    fake = _event_fake()
    start = datetime.datetime(2025, 3, 4, 10, 0, tzinfo=datetime.timezone.utc)
    QueryJob(fake, FakeAsyncJob(Q1), started_at=start).logs()
    sql = fake.statements[-1]
    assert "TIMESTAMP >= '2025-03-04 09:55:00'::TIMESTAMP_NTZ" in sql
    # a local time zone is converted to UTC
    pst = datetime.timezone(datetime.timedelta(hours=-8))
    local = datetime.datetime(2025, 3, 4, 2, 0, tzinfo=pst)
    QueryJob(fake, FakeAsyncJob(Q1), started_at=local).logs()
    assert "'2025-03-04 09:55:00'" in fake.statements[-1]


def test_logs_without_start_time_scans_the_last_seven_days():
    fake = _event_fake()
    fake.responses[r"QUERY_HISTORY_BY_USER"] = []
    QueryJob(fake, FakeAsyncJob(Q1)).logs()
    assert "TIMESTAMP >= DATEADD(day, -7, SYSDATE())" in fake.statements[-1]


def test_logs_look_the_start_time_up_for_reattached_jobs():
    start = datetime.datetime(2025, 3, 4, 10, 0)
    fake = FakeSession(
        {
            r"SHOW PARAMETERS": [{"value": "DB.EV.EVENTS"}],
            r"SELECT START_TIME FROM": [[start]],
        }
    )
    QueryJob(fake, FakeAsyncJob(Q1)).logs()
    assert "'2025-03-04 09:55:00'" in fake.statements[-1]


def test_bundle_and_task_logs_use_their_start_times():
    start = datetime.datetime(2025, 3, 4, 10, 0)
    fake = FakeSession(
        {
            r"SHOW PARAMETERS": [{"value": "DB.EV.EVENTS"}],
            r"CODE_BUNDLE_HISTORY": [
                {"STATUS": "DONE", "COMPUTE_TYPE": "WAREHOUSE", "START_TIME": start}
            ],
            r"TASK_HISTORY": [
                {
                    "QUERY_ID": Q1,
                    "STATE": "SUCCEEDED",
                    "SCHEDULED_TIME": start,
                    "QUERY_TEXT": "CALL DB.SCH.P(ARRAY_CONSTRUCT())",
                }
            ],
        }
    )
    BundleRun(fake, Q1).logs()
    assert "'2025-03-04 09:55:00'" in fake.statements[-1]
    fake.statements.clear()
    TaskRun(fake, "DB.SCH.T", Q1).logs()
    assert "'2025-03-04 09:55:00'" in fake.statements[-1]


def test_internal_sql_never_emits_ast(fake, jobs, script, pkg_dir):
    fake.responses[r"SHOW PARAMETERS"] = [{"value": "DB.EV.EVENTS"}]
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [
        {
            "STATUS": "DONE",
            "QUERY_ID": Q1,
            "EXECUTION_NAME": "SNOWPARK_JOB_X",
            "START_TIME": datetime.datetime(2025, 1, 1),
        }
    ]
    dep = jobs.deploy(Program.directory(pkg_dir), CodeBundle(), name="B")
    dep.run(entrypoint="top.py")
    dep.schedule("0 * * * * UTC", entrypoint="top.py").runs()
    jobs.submit(Program.file(script), CodeBundle(compute_pool="POOL")).status()
    jobs.list()
    jobs.cleanup()
    BundleRun(fake, Q1).logs()
    dep.drop()
    assert fake.statements
    assert all(kwargs.get("_emit_ast") is False for kwargs in fake.sql_kwargs)


# F5: the deployment metadata comes from the bundle itself

VERSION_URI = "snow://code bundle/DB.SCH.B/versions/version$2/"


def _f5_fake(*, version=True, comment=None, source=True, spec=POOL_SPEC):
    """Which of the three metadata sources are available."""
    describe = {"comment": comment}
    if version:
        describe["default_version_location_uri"] = VERSION_URI
        describe["last_version_location_uri"] = VERSION_URI
    if source:
        describe["last_version_source_location_uri"] = "@GONE/snowpark_jobs/abc/"
    fake = FakeSession(
        {r"^SHOW CODE BUNDLES": [{"name": "B"}], r"^DESCRIBE CODE BUNDLE": [describe]}
    )
    if version:
        fake.file.streams[VERSION_URI + "code_bundle.yml"] = spec.encode()
        fake.file.streams[VERSION_URI + "requirements.txt"] = b"pandas==2.2.0\n"
        fake.responses[r"^LIST 'snow://code bundle/DB.SCH.B/versions/version\$2/"] = [
            {"name": LAUNCHER}
        ]
    if source:
        fake.file.streams["@GONE/snowpark_jobs/abc/code_bundle.yml"] = b"bundle: {}\n"
    return fake


def test_metadata_is_read_from_the_stored_version_first():
    fake = _f5_fake(version=True, source=True)  # the source stage holds other data
    dep = JobManager(fake).get_deployment("B")
    assert dep._target_config.compute_pool == "MY_POOL"
    assert dep._has_launcher is True
    assert dep._env.packages == ["pandas==2.2.0"]
    assert "ARGUMENTS = ('x.py:f')" in dep.to_sql(entrypoint="x.py:f")
    # the source stage was not needed
    assert not any("GONE" in st for st in fake.statements if st.startswith("LIST"))


def test_metadata_falls_back_to_the_bundle_comment():
    meta = {
        "spec": yaml.safe_load(POOL_SPEC),
        "has_launcher": True,
        "packages": ["numpy"],
    }
    comment = "snowpark.jobs:" + json.dumps(meta)
    fake = _f5_fake(version=False, comment=comment, source=False)
    dep = JobManager(fake).get_deployment("B")
    assert dep._target_config.compute_pool == "MY_POOL"
    assert dep._compute_type == "compute_pool"
    assert dep._has_launcher is True
    assert dep._env.packages == ["numpy"]  # not read from a requirements file
    assert dep._env_problem is None and dep._spec_problem is None
    dep.run(entrypoint="x.py:f", overrides={"env_vars": [{"Z": "1"}]})
    assert "WITH SPECIFICATION" in last_statement(fake)


def test_unreadable_version_falls_back_to_comment_before_source_stage():
    meta = {"spec": yaml.safe_load(POOL_SPEC), "has_launcher": False, "packages": []}
    fake = _f5_fake(version=True, comment="snowpark.jobs:" + json.dumps(meta))
    del fake.file.streams[VERSION_URI + "code_bundle.yml"]
    dep = JobManager(fake).get_deployment("B")
    assert dep._target_config.compute_pool == "MY_POOL"
    assert dep._has_launcher is False  # as recorded in the comment


def test_metadata_last_resort_is_the_source_stage():
    fake = _f5_fake(version=False, comment="a plain user comment", source=True)
    dep = JobManager(fake).get_deployment("B")
    assert dep._spec == {"bundle": {}}  # read from the source stage
    assert dep._spec_problem is None


def test_no_metadata_source_available_is_a_clear_problem(pkg_dir):
    fake = _f5_fake(version=False, comment=None, source=False)
    dep = JobManager(fake).get_deployment("B")
    with pytest.raises(ValueError, match="manifest could not be read"):
        dep.add_version(Program.directory(pkg_dir))
    with pytest.raises(ValueError, match="specification.*could not be loaded"):
        dep.run(entrypoint="a.py", overrides={"x": 1})
    with pytest.raises(ValueError, match="does not contain the session.jobs launcher"):
        dep.run(entrypoint="a.py:f")
    dep.run(entrypoint="a.py")  # plain runs need no metadata


def test_invalid_comment_is_ignored():
    for comment in ("snowpark.jobs:{not json", 'snowpark.jobs:{"spec": 1}', "other"):
        fake = _f5_fake(version=False, comment=comment, source=False)
        dep = JobManager(fake).get_deployment("B")
        assert dep._spec is None and dep._spec_problem


def test_deploy_records_the_metadata_in_the_bundle_comment(fake, jobs, pkg_dir):
    jobs.deploy(
        Program.directory(pkg_dir, "top.py"),
        CodeBundle(compute_pool="P", warehouse="QWH"),
        name="B",
        env=Environment(packages=["pandas"], env_vars={"A": "it's"}),
    )
    create = last_statement(fake)
    assert " COMMENT = 'snowpark.jobs:" in create
    literal = create.split(" COMMENT = ", 1)[1]
    text = literal[1:-1].replace("''", "'").replace("\\\\", "\\")
    meta = parse_metadata_comment(text)
    assert meta["spec"]["bundle"]["compute_options"]["compute_pool"] == "P"
    assert meta["spec"]["bundle"]["env_vars"] == [{"A": "it's"}]
    assert meta["has_launcher"] is True and meta["packages"] == ["pandas"]


def test_staged_programs_leave_the_comment_to_their_own_manifest(fake, jobs):
    jobs.deploy(Program.stage("@STG/b", "m.py"), CodeBundle(), name="B")
    assert "COMMENT" not in last_statement(fake)


def test_comment_that_does_not_fit_is_skipped_with_a_warning(caplog):
    spec = {"bundle": {"env_vars": [{"A": "x" * (MAX_COMMENT_CHARS + 10)}]}}
    with caplog.at_level(
        logging.WARNING, logger="snowflake.snowpark.jobs._bundle_spec"
    ):
        assert metadata_comment(spec, True, []) is None
    assert "does not fit" in caplog.text
    small = metadata_comment({"bundle": {}}, False, ["a"])
    assert len(small) < MAX_COMMENT_CHARS and parse_metadata_comment(small) == {
        "spec": {"bundle": {}},
        "has_launcher": False,
        "packages": ["a"],
    }


def test_add_version_updates_the_comment(pkg_dir):
    fake = _f5_fake()
    dep = JobManager(fake).get_deployment("B")
    dep.add_version(Program.directory(pkg_dir))
    comment = last_statement(fake)
    assert comment.startswith(
        "ALTER CODE BUNDLE DB.SCH.B SET COMMENT = 'snowpark.jobs:"
    )
    assert '"compute_pool":"MY_POOL"' in comment and '"has_launcher":true' in comment
    # a staged version brings its own manifest: the comment is cleared
    dep.add_version(Program.stage("@STG/v3"))
    assert "ALTER CODE BUNDLE DB.SCH.B SET COMMENT = ''" in fake.statements
    # a failing comment update does not undo the version
    original = fake.sql

    def failing(statement, **kwargs):
        if "SET COMMENT" in statement:
            raise RuntimeError("no privilege")
        return original(statement, **kwargs)

    fake.sql = failing
    dep.add_version(Program.directory(pkg_dir))


# ---- third review ----

# R2: the target's warehouse is recorded with the deployment


def test_deploy_records_the_target_warehouse(fake, jobs, pkg_dir):
    jobs.deploy(
        Program.directory(pkg_dir, "top.py"), CodeBundle(warehouse="BIG"), name="B"
    )
    create = last_statement(fake)
    literal = create.split(" COMMENT = ", 1)[1]
    assert parse_metadata_comment(literal[1:-1])["warehouse"] == "BIG"
    jobs.deploy(Program.directory(pkg_dir, "top.py"), CodeBundle(), name="B2")
    literal = last_statement(fake).split(" COMMENT = ", 1)[1]
    assert "warehouse" not in parse_metadata_comment(literal[1:-1])


def test_reattached_deployment_keeps_the_saved_warehouse(pkg_dir):
    meta = {"spec": yaml.safe_load(POOL_SPEC), "has_launcher": True, "packages": []}
    meta["warehouse"] = "SAVED"
    fake = _f5_fake(
        version=False, comment="snowpark.jobs:" + json.dumps(meta), source=False
    )
    manager = JobManager(fake)
    dep = manager.get_deployment("B")
    assert dep._target_config.warehouse == "SAVED"  # also for a warehouse bundle below
    wh_spec = {"bundle": {"compute_type": "warehouse"}}
    meta = {"spec": wh_spec, "has_launcher": True, "packages": [], "warehouse": "SAVED"}
    fake = _f5_fake(
        version=False, comment="snowpark.jobs:" + json.dumps(meta), source=False
    )
    dep = JobManager(fake).get_deployment("B")
    assert dep._target_config.warehouse == "SAVED"
    dep.run(entrypoint="a.py")
    assert _uses(fake) == ["USE WAREHOUSE SAVED", "USE WAREHOUSE WH"]
    # schedule() and add_version() keep it too
    dep.schedule("0 * * * * UTC", entrypoint="a.py")
    assert (
        " WAREHOUSE = SAVED "
        in [s for s in fake.statements if s.startswith("CREATE TASK")][-1]
    )
    dep.add_version(Program.directory(pkg_dir))
    assert '"warehouse":"SAVED"' in last_statement(fake)


def test_saved_warehouse_applies_when_the_manifest_comes_from_the_version():
    fake = _f5_fake(
        version=True,
        comment="snowpark.jobs:"
        + json.dumps({"spec": {"bundle": {}}, "warehouse": "SAVED"}),
    )
    fake.file.streams[VERSION_URI + "code_bundle.yml"] = WAREHOUSE_MANIFEST
    assert JobManager(fake).get_deployment("B")._target_config.warehouse == "SAVED"


# R3: schedule() compute


def test_schedule_without_any_warehouse_is_an_error():
    fake = FakeSession(warehouse=None)
    with pytest.raises(ValueError, match="No warehouse for the task.*serverless=True"):
        ProcedureDeployment(fake, "DB.SCH.P").schedule("0 * * * * UTC")
    assert fake.statements == []
    ProcedureDeployment(fake, "DB.SCH.P").schedule("0 * * * * UTC", serverless=True)
    assert "WAREHOUSE" not in fake.statements[0]


def test_serverless_cannot_be_combined_with_a_warehouse(fake):
    with pytest.raises(ValueError, match="serverless=True cannot be combined"):
        ProcedureDeployment(fake, "DB.SCH.P").schedule(
            "0 * * * * UTC", warehouse="W", serverless=True
        )
    assert fake.statements == []


def test_serverless_ignores_the_target_warehouse(fake, jobs, pkg_dir):
    dep = jobs.deploy(
        Program.directory(pkg_dir, "top.py"), CodeBundle(warehouse="TWH"), name="B"
    )
    dep.schedule("0 * * * * UTC", serverless=True)
    task = [s for s in fake.statements if s.startswith("CREATE TASK")][-1]
    assert "WAREHOUSE" not in task


# R4: a succeeded EXECUTE without a history row is done


def test_succeeded_execute_without_history_row_is_done_after_the_grace(
    fake, monkeypatch
):
    run = BundleRun(fake, Q1)
    now = [100.0]
    monkeypatch.setattr(_handles.time, "monotonic", lambda: now[0])
    grace = _handles.BUNDLE_SUCCESS_WITHOUT_HISTORY_GRACE_SECONDS

    def status_at(seconds, query_status):
        now[0] = seconds
        with mock.patch.object(
            _handles, "_query_status_name", return_value=query_status
        ):
            return run.status()

    assert status_at(100.0, "RUNNING") == JobStatus.PENDING  # running, seen
    assert status_at(105.0, "SUCCESS") == JobStatus.PENDING  # the grace starts here
    assert status_at(105.0 + grace - 1, "SUCCESS") == JobStatus.PENDING
    assert status_at(105.0 + grace + 1, "SUCCESS") == JobStatus.SUCCEEDED


def test_succeeded_execute_without_history_row_never_hangs_wait(fake, monkeypatch):
    monkeypatch.setattr(_handles, "BUNDLE_SUCCESS_WITHOUT_HISTORY_GRACE_SECONDS", 0.05)
    with mock.patch.object(_handles, "_query_status_name", return_value="SUCCESS"):
        assert BundleRun(fake, Q1).wait(poll_interval=0.02) == JobStatus.SUCCEEDED


# R5: Session.jobs is created once


def test_session_jobs_is_created_once_across_threads():
    import snowflake.snowpark.jobs as jobs_module

    created = []

    class SlowManager(jobs_module.JobManager):
        def __init__(self, session) -> None:
            created.append(self)
            time.sleep(0.05)  # widens the race window
            super().__init__(session)

    session = Session(mock.MagicMock())
    barrier = threading.Barrier(2)
    seen = []

    def worker():
        barrier.wait()
        seen.append(session.jobs)

    with mock.patch.object(jobs_module, "JobManager", SlowManager):
        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    assert len(created) == 1 and seen[0] is seen[1] is created[0]


# R6: the age of unused temporary bundles


def test_unused_temporary_bundle_needs_a_day_to_be_dropped():
    assert _manager._UNUSED_TEMP_BUNDLE_MIN_AGE == datetime.timedelta(hours=24)
    now = datetime.datetime.now(datetime.timezone.utc)
    young, old = _bundle_names(2)
    fake = _cleanup_fake(
        [
            {"name": young, "created_on": now - datetime.timedelta(hours=23)},
            {"name": old, "created_on": now - datetime.timedelta(hours=25)},
        ],
        {},
    )
    assert JobManager(fake).cleanup() == [f'"DB"."SCH"."{old}"']


# R8: a failed restore does not hide the submit


def test_failed_warehouse_restore_keeps_the_job(fake, jobs, script, caplog):
    original = fake.use_warehouse

    def use(warehouse):
        if warehouse == "WH":
            raise RuntimeError("warehouse WH was dropped")
        original(warehouse)

    fake.use_warehouse = use
    with caplog.at_level(logging.WARNING, logger="snowflake.snowpark.jobs._utils"):
        run = jobs.submit(Program.file(script), CodeBundle(warehouse="OTHER"))
    assert isinstance(run, BundleRun) and run.query_id
    assert "WH" in caplog.text and "OTHER" in caplog.text
    # an error of the submission itself still propagates
    fake.sql_original = fake.sql

    def failing(statement, **kwargs):
        if statement.startswith("EXECUTE CODE BUNDLE"):
            raise RuntimeError("submit failed")
        return fake.sql_original(statement, **kwargs)

    fake.sql = failing
    with pytest.raises(RuntimeError, match="submit failed"):
        jobs.submit(Program.file(script), CodeBundle(warehouse="OTHER"))


# R10: a staged .py is its own entrypoint


def test_staged_py_file_must_match_the_entrypoint(fake, jobs):
    with pytest.raises(ValueError, match="does not match the staged file 'a.py'"):
        jobs.submit(Program.stage("@s/a.py", "b.py"))
    with pytest.raises(ValueError, match="does not match the staged file"):
        _sproc.prepare_program(Program.stage("@s/a.py", "b.py:f"), None, Environment())
    ok = _sproc.prepare_program(Program.stage("@s/a.py", "a.py"), None, Environment())
    assert ok.imports == ["@s/a.py"]
    # zips can contain any entrypoint
    _sproc.prepare_program(
        Program.stage("@s/code.zip", "pkg/main.py"), None, Environment()
    )


# R12: the status filter of list() is case-insensitive


def test_list_status_filter_accepts_any_case(fake, jobs):
    fake.responses[r"CODE_BUNDLE_HISTORY"] = [
        {
            "QUERY_ID": "B1",
            "START_TIME": datetime.datetime(2025, 1, 2),
            "STATUS": "done",
            "EXECUTION_NAME": "SNOWPARK_JOB_X",
        }
    ]
    assert [j.id for j in jobs.list(target="code_bundle", status="SUCCEEDED")] == [
        "bundle:B1"
    ]


# R14: overrides are documented as unstable


def test_overrides_are_documented_as_unstable():
    assert "unstable" in _deployments.Deployment.run.__doc__


# R11: the server-side runner


@pytest.fixture
def import_dir(tmp_path, monkeypatch):
    """Fakes the directory in which Snowflake places the imports of a procedure."""
    directory = tmp_path / "imports"
    directory.mkdir()
    monkeypatch.setitem(sys._xoptions, "snowflake_import_directory", str(directory))
    before = (list(sys.path), set(sys.modules))
    yield directory
    sys.path[:] = before[0]
    for name in set(sys.modules) - before[1]:
        del sys.modules[name]


def _zip(directory, name, files):
    import zipfile

    with zipfile.ZipFile(directory / name, "w") as zf:
        for path, text in files.items():
            zf.writestr(path, text)


def test_runner_function_form_calls_fn_with_session_and_args():
    session = object()
    spec = {"kind": "function", "fn": lambda s, a, b: {"s": s is session, "ab": a + b}}
    assert _runner.run_program(session, spec, [1, 2]) == {"s": True, "ab": 3}
    assert (
        _runner.run_program(session, {"kind": "function", "fn": lambda s: 1}, None) == 1
    )


def test_runner_script_form_sets_argv_and_returns_dunder_return(import_dir):
    _zip(
        import_dir,
        "snowpark_job_x.zip",
        {"snowpark_job_x/script.py": "import sys\n__return__ = {'argv': sys.argv}\n"},
    )
    spec = {
        "kind": "files",
        "archive": "snowpark_job_x.zip",
        "root": "snowpark_job_x",
        "file": "script.py",
    }
    before = list(sys.argv)
    result = _runner.run_program(object(), spec, ["a", 2])
    assert result == {"argv": ["script.py", "a", "2"]}  # arguments become strings
    assert sys.argv == before  # restored
    _zip(import_dir, "snowpark_job_y.zip", {"snowpark_job_y/s.py": "x = 1\n"})
    spec.update(archive="snowpark_job_y.zip", root="snowpark_job_y", file="s.py")
    assert _runner.run_program(object(), spec, []) is None  # no __return__


def test_runner_function_in_a_package_imports_siblings(import_dir):
    _zip(
        import_dir,
        "snowpark_job_p.zip",
        {
            "snowpark_job_p/pkg/__init__.py": "",
            "snowpark_job_p/pkg/helper.py": "def twice(x):\n    return x * 2\n",
            "snowpark_job_p/pkg/main.py": (
                "from pkg import helper\n"
                "def go(session, n):\n"
                "    return {'session': session, 'twice': helper.twice(n)}\n"
            ),
        },
    )
    spec = {
        "kind": "files",
        "archive": "snowpark_job_p.zip",
        "root": "snowpark_job_p",
        "file": "pkg/main.py",
        "func": "go",
    }
    assert _runner.run_program("SESSION", spec, ["ab"]) == {
        "session": "SESSION",
        "twice": "abab",
    }


def test_runner_converts_results_to_json_types(import_dir):
    import decimal

    spec = {
        "kind": "function",
        "fn": lambda s: {
            "when": datetime.date(2025, 1, 2),
            "amount": decimal.Decimal("1.50"),
            "items": (1, 2),
        },
    }
    assert _runner.run_program(None, spec, []) == {
        "when": "2025-01-02",
        "amount": "1.50",
        "items": [1, 2],
    }
    assert (
        _runner.run_program(None, {"kind": "function", "fn": lambda s: None}, [])
        is None
    )


def test_runner_copies_a_plain_staged_py_file(import_dir):
    (import_dir / "tool.py").write_text("__return__ = 'plain'\n")
    spec = {"kind": "files", "archive": "tool.py", "root": "", "file": "tool.py"}
    assert _runner.run_program(None, spec, []) == "plain"


def test_runner_without_import_directory_fails_clearly(monkeypatch):
    monkeypatch.delitem(sys._xoptions, "snowflake_import_directory", raising=False)
    spec = {"kind": "files", "archive": "x.zip", "root": "", "file": "x.py"}
    with pytest.raises(RuntimeError, match="snowflake_import_directory"):
        _runner.run_program(None, spec, [])


def test_make_runner_returns_a_two_argument_function():
    runner = _runner.make_runner({"kind": "function", "fn": lambda s, x: x + 1})
    assert runner("SESSION", [1]) == 2
