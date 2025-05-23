#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import gc
import hashlib
import logging
import os
import re
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple  # noqa: F401
from unittest.mock import patch

import pytest

from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
    PlanState,
)
from snowflake.snowpark._internal.compiler.cte_utils import find_duplicate_subtrees
from snowflake.snowpark.session import (
    _PYTHON_SNOWPARK_ENABLE_THREAD_SAFE_SESSION,
    Session,
)
from snowflake.snowpark.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from tests.integ.test_temp_table_cleanup import wait_for_drop_table_sql_done

try:
    import dateutil

    # six is the dependency of dateutil
    import six

    is_dateutil_available = True
except ImportError:
    is_dateutil_available = False

from snowflake.snowpark.functions import col, lit, sum_distinct
from snowflake.snowpark.row import Row
from tests.utils import (
    IS_IN_STORED_PROC,
    IS_IN_STORED_PROC_LOCALFS,
    IS_LINUX,
    IS_WINDOWS,
    TestFiles,
    Utils,
)


@pytest.fixture(scope="function")
def threadsafe_session(
    db_parameters,
    session,
    sql_simplifier_enabled,
    local_testing_mode,
):
    if IS_IN_STORED_PROC:
        yield session
    else:
        new_db_parameters = db_parameters.copy()
        new_db_parameters["local_testing"] = local_testing_mode
        with Session.builder.configs(new_db_parameters).create() as session:
            session._sql_simplifier_enabled = sql_simplifier_enabled
            yield session


@pytest.fixture(scope="function")
def threadsafe_temp_stage(threadsafe_session, resources_path, local_testing_mode):
    tmp_stage_name = Utils.random_stage_name()
    test_files = TestFiles(resources_path)

    if not local_testing_mode:
        Utils.create_stage(threadsafe_session, tmp_stage_name, is_temporary=True)
    Utils.upload_to_stage(
        threadsafe_session, tmp_stage_name, test_files.test_file_parquet, compress=False
    )
    yield tmp_stage_name
    if not local_testing_mode:
        Utils.drop_stage(threadsafe_session, tmp_stage_name)


def test_threadsafe_session_uses_locks(threadsafe_session):
    rlock_class = threading.RLock().__class__
    assert isinstance(threadsafe_session._lock, rlock_class)
    assert isinstance(threadsafe_session._temp_table_auto_cleaner.lock, rlock_class)
    assert isinstance(threadsafe_session._conn._lock, rlock_class)


def test_concurrent_select_queries(threadsafe_session):
    def run_select(session_, thread_id):
        df = session_.sql(f"SELECT {thread_id} as A")
        assert df.collect()[0][0] == thread_id

    with ThreadPoolExecutor(max_workers=10) as executor:
        for i in range(10):
            executor.submit(run_select, threadsafe_session, i)


def test_concurrent_dataframe_operations(threadsafe_session):
    try:
        table_name = Utils.random_table_name()
        data = [(i, 11 * i) for i in range(10)]
        df = threadsafe_session.create_dataframe(data, ["A", "B"])
        df.write.save_as_table(table_name, table_type="temporary")

        def run_dataframe_operation(session_, thread_id):
            df = session_.table(table_name)
            df = df.filter(df.a == lit(thread_id))
            df = df.with_column("C", df.b + 100 * df.a)
            df = df.rename(df.a, "D").limit(1)
            return df

        dfs = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            df_futures = [
                executor.submit(run_dataframe_operation, threadsafe_session, i)
                for i in range(10)
            ]

            for future in as_completed(df_futures):
                dfs.append(future.result())

        main_df = dfs[0]
        for df in dfs[1:]:
            main_df = main_df.union(df)

        Utils.check_answer(
            main_df, [Row(D=i, B=11 * i, C=11 * i + 100 * i) for i in range(10)]
        )

    finally:
        Utils.drop_table(threadsafe_session, table_name)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL query and query listeners are not supported",
    run=False,
)
def test_query_listener(threadsafe_session):
    def run_select(session_, thread_id):
        session_.sql(f"SELECT {thread_id} as A").collect()

    with threadsafe_session.query_history() as history:
        with ThreadPoolExecutor(max_workers=10) as executor:
            for i in range(10):
                executor.submit(run_select, threadsafe_session, i)

    queries_sent = [query.sql_text for query in history.queries]
    assert len(queries_sent) == 10
    for i in range(10):
        assert f"SELECT {i} as A" in queries_sent


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="Query tag is a SQL feature",
    run=False,
)
@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="show parameters is not supported in stored procedure"
)
def test_query_tagging(threadsafe_session):
    def set_query_tag(session_, thread_id):
        session_.query_tag = f"tag_{thread_id}"

    with ThreadPoolExecutor(max_workers=10) as executor:
        for i in range(10):
            executor.submit(set_query_tag, threadsafe_session, i)

    actual_query_tag = threadsafe_session.sql(
        "SHOW PARAMETERS LIKE 'QUERY_TAG'"
    ).collect()[0][1]
    assert actual_query_tag == threadsafe_session.query_tag


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL query is not supported",
    run=False,
)
def test_session_stage_created_once(threadsafe_session):
    with patch.object(
        threadsafe_session._conn, "run_query", wraps=threadsafe_session._conn.run_query
    ) as patched_run_query:
        with ThreadPoolExecutor(max_workers=10) as executor:
            for _ in range(10):
                executor.submit(threadsafe_session.get_session_stage)

        assert patched_run_query.call_count == 1


def test_action_ids_are_unique(threadsafe_session):
    with ThreadPoolExecutor(max_workers=10) as executor:
        action_ids = set()
        futures = [
            executor.submit(threadsafe_session._generate_new_action_id)
            for _ in range(10)
        ]

        for future in as_completed(futures):
            action_ids.add(future.result())

    assert len(action_ids) == 10


@pytest.mark.skipif(IS_IN_STORED_PROC_LOCALFS, reason="Skip file IO tests in localfs")
@pytest.mark.parametrize("use_stream", [True, False])
@pytest.mark.skipif(
    pytest.param("local_testing_mode"),
    reason="TODO SNOW-1826001: Bug in local testing mode.",
)
def test_file_io(threadsafe_session, resources_path, threadsafe_temp_stage, use_stream):
    stage_prefix = f"prefix_{Utils.random_alphanumeric_str(10)}"
    stage_with_prefix = f"@{threadsafe_temp_stage}/{stage_prefix}"
    test_files = TestFiles(resources_path)

    resources_files = [
        test_files.test_file_csv,
        test_files.test_file2_csv,
        test_files.test_file_json,
        test_files.test_file_csv_header,
        test_files.test_file_csv_colon,
        test_files.test_file_csv_quotes,
        test_files.test_file_csv_special_format,
        test_files.test_file_json_special_format,
        test_files.test_file_csv_quotes_special,
        test_files.test_concat_file1_csv,
        test_files.test_concat_file2_csv,
    ]

    def get_file_hash(fd):
        return hashlib.md5(fd.read()).hexdigest()

    def put_and_get_file(upload_file_path, download_dir):
        if use_stream:
            with open(upload_file_path, "rb") as fd:
                stage_file_name = (
                    f"{stage_with_prefix}/{os.path.basename(upload_file_path)}"
                )
                result = threadsafe_session.file.put_stream(
                    fd, stage_file_name, auto_compress=False, overwrite=False
                )
        else:
            results = threadsafe_session.file.put(
                upload_file_path,
                stage_with_prefix,
                auto_compress=False,
                overwrite=False,
            )
            assert len(results) == 1
            result = results[0]
        # assert file is uploaded successfully
        assert result.status == "UPLOADED"

        stage_file_name = f"{stage_with_prefix}/{result.target}"
        if use_stream:
            fd = threadsafe_session.file.get_stream(stage_file_name)
            with open(upload_file_path, "rb") as upload_fd:
                assert get_file_hash(upload_fd) == get_file_hash(fd)

        else:
            results = threadsafe_session.file.get(stage_file_name, download_dir)
            # assert file is downloaded successfully
            assert len(results) == 1
            assert results[0].status == "DOWNLOADED"
            download_file_path = os.path.join(download_dir, results[0].file)
            # assert two files are identical
            with open(upload_file_path, "rb") as upload_fd, open(
                download_file_path, "rb"
            ) as download_fd:
                assert get_file_hash(upload_fd) == get_file_hash(download_fd)

    with tempfile.TemporaryDirectory() as download_dir:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(put_and_get_file, file_path, download_dir)
                for file_path in resources_files
            ]

            for future in as_completed(futures):
                future.result()

        if not use_stream:
            # assert all files are downloaded
            assert set(os.listdir(download_dir)) == {
                os.path.basename(file_path) for file_path in resources_files
            }


def test_concurrent_add_packages(threadsafe_session):
    # this is a list of packages available in snowflake anaconda. If this
    # test fails due to packages not being available, please update the list
    existing_packages = threadsafe_session.get_packages()
    package_list = {
        "cloudpickle",
        "numpy",
        "pandas",
        "scipy",
        "scikit-learn",
        "pyyaml",
    }

    try:
        with ThreadPoolExecutor(max_workers=10) as executor:
            for package in package_list:
                executor.submit(threadsafe_session.add_packages, package)

        final_packages = threadsafe_session.get_packages()
        for package in package_list:
            assert package in final_packages
    finally:
        for package in package_list:
            if package not in existing_packages:
                threadsafe_session.remove_package(package)


def test_concurrent_remove_package(threadsafe_session):
    def remove_package(session_, package_name):
        try:
            session_.remove_package(package_name)
            return True
        except ValueError:
            return False
        except Exception as e:
            raise e

    try:
        threadsafe_session.add_packages("numpy")
        with ThreadPoolExecutor(max_workers=10) as executor:

            futures = [
                executor.submit(remove_package, threadsafe_session, "numpy")
                for _ in range(10)
            ]
            success_count, failure_count = 0, 0
            for future in as_completed(futures):
                if future.result():
                    success_count += 1
                else:
                    failure_count += 1

            # assert that only one thread was able to remove the package
            assert success_count == 1
            assert failure_count == 9
    finally:
        threadsafe_session.clear_packages()


@pytest.mark.skipif(not is_dateutil_available, reason="dateutil is not available")
def test_concurrent_add_import(threadsafe_session, resources_path):
    test_files = TestFiles(resources_path)
    existing_imports = set(threadsafe_session.get_imports())
    import_files = [
        test_files.test_udf_py_file,
        os.path.relpath(test_files.test_udf_py_file),
        test_files.test_udf_directory,
        os.path.relpath(test_files.test_udf_directory),
        six.__file__,
        os.path.relpath(six.__file__),
        os.path.dirname(dateutil.__file__),
    ]
    try:
        with ThreadPoolExecutor(max_workers=10) as executor:
            for file in import_files:
                executor.submit(
                    threadsafe_session.add_import,
                    file,
                )

        assert set(threadsafe_session.get_imports()) == {
            os.path.abspath(file) for file in import_files
        }.union(existing_imports)
    finally:
        threadsafe_session.clear_imports()


def test_concurrent_remove_import(threadsafe_session, resources_path):
    test_files = TestFiles(resources_path)

    def remove_import(session_, import_file):
        try:
            session_.remove_import(import_file)
            return True
        except KeyError:
            return False
        except Exception as e:
            raise e

    try:
        threadsafe_session.add_import(test_files.test_udf_py_file)
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(
                    remove_import, threadsafe_session, test_files.test_udf_py_file
                )
                for _ in range(10)
            ]

            success_count, failure_count = 0, 0
            for future in as_completed(futures):
                if future.result():
                    success_count += 1
                else:
                    failure_count += 1

            # assert that only one thread was able to remove the import
            assert success_count == 1
            assert failure_count == 9
    finally:
        threadsafe_session.clear_imports()


def test_concurrent_sp_register(threadsafe_session, tmpdir):
    try:
        threadsafe_session.add_packages("snowflake-snowpark-python")

        def register_and_test_sp(session_, thread_id):
            prefix = Utils.random_alphanumeric_str(10)
            sp_file_path = os.path.join(tmpdir, f"{prefix}_add_{thread_id}.py")
            sproc_body = f"""
from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col,
    lit
)
def add_{thread_id}(session_: Session, x: int) -> int:
    return (
        session_.create_dataframe([[x, ]], schema=["x"])
        .select(col("x") + lit({thread_id}))
        .collect()[0][0]
    )
"""
            with open(sp_file_path, "w") as f:
                f.write(sproc_body)
                f.flush()

            add_sp_from_file = session_.sproc.register_from_file(
                sp_file_path, f"add_{thread_id}"
            )
            add_sp = session_.sproc.register(
                lambda sess_, x: sess_.sql(f"select {x} + {thread_id}").collect()[0][0],
                return_type=IntegerType(),
                input_types=[IntegerType()],
            )

            assert add_sp_from_file(1) == thread_id + 1
            assert add_sp(1) == thread_id + 1

        with ThreadPoolExecutor(max_workers=10) as executor:
            for i in range(10):
                executor.submit(register_and_test_sp, threadsafe_session, i)
    finally:
        threadsafe_session.clear_packages()


def test_concurrent_udf_register(threadsafe_session, tmpdir):
    df = threadsafe_session.range(-5, 5).to_df("a")

    def register_and_test_udf(session_, thread_id):
        prefix = Utils.random_alphanumeric_str(10)
        file_path = os.path.join(tmpdir, f"{prefix}_add_{thread_id}.py")
        with open(file_path, "w") as f:
            func = f"""
def add_{thread_id}(x: int) -> int:
    return x + {thread_id}
"""
            f.write(func)
            f.flush()
        add_i_udf_from_file = session_.udf.register_from_file(
            file_path, f"add_{thread_id}"
        )
        add_i_udf = session_.udf.register(
            lambda x: x + thread_id,
            return_type=IntegerType(),
            input_types=[IntegerType()],
        )

        Utils.check_answer(
            df.select(add_i_udf(df.a), add_i_udf_from_file(df.a)),
            [(thread_id + i, thread_id + i) for i in range(-5, 5)],
        )

    with ThreadPoolExecutor(max_workers=10) as executor:
        for i in range(10):
            executor.submit(register_and_test_udf, threadsafe_session, i)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="UDTFs is not supported in local testing mode",
    run=False,
)
def test_concurrent_udtf_register(threadsafe_session, tmpdir):
    def register_and_test_udtf(session_, thread_id):
        udtf_body = f"""
from typing import List, Tuple

class UDTFEcho:
    def process(
        self,
        num: int,
    ) -> List[Tuple[int]]:
        return [(num + {thread_id},)]
"""
        prefix = Utils.random_alphanumeric_str(10)
        file_path = os.path.join(tmpdir, f"{prefix}_udtf_echo_{thread_id}.py")
        with open(file_path, "w") as f:
            f.write(udtf_body)
            f.flush()

        d = {}
        exec(udtf_body, {**globals(), **locals()}, d)
        echo_udtf_from_file = session_.udtf.register_from_file(
            file_path, "UDTFEcho", output_schema=["num"]
        )
        echo_udtf = session_.udtf.register(d["UDTFEcho"], output_schema=["num"])

        df_local = threadsafe_session.table_function(echo_udtf(lit(1)))
        df_from_file = threadsafe_session.table_function(echo_udtf_from_file(lit(1)))
        assert df_local.collect() == [(thread_id + 1,)]
        assert df_from_file.collect() == [(thread_id + 1,)]

    with ThreadPoolExecutor(max_workers=10) as executor:
        for i in range(10):
            executor.submit(register_and_test_udtf, threadsafe_session, i)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="UDAFs is not supported in local testing mode",
    run=False,
)
def test_concurrent_udaf_register(threadsafe_session, tmpdir):
    df = threadsafe_session.create_dataframe([[1, 3], [1, 4], [2, 5], [2, 6]]).to_df(
        "a", "b"
    )

    def register_and_test_udaf(session_, thread_id):
        udaf_body = f"""
class OffsetSumUDAFHandler:
    def __init__(self) -> None:
        self._sum = 0

    @property
    def aggregate_state(self):
        return self._sum

    def accumulate(self, input_value):
        self._sum += input_value

    def merge(self, other_sum):
        self._sum += other_sum

    def finish(self):
        return self._sum + {thread_id}
    """
        prefix = Utils.random_alphanumeric_str(10)
        file_path = os.path.join(tmpdir, f"{prefix}_udaf_{thread_id}.py")
        with open(file_path, "w") as f:
            f.write(udaf_body)
            f.flush()
        d = {}
        exec(udaf_body, {**globals(), **locals()}, d)

        offset_sum_udaf_from_file = session_.udaf.register_from_file(
            file_path,
            "OffsetSumUDAFHandler",
            return_type=IntegerType(),
            input_types=[IntegerType()],
        )
        offset_sum_udaf = session_.udaf.register(
            d["OffsetSumUDAFHandler"],
            return_type=IntegerType(),
            input_types=[IntegerType()],
        )

        Utils.check_answer(
            df.agg(offset_sum_udaf_from_file(df.a)), [Row(6 + thread_id)]
        )
        Utils.check_answer(df.agg(offset_sum_udaf(df.a)), [Row(6 + thread_id)])

    with ThreadPoolExecutor(max_workers=10) as executor:
        for i in range(10):
            executor.submit(register_and_test_udaf, threadsafe_session, i)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in local testing mode",
    run=False,
)
@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="SNOW-609328: support caplog in SP regression test"
)
def test_auto_temp_table_cleaner(threadsafe_session, caplog):
    threadsafe_session._temp_table_auto_cleaner.ref_count_map.clear()
    original_auto_clean_up_temp_table_enabled = (
        threadsafe_session.auto_clean_up_temp_table_enabled
    )
    threadsafe_session.auto_clean_up_temp_table_enabled = True

    def create_temp_table(session_, thread_id):
        df = threadsafe_session.sql(f"select {thread_id} as A").cache_result()
        table_name = df.table_name
        del df
        return table_name

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        table_names = []
        for i in range(10):
            futures.append(executor.submit(create_temp_table, threadsafe_session, i))

        for future in as_completed(futures):
            table_names.append(future.result())

    gc.collect()
    wait_for_drop_table_sql_done(threadsafe_session, caplog, expect_drop=True)

    try:
        for table_name in table_names:
            assert (
                threadsafe_session._temp_table_auto_cleaner.ref_count_map[table_name]
                == 0
            )
        assert threadsafe_session._temp_table_auto_cleaner.num_temp_tables_created == 10
        assert threadsafe_session._temp_table_auto_cleaner.num_temp_tables_cleaned == 10
    finally:
        threadsafe_session.auto_clean_up_temp_table_enabled = (
            original_auto_clean_up_temp_table_enabled
        )


@pytest.mark.skipif(
    IS_LINUX or IS_WINDOWS,
    reason="Linux and Windows test show multiple active threads when no threadpool is enabled",
)
@pytest.mark.parametrize(
    "config,value",
    [
        ("cte_optimization_enabled", True),
        ("sql_simplifier_enabled", True),
        ("eliminate_numeric_sql_value_cast_enabled", True),
        ("auto_clean_up_temp_table_enabled", True),
        ("large_query_breakdown_enabled", True),
        ("large_query_breakdown_complexity_bounds", (20, 30)),
    ],
)
def test_concurrent_update_on_sensitive_configs(
    threadsafe_session, config, value, caplog
):
    def change_config_value(session_):
        session_.conf.set(config, value)

    caplog.clear()
    if threading.active_count() == 1:
        change_config_value(threadsafe_session)
        assert (
            f"You might have more than one threads sharing the Session object trying to update {config}"
            not in caplog.text
        )

    with caplog.at_level(logging.WARNING):
        with ThreadPoolExecutor(max_workers=5) as executor:
            for _ in range(5):
                executor.submit(change_config_value, threadsafe_session)
    assert (
        f"You might have more than one threads sharing the Session object trying to update {config}"
        in caplog.text
    )


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="large query breakdown is not supported in local testing mode",
    run=False,
)
def test_large_query_breakdown_with_cte(threadsafe_session):
    bounds = (300, 520) if threadsafe_session.sql_simplifier_enabled else (50, 70)
    try:
        original_query_compilation_stage_enabled = (
            threadsafe_session._query_compilation_stage_enabled
        )
        original_cte_optimization_enabled = threadsafe_session._cte_optimization_enabled
        original_large_query_breakdown_enabled = (
            threadsafe_session._large_query_breakdown_enabled
        )
        original_complexity_bounds = (
            threadsafe_session._large_query_breakdown_complexity_bounds
        )
        threadsafe_session._query_compilation_stage_enabled = True
        threadsafe_session._cte_optimization_enabled = True
        threadsafe_session._large_query_breakdown_enabled = True
        threadsafe_session._large_query_breakdown_complexity_bounds = bounds

        df0 = threadsafe_session.sql("select 1 as a, 2 as b").filter(col("a") == 1)
        df1 = threadsafe_session.sql("select 2 as b, 3 as c")
        df_join = df0.join(df1, on=["b"], how="inner")

        # this will trigger repeated subquery elimination
        df2 = df_join.filter(col("b") == 2).union_all(df_join)
        df3 = threadsafe_session.sql("select 3 as b, 4 as c").with_column(
            "a", col("b") + 1
        )
        for i in range(7):
            # this will increase the complexity of the query and trigger large query breakdown
            df2 = df2.with_column("a", col("a") + i + col("a"))
            df3 = df3.with_column("b", col("b") + i + col("b"))

        df2 = df2.group_by("a").agg(sum_distinct(col("b")).alias("b"))
        df3 = df3.group_by("b").agg(sum_distinct(col("a")).alias("a"))

        df4 = df2.union_all(df3)

        def apply_filter_and_collect(df, thread_id):
            final_df = df.filter(col("a") > thread_id * 5)
            queries = final_df.queries
            result = final_df.collect()
            return (queries, result)

        results = []
        with threadsafe_session.query_history() as history:
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [
                    executor.submit(apply_filter_and_collect, df4, i) for i in range(10)
                ]

                for future in futures:
                    results.append(future.result())

        unique_temp_tables_created = set()
        unique_ctes_created = set()
        for query in history.queries:
            query_text = query.sql_text
            if query_text.startswith("CREATE  SCOPED TEMPORARY  TABLE"):
                match = re.search(r"SNOWPARK_TEMP_TABLE_[\w]+", query_text)
                assert match is not None, query_text
                table_name = match.group()
                unique_temp_tables_created.add(table_name)
            elif query_text.startswith("WITH SNOWPARK_TEMP_CTE_"):
                match = re.search(r"SNOWPARK_TEMP_CTE_[\w]+", query_text)
                assert match is not None, query_text
                cte_name = match.group()
                unique_ctes_created.add(cte_name)

        assert len(unique_temp_tables_created) == 10, unique_temp_tables_created
        assert len(unique_ctes_created) == 10, unique_ctes_created

        threadsafe_session._query_compilation_stage_enabled = False
        threadsafe_session._cte_optimization_enabled = False
        for i, result in enumerate(results):
            queries, optimized_collect = result
            _, non_optimized_collect = apply_filter_and_collect(df4, i)
            Utils.check_answer(optimized_collect, non_optimized_collect)

            assert len(queries["queries"]) == 2
            assert queries["queries"][0].startswith("CREATE  SCOPED TEMPORARY  TABLE")
            assert queries["queries"][1].startswith("WITH SNOWPARK_TEMP_CTE_")

            assert len(queries["post_actions"]) == 1
            assert queries["post_actions"][0].startswith("DROP  TABLE  If  EXISTS")

    finally:
        threadsafe_session._query_compilation_stage_enabled = (
            original_query_compilation_stage_enabled
        )
        threadsafe_session._cte_optimization_enabled = original_cte_optimization_enabled
        threadsafe_session._large_query_breakdown_enabled = (
            original_large_query_breakdown_enabled
        )
        threadsafe_session._large_query_breakdown_complexity_bounds = (
            original_complexity_bounds
        )


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="local testing does not execute sql queries",
    run=False,
)
@pytest.mark.skipif(IS_IN_STORED_PROC, reason="cannot create new session in SP")
@pytest.mark.parametrize("thread_safe_enabled", [True, False])
def test_temp_name_placeholder_for_sync(db_parameters, thread_safe_enabled):
    new_db_params = db_parameters.copy()
    new_db_params["session_parameters"] = {
        _PYTHON_SNOWPARK_ENABLE_THREAD_SAFE_SESSION: thread_safe_enabled
    }
    with Session.builder.configs(new_db_params).create() as session:
        from snowflake.snowpark._internal.analyzer import analyzer

        original_value = analyzer.ARRAY_BIND_THRESHOLD

        def process_data(df_, thread_id):
            df_cleaned = df_.filter(df.A == thread_id)
            try:
                df_cleaned.collect()
            except Exception as e:
                if thread_safe_enabled:
                    raise e
                # when thread_safe is disable, this will throw an error
                # because the temp table is already dropped by another thread
                pass

        try:
            analyzer.ARRAY_BIND_THRESHOLD = 4
            df = session.create_dataframe([[1, 2], [3, 4]], ["A", "B"])

            with session.query_history() as history:
                with ThreadPoolExecutor(
                    max_workers=(5 if thread_safe_enabled else 1)
                ) as executor:
                    futures = []
                    for i in range(10):
                        futures.append(executor.submit(process_data, df, i))
                    for future in as_completed(futures):
                        future.result()

            queries_sent = [query.sql_text for query in history.queries]
            unique_create_table_queries = set()
            unique_drop_table_queries = set()
            for query in queries_sent:
                assert "temp_name_placeholder" not in query
                if query.startswith("CREATE  OR  REPLACE"):
                    match = re.search(r"SNOWPARK_TEMP_TABLE_[\w]+", query)
                    assert match is not None, query
                    table_name = match.group()
                    unique_create_table_queries.add(table_name)
                elif query.startswith("DROP  TABLE"):
                    match = re.search(r"SNOWPARK_TEMP_TABLE_[\w]+", query)
                    assert match is not None, query
                    table_name = match.group()
                    unique_drop_table_queries.add(table_name)
            expected_num_queries = 10 if thread_safe_enabled else 1
            assert (
                len(unique_create_table_queries) == expected_num_queries
            ), queries_sent
            assert len(unique_drop_table_queries) == expected_num_queries, queries_sent

        finally:
            analyzer.ARRAY_BIND_THRESHOLD = original_value


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="local testing does not execute sql queries",
    run=False,
)
@pytest.mark.skipif(IS_IN_STORED_PROC_LOCALFS, reason="Skip file IO tests in localfs")
def test_temp_name_placeholder_for_async(
    threadsafe_session, resources_path, threadsafe_temp_stage
):
    stage_prefix = f"prefix_{Utils.random_alphanumeric_str(10)}"
    stage_with_prefix = f"@{threadsafe_temp_stage}/{stage_prefix}/"
    test_files = TestFiles(resources_path)
    threadsafe_session.file.put(
        test_files.test_file_csv, stage_with_prefix, auto_compress=False
    )
    filename = os.path.basename(test_files.test_file_csv)

    def process_data(df_, thread_id):
        df_cleaned = df_.filter(df.A == thread_id)
        job = df_cleaned.collect(block=False)
        job.result()

    df = threadsafe_session.read.schema(
        StructType(
            [
                StructField("A", LongType()),
                StructField("B", StringType()),
                StructField("C", DoubleType()),
            ]
        )
    ).csv(f"{stage_with_prefix}/{filename}")

    with threadsafe_session.query_history() as history:
        futures = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            for i in range(10):
                futures.append(executor.submit(process_data, df, i))

            for future in as_completed(futures):
                future.result()

    queries_sent = [query.sql_text for query in history.queries]

    unique_create_file_format_queries = set()
    unique_drop_file_format_queries = set()
    for query in queries_sent:
        assert "temp_name_placeholder" not in query
        if query.startswith(" CREATE SCOPED TEMPORARY FILE  FORMAT"):
            match = re.search(r"SNOWPARK_TEMP_FILE_FORMAT_[\w]+", query)
            assert match is not None, query
            file_format_name = match.group()
            unique_create_file_format_queries.add(file_format_name)
        elif query.startswith("DROP  FILE  FORMAT"):
            match = re.search(r"SNOWPARK_TEMP_FILE_FORMAT_[\w]+", query)
            assert match is not None, query
            file_format_name = match.group()
            unique_drop_file_format_queries.add(file_format_name)

    assert len(unique_create_file_format_queries) == 10
    assert len(unique_drop_file_format_queries) == 10


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="cursor are not created in local testing mode",
    run=False,
)
def test_num_cursors_created(threadsafe_session):
    num_workers = 5

    def run_query(session_, thread_id):
        assert session_.sql(f"SELECT {thread_id} as A").collect()[0][0] == thread_id

    with patch.object(
        threadsafe_session._conn._telemetry_client, "send_cursor_created_telemetry"
    ) as mock_telemetry:
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            for i in range(10):
                executor.submit(run_query, threadsafe_session, i)

        # when multithreading is enabled, each worker will create a cursor
        # otherwise, we will use the same cursor created by the main thread
        # thus creating 0 new cursors.
        assert mock_telemetry.call_count == num_workers


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="local testing does not execute sql queries",
    run=False,
)
@patch("snowflake.snowpark._internal.analyzer.snowflake_plan.find_duplicate_subtrees")
def test_critical_lazy_evaluation_for_plan(
    mock_find_duplicate_subtrees, threadsafe_session
):
    mock_find_duplicate_subtrees.side_effect = find_duplicate_subtrees

    df = threadsafe_session.sql("select 1 as a, 2 as b").filter(col("a") == 1)
    for i in range(10):
        df = df.with_column("a", col("a") + i + col("a"))
    df = df.union_all(df)

    def call_critical_lazy_methods(df_):
        assert df_._plan.cumulative_node_complexity == {
            PlanNodeCategory.FILTER: 2,
            PlanNodeCategory.LITERAL: 22,
            PlanNodeCategory.COLUMN: 64
            if threadsafe_session.sql_simplifier_enabled
            else 62,
            PlanNodeCategory.LOW_IMPACT: 42,
            PlanNodeCategory.SET_OPERATION: 1,
        }
        assert df_._plan.plan_state == {
            PlanState.PLAN_HEIGHT: 13,
            PlanState.NUM_CTE_NODES: 1,
            PlanState.NUM_SELECTS_WITH_COMPLEXITY_MERGED: 0,
            PlanState.DUPLICATED_NODE_COMPLEXITY_DISTRIBUTION: [2, 0, 0, 0, 0, 0, 0],
        }
        if threadsafe_session.sql_simplifier_enabled:
            assert df_._select_statement.encoded_node_id_with_query.endswith(
                "_SelectStatement"
            )

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(call_critical_lazy_methods, df) for _ in range(10)]

        for future in as_completed(futures):
            future.result()

    # SnowflakePlan.plan_state calls find_duplicate_subtrees. This should be
    # called only once and the cached result should be used for the rest of
    # the calls.
    mock_find_duplicate_subtrees.assert_called_once()


def create_and_join(_session):
    df1 = _session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df2 = _session.create_dataframe([[1, 7], [3, 8]], schema=["a", "b"])
    df3 = df1.join(df2)
    expected = [Row(1, 2, 1, 7), Row(1, 2, 3, 8), Row(3, 4, 1, 7), Row(3, 4, 3, 8)]
    Utils.check_answer(df3, expected)
    return [df1, df2, df3]


def join_again(df1, df2, df3):
    df3 = df1.join(df2).select(df1.a)
    expected = [Row(1, 2, 1, 7), Row(1, 2, 3, 8), Row(3, 4, 1, 7), Row(3, 4, 3, 8)]
    Utils.check_answer(df3, expected)


def create_aliased_df(_session):
    df1 = _session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df2 = df1.join(df1.filter(col("a") == 1)).select(df1.a.alias("a1"))
    Utils.check_answer(df2, [Row(A1=1), Row(A1=3)])
    return [df2]


def select_aliased_col(df2):
    df2 = df2.select(df2.a1)
    Utils.check_answer(df2, [Row(A1=1), Row(A1=3)])


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1373887: Support basic diamond shaped joins in Local Testing",
    run=False,
)
@pytest.mark.parametrize(
    "f1,f2", [(create_and_join, join_again), (create_aliased_df, select_aliased_col)]
)
def test_SNOW_1878372(threadsafe_session, f1, f2):
    class ReturnableThread(threading.Thread):
        def __init__(self, target, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self._target = target
            self.result = None

        def run(self):
            if self._target is not None:
                self.result = self._target(*self._args, **self._kwargs)

    t1 = ReturnableThread(target=f1, args=(threadsafe_session,))
    t1.start()
    t1.join()

    t2 = ReturnableThread(target=f2, args=tuple(t1.result))
    t2.start()
    t2.join()
