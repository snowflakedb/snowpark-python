#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import gc
import hashlib
import logging
import os
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple  # noqa: F401
from unittest.mock import patch

import pytest

from snowflake.snowpark.session import (
    _PYTHON_SNOWPARK_ENABLE_THREAD_SAFE_SESSION,
    Session,
)
from snowflake.snowpark.types import IntegerType
from tests.integ.test_temp_table_cleanup import wait_for_drop_table_sql_done

try:
    import dateutil

    # six is the dependency of dateutil
    import six

    is_dateutil_available = True
except ImportError:
    is_dateutil_available = False

from snowflake.snowpark.functions import lit
from snowflake.snowpark.row import Row
from tests.utils import IS_IN_STORED_PROC, IS_LINUX, IS_WINDOWS, TestFiles, Utils


@pytest.fixture(scope="module")
def threadsafe_session(
    db_parameters,
    session,
    sql_simplifier_enabled,
    cte_optimization_enabled,
    local_testing_mode,
):
    if IS_IN_STORED_PROC:
        yield session
    else:
        new_db_parameters = db_parameters.copy()
        new_db_parameters["local_testing"] = local_testing_mode
        new_db_parameters["session_parameters"] = {
            _PYTHON_SNOWPARK_ENABLE_THREAD_SAFE_SESSION: True
        }
        with Session.builder.configs(new_db_parameters).create() as session:
            session._sql_simplifier_enabled = sql_simplifier_enabled
            session._cte_optimization_enabled = cte_optimization_enabled
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
    assert isinstance(threadsafe_session._lock, threading.RLock)
    assert isinstance(threadsafe_session._temp_table_auto_cleaner.lock, threading.RLock)
    assert isinstance(threadsafe_session._conn._lock, threading.RLock)


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


@pytest.mark.parametrize("use_stream", [True, False])
def test_file_io(threadsafe_session, resources_path, threadsafe_temp_stage, use_stream):
    stage_prefix = f"prefix_{Utils.random_alphanumeric_str(10)}"
    stage_with_prefix = f"@{threadsafe_temp_stage}/{stage_prefix}/"
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
                results = threadsafe_session.file.put_stream(
                    fd, stage_with_prefix, auto_compress=False, overwrite=False
                )
        else:
            results = threadsafe_session.file.put(
                upload_file_path,
                stage_with_prefix,
                auto_compress=False,
                overwrite=False,
            )
        # assert file is uploaded successfully
        assert len(results) == 1
        assert results[0].status == "UPLOADED"

        stage_file_name = f"{stage_with_prefix}{os.path.basename(upload_file_path)}"
        if use_stream:
            fd = threadsafe_session.file.get_stream(stage_file_name, download_dir)
            with open(upload_file_path, "rb") as upload_fd:
                assert get_file_hash(upload_fd) == get_file_hash(fd)

        else:
            results = threadsafe_session.file.get(stage_file_name, download_dir)
            # assert file is downloaded successfully
            assert len(results) == 1
            assert results[0].status == "DOWNLOADED"
            download_file_path = results[0].file
            # assert two files are identical
            with open(upload_file_path, "rb") as upload_fd, open(
                download_file_path, "rb"
            ) as download_fd:
                assert get_file_hash(upload_fd) == get_file_hash(download_fd)

    with tempfile.TemporaryDirectory() as download_dir:
        with ThreadPoolExecutor(max_workers=10) as executor:
            for file_path in resources_files:
                executor.submit(put_and_get_file, file_path, download_dir)

        if not use_stream:
            # assert all files are downloaded
            assert set(os.listdir(download_dir)) == {
                os.path.basename(file_path) for file_path in resources_files
            }


def test_concurrent_add_packages(threadsafe_session):
    # this is a list of packages available in snowflake anaconda. If this
    # test fails due to packages not being available, please update the list
    package_list = {
        "graphviz",
        "numpy",
        "pandas",
        "scipy",
        "scikit-learn",
        "matplotlib",
    }

    try:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(threadsafe_session.add_packages, package)
                for package in package_list
            ]

            for future in as_completed(futures):
                future.result()

            assert threadsafe_session.get_packages() == {
                package: package for package in package_list
            }
    finally:
        threadsafe_session.clear_packages()


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
        }
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


@pytest.mark.parametrize("is_enabled", [True, False])
def test_num_cursors_created(db_parameters, is_enabled, local_testing_mode):
    if is_enabled and local_testing_mode:
        pytest.skip("Multithreading is enabled by default in local testing mode")

    num_workers = 5 if is_enabled else 1
    new_db_parameters = db_parameters.copy()
    new_db_parameters["session_parameters"] = {
        _PYTHON_SNOWPARK_ENABLE_THREAD_SAFE_SESSION: is_enabled
    }

    with Session.builder.configs(new_db_parameters).create() as new_session:

        def run_query(session_, thread_id):
            assert session_.sql(f"SELECT {thread_id} as A").collect()[0][0] == thread_id

        with patch.object(
            new_session._conn._telemetry_client, "send_cursor_created_telemetry"
        ) as mock_telemetry:
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                for i in range(10):
                    executor.submit(run_query, new_session, i)

        # when multithreading is enabled, each worker will create a cursor
        # otherwise, we will use the same cursor created by the main thread
        # thus creating 0 new cursors.
        assert mock_telemetry.call_count == (num_workers if is_enabled else 0)
