#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import gc
import hashlib
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple  # noqa: F401
from unittest.mock import patch

import pytest

from snowflake.snowpark.session import Session
from snowflake.snowpark.types import IntegerType

try:
    import dateutil

    # six is the dependency of dateutil
    import six

    is_dateutil_available = True
except ImportError:
    is_dateutil_available = False

from snowflake.snowpark.functions import lit
from snowflake.snowpark.row import Row
from tests.utils import IS_IN_STORED_PROC, TestFiles, Utils


def test_concurrent_select_queries(session):
    def run_select(session_, thread_id):
        df = session_.sql(f"SELECT {thread_id} as A")
        assert df.collect()[0][0] == thread_id

    with ThreadPoolExecutor(max_workers=10) as executor:
        for i in range(10):
            executor.submit(run_select, session, i)


def test_concurrent_dataframe_operations(session):
    try:
        table_name = Utils.random_table_name()
        data = [(i, 11 * i) for i in range(10)]
        df = session.create_dataframe(data, ["A", "B"])
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
                executor.submit(run_dataframe_operation, session, i) for i in range(10)
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
        Utils.drop_table(session, table_name)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL query and query listeners are not supported",
    run=False,
)
def test_query_listener(session):
    def run_select(session_, thread_id):
        session_.sql(f"SELECT {thread_id} as A").collect()

    with session.query_history() as history:
        with ThreadPoolExecutor(max_workers=10) as executor:
            for i in range(10):
                executor.submit(run_select, session, i)

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
def test_query_tagging(session):
    def set_query_tag(session_, thread_id):
        session_.query_tag = f"tag_{thread_id}"

    with ThreadPoolExecutor(max_workers=10) as executor:
        for i in range(10):
            executor.submit(set_query_tag, session, i)

    actual_query_tag = session.sql("SHOW PARAMETERS LIKE 'QUERY_TAG'").collect()[0][1]
    assert actual_query_tag == session.query_tag


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL query is not supported",
    run=False,
)
def test_session_stage_created_once(session):
    with patch.object(
        session._conn, "run_query", wraps=session._conn.run_query
    ) as patched_run_query:
        with ThreadPoolExecutor(max_workers=10) as executor:
            for _ in range(10):
                executor.submit(session.get_session_stage)

        assert patched_run_query.call_count == 1


def test_action_ids_are_unique(session):
    with ThreadPoolExecutor(max_workers=10) as executor:
        action_ids = set()
        futures = [executor.submit(session._generate_new_action_id) for _ in range(10)]

        for future in as_completed(futures):
            action_ids.add(future.result())

    assert len(action_ids) == 10


@pytest.mark.parametrize("use_stream", [True, False])
def test_file_io(session, resources_path, temp_stage, use_stream):
    stage_prefix = f"prefix_{Utils.random_alphanumeric_str(10)}"
    stage_with_prefix = f"@{temp_stage}/{stage_prefix}/"
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
                results = session.file.put_stream(
                    fd, stage_with_prefix, auto_compress=False, overwrite=False
                )
        else:
            results = session.file.put(
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
            fd = session.file.get_stream(stage_file_name, download_dir)
            with open(upload_file_path, "rb") as upload_fd:
                assert get_file_hash(upload_fd) == get_file_hash(fd)

        else:
            results = session.file.get(stage_file_name, download_dir)
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


def test_concurrent_add_packages(session):
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
                executor.submit(session.add_packages, package)
                for package in package_list
            ]

            for future in as_completed(futures):
                future.result()

            assert session.get_packages() == {
                package: package for package in package_list
            }
    finally:
        session.clear_packages()


def test_concurrent_remove_package(session):
    def remove_package(session_, package_name):
        try:
            session_.remove_package(package_name)
            return True
        except ValueError:
            return False
        except Exception as e:
            raise e

    try:
        session.add_packages("numpy")
        with ThreadPoolExecutor(max_workers=10) as executor:

            futures = [
                executor.submit(remove_package, session, "numpy") for _ in range(10)
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
        session.clear_packages()


@pytest.mark.skipif(not is_dateutil_available, reason="dateutil is not available")
def test_concurrent_add_import(session, resources_path):
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
                    session.add_import,
                    file,
                )

        assert set(session.get_imports()) == {
            os.path.abspath(file) for file in import_files
        }
    finally:
        session.clear_imports()


def test_concurrent_remove_import(session, resources_path):
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
        session.add_import(test_files.test_udf_py_file)
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(remove_import, session, test_files.test_udf_py_file)
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
        session.clear_imports()


def test_concurrent_sp_register(session, tmpdir):
    try:
        session.add_packages("snowflake-snowpark-python")

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
                executor.submit(register_and_test_sp, session, i)
    finally:
        session.clear_packages()


def test_concurrent_udf_register(session, tmpdir):
    df = session.range(-5, 5).to_df("a")

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
            executor.submit(register_and_test_udf, session, i)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="UDTFs is not supported in local testing mode",
    run=False,
)
def test_concurrent_udtf_register(session, tmpdir):
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

        df_local = session.table_function(echo_udtf(lit(1)))
        df_from_file = session.table_function(echo_udtf_from_file(lit(1)))
        assert df_local.collect() == [(thread_id + 1,)]
        assert df_from_file.collect() == [(thread_id + 1,)]

    with ThreadPoolExecutor(max_workers=10) as executor:
        for i in range(10):
            executor.submit(register_and_test_udtf, session, i)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="UDAFs is not supported in local testing mode",
    run=False,
)
def test_concurrent_udaf_register(session: Session, tmpdir):
    df = session.create_dataframe([[1, 3], [1, 4], [2, 5], [2, 6]]).to_df("a", "b")

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
            executor.submit(register_and_test_udaf, session, i)


def test_auto_temp_table_cleaner(session):
    session._temp_table_auto_cleaner.ref_count_map.clear()
    original_auto_clean_up_temp_table_enabled = session.auto_clean_up_temp_table_enabled
    session.auto_clean_up_temp_table_enabled = True

    def create_temp_table(session_, thread_id):
        df = session.sql(f"select {thread_id} as A").cache_result()
        table_name = df.table_name
        del df
        return table_name

    def create_table_and_garbage_collect(session_, thread_id):
        name = create_temp_table(session_, thread_id)
        return name

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        table_names = []
        for i in range(10):
            futures.append(
                executor.submit(create_table_and_garbage_collect, session, i)
            )

        for future in as_completed(futures):
            table_names.append(future.result())

    gc.collect()
    time.sleep(1)

    try:
        for table_name in table_names:
            assert session._temp_table_auto_cleaner.ref_count_map[table_name] == 0
        assert session._temp_table_auto_cleaner.num_temp_tables_created == 10
        assert session._temp_table_auto_cleaner.num_temp_tables_cleaned == 10
    finally:
        session.auto_clean_up_temp_table_enabled = (
            original_auto_clean_up_temp_table_enabled
        )
