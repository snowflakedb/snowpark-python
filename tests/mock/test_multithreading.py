#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import io
import json
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Thread

import pytest

from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    SaveMode,
)
from snowflake.snowpark._internal.utils import normalize_local_file
from snowflake.snowpark.functions import lit, when_matched
from snowflake.snowpark.mock._functions import MockedFunctionRegistry
from snowflake.snowpark.mock._plan import MockExecutionPlan
from snowflake.snowpark.mock._snowflake_data_type import TableEmulator
from snowflake.snowpark.mock._stage_registry import StageEntityRegistry
from snowflake.snowpark.mock._telemetry import LocalTestOOBTelemetryService
from snowflake.snowpark.row import Row
from snowflake.snowpark.session import Session
from tests.utils import Utils


@pytest.fixture(scope="function")
def threadsafe_session(mock_server_connection):
    with Session(mock_server_connection) as s:
        yield s


@pytest.mark.skipif(
    pytest.param("local_testing_mode"),
    reason="TODO SNOW-1826001: Bug in local testing mode.",
)
def test_table_update_merge_delete(threadsafe_session):
    table_name = Utils.random_table_name()
    num_threads = 10
    data = [[v, 11 * v] for v in range(10)]
    df = threadsafe_session.create_dataframe(data, schema=["a", "b"])
    df.write.save_as_table(table_name, table_type="temp")

    source_df = df
    t = threadsafe_session.table(table_name)

    def update_table(thread_id: int):
        t.update({"b": 0}, t.a == lit(thread_id))

    def merge_table(thread_id: int):
        t.merge(
            source_df, t.a == source_df.a, [when_matched().update({"b": source_df.b})]
        )

    def delete_table(thread_id: int):
        t.delete(t.a == lit(thread_id))

    # all threads will update column b to 0 where a = thread_id
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # update
        futures = [executor.submit(update_table, i) for i in range(num_threads)]
        for future in as_completed(futures):
            future.result()

        # all threads will set column b to 0
        Utils.check_answer(t.select(t.b), [Row(B=0) for _ in range(10)])

        # merge
        futures = [executor.submit(merge_table, i) for i in range(num_threads)]
        for future in as_completed(futures):
            future.result()

        # all threads will set column b to 11 * a
        Utils.check_answer(t.select(t.b), [Row(B=11 * i) for i in range(10)])

        # delete
        futures = [executor.submit(delete_table, i) for i in range(num_threads)]
        for future in as_completed(futures):
            future.result()

        # all threads will delete their row
        assert t.count() == 0


def test_udf_register_and_invoke(threadsafe_session):
    df = threadsafe_session.create_dataframe([[1], [2]], schema=["num"])
    num_threads = 10

    def register_udf(x: int):
        def echo(x: int) -> int:
            return x

        return threadsafe_session.udf.register(echo, name="echo", replace=True)

    def invoke_udf():
        result = df.select(threadsafe_session.udf.call_udf("echo", df.num)).collect()
        assert result[0][0] == 1
        assert result[1][0] == 2

    threads = []
    for i in range(num_threads):
        thread_register = Thread(target=register_udf, args=(i,))
        threads.append(thread_register)
        thread_register.start()

        thread_invoke = Thread(target=invoke_udf)
        threads.append(thread_invoke)
        thread_invoke.start()

    for thread in threads:
        thread.join()


def test_sp_register_and_invoke(threadsafe_session):
    num_threads = 10

    def increment_by_one_fn(session_: Session, x: int) -> int:
        return x + 1

    def register_sproc():
        threadsafe_session.sproc.register(
            increment_by_one_fn, name="increment_by_one", replace=True
        )

    def invoke_sproc():
        result = threadsafe_session.call("increment_by_one", 1)
        assert result == 2

    threads = []
    for i in range(num_threads):
        thread_register = Thread(target=register_sproc, args=(i,))
        threads.append(thread_register)
        thread_register.start()

        thread_invoke = Thread(target=invoke_sproc)
        threads.append(thread_invoke)
        thread_invoke.start()

    for thread in threads:
        thread.join()


def test_mocked_function_registry_created_once():
    num_threads = 10

    result = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(MockedFunctionRegistry.get_or_create)
            for _ in range(num_threads)
        ]

        for future in as_completed(futures):
            result.append(future.result())

    registry = MockedFunctionRegistry.get_or_create()
    assert all([registry is r for r in result])


@pytest.mark.parametrize("test_table", [True, False])
def test_tabular_entity_registry(test_table, mock_server_connection):
    entity_registry = mock_server_connection.entity_registry
    num_threads = 10

    def write_read_and_drop_table():
        table_name = Utils.random_table_name()
        table_emulator = TableEmulator()

        entity_registry.write_table(table_name, table_emulator, SaveMode.OVERWRITE)

        optional_table = entity_registry.read_table_if_exists(table_name)
        if optional_table is not None:
            assert optional_table.empty

        entity_registry.drop_table(table_name)

    def write_read_and_drop_view():
        view_name = Utils.random_view_name()
        empty_logical_plan = LogicalPlan()
        plan = MockExecutionPlan(empty_logical_plan, None)

        entity_registry.create_or_replace_view(plan, view_name, True)

        optional_view = entity_registry.read_view_if_exists(view_name)
        if optional_view is not None:
            assert optional_view.source_plan == empty_logical_plan

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        if test_table:
            test_fn = write_read_and_drop_table
        else:
            test_fn = write_read_and_drop_view
        futures = [executor.submit(test_fn) for _ in range(num_threads)]

        for future in as_completed(futures):
            future.result()


def test_stage_entity_registry_put_and_get(mock_server_connection):
    stage_registry = StageEntityRegistry(mock_server_connection)
    num_threads = 10

    def put_and_get_file():
        stage_registry.put(
            normalize_local_file(
                f"{os.path.dirname(os.path.abspath(__file__))}/files/test_file_1"
            ),
            "@test_stage/test_parent_dir/test_child_dir",
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            stage_registry.get(
                "@test_stage/test_parent_dir/test_child_dir/test_file_1",
                temp_dir,
            )
            assert os.path.isfile(os.path.join(temp_dir, "test_file_1"))

    threads = []
    for _ in range(num_threads):
        thread = Thread(target=put_and_get_file)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def test_stage_entity_registry_upload_and_read(
    threadsafe_session, mock_server_connection
):
    stage_registry = StageEntityRegistry(mock_server_connection)
    num_threads = 10

    def upload_and_read_json(thread_id: int):
        json_string = json.dumps({"thread_id": thread_id})
        bytes_io = io.BytesIO(json_string.encode("utf-8"))
        stage_registry.upload_stream(
            input_stream=bytes_io,
            stage_location="@test_stage/test_parent_dir",
            file_name=f"test_file_{thread_id}",
        )

        df = stage_registry.read_file(
            f"@test_stage/test_parent_dir/test_file_{thread_id}",
            "json",
            [],
            threadsafe_session._analyzer,
            {"INFER_SCHEMA": "True"},
        )

        assert df['"thread_id"'].iloc[0] == thread_id

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(upload_and_read_json, i) for i in range(num_threads)]

        for future in as_completed(futures):
            future.result()


def test_stage_entity_registry_create_or_replace(mock_server_connection):
    stage_registry = StageEntityRegistry(mock_server_connection)
    num_threads = 10

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(stage_registry.create_or_replace_stage, f"test_stage_{i}")
            for i in range(num_threads)
        ]

        for future in as_completed(futures):
            future.result()

    assert len(stage_registry._stage_registry) == num_threads
    for i in range(num_threads):
        assert f"test_stage_{i}" in stage_registry._stage_registry


def test_oob_telemetry_add():
    oob_service = LocalTestOOBTelemetryService.get_instance()
    # clean up queue first
    oob_service.export_queue_to_string()
    num_threads = 10
    num_events_per_thread = 10

    # create a function that adds 10 events to the queue
    def add_events(thread_id: int):
        for i in range(num_events_per_thread):
            oob_service.add(
                {f"thread_{thread_id}_event_{i}": f"dummy_event_{thread_id}_{i}"}
            )

    # set batch_size to 101
    is_enabled = oob_service.enabled
    oob_service.enable()
    original_batch_size = oob_service.batch_size
    oob_service.batch_size = num_threads * num_events_per_thread + 1
    try:
        # create 10 threads
        threads = []
        for thread_id in range(num_threads):
            thread = Thread(target=add_events, args=(thread_id,))
            threads.append(thread)
            thread.start()

        # wait for all threads to finish
        for thread in threads:
            thread.join()

        # assert that the queue size is 100
        assert oob_service.queue.qsize() == num_threads * num_events_per_thread
    finally:
        oob_service.batch_size = original_batch_size
        if not is_enabled:
            oob_service.disable()


def test_oob_telemetry_flush():
    oob_service = LocalTestOOBTelemetryService.get_instance()
    # clean up queue first
    oob_service.export_queue_to_string()

    is_enabled = oob_service.enabled
    oob_service.enable()
    # add a dummy event
    oob_service.add({"event": "dummy_event"})

    try:
        # flush the queue in multiple threads
        num_threads = 10
        threads = []
        for _ in range(num_threads):
            thread = Thread(target=oob_service.flush)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # assert that the queue is empty
        assert oob_service.size() == 0
    finally:
        if not is_enabled:
            oob_service.disable()
