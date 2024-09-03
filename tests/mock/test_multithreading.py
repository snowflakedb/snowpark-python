#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
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
from snowflake.snowpark.mock._connection import MockServerConnection
from snowflake.snowpark.mock._functions import MockedFunctionRegistry
from snowflake.snowpark.mock._plan import MockExecutionPlan
from snowflake.snowpark.mock._snowflake_data_type import TableEmulator
from snowflake.snowpark.mock._stage_registry import StageEntityRegistry
from snowflake.snowpark.mock._telemetry import LocalTestOOBTelemetryService
from snowflake.snowpark.session import Session


def test_table_update_delete_insert():
    pass


def test_udf():
    pass


def test_sp():
    pass


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
def test_tabular_entity_registry(test_table):
    conn = MockServerConnection()
    entity_registry = conn.entity_registry
    num_threads = 10

    def write_read_and_drop_table():
        table_name = "test_table"
        table_emulator = TableEmulator()

        entity_registry.write_table(table_name, table_emulator, SaveMode.OVERWRITE)

        optional_table = entity_registry.read_table_if_exists(table_name)
        if optional_table is not None:
            assert optional_table.empty

        entity_registry.drop_table(table_name)

    def write_read_and_drop_view():
        view_name = "test_view"
        empty_logical_plan = LogicalPlan()
        plan = MockExecutionPlan(empty_logical_plan, None)

        entity_registry.create_or_replace_view(plan, view_name)

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


def test_stage_entity_registry_put_and_get():
    stage_registry = StageEntityRegistry(MockServerConnection())
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


def test_stage_entity_registry_upload_and_read():
    # upload a json to stage_i and read it back
    test_parameter = {
        "account": "test_account",
        "user": "test_user",
        "schema": "test_schema",
        "database": "test_database",
        "warehouse": "test_warehouse",
        "role": "test_role",
        "local_testing": True,
    }
    session = Session.builder.configs(options=test_parameter).create()
    stage_registry = StageEntityRegistry(MockServerConnection())

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
            session._analyzer,
            {"INFER_SCHEMA": "True"},
        )
        # TODO: read table emulator and compare results
        assert df == thread_id


def test_stage_entity_registry_create_or_replace():
    stage_registry = StageEntityRegistry(MockServerConnection())
    num_threads = 100

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

    # set batch_size to 101
    oob_service.batch_size = num_threads * num_events_per_thread + 1

    # create a function that adds 10 events to the queue
    def add_events(thread_id: int):
        for i in range(num_events_per_thread):
            oob_service.add(
                {f"thread_{thread_id}_event_{i}": f"dummy_event_{thread_id}_{i}"}
            )

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


def test_oob_telemetry_flush():
    oob_service = LocalTestOOBTelemetryService.get_instance()
    # clean up queue first
    oob_service.export_queue_to_string()
    # add a dummy event
    oob_service.add({"event": "dummy_event"})

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
    assert oob_service.queue.qsize() == 0
