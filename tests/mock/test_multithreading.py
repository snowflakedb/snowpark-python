#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from threading import Thread

from snowflake.snowpark.mock._telemetry import LocalTestOOBTelemetryService


def test_table_update_delete_insert():
    pass


def test_udf():
    pass


def test_sp():
    pass


def test_mocked_function_registry_created_once():
    pass


def test_mocked_function_registry_updates_not_lost():
    pass


def test_tabular_entity_registry():
    pass


def test_stage_entity_registry():
    pass


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
