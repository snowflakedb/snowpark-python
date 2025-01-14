#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import threading
import time
from threading import Event, RLock

import cachetools
import pytest

lock = RLock()


def lock_function_once(f, flag):
    def wrapper(*args, **kwargs):
        if not flag.is_set():
            with lock:
                if not flag.is_set():
                    result = f(*args, **kwargs)
                    flag.set()
                    return result
                return f(*args, **kwargs)
        return f(*args, **kwargs)

    return wrapper


@pytest.mark.parametrize("has_lock", [True, False])
def test_lock_function(has_lock):
    load_model_called = 0
    inc_lock = RLock()

    @cachetools.cached({})
    def load_model():
        nonlocal load_model_called
        time.sleep(1.0)  # simulate a long operation
        with inc_lock:
            load_model_called += 1

    def mock_udf_handler():
        load_model()

    locked_mock_udf_handler = lock_function_once(mock_udf_handler, Event())

    threads = []
    for _ in range(10):
        threads.append(
            threading.Thread(
                target=locked_mock_udf_handler if has_lock else mock_udf_handler
            )
        )
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if has_lock:
        assert load_model_called == 1
    else:
        assert load_model_called >= 1
