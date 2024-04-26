#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import inspect
import os.path
from contextlib import contextmanager
from logging import getLogger
from typing import Tuple

logger = getLogger(__name__)
target_class = ["dataframe.py", "dataframe_writer.py"]
# this parameter make sure no error when open telemetry is not installed
open_telemetry_found = True
try:
    from opentelemetry import trace

except ImportError:
    open_telemetry_found = False


@contextmanager
def open_telemetry_context_manager(func, dataframe):

    # trace when required package is installed
    if open_telemetry_found:
        class_name = func.__qualname__
        name = func.__name__
        tracer = (
            trace.get_tracer(f"snow.snowpark.{class_name.split('.')[0].lower()}")
            if "." in class_name
            else class_name
        )
        with tracer.start_as_current_span(name) as cur_span:
            try:
                if cur_span.is_recording():
                    # store execution location in span
                    filename, lineno = context_manager_code_location(
                        inspect.stack(), func
                    )
                    cur_span.set_attribute("code.filepath", f"{filename}")
                    cur_span.set_attribute("code.lineno", lineno)
                    # stored method chain
                    method_chain = build_method_chain(dataframe._plan.api_calls, name)
                    cur_span.set_attribute("method.chain", method_chain)
            except Exception as e:
                logger.warning(f"Error when acquiring span attributes. {e}")
            finally:
                yield

    else:
        yield


def decorator_count(func):
    count = 0
    current_func = func
    while hasattr(current_func, "__wrapped__"):
        count += 1
        current_func = current_func.__wrapped__
    return count


def context_manager_code_location(frame_info, func) -> Tuple[str, int]:
    decorator_number = decorator_count(func)
    target_index = -1
    for i, frame in enumerate(frame_info):
        file_name = os.path.basename(frame.filename)
        if file_name in target_class:
            target_index = i + decorator_number + 1
            break
    frame = frame_info[target_index]
    return frame.filename, frame.lineno


def build_method_chain(api_calls, name) -> str:
    method_chain = "DataFrame."
    for method in api_calls:
        method_name = method["name"]
        if method_name.startswith("Session"):
            continue
        method_name = method_name.split(".")[-1]
        method_chain = f"{method_chain}{method_name}()."
    method_chain = f"{method_chain}{name.split('.')[-1]}()"
    return method_chain
