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
target_modules = [
    "dataframe.py",
    "dataframe_writer.py",
    "udf.py",
    "udtf.py",
    "udaf.py",
    "functions.py",
    "stored_procedure.py",
]
registration_modules = ["udf.py", "udtf.py", "udaf.py", "stored_procedure.py"]
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
        tracer = trace.get_tracer(extract_tracer_name(class_name))
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


@contextmanager
def open_telemetry_udf_context_manager(
    registration_function,
    func=None,
    handler=None,
    func_name=None,
    handler_name=None,
    name=None,
    file_path=None,
):
    # trace when required package is installed
    if open_telemetry_found:
        class_name = registration_function.__qualname__
        span_name = registration_function.__name__
        tracer = trace.get_tracer(extract_tracer_name(class_name))
        with tracer.start_as_current_span(span_name) as cur_span:
            try:
                # first try to get func if it is udf, then try to get handler if it is udtf/udaf, if still None, means it is
                # loading from file
                udf_func = func if func else handler
                # if udf_func is not None, meaning it is a udf function or udf handler class, get handler_name from it, otherwise find
                # function name or handler name from parameter
                handler_name = (
                    udf_func.__name__
                    if udf_func
                    else (func_name if func_name else handler_name)
                )
                if cur_span.is_recording():
                    # store execution location in span
                    filename, lineno = context_manager_code_location(
                        inspect.stack(), registration_function
                    )
                    cur_span.set_attribute("code.filepath", f"{filename}")
                    cur_span.set_attribute("code.lineno", lineno)
                    cur_span.set_attribute("snow.executable.name", name)
                    cur_span.set_attribute("snow.executable.handler", handler_name)
                    cur_span.set_attribute("snow.executable.filepath", file_path)
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
    # we know what function we are tracking, with this information, we can locate where target function is called
    decorator_number = decorator_count(func)
    target_index = -1
    for i, frame in enumerate(frame_info):
        file_name = os.path.basename(frame.filename)
        if file_name in target_modules:
            target_index = i + decorator_number + 1
            if file_name in registration_modules:
                continue
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


def extract_tracer_name(class_name):
    return (
        f"snow.snowpark.{class_name.split('.')[0].lower()}"
        if "." in class_name
        else class_name
    )
