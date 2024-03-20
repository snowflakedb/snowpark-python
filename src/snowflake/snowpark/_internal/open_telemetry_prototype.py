#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import inspect
from logging import getLogger
from typing import Dict, List, Tuple

logger = getLogger(__name__)
# this parameter make sure no error when open telemetry is not installed
open_telemetry_found = True
try:
    from opentelemetry import trace

except ImportError:
    open_telemetry_found = False

if open_telemetry_found:
    tracer = trace.get_tracer("snow.snowpark.dataframe")



def open_telemetry(func):
    def open_telemetry_wrapper(*df, **params):
        # dataframe, function name
        name = func.__qualname__
        dataframe = parameter_decoder(df, name)
        with tracer.start_as_current_span(name) as cur_span:
            try:
                if cur_span.is_recording():
                    # store execution location in span
                    filename, lineno = find_code_location(inspect.stack(), name)
                    cur_span.set_attribute("code.filepath", f"{filename}")
                    cur_span.set_attribute("code.lineno", f"{lineno}")
                    # stored method chain
                    method_chain = build_method_chain(dataframe._plan.api_calls, name)
                    cur_span.set_attribute("method.chain", method_chain)
            except Exception as e:
                logger.debug(f"Error when acquiring span attributes. {e}")
            # get result of the dataframe
            result = func(*df, **params)
        return result

    def noop_wrapper(*df, **params):
        return func(*df, **params)

    if open_telemetry_found:
        return open_telemetry_wrapper
    else:
        return noop_wrapper


def find_code_location(frame_info, name) -> Tuple[str, int]:
    function_name = name.split(".")[-1]
    # for f in frame_info:
    #     print(f)
    for frame in frame_info:
        if frame.code_context is not None and len(frame.code_context) != 0:
            for line in frame.code_context:
                if function_name in line:
                    return frame.filename, frame.lineno
    raise RuntimeError("Cannot find function name in stack. Function possibly called at wrong place")


def build_method_chain(api_calls: List[Dict], name: str) -> str:
    method_chain = "DataFrame."
    for method in api_calls:
        method_name = method["name"]
        if method_name.startswith("Session"):
            continue
        method_name = method_name.split(".")[-1]
        method_chain = f"{method_chain}{method_name}()."
    method_chain = f"{method_chain}{name.split('.')[-1]}()"
    return method_chain


def parameter_decoder(df, name) -> ["DataFrame", Dict]:
    # collect parameters that are explicitly given a value
    if "DataFrameWriter" in name:
        dataframe = df[0]._dataframe
    else:
        dataframe = df[0]
    return dataframe
