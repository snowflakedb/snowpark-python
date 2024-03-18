#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import inspect
from typing import Dict, List, Union, Any

# this parameter make sure no error when open telemetry is not installed
open_telemetry_found = True
try:
    from opentelemetry import trace
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource

except ImportError:
    open_telemetry_found = False

if open_telemetry_found:
    tracer = trace.get_tracer("snowflake.snowpark.dataframe")


def open_telemetry(name):
    def open_telemetry_decorator(func):
        def wrapper(*df, **params):
            # get complete parameter list of the function
            dataframe = parameter_decoder(df, name)
            with tracer.start_as_current_span(name) as cur_span:
                if cur_span.is_recording():
                    # store execution location in span
                    frame_info = inspect.stack()[-1]
                    cur_span.set_attribute("code.filepath", f"{frame_info.filename}")
                    cur_span.set_attribute("code.lineno", f"{frame_info.lineno}")

                    # stored method chain
                    method_chain = build_method_chain(dataframe._plan.api_calls, name)
                    cur_span.set_attribute("method.chain", method_chain)

                # get result of the dataframe
                result = func(*df, **params)
            return result

        def noop_wrapper(*df, **params):
            return func(*df, **params)

        if open_telemetry_found:
            return wrapper
        else:
            return noop_wrapper

    return open_telemetry_decorator


def get_queries(dataframe: "DataFrame", func, df, params) -> Union[List[Dict], Any]:
    with dataframe._session.query_history() as query_listener:
        result = func(*df, **params)
        queries = []
        # remove query text for now as they can put it on the server side
        for query in query_listener.queries:
            queries.append({"dataframe.query.id": query.query_id, "dataframe.query.text": query.sql_text})
    return queries, result


def build_method_chain(api_calls: List[Dict], name: str) -> str:
    method_chain = "Dataframe."
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
    if "save_as_table" in name:
        dataframe = df[0]._dataframe
    else:
        dataframe = df[0]
    return dataframe
