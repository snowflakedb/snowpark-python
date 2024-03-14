#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import inspect
from typing import Dict

# this parameter make sure no error when open telemetry is not installed
open_telemetry_not_found = False
try:
    from opentelemetry import trace
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

except ImportError:
    open_telemetry_not_found = True

if not open_telemetry_not_found:
    resource = Resource(attributes={SERVICE_NAME: "snowpark-python-open-telemetry"})
    # output to console for debug
    traceProvider = TracerProvider(resource=resource)
    processor = BatchSpanProcessor(ConsoleSpanExporter())
    traceProvider.add_span_processor(processor)
    trace.set_tracer_provider(traceProvider)

    tracer = trace.get_tracer("snowflake.snowpark.dataframe")


def open_telemetry(name):
    def open_telemetry_decorator(func):
        def wrapper(*df, **params):
            # get complete parameter list of the function
            dataframe, parameters = parameter_decoder(df, params, func, name)
            with tracer.start_as_current_span(name) as cur_span:
                # store parameters passed into action function in span
                cur_span.set_attribute("function_parameters", str(parameters))

                # store execution location in span
                frame_info = inspect.stack()[-1]
                cur_span.set_attribute("code.filepath", f"{frame_info.filename}")
                cur_span.set_attribute("code.lineno", f"{frame_info.lineno}")

                # stored method chain
                method_chain = "Dataframe."
                for method in dataframe._plan.api_calls:
                    method_name = method["name"]
                    if method_name.startswith("Session"):
                        continue
                    method_name = method_name.split(".")[-1]
                    method_chain = f"{method_chain}{method_name}."
                method_chain = f"{method_chain}{name.split('.')[-1]}"
                cur_span.set_attribute("method.chain", method_chain)

                # collect query and query id after execution with query_history()
                with dataframe._session.query_history() as query_listener:
                    result = func(*df, **params)
                    queries = []
                    # remove query text for now as they can put it on the server side
                    for query in query_listener.queries:
                        queries.append({"dataframe.query.id": query.query_id})
                    cur_span.set_attribute("queries", str(queries))
            return result

        def noop_wrapper(*df, **params):
            return func(*df, **params)

        if open_telemetry_not_found:
            return noop_wrapper
        else:
            return wrapper

    return open_telemetry_decorator


def parameter_decoder(df, params, func, name) -> ["DataFrame", Dict]:

    # collect parameters that are explicitly given a value
    if "save_as_table" in name:
        dataframe = df[0]._dataframe
    else:
        dataframe = df[0]
    param_names = list(func.__code__.co_varnames)[1:]
    parameters = {}
    if len(df) > 1:
        for value in df[1:]:
            parameters[param_names.pop(0)] = value
    parameters = {**parameters, **params}
    # collect parameter that use default value
    signature = inspect.signature(func)
    for param_name, param in signature.parameters.items():
        if param_name == "self":
            continue
        if param_name not in parameters:
            parameters[param_name] = param.default
    return dataframe, parameters
