import inspect
from typing import Dict, Union

from opentelemetry.sdk.resources import SERVICE_NAME, Resource

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader, ConsoleMetricExporter


resource = Resource(attributes={
    SERVICE_NAME: "snowpark-python-open-telemetry"
})
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
            dataframe, parameters = parameter_decoder(df, params, func)
            with tracer.start_as_current_span(name) as cur_span:
                # store parameters in span
                cur_span.set_attribute("function_parameters", str(parameters))

                # store execution location in span
                frame_info = inspect.stack()[-1]
                cur_span.set_attribute("filepath", f"{frame_info.filename}#{frame_info.lineno}")

                # stored method chain
                cur_span.set_attribute("method chain", str(dataframe._plan.api_calls))
                # collect query and query id after execution with query_history()
                with dataframe._session.query_history() as query_listener:
                    result = func(*df, **params)
                    queries = []
                    # remove query id for now as they can put it on the server side
                    for query in query_listener.queries:
                        queries.append({"dataframe.query.text": query.sql_text})
                    cur_span.set_attribute("queries", str(queries))
            return result
        return wrapper
    return open_telemetry_decorator


def parameter_decoder(df, params, func) -> ["DataFrame", Dict]:

    # collect parameters that are explicitly given a value
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


