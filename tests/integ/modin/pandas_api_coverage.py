#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import csv
import inspect
import time
from datetime import datetime
from functools import update_wrapper

import modin.pandas as pdi
from modin.pandas import DataFrame as DataFrameClazz, Series as SeriesClazz
from modin.pandas.groupby import (
    DataFrameGroupBy as DataFrameGroupByClazz,
    SeriesGroupBy as SeriesGroupByClazz,
)
from modin.pandas.plotting import Plotting as PlottingClazz
from modin.pandas.series_utils import DatetimeProperties as DatetimePropertiesClazz

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin.extensions.resample_overrides import (
    Resampler as ResamplerClazz,
)

# Not in current version of Modin
# from modin.pandas.window import Expanding as ExpandingClazz
from snowflake.snowpark.modin.plugin.extensions.window_overrides import (
    Rolling as RollingClazz,
    Window as WindowClazz,
)

# Used for instrumenting customer or client code.
# import modin.pandas as pdi
# import pandas
# from pandas.core.groupby import DataFrameGroupBy as DataFrameGroupByClazz
# from pandas.core.groupby import SeriesGroupBy as SeriesGroupByClazz
# from pandas.core.resample import Resampler as ResamplerClazz
# from pandas.core.indexes.accessors import DatetimeProperties as DatetimePropertiesClazz
# from pandas.core.window import Expanding as ExpandingClazz
# from pandas.core.window import Rolling as RollingClazz
# from pandas.core.window import Window as WindowClazz

dunder_api_methods = ["__add__", "__dataframe__", "__array__", "__iter__"]


class PandasAPICoverageGenerator:
    _instrumentation_initialized = False
    _writer = None

    def __init__(self) -> "PandasAPICoverageGenerator":
        self.init_instrumentation()

    def _instrument_function(self, func, classname, funcname, writer):
        def wrapped(*args, **kwargs):
            excp = None
            start_time = time.perf_counter()
            try:
                output = func(*args, **kwargs)
            except Exception as e:
                excp = e
                raise e
            finally:
                stop_time = time.perf_counter()
                expname = None if excp is None else excp.__class__.__name__
                non_none_params = list(
                    dict(filter(lambda x: x[1] is not None, kwargs.items())).keys()
                )
                data = {
                    "class": classname,
                    "method": funcname,
                    "params": non_none_params,
                    "exception": expname,
                    "start": start_time,
                    "stop": stop_time,
                }
                writer.write(data)
            return output

        update_wrapper(wrapped, func)
        return wrapped

    def _instrument_class(self, clazz, writer):
        # The Dtypes are not only noisy to instrument
        # they also introduce some unique issues with
        # typecasting once they are modified
        if clazz.__name__.endswith("Dtype"):
            return
        if hasattr(clazz, f"_snowpark_instrumented_{clazz.__name__}"):
            return
        for funcname in dir(clazz):
            if funcname.startswith("_") and funcname not in dunder_api_methods:
                continue
            func = getattr(clazz, funcname)
            # Properties need to have their getter wrapped
            if isinstance(func, property):
                wrapped_getter = self._instrument_function(
                    func.__get__, clazz.__name__, funcname, writer
                )
                new_property = property(wrapped_getter, func.__set__, func.__delattr__)
                try:
                    setattr(clazz, funcname, new_property)
                except Exception:  # noqa: E722 # nosec
                    pass
            # Anything else that is not callable should be ignored
            if not callable(func):
                continue
            # A property reference pointing at an implementation
            # class using __get__ or other methods. Instead of
            # modifying this property on the class we descend one
            # level into the implementation class
            if inspect.isclass(func):
                self._instrument_class(func, writer)
                continue
            wrapped_func = self._instrument_function(
                func, clazz.__name__, funcname, writer
            )
            try:
                setattr(clazz, funcname, wrapped_func)
            except Exception:  # noqa: E722 # nosec
                pass
        try:
            setattr(clazz, f"_snowpark_instrumented_{clazz.__name__}", True)
        except Exception:  # noqa: E722 # nosec
            pass

    def _instrument_module(self, module, writer):
        exports = module.__all__
        for name in exports:
            if name.startswith("_") and name not in dunder_api_methods:
                continue
            object = ""
            try:
                object = getattr(module, name)
            except Exception:
                # Snowpandas Modin __init__ seems to reference functions
                # that do not exist
                pass
            if inspect.isclass(object):
                self._instrument_class(object, writer)
                continue
            if callable(object):
                self._instrument_function
                wrapped_func = self._instrument_function(
                    object, module.__name__, name, writer
                )
                try:
                    setattr(module, name, wrapped_func)
                except Exception:  # noqa: E722 # nosec
                    pass

    def init_instrumentation(self):
        if PandasAPICoverageGenerator._instrumentation_initialized is True:
            return
        PandasAPICoverageGenerator._instrumentation_initialized = True
        header = ["class", "method", "params", "exception", "start", "stop"]
        PandasAPICoverageGenerator._writer = RecordWriter(header)

        clazzes = [
            DataFrameGroupByClazz,
            PlottingClazz,
            SeriesGroupByClazz,
            ResamplerClazz,
            RollingClazz,
            DatetimePropertiesClazz,
            WindowClazz,
            SeriesClazz,
            DataFrameClazz,
            # ExpandingClazz  # Not pulled in from upstream yet
        ]
        for clazz in clazzes:
            self._instrument_class(clazz, PandasAPICoverageGenerator._writer)

        self._instrument_module(pdi, PandasAPICoverageGenerator._writer)


class RecordWriter:
    _filename = None

    def __init__(self, header) -> "RecordWriter":
        now = datetime.now()
        dt_string = now.strftime("%Y%m%d_%H%M%S")
        RecordWriter._filename = f"record-{dt_string}.csv"
        output = open(RecordWriter._filename, "a")
        writer = csv.DictWriter(output, fieldnames=header)
        writer.writeheader()
        self._writer = writer
        self._file = output

    def write(self, data):
        self._writer.writerow(data)
        # dtor not reliably called on process exit
        self._file.flush()

    def close(self):
        self._file.flush()
        self._file.close()

    def __del__(self):
        self.close()
