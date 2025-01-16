#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# Licensed to Modin Development Team under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding
# copyright ownership.  The Modin Development Team licenses this file to you under the
# Apache License, Version 2.0 (the "License"); you may not use this file except in
# compliance with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.

# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Modin project, under the Apache License,
# Version 2.0.

"""
Module contains Factories for all of the supported Modin executions.

Factory is a bridge between calls of IO function from high-level API and its
actual implementation in the execution, bound to that factory. Each execution is represented
with a Factory class.
"""

from modin.core.execution.dispatching.factories.factories import BaseFactory
from pandas.util._decorators import doc

from snowflake.snowpark.modin.plugin.io.snow_io import PandasOnSnowflakeIO

_doc_factory_class = """
Factory of {execution_name} execution.

This class is responsible for dispatching calls of IO-functions to its
actual execution-specific implementations.

Attributes
----------
io_cls : {execution_name}IO
    IO module class of the underlying execution. The place to dispatch calls to.
"""

_doc_factory_prepare_method = """
Initialize Factory.

Fills in `.io_cls` class attribute with {io_module_name} lazily.
"""

_doc_io_method_raw_template = """
Build query compiler from {source}.

Parameters
----------
{params}

Returns
-------
QueryCompiler
    Query compiler of the selected storage format.
"""

_doc_io_method_template = (
    _doc_io_method_raw_template
    + """
See Also
--------
modin.pandas.{method}
"""
)

_doc_io_method_kwargs_params = """**kwargs : kwargs
    Arguments to pass to the QueryCompiler builder method."""


@doc(_doc_factory_class, backend_name="PandasOnSnowflake", execution_name="Snowflake")
class PandasOnSnowflakeFactory(BaseFactory):
    @classmethod
    @doc(_doc_factory_prepare_method, io_module_name="``PandasOnSnowflakeIO``")
    def prepare(cls):
        cls.io_cls = PandasOnSnowflakeIO

    # following are snowflake specific functions
    @classmethod
    @doc(
        _doc_io_method_template,
        source="read from Snowflake table",
        params=_doc_io_method_kwargs_params,
        method="read_snowflake",
    )
    def _read_snowflake(cls, *args, **kwargs):
        return cls.io_cls.read_snowflake(*args, **kwargs)

    @classmethod
    @doc(
        _doc_io_method_template,
        source="save to Snowflake table",
        params=_doc_io_method_kwargs_params,
        method="to_snowflake",
    )
    def _to_snowflake(cls, *args, **kwargs):
        return cls.io_cls.to_snowflake(*args, **kwargs)

    @classmethod
    def _to_local(cls, *args, **kwargs):
        return cls.io_cls.to_local(*args, **kwargs)

    @classmethod
    def _to_remote(cls, *args, **kwargs):
        return cls.io_cls.to_remote(*args, **kwargs)
