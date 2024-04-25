#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
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
Contain IO dispatcher class.

Dispatcher routes the work to execution-specific functions.
"""

from snowflake.snowpark.modin.core.execution.dispatching.factories import factories
from snowflake.snowpark.modin.core.execution.dispatching.factories.factories import (
    PandasOnSnowflakeFactory,
)
from snowflake.snowpark.modin.utils import _inherit_docstrings


class FactoryNotFoundError(AttributeError):
    """
    ``FactoryNotFound`` exception class.

    Raise when no matching factory could be found.
    """

    pass


class StubIoEngine:
    """
    IO-Engine that does nothing more than raise NotImplementedError when any method is called.

    Parameters
    ----------
    factory_name : str
        Factory name, which will be reflected in error messages.

    Notes
    -----
    Used for testing purposes.
    """

    def __init__(self, factory_name="") -> None:
        self.factory_name = factory_name or "Unknown"

    def __getattr__(self, name):
        """
        Return a function that raises `NotImplementedError` for the `name` method.

        Parameters
        ----------
        name : str
            Method name to indicate in `NotImplementedError`.

        Returns
        -------
        callable
        """

        def stub(*args, **kw):
            raise NotImplementedError(
                f"Method {self.factory_name}.{name} is not implemented"
            )

        return stub


class StubFactory(factories.BaseFactory):
    """
    Factory that does nothing more than raise NotImplementedError when any method is called.

    Notes
    -----
    Used for testing purposes.
    """

    io_cls = StubIoEngine()

    @classmethod
    def set_failing_name(cls, factory_name):
        """
        Fill in `.io_cls` class attribute with ``StubIoEngine`` engine.

        Parameters
        ----------
        factory_name : str
            Name to pass to the ``StubIoEngine`` constructor.
        """
        cls.io_cls = StubIoEngine(factory_name)
        return cls


class FactoryDispatcher:
    """
    Class that routes IO-work to the factories.

    This class is responsible for keeping selected factory up-to-date and dispatching
    calls of IO-functions to its actual execution-specific implementations.
    """

    __factory: factories.BaseFactory = None

    @classmethod
    def get_factory(cls) -> factories.BaseFactory:
        """Get current factory."""
        if cls.__factory is None:
            # set default factory to be PandasOnPythonFactory, TODO: replace with Snowflake factory
            # lazy initialize
            cls.__factory = PandasOnSnowflakeFactory()
            cls.__factory.prepare()
        return cls.__factory

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._from_pandas)
    def from_pandas(cls, df):
        return cls.get_factory()._from_pandas(df)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._from_arrow)
    def from_arrow(cls, at):
        return cls.get_factory()._from_arrow(at)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._from_non_pandas)
    def from_non_pandas(cls, *args, **kwargs):
        return cls.get_factory()._from_non_pandas(*args, **kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._from_dataframe)
    def from_dataframe(cls, *args, **kwargs):
        return cls.get_factory()._from_dataframe(*args, **kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._read_parquet)
    def read_parquet(cls, **kwargs):
        return cls.get_factory()._read_parquet(**kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._read_csv)
    def read_csv(cls, **kwargs):
        return cls.get_factory()._read_csv(**kwargs)

    # @classmethod
    # #@_inherit_docstrings(factories.ExperimentalPandasOnRayFactory._read_csv_glob)
    # def read_csv_glob(cls, **kwargs):
    #     return cls.get_factory()._read_csv_glob(**kwargs)
    #
    # @classmethod
    # @_inherit_docstrings(
    #     factories.ExperimentalPandasOnRayFactory._read_pickle_distributed
    # )
    # def read_pickle_distributed(cls, **kwargs):
    #     return cls.get_factory()._read_pickle_distributed(**kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._read_json)
    def read_json(cls, **kwargs):
        return cls.get_factory()._read_json(**kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._read_gbq)
    def read_gbq(cls, **kwargs):
        return cls.get_factory()._read_gbq(**kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._read_html)
    def read_html(cls, **kwargs):
        return cls.get_factory()._read_html(**kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._read_clipboard)
    def read_clipboard(cls, **kwargs):
        return cls.get_factory()._read_clipboard(**kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._read_excel)
    def read_excel(cls, **kwargs):
        return cls.get_factory()._read_excel(**kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._read_hdf)
    def read_hdf(cls, **kwargs):
        return cls.get_factory()._read_hdf(**kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._read_feather)
    def read_feather(cls, **kwargs):
        return cls.get_factory()._read_feather(**kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._read_stata)
    def read_stata(cls, **kwargs):
        return cls.get_factory()._read_stata(**kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._read_sas)
    def read_sas(cls, **kwargs):  # pragma: no cover
        return cls.get_factory()._read_sas(**kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._read_pickle)
    def read_pickle(cls, **kwargs):
        return cls.get_factory()._read_pickle(**kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._read_sql)
    def read_sql(cls, **kwargs):
        return cls.get_factory()._read_sql(**kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._read_fwf)
    def read_fwf(cls, **kwargs):
        return cls.get_factory()._read_fwf(**kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._read_sql_table)
    def read_sql_table(cls, **kwargs):
        return cls.get_factory()._read_sql_table(**kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._read_sql_query)
    def read_sql_query(cls, **kwargs):
        return cls.get_factory()._read_sql_query(**kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._read_spss)
    def read_spss(cls, **kwargs):
        return cls.get_factory()._read_spss(**kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._to_sql)
    def to_sql(cls, *args, **kwargs):
        return cls.get_factory()._to_sql(*args, **kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._to_pickle)
    def to_pickle(cls, *args, **kwargs):
        return cls.get_factory()._to_pickle(*args, **kwargs)

    # @classmethod
    # @_inherit_docstrings(
    #     factories.ExperimentalPandasOnRayFactory._to_pickle_distributed
    # )
    # def to_pickle_distributed(cls, *args, **kwargs):
    #     return cls.get_factory()._to_pickle_distributed(*args, **kwargs)

    # @classmethod
    # @_inherit_docstrings(factories.ExperimentalPandasOnRayFactory._read_custom_text)
    # def read_custom_text(cls, **kwargs):
    #     return cls.get_factory()._read_custom_text(**kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._to_csv)
    def to_csv(cls, *args, **kwargs):
        return cls.get_factory()._to_csv(*args, **kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._to_parquet)
    def to_parquet(cls, *args, **kwargs):
        return cls.get_factory()._to_parquet(*args, **kwargs)

    # Snowflake added methods
    @classmethod
    @_inherit_docstrings(factories.BaseFactory._read_snowflake)
    def read_snowflake(cls, *args, **kwargs):
        return cls.get_factory()._read_snowflake(*args, **kwargs)

    @classmethod
    @_inherit_docstrings(factories.BaseFactory._to_snowflake)
    def to_snowflake(cls, *args, **kwargs):
        return cls.get_factory()._to_snowflake(*args, **kwargs)
