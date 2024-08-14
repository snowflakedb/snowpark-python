#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import datetime
import inspect
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Optional, Union

import numpy as np
import pandas as native_pd

from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.types import LongType

TIMEDELTA_WARNING_MESSAGE = (
    "Snowpark pandas support for Timedelta is not currently available."
)


_python_type_to_from_pandas: dict[type, Callable[[Any], Any]] = {}
_pandas_type_to_snowpark_pandas_type: dict[type, "SnowparkPandasType"] = {}


class SnowparkPandasTypeMetaclass(
    # Inherit from ABCMeta so that this metaclass makes ABCs.
    ABCMeta
):
    """
    This class is a Metaclass for Snowpark pandas types.

    Defining a class through this metaclass updates some global type conversion
    information. We can refer to that information anywhere we need to do Snowpark
    pandas type conversion, e.g. in from_pandas and to_pandas.
    """

    def __new__(cls: type, clsname: str, bases: Any, attrs: Any) -> type:
        # Create a type using this class's superclass. mypy raises the error
        # 'Argument 2 for "super" not an instance of argument 1', which is
        # difficult to fix, so ignore that error.
        # Wrap the type in dataclass(frozen=True) so that it's immutable.
        new_snowpark_python_type = dataclass(
            super().__new__(cls, clsname, bases, attrs), frozen=True  # type: ignore
        )

        if inspect.isabstract(new_snowpark_python_type):
            return new_snowpark_python_type

        for cls in new_snowpark_python_type.types_to_convert_with_from_pandas:
            for existing_cls in _python_type_to_from_pandas:
                # we don't want any class in _python_type_to_from_pandas to be
                # a subclass of another type in _python_type_to_from_pandas.
                # Otherwise, the rewriting rules for the two types may
                # conflict.
                assert not issubclass(
                    cls, existing_cls
                ), f"Already registered from_pandas for class {cls} with {existing_cls}"
            _python_type_to_from_pandas[cls] = new_snowpark_python_type.from_pandas

        for existing_cls in _pandas_type_to_snowpark_pandas_type:
            # we don't want any class in _pandas_type_to_snowpark_pandas_type to be
            # a subclass of another type in _pandas_type_to_snowpark_pandas_type.
            # Otherwise, the conversions rules for the two types may conflict.
            assert not issubclass(
                new_snowpark_python_type.pandas_type, existing_cls
            ), f"Already registered Snowpark pandas type for class {cls} with {existing_cls}"
        _pandas_type_to_snowpark_pandas_type[
            new_snowpark_python_type.pandas_type
        ] = new_snowpark_python_type

        return new_snowpark_python_type


class SnowparkPandasType(metaclass=SnowparkPandasTypeMetaclass):
    """Abstract class for Snowpark pandas types."""

    @staticmethod
    @abstractmethod
    def from_pandas(value: Any) -> Any:
        """
        Convert a pandas representation of an object of this type to its representation in Snowpark Python.
        """

    @staticmethod
    @abstractmethod
    def to_pandas(value: Any) -> Any:
        """
        Convert an object representing this type in Snowpark Python to the pandas representation.
        """

    @staticmethod
    def get_snowpark_pandas_type_for_pandas_type(
        pandas_type: type,
    ) -> Optional["SnowparkPandasType"]:
        """
        Get the corresponding Snowpark pandas type, if it exists, for a given pandas type.
        """
        return _pandas_type_to_snowpark_pandas_type.get(pandas_type, None)


class TimedeltaType(SnowparkPandasType, LongType):
    """
    Timedelta represents the difference between two times.

    We represent Timedelta as the integer number of nanoseconds between the
    two times.
    """

    snowpark_type = LongType()
    pandas_type = np.dtype("timedelta64[ns]")
    types_to_convert_with_from_pandas = [native_pd.Timedelta, datetime.timedelta]

    def __init__(self) -> None:
        # TODO(SNOW-1620452): Remove this warning message before releasing
        # Timedelta support.
        WarningMessage.single_warning(TIMEDELTA_WARNING_MESSAGE)
        super().__init__()

    @staticmethod
    def to_pandas(value: int) -> native_pd.Timedelta:
        """
        Convert the Snowpark Python representation of Timedelta to native_pd.Timedelta.
        """
        return native_pd.Timedelta(value, unit="nanosecond")

    @staticmethod
    def from_pandas(value: Union[native_pd.Timedelta, datetime.timedelta]) -> int:
        """
        Convert a pandas representation of a Timedelta to its nanoseconds.
        """
        # `Timedelta.value` converts Timedelta to nanoseconds.
        if isinstance(value, native_pd.Timedelta):
            return value.value
        return native_pd.Timedelta(value).value


def ensure_snowpark_python_type(value: Any) -> Any:
    """
    If a python object is an instance of a Snowpark pandas type, rewrite it into its Snowpark Python representation.
    """
    for cls, from_pandas in _python_type_to_from_pandas.items():
        if isinstance(value, cls):
            return from_pandas(value)
    return value
