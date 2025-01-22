#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import inspect
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, NamedTuple, Optional, Tuple, Type, Union

import numpy as np
import pandas as native_pd

from snowflake.snowpark.column import Column
from snowflake.snowpark.types import DataType, LongType

"""Map Python type to its from_pandas method"""
_python_type_to_from_pandas: dict[type, Callable[[Any], Any]] = {}

"""Map Python type and pandas dtype to Snowpark pandas type"""
_type_to_snowpark_pandas_type: dict[Union[type, np.dtype], type] = {}


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

        for type in new_snowpark_python_type.types_to_convert_with_from_pandas:
            assert inspect.isclass(type), f"{type} is not a class"
            for existing_type in _python_type_to_from_pandas:
                # we don't want any class in _python_type_to_from_pandas to be
                # a subclass of another type in _python_type_to_from_pandas.
                # Otherwise, the rewriting rules for the two types may
                # conflict.
                assert not issubclass(
                    type, existing_type
                ), f"Already registered from_pandas for class {type} with {existing_type}"
            _python_type_to_from_pandas[type] = new_snowpark_python_type.from_pandas
            _type_to_snowpark_pandas_type[type] = new_snowpark_python_type

        assert (
            new_snowpark_python_type.pandas_type not in _type_to_snowpark_pandas_type
        ), f"Already registered Snowpark pandas type for pandas type {new_snowpark_python_type.pandas_type}"
        _type_to_snowpark_pandas_type[
            new_snowpark_python_type.pandas_type
        ] = new_snowpark_python_type

        return new_snowpark_python_type


class SnowparkPandasType(DataType, metaclass=SnowparkPandasTypeMetaclass):
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
        pandas_type: Union[type, np.dtype],
    ) -> Optional["SnowparkPandasType"]:
        """
        Get the corresponding Snowpark pandas type, if it exists, for a given pandas type.
        """
        if pandas_type in _type_to_snowpark_pandas_type:
            return _type_to_snowpark_pandas_type[pandas_type]()
        return None

    def type_match(self, value: Any) -> bool:
        """Return True if the value's type matches self."""
        val_type = SnowparkPandasType.get_snowpark_pandas_type_for_pandas_type(
            type(value)
        )
        return self == val_type


class SnowparkPandasColumn(NamedTuple):
    """A Snowpark Column that has an optional SnowparkPandasType."""

    # The Snowpark Column.
    snowpark_column: Column
    # The SnowparkPandasType for the column, if the type of the column is a SnowparkPandasType.
    snowpark_pandas_type: Optional[SnowparkPandasType]


class TimedeltaType(SnowparkPandasType, LongType):
    """
    Timedelta represents the difference between two times.

    We represent Timedelta as the integer number of nanoseconds between the
    two times.
    """

    snowpark_type: DataType = LongType()
    pandas_type: np.dtype = np.dtype("timedelta64[ns]")
    types_to_convert_with_from_pandas: Tuple[Type] = (  # type: ignore[assignment]
        native_pd.Timedelta,
        datetime.timedelta,
        np.timedelta64,
    )

    def __init__(self) -> None:
        super().__init__()

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    @staticmethod
    def to_pandas(value: int) -> native_pd.Timedelta:
        """
        Convert the Snowpark Python representation of Timedelta to native_pd.Timedelta.
        """
        return native_pd.Timedelta(value, unit="nanosecond")

    @staticmethod
    def from_pandas(
        value: Union[native_pd.Timedelta, datetime.timedelta, np.timedelta64]
    ) -> int:
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
