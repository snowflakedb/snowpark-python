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

"""Module houses default functions builder class."""

from typing import Any, Callable, Optional, Union

import pandas
from pandas.core.dtypes.common import is_list_like

from snowflake.snowpark.modin.utils import (
    MODIN_UNNAMED_SERIES_LABEL,
    is_property,
    try_cast_to_pandas,
)


class ObjTypeDeterminer:
    """
    Class that routes work to the frame.

    Provides an instance which forwards all of the `__getattribute__` calls
    to an object under which `key` function is applied.
    """

    def __getattr__(self, key: str) -> Callable:
        """
        Build function that executes `key` function over passed frame.

        Parameters
        ----------
        key : str

        Returns
        -------
        callable
            Function that takes DataFrame and executes `key` function on it.
        """

        def func(df: object, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
            """Access specified attribute of the passed object and call it if it's callable."""
            prop = getattr(df, key)
            if callable(prop):
                return prop(*args, **kwargs)
            else:
                return prop

        return func


class DefaultMethod:
    """
    Builder for default-to-pandas methods.

    Attributes
    ----------
    OBJECT_TYPE : str
        Object type name that will be shown in default-to-pandas warning message.
    DEFAULT_OBJECT_TYPE : object
        Default place to search for a function.
    """

    OBJECT_TYPE = "DataFrame"
    DEFAULT_OBJECT_TYPE = ObjTypeDeterminer

    # This function is pulled from the Operator class in modin/core/dataframe/algebra/operator.py
    def __init__(self) -> None:
        raise ValueError(  # pragma: no cover
            "Please use {}.register instead of the constructor".format(
                type(self).__name__
            )
        )

    @classmethod
    def get_func_name_for_registered_method(
        cls, fn: Union[Callable, property, str]
    ) -> str:
        """
        Function that takes in a Callable or a property and returns its name
        """

        if is_property(fn):
            # when a property method without a name, fn_name will be something like
            # "<property object at 0x7f8671e09d10>", here we use fget to get the name of the property. Note that this
            # method is still not perfect because we cannot get the class name of the property, e.g., we can only get
            # "hour" from series.dt.hour
            fn_name = f"<property fget:{getattr(fn.fget, '__name__', 'noname')}>"  # type: ignore[union-attr]
        else:
            fn_name = getattr(fn, "__name__", str(fn))

        return fn_name

    @classmethod
    def register(
        cls,
        func: Union[Callable, property, str],
        obj_type: Optional[object] = None,
        inplace: Optional[bool] = None,
        fn_name: Optional[str] = None,
    ) -> Callable:
        """
        Build function that do fallback to default pandas implementation for passed `func`.

        Parameters
        ----------
        func : callable or str,
            Function to apply to the casted to pandas frame or its property accesed
            by ``cls.frame_wrapper``.
        obj_type : object, optional
            If `func` is a string with a function name then `obj_type` provides an
            object to search function in.
        inplace : bool, optional
            If True return an object to which `func` was applied, otherwise return
            the result of `func`.
        fn_name : str, optional
            Function name which will be shown in default-to-pandas warning message.
            If not specified, name will be deducted from `func`.

        Returns
        -------
        callable
            Function that takes query compiler, does fallback to pandas and applies `func`
            to the casted to pandas frame or its property accessed by ``cls.frame_wrapper``.
        """

        if isinstance(func, str):
            if obj_type is None:
                obj_type = cls.DEFAULT_OBJECT_TYPE
            fn = getattr(obj_type, func)
        else:
            fn = func

        if fn_name is None:
            fn_name = cls.get_func_name_for_registered_method(func)

        if type(fn) == property:
            fn = cls.build_property_wrapper(fn)

        def applyier(df: pandas.DataFrame, *args: Any, **kwargs: Any) -> Callable:
            """
            Apply target function to the casted to pandas frame.

            This function is directly applied to the casted to pandas frame, executes target
            function under it and processes result so it is possible to create a valid
            query compiler from it.
            """
            args = try_cast_to_pandas(args)  # pragma: no cover
            kwargs = try_cast_to_pandas(kwargs)  # pragma: no cover

            # pandas default implementation doesn't know how to handle `dtypes` keyword argument
            kwargs.pop("dtypes", None)
            df = cls.frame_wrapper(df)
            result = fn(df, *args, **kwargs)

            if not isinstance(
                result, pandas.Series
            ) and not isinstance(  # pragma: no cover
                result, pandas.DataFrame
            ):
                # When applying a DatetimeProperties or TimedeltaProperties function,
                # if we don't specify the dtype for the DataFrame, the frame might
                # get the wrong dtype, e.g. for to_pydatetime in
                # https://github.com/modin-project/modin/issues/4436
                astype_kwargs = {}
                dtype = getattr(result, "dtype", None)
                if dtype and isinstance(
                    df,
                    (
                        pandas.core.indexes.accessors.DatetimeProperties,
                        pandas.core.indexes.accessors.TimedeltaProperties,
                    ),
                ):
                    astype_kwargs["dtype"] = dtype
                result = (
                    pandas.DataFrame(result, **astype_kwargs)
                    if is_list_like(result)
                    else pandas.DataFrame([result], **astype_kwargs)
                )
            if isinstance(result, pandas.Series):
                if result.name is None:
                    result.name = MODIN_UNNAMED_SERIES_LABEL
                result = result.to_frame()

            inplace_method = kwargs.get("inplace", False)
            if inplace is not None:
                inplace_method = inplace
            return result if not inplace_method else df

        return cls.build_default_to_pandas(applyier, fn_name)  # type: ignore[arg-type]

    @classmethod
    def build_property_wrapper(cls, prop: property) -> Callable:
        """
        Build function that accesses specified property of the frame.

        Parameters
        ----------
        prop : str
            Property name to access.

        Returns
        -------
        callable
            Function that takes DataFrame and returns its value of `prop` property.
        """

        def property_wrapper(df: Any) -> Any:
            """Get specified property of the passed object."""
            return prop.fget(df)  # type: ignore[misc]  # pragma: no cover

        return property_wrapper

    @classmethod
    def build_default_to_pandas(cls, fn: Callable, fn_name: str) -> Callable:
        """
        Build function that do fallback to pandas for passed `fn`.

        Parameters
        ----------
        fn : callable
            Function to apply to the defaulted frame.
        fn_name : str
            Function name which will be shown in default-to-pandas warning message.

        Returns
        -------
        callable
            Method that does fallback to pandas and applies `fn` to the pandas frame.
        """
        fn.__name__ = f"<function {cls.OBJECT_TYPE}.{fn_name}>"

        def wrapper(  # type: ignore[no-untyped-def]
            self, *args: Any, **kwargs: Any
        ) -> Callable:
            """Do fallback to pandas for the specified function."""
            return self.default_to_pandas(fn, *args, **kwargs)

        return wrapper

    @classmethod
    def frame_wrapper(cls, df: pandas.DataFrame) -> pandas.DataFrame:
        """
        Extract frame property to apply function on.

        This method is executed under casted to pandas frame right before applying
        a function passed to `register`, which gives an ability to transform frame somehow
        or access its properties, by overriding this method in a child class.

        Parameters
        ----------
        df : pandas.DataFrame

        Returns
        -------
        pandas.DataFrame

        Notes
        -----
        Being a base implementation, this particular method does nothing with passed frame.
        """
        return df
