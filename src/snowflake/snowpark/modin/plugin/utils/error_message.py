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

from functools import wraps

# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Modin project, under the Apache License,
# Version 2.0.
from logging import getLogger
from typing import Any, Callable, NoReturn, Optional, Union

logger = getLogger(__name__)

_snowpark_pandas_does_not_yet_support = "Snowpark pandas does not yet support the"


def _make_not_implemented_decorator(
    decorating_functions: bool, attribute_prefix: Optional[str] = None
) -> Callable:
    """
    Make a decorator that wraps a function or property in an outer function that raises NotImplementedError.

    Args:
        decorating_functions:
            Whether the decorator will decorate functions and not methods, e.g.
            pd.cut as opposed to pd.DataFrame.max
        attribute_prefix:
            The prefix for describing the attribute, e.g. for DataFrame methods
            this would be "DataFrame." If None, infer the prefix from the object
            that the method is called on. Set to None for superclasses like
            BasePandasDataset where the subtype of the object isn't known till
            runtime. Note that it doesn't make sense to set atribute_prefix to
            None when decorating functions, because functions aren't called on
            an object.

    Returns:
        A decorator that wraps a function or property in an outer function that raises NotImplementedError.
    """

    def not_implemented_decorator() -> Callable:
        def make_error_raiser(f: Any) -> Union[Callable, property]:
            if isinstance(f, classmethod):
                raise ValueError(
                    "classmethod objects do not have a name. Instead of trying to "
                    + "decorate a classmethod, decorate a regular function, then "
                    + "apply the decorator @classmethod to the result."
                )
            name = (
                # properties seem to not have __name__, but their fget tends
                # to have __name__.
                getattr(f, "__name__", getattr(f.fget, "__name__", repr(f)))
                if isinstance(f, property)
                else getattr(f, "__name__", repr(f))
            )

            if decorating_functions:

                @wraps(f)
                def raise_not_implemented_function_error(
                    *args: tuple[Any, ...], **kwargs: dict[str, Any]
                ) -> NoReturn:
                    assert attribute_prefix is not None
                    ErrorMessage.not_implemented(
                        message=f"{_snowpark_pandas_does_not_yet_support} property {attribute_prefix}.{name}"
                    )

                return raise_not_implemented_function_error

            if isinstance(f, property):

                def raise_not_implemented_property_error(
                    self: Any, *args: tuple[Any, ...], **kwargs: dict[str, Any]
                ) -> NoReturn:
                    if attribute_prefix is None:
                        non_null_attribute_prefix = type(self).__name__
                    else:
                        non_null_attribute_prefix = attribute_prefix
                    ErrorMessage.not_implemented(
                        message=f"{_snowpark_pandas_does_not_yet_support} property {non_null_attribute_prefix}.{name}"
                    )

                return property(
                    fget=raise_not_implemented_property_error,
                    fset=raise_not_implemented_property_error,
                    fdel=raise_not_implemented_property_error,
                    doc=f.__doc__,
                )

            @wraps(f)
            def raise_not_implemented_method_error(
                cls_or_self: Any, *args: tuple[Any, ...], **kwargs: dict[str, Any]
            ) -> NoReturn:
                if attribute_prefix is None:
                    non_null_attribute_prefix = (
                        # cls_or_self is a class if this is a classmethod.
                        cls_or_self
                        if isinstance(cls_or_self, type)
                        # Otherwise, look at the type of self.
                        else type(cls_or_self)
                    ).__name__
                else:
                    non_null_attribute_prefix = attribute_prefix
                ErrorMessage.not_implemented(
                    message=f"{_snowpark_pandas_does_not_yet_support} method {non_null_attribute_prefix}.{name}"
                )

            return raise_not_implemented_method_error

        return make_error_raiser

    return not_implemented_decorator


base_not_implemented = _make_not_implemented_decorator(decorating_functions=False)

dataframe_not_implemented = _make_not_implemented_decorator(
    decorating_functions=False, attribute_prefix="DataFrame"
)

series_not_implemented = _make_not_implemented_decorator(
    decorating_functions=False, attribute_prefix="Series"
)

index_not_implemented = _make_not_implemented_decorator(
    decorating_functions=False, attribute_prefix="Index"
)

pandas_module_level_function_not_implemented = _make_not_implemented_decorator(
    decorating_functions=True, attribute_prefix="pd"
)


class ErrorMessage:
    # Only print full ``default to pandas`` warning one time.
    printed_default_to_pandas = False
    printed_warnings: set[int] = set()  # Set of hashes of printed warnings

    @classmethod
    def not_implemented(cls, message: str) -> NoReturn:  # pragma: no cover
        logger.debug(f"NotImplementedError: {message}")
        raise NotImplementedError(message)

    @staticmethod
    def method_not_implemented_error(
        name: str, class_: str
    ) -> None:  # pragma: no cover
        """
        Invokes ``ErrorMessage.not_implemented()`` with specified method name and class.

        Parameters
        ----------
        name: str
            The method that is not implemented.
        class_: str
            The class of Snowpark pandas function associated with the method.
        """
        message = f"{name} is not yet implemented for {class_}"
        ErrorMessage.not_implemented(message)

    @staticmethod
    def parameter_not_implemented_error(parameter_name: str, method_name: str) -> None:
        """
        Raises not implemented error for specified param and method.
        Args:
            parameter_name: Name of the parameter.
            method_name: Name of the method.
        """
        ErrorMessage.not_implemented(
            f"Snowpark pandas method {method_name} does not yet support the '{parameter_name}' parameter"
        )

    # TODO SNOW-840704: using Snowpark pandas exception class for the internal error
    @classmethod
    def internal_error(
        cls, failure_condition: bool, extra_log: str = ""
    ) -> None:  # pragma: no cover
        if failure_condition:
            raise Exception(f"Internal Error: {extra_log}")

    @classmethod
    def catch_bugs_and_request_email(
        cls, failure_condition: bool, extra_log: str = ""
    ) -> None:  # pragma: no cover
        if failure_condition:
            logger.info(f"Modin Error: Internal Error: {extra_log}")
            raise Exception(
                "Internal Error. "
                + "Please visit https://github.com/modin-project/modin/issues "
                + "to file an issue with the traceback and the command that "
                + "caused this error. If you can't file a GitHub issue, "
                + f"please email bug_reports@modin.org.\n{extra_log}"
            )
