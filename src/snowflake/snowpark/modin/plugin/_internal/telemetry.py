#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import functools
import inspect
import re
from contextlib import nullcontext
from enum import Enum, unique
from typing import Any, Callable, Optional, TypeVar, Union, cast

from typing_extensions import ParamSpec

import snowflake.snowpark.session
from snowflake.connector.telemetry import TelemetryField as PCTelemetryField
from snowflake.snowpark._internal.telemetry import TelemetryField, safe_telemetry
from snowflake.snowpark.exceptions import SnowparkSessionException
from snowflake.snowpark.modin.plugin._internal.utils import (
    is_snowpark_pandas_dataframe_or_series_type,
)
from snowflake.snowpark.query_history import QueryHistory
from snowflake.snowpark.session import Session

# Define ParamSpec with "_Args" as the generic parameter specification similar to Any
_Args = ParamSpec("_Args")

T = TypeVar("T", bound=Callable[..., Any])


@unique
class SnowparkPandasTelemetryField(Enum):
    TYPE_SNOWPARK_PANDAS_FUNCTION_USAGE = "snowpark_pandas_function_usage"
    # function categories
    FUNC_CATEGORY_SNOWPARK_PANDAS = "snowpark_pandas"
    # keyword argument
    ARGS = "argument"
    # fallback flag
    IS_FALLBACK = "is_fallback"


# Argument truncating size after converted to str. Size amount can be later specified after analysis and needs.
ARG_TRUNCATE_SIZE = 100


@unique
class PropertyMethodType(Enum):
    FGET = "get"
    FSET = "set"
    FDEL = "delete"


@safe_telemetry
def _send_snowpark_pandas_telemetry_helper(
    *,
    session: Session,
    telemetry_type: str,
    error_msg: Optional[str] = None,
    func_name: str,
    query_history: Optional[QueryHistory],
    api_calls: Union[str, list[dict[str, Any]]],
) -> None:
    """
    A helper function that sends Snowpark pandas API telemetry data.
    _send_snowpark_pandas_telemetry_helper does not raise exception by using @safe_telemetry

    Args:
        session: The Snowpark session.
        telemetry_type: telemetry type. e.g. TYPE_SNOWPARK_PANDAS_FUNCTION_USAGE.value
        error_msg: Optional error message if telemetry_type is a Snowpark pandas error
        func_name: The name of the function being tracked.
        query_history: The query history context manager to record queries that are pushed down to the Snowflake
        database in the session.
        api_calls: Optional list of Snowpark pandas API calls made during the function execution.

    Returns:
        None
    """
    data: dict[str, Union[str, list[dict[str, Any]], list[str], Optional[str]]] = {
        TelemetryField.KEY_FUNC_NAME.value: func_name,
        TelemetryField.KEY_CATEGORY.value: SnowparkPandasTelemetryField.FUNC_CATEGORY_SNOWPARK_PANDAS.value,
        TelemetryField.KEY_ERROR_MSG.value: error_msg,
    }
    if len(api_calls) > 0:
        data[TelemetryField.KEY_API_CALLS.value] = api_calls
    if query_history is not None and len(query_history.queries) > 0:
        data[TelemetryField.KEY_SFQIDS.value] = [
            q.query_id for q in query_history.queries
        ]
    message: dict = {
        **session._conn._telemetry_client._create_basic_telemetry_data(telemetry_type),
        TelemetryField.KEY_DATA.value: data,
        PCTelemetryField.KEY_SOURCE.value: "SnowparkPandas",
    }
    session._conn._telemetry_client.send(message)


def _not_equal_to_default(arg_val: Any, default_val: Any) -> bool:
    # Return True if argument arg_val is not equal to its default value.
    try:
        # First check type to early return True if equality assertion could be avoided
        # to avoid potential undesired behaviour equality assertion of two different types.
        if type(arg_val) != type(default_val):
            return True
        # We assume dataframe/series type default value is not a dataframe/series type
        # to avoid equality assertion since the equality assertion of DataFrame/Series might cause additional
        # API calls which we don't want to be added to telemetry.
        if is_snowpark_pandas_dataframe_or_series_type(default_val):
            return True
        return arg_val != default_val
    except Exception:
        # Similar to @Safe_telemetry but return a value
        # We don't want telemetry to raise exception and returning False makes sure
        # arguments that raise exception will not be collected in telemetry.
        return False


def _try_get_kwargs_telemetry(
    *,
    func: Callable,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> list[str]:
    """
    Try to get the key word argument names for telemetry.

    These arguments:
        Must be passed-in;
        Must have a default value;
        The overridden value must be different from the default one
    Arguments are in the original order of their definition.

    Args:
        func: The function being decorated.
        args: The positional arguments passed to the function.
        kwargs: The keyword arguments passed to the function.

    Returns:
        List: a List of function arguments names
    """
    signature = inspect.signature(func)
    try:
        bound_args = signature.bind(*args, **kwargs)
        return [
            param_name
            for param_name, param in signature.parameters.items()
            if (
                param_name in bound_args.arguments
                and param.default is not inspect.Parameter.empty
                and _not_equal_to_default(
                    bound_args.arguments[param_name], param.default
                )
            )
        ]
    except Exception:
        # silence any exception from inspect to make telemetry safe, e.g., signature.bind may raise TypeError when
        # missing a required argument
        return []


def _run_func_helper(
    func: Callable[_Args, Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> Any:
    """
    The helper function that run func, suppressing the possible previous telemetry exception context.

    Args:
        func: The function being run.
        args: The positional arguments passed to the function.
        kwargs: The keyword arguments passed to the function.

    Returns:
        The return value of the function
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        # Raise error caused by func, i.e. the api call
        # while suppressing Telemetry caused exceptions like SnowparkSessionException from telemetry in the stack trace.
        # This prevents from adding telemetry error messages to regular API calls error messages.
        raise e from None


def error_to_telemetry_type(e: Exception) -> str:
    """
    Convert Error to Telemetry Type string
    Ex. NotImplementedError --> "snowpark_pandas_not_implemented_error"

    Parameters
    ----------
    e: The desired exception to convert to telemetry type

    Returns
    -------
    The telemetry type used to send telemetry.
    """
    error_class = re.findall("[A-Z]?[a-z]+", type(e).__name__)
    telemetry_type = "snowpark_pandas_" + "_".join(
        [word.lower() for word in error_class]
    )
    return telemetry_type


def _gen_func_name(
    class_prefix: str,
    func: Callable[_Args, Any],
    property_name: Optional[str] = None,
    property_method_type: Optional[PropertyMethodType] = None,
) -> str:
    """
    Generate function name for telemetry.

    Args:
        class_prefix: the class name as the prefix of the function name
        func: the main function
        property_name: the property name if the function is used by a property, e.g., `index`, `name`, `iloc`, `loc`,
        `dtype`, etc
        property_method_type: The property method (`FGET`/`FSET`/`FDEL`) that
        this function implements, if this method is used by a property.
        `property_name` must also be specified.

    Returns:
        The generated function name
    """
    func_name = func.__qualname__
    if property_name:
        assert property_method_type is not None
        func_name = f"property.{property_name}_{property_method_type.value}"
    return f"{class_prefix}.{func_name}"


def _telemetry_helper(
    *,
    func: Callable[_Args, Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    is_standalone_function: bool,
    property_name: Optional[str] = None,
    property_method_type: Optional[PropertyMethodType] = None,
) -> Any:
    """
    Helper function for the main process of all two telemetry decorators: snowpark_pandas_telemetry_method_decorator &
    snowpark_pandas_telemetry_standalone_function_decorator.
    It prepares telemetry message, deals with errors, runs the decorated function and sends telemetry

    Note:
        _telemetry_helper does not interfere with the normal execution of the decorated function, meaning that
    most telemetry related exceptions are suppressed, ensuring telemetry does not introduce new exceptions.
    However, if the decorated function raises an exception and fails, such exception will be raised.

    Args:
        func: The API function to be called.
        args: The arguments to be passed to the API function.
        kwargs: The keyword arguments to be passed to the API function.
        is_standalone_function: Indicate whether the decorated function is a standalone function. A standalone function
        in Python is a function defined outside a class or any other enclosing structure, callable directly without
        an instance of a class.
        property_name: the property name if the `func` is from a property.
        property_method_type: The property method (`FGET`/`FSET`/`FDEL`) that
        this function implements, if this method is used by a property.
        `property_name` must also be specified.

    Returns:
        The return value of the API function.

    Raises:
        Any exceptions raised by the API function.
    """
    # We manage existing api calls in this telemetry decorator so API developer does not need to worry about it. The way
    # we do so is that we first move it out of current args[0] and cached in existing_api_calls. The after generate the
    # current api call for telemetry, we will append it back. We did this way because this telemetry method can be
    # called recursively, e.g., df.where will trigger this decorator twice: once for itself and once for base.where.
    # Moving existing api call out first can avoid to generate duplicates.
    existing_api_calls = []
    need_to_restore_args0_api_calls = False

    # If the decorated func is a class method or a standalone function, we need to get an active session:
    if is_standalone_function or (len(args) > 0 and isinstance(args[0], type)):
        try:
            session = snowflake.snowpark.session._get_active_session()
        except SnowparkSessionException:
            return _run_func_helper(func, args, kwargs)
        class_prefix = (
            func.__module__.split(".")[-1]
            if is_standalone_function
            else args[0].__name__
        )
    # Else the decorated func is an instance method:
    else:
        try:
            # Methods decorated in (snow_)dataframe/series.py
            existing_api_calls = args[0]._query_compiler.snowpark_pandas_api_calls
            args[0]._query_compiler.snowpark_pandas_api_calls = []
            need_to_restore_args0_api_calls = True
            session = args[0]._query_compiler._modin_frame.ordered_dataframe.session
            class_prefix = args[0].__class__.__name__
        except (TypeError, IndexError, AttributeError):
            # TypeError: args might not support indexing; IndexError: args is empty; AttributeError: args[0] might not
            # have _query_compiler attribute.
            # If an exception is raised, mostly due to args[0]._query_compiler, it indicates that the
            # decorated function does not have the first argument for args[0] or self is a native pandas DataFrame thus
            # does not have attribute _query_compiler. Such exceptions will not be raised. And we ignore its telemetry.
            return _run_func_helper(func, args, kwargs)

    # Prepare api_calls' entry: curr_api_call
    kwargs_telemetry = _try_get_kwargs_telemetry(
        func=func,
        args=args,
        kwargs=kwargs,
    )
    # function name will be separated with ".". The first element is the class_prefix, i.e., the object class of the
    # caller and the rest will be the qualname of the func, which gives more complete information than __name__ and
    # therefore can be more helpful in debugging, e.g., func_name "DataFrame.DataFrame.dropna" shows the
    # caller object is Snowpark pandas DataFrame and the function used is from DataFrame's dropna method
    func_name = _gen_func_name(class_prefix, func, property_name, property_method_type)
    curr_api_call: dict[str, Any] = {TelemetryField.NAME.value: func_name}
    if kwargs_telemetry:
        curr_api_call[SnowparkPandasTelemetryField.ARGS.value] = kwargs_telemetry

    try:
        # query_history is a QueryHistory instance which is a Context Managers
        # See example in https://github.com/snowflakedb/snowpark-python/blob/main/src/snowflake/snowpark/session.py#L2052
        # Use `nullcontext` to handle `session` lacking `query_history` attribute without raising an exception.
        # This prevents telemetry from interfering with regular API calls.
        with getattr(session, "query_history", nullcontext)() as query_history:
            result = func(*args, **kwargs)
    except Exception as e:
        # Send Telemetry and Raise Error
        _send_snowpark_pandas_telemetry_helper(
            session=session,
            telemetry_type=error_to_telemetry_type(e),
            # Only track error messages for NotImplementedError
            error_msg=e.args[0]
            if isinstance(e, NotImplementedError) and e.args
            else None,
            func_name=func_name,
            query_history=query_history,
            api_calls=existing_api_calls + [curr_api_call],
        )
        raise e

    # Not inplace lazy APIs: add curr_api_call to the result
    if is_snowpark_pandas_dataframe_or_series_type(result):
        result._query_compiler.snowpark_pandas_api_calls = (
            existing_api_calls
            + result._query_compiler.snowpark_pandas_api_calls
            + [curr_api_call]
        )
        if need_to_restore_args0_api_calls:
            args[0]._query_compiler.snowpark_pandas_api_calls = existing_api_calls
    # TODO: SNOW-911654 Fix telemetry for cases like pd.merge([df1, df2]) and df1.merge(df2)
    # Inplace lazy APIs: those APIs won't return anything. We also need to exclude one exception "to_snowflake" which
    # also returns None, since it is an eager API
    elif (
        result is None
        and is_snowpark_pandas_dataframe_or_series_type(args[0])
        and func.__name__ != "to_snowflake"
    ):
        args[0]._query_compiler.snowpark_pandas_api_calls = (
            existing_api_calls
            + args[0]._query_compiler.snowpark_pandas_api_calls
            + [curr_api_call]
        )
    # Eager APIs:
    else:
        # eager api call should not be stored inside api_calls
        _send_snowpark_pandas_telemetry_helper(
            session=session,
            telemetry_type=SnowparkPandasTelemetryField.TYPE_SNOWPARK_PANDAS_FUNCTION_USAGE.value,
            func_name=func_name,
            query_history=query_history,
            api_calls=existing_api_calls + [curr_api_call],
        )
        if need_to_restore_args0_api_calls:
            args[0]._query_compiler.snowpark_pandas_api_calls = existing_api_calls
    return result


def snowpark_pandas_telemetry_method_decorator(
    func: T,
    property_name: Optional[str] = None,
    property_method_type: Optional[PropertyMethodType] = None,
) -> T:
    """
    Decorator function for telemetry of API calls in BasePandasDataset and its subclasses.

    When the decorated function is called, the decorator gets an active session if the decorated function is a
    class method, and captures any NotImplementedError raised by the function.
    If the return types is (snow)dataframe/series:
        then it's lazy not inplace API: set return dataframe's snowpark_pandas_api_calls =
        old snowpark_pandas_api_calls + return's snowpark_pandas_api_calls + current api call
    Else if the return types is None:
        then it's lazy inplace API: set return dataframe's snowpark_pandas_api_calls =
        old snowpark_pandas_api_calls + current api call
    Else:
        it's eager API: send snowpark_pandas_api_calls + current api call


    Args:
        func: the method of (Snowpark pandas) DataFrame/Series whose telemetry is to be collected.
        property_name: the property name if the `func` is from a property.
    Returns:
        The decorator function.
    """

    @functools.wraps(func)
    def wrap(*args, **kwargs):  # type: ignore
        # add a `type: ignore` for this function definition because the
        # function should be of type `T`, but it's too much work to
        # extract the input and output types from T in order to add type
        # hints in-line here. We'll fix up the type with a `cast` before
        # returning the function.
        return _telemetry_helper(
            func=func,
            args=args,
            kwargs=kwargs,
            is_standalone_function=False,
            property_name=property_name,
            property_method_type=property_method_type,
        )

    # need cast to convince mypy that we are returning a function with the same
    # signature as func.
    return cast(T, wrap)


def snowpark_pandas_telemetry_standalone_function_decorator(func: T) -> T:
    """
    Telemetry decorator for standalone functions.

    When the decorated function is called, the decorator gets an active session and captures any NotImplementedError
    raised by the function.
    If the return types is Snowpark pandas Dataframe/Series:
        then it's lazy not inplace API: set return dataframe's snowpark_pandas_api_calls =
        old snowpark_pandas_api_calls + return's snowpark_pandas_api_calls + current api call
    Else:
        send current api call


    Args:
        func: the method of (Snowpark pandas) DataFrame/Series whose telemetry is to be collected
    Returns:
        The decorator function.
    """

    @functools.wraps(func)
    def wrap(*args, **kwargs):  # type: ignore
        # add a `type: ignore` for this function definition because the
        # function should be of type `T`, but it's too much work to
        # extract the input and output types from T in order to add type
        # hints in-line here. We'll fix up the type with a `cast` before
        # returning the function.
        return _telemetry_helper(
            func=func,
            args=args,
            kwargs=kwargs,
            is_standalone_function=True,
        )

    # need cast to convince mypy that we are returning a function with the same
    # signature as func.
    return cast(T, wrap)


# The list of private methods that telemetry is enabled. Only those methods are interested to use are collected. Note
# that we cannot collect "__setattr__" or "__getattr__" because it will cause recursive calls.
TELEMETRY_PRIVATE_METHODS = {
    "__getitem__",
    "__setitem__",
    "__iter__",
    "__repr__",
    "__add__",
    "__iadd__",
    "__radd__",
    "__mul__",
    "__imul__",
    "__rmul__",
    "__pow__",
    "__ipow__",
    "__rpow__",
    "__sub__",
    "__isub__",
    "__rsub__",
    "__floordiv__",
    "__ifloordiv__",
    "__rfloordiv__",
    "__truediv__",
    "__itruediv__",
    "__rtruediv__",
    "__mod__",
    "__imod__",
    "__rmod__",
    "__rdiv__",
}


class TelemetryMeta(type):
    def __new__(
        cls, name: str, bases: tuple, attrs: dict[str, Any]
    ) -> Union[
        "snowflake.snowpark.modin.pandas.series.Series",
        "snowflake.snowpark.modin.pandas.dataframe.DataFrame",
        "snowflake.snowpark.modin.pandas.groupby.DataFrameGroupBy",
        "snowflake.snowpark.modin.pandas.resample.Resampler",
        "snowflake.snowpark.modin.pandas.window.Window",
        "snowflake.snowpark.modin.pandas.window.Rolling",
    ]:
        """
        Metaclass for enabling telemetry data collection on class/instance methods of
        Series, DataFrame, GroupBy, Resample, Window, Rolling and their subclasses, i.e. Snowpark pandas DataFrame/Series.

        This metaclass decorates callable class/instance methods which are public or are ``TELEMETRY_PRIVATE_METHODS``
        with ``snowpark_pandas_telemetry_api_usage`` telemetry decorator.
        Method arguments returned by _get_kwargs_telemetry are collected otherwise set telemetry_args=list().
        TelemetryMeta is only set as the metaclass of: snowflake.snowpark.modin.pandas.series.Series,
         snowflake.snowpark.modin.pandas.dataframe.DataFrame,
         snowflake.snowpark.modin.pandas.groupby.DataFrameGroupBy,
         snowflake.snowpark.modin.pandas.resample.Resampler,
         snowflake.snowpark.modin.pandas.window.Window,
         snowflake.snowpark.modin.pandas.window.Rolling, and their subclasses.


        Args:
            name (str): The name of the class.
            bases (tuple): The base classes of the class.
            attrs (Dict[str, Any]): The attributes of the class.

        Returns:
            Union[snowflake.snowpark.modin.pandas.series.Series,
                snowflake.snowpark.modin.pandas.dataframe.DataFrame,
                snowflake.snowpark.modin.pandas.groupby.DataFrameGroupBy,
                snowflake.snowpark.modin.pandas.resample.Resampler,
                snowflake.snowpark.modin.pandas.window.Window,
                snowflake.snowpark.modin.pandas.window.Rolling]:
                The modified class with decorated methods.
        """
        for attr_name, attr_value in attrs.items():
            if callable(attr_value) and (
                not attr_name.startswith("_")
                or (attr_name in TELEMETRY_PRIVATE_METHODS)
            ):
                attrs[attr_name] = snowpark_pandas_telemetry_method_decorator(
                    attr_value
                )
            elif isinstance(attr_value, property):
                # wrap on getter and setter
                attrs[attr_name] = property(
                    snowpark_pandas_telemetry_method_decorator(
                        cast(
                            # add a cast because mypy doesn't recognize that
                            # non-None fget and __get__ are both callable
                            # arguments to snowpark_pandas_telemetry_method_decorator.
                            Callable,
                            attr_value.__get__  # pragma: no cover: we don't encounter this case in pandas or modin because every property has an fget method.
                            if attr_value.fget is None
                            else attr_value.fget,
                        ),
                        property_name=attr_name,
                        property_method_type=PropertyMethodType.FGET,
                    ),
                    snowpark_pandas_telemetry_method_decorator(
                        attr_value.__set__
                        if attr_value.fset is None
                        else attr_value.fset,
                        property_name=attr_name,
                        property_method_type=PropertyMethodType.FSET,
                    ),
                    snowpark_pandas_telemetry_method_decorator(
                        attr_value.__delete__
                        if attr_value.fdel is None
                        else attr_value.fdel,
                        property_name=attr_name,
                        property_method_type=PropertyMethodType.FDEL,
                    ),
                    doc=attr_value.__doc__,
                )
        return type.__new__(cls, name, bases, attrs)
