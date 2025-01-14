#!/usr/bin/env python3

#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import tempfile
from typing import Callable, Optional
from unittest.mock import ANY, MagicMock, patch

import pytest
from pandas.errors import IndexingError, SpecificationError

from ci.check_standalone_function_snowpark_pandas_telemetry_decorator import (
    DecoratorError,
    check_standalone_function_snowpark_pandas_telemetry_decorator,
)
from snowflake.connector.errors import DatabaseError
from snowflake.snowpark.exceptions import (
    SnowparkDataframeException,
    SnowparkSessionException,
)
from snowflake.snowpark.modin.plugin._internal.telemetry import (
    error_to_telemetry_type,
    snowpark_pandas_telemetry_method_decorator,
)


@patch(
    "snowflake.snowpark.modin.plugin._internal.telemetry._send_snowpark_pandas_telemetry_helper"
)
def snowpark_pandas_error_test_helper(
    send_telemetry_helper_mock,
    func: Callable,
    error: Exception,
    telemetry_type: str,
    loc_pref: Optional[str] = "SnowflakeQueryCompiler",
    mock_arg: Optional[MagicMock] = None,
    error_msg: Optional[str] = None,
):
    decorated_func = MagicMock(side_effect=error)
    decorated_func.__qualname__ = "magic_mock"
    with pytest.raises(type(error)):
        wrap_func = func(decorated_func)
        wrap_func(mock_arg)
    send_telemetry_helper_mock.assert_called_with(
        session=ANY,
        func_name=f"{loc_pref}.magic_mock",
        api_calls=[
            {
                "name": f"{loc_pref}.magic_mock",
            }
        ],
        query_history=ANY,
        telemetry_type=telemetry_type,
        error_msg=error_msg,
        method_call_count=ANY,
    )


@patch(
    "snowflake.snowpark.modin.plugin._internal.telemetry._send_snowpark_pandas_telemetry_helper"
)
@patch("snowflake.snowpark.session._get_active_session")
def test_snowpark_pandas_telemetry_method_decorator(
    _get_active_session_mock, send_telemetry_mock
):
    # SnowparkSessionException: test SnowparkSessionException is suppressed
    def raise_session_error():
        raise SnowparkSessionException("Mock Session Error")

    def raise_real_type_error(_):
        raise TypeError("Mock Real Error")

    _get_active_session_mock.side_effect = raise_session_error
    decorated_func1 = MagicMock()
    decorated_func1.__qualname__ = "magic_mock"
    decorated_func1.return_value = 10  # eager API is supposed to be sent
    mock_arg1 = MagicMock(spec=type)  # mock a class instance
    mock_arg1.__name__ = "MockClass"
    mock_arg1._query_compiler = MagicMock()
    mock_arg1._query_compiler.snowpark_pandas_api_calls = []
    wrap_func1 = snowpark_pandas_telemetry_method_decorator(decorated_func1)
    wrap_func1(mock_arg1)
    # Test that the SnowparkSessionException raising _get_active_session is called once.
    assert _get_active_session_mock.call_count == 1
    send_telemetry_mock.assert_not_called()
    assert len(mock_arg1._query_compiler.snowpark_pandas_api_calls) == 0

    # Test user errors + SnowparkSessionException
    decorated_func1.side_effect = raise_real_type_error
    wrap_func2 = snowpark_pandas_telemetry_method_decorator(decorated_func1)
    with pytest.raises(TypeError) as exc_info:
        wrap_func2(mock_arg1)
    exception = exc_info.value
    # Test "Mock Session Error" is suppressed from real error msg
    assert str(exception) == "Mock Real Error"
    assert _get_active_session_mock.call_count == 2
    send_telemetry_mock.assert_not_called()
    assert len(mock_arg1._query_compiler.snowpark_pandas_api_calls) == 0

    # Test user errors
    mock_arg2 = MagicMock()
    mock_arg2._query_compiler.snowpark_pandas_api_calls = []
    mock_arg2.__class__.__name__ = "mock_class"
    with pytest.raises(TypeError):
        wrap_func2(mock_arg2)
    send_telemetry_mock.assert_called_with(
        session=ANY,
        func_name="mock_class.magic_mock",
        api_calls=[
            {
                "name": "mock_class.magic_mock",
            }
        ],
        query_history=ANY,
        telemetry_type="snowpark_pandas_type_error",
        error_msg=None,
        method_call_count=ANY,
    )
    assert len(mock_arg2._query_compiler.snowpark_pandas_api_calls) == 0

    # Test instance method TypeError, IndexError, AttributeError
    # from `api_calls = copy.deepcopy(args[0]._query_compiler.snowpark_pandas_api_calls)`
    decorated_func1.side_effect = None
    wrap_func2()
    send_telemetry_mock.assert_called_with(
        session=ANY,
        func_name="mock_class.magic_mock",
        api_calls=[
            {
                "name": "mock_class.magic_mock",
            }
        ],
        query_history=ANY,
        telemetry_type="snowpark_pandas_type_error",
        error_msg=None,
        method_call_count=ANY,
    )


@pytest.mark.parametrize(
    "error",
    [
        NotImplementedError("test"),
        TypeError("test"),
        ValueError("test"),
        KeyError("test"),
        AttributeError("test"),
        ZeroDivisionError("test"),
        IndexError("test"),
        AssertionError("test"),
        IndexingError("test"),
        SpecificationError("test"),
        DatabaseError("test"),
    ],
)
def test_snowpark_pandas_telemetry_method_error(error):
    mock_arg = MagicMock()
    mock_arg._query_compiler.snowpark_pandas_api_calls = []
    mock_arg.__class__.__name__ = "mock_class"
    snowpark_pandas_error_test_helper(
        func=snowpark_pandas_telemetry_method_decorator,
        error=error,
        telemetry_type=error_to_telemetry_type(error),
        loc_pref="mock_class",
        mock_arg=mock_arg,
        error_msg="test"
        if isinstance(error, (AssertionError, NotImplementedError))
        else None,
    )


@pytest.mark.parametrize(
    "error, telemetry_type",
    [
        (NotImplementedError, "snowpark_pandas_not_implemented_error"),
        (AssertionError, "snowpark_pandas_assertion_error"),
        (TypeError, "snowpark_pandas_type_error"),
        (SpecificationError, "snowpark_pandas_specification_error"),
        (DatabaseError, "snowpark_pandas_database_error"),
        (SnowparkDataframeException, "snowpark_pandas_snowpark_dataframe_exception"),
    ],
)
def test_error_to_telemetry_type(error, telemetry_type):
    assert error_to_telemetry_type(error("error_msg")) == telemetry_type


def test_check_standalone_function_snowpark_pandas_telemetry_decorator():
    # Create a temporary file with sample code
    code = """
import modin.pandas as pd
from modin.pandas.dataframe import DataFrame
from modin.pandas.series import Series
import test_decorator

def func1() -> DataFrame:
    def sub_func() -> DataFrame: #sub function should be excluded
        return pd.DataFrame()
    return pd.DataFrame()

def func2() -> Series:
    return pd.Series()

@test_decorator
def _private_func() -> DataFrame:
    return pd.DataFrame()

def func3() -> int:
    return 0

@test_decorator
def func4() -> DataFrame:
    return pd.DataFrame()

# Test class methods/instance methods will not be decorated
class TestClass:
    def test_instance_method(self) -> DataFrame:
        return pd.DataFrame()

    def test_class_method(cls) -> DataFrame:
        return pd.DataFrame()
    """
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp_file:
        tmp_file.write(code)
        tmp_file.flush()

        with pytest.raises(DecoratorError) as exc_info:
            check_standalone_function_snowpark_pandas_telemetry_decorator(
                target_file=tmp_file.name,
                telemetry_decorator_name="test_decorator",
            )
        assert (
            str(exc_info.value)
            == "functions ['func1', 'func2', 'func3'] should be decorated with test_decorator"
        )

    # Clean up the temporary file
    os.remove(tmp_file.name)
