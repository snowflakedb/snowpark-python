#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import json
from typing import Any, Optional
from unittest.mock import ANY, MagicMock, patch

import modin.pandas as pd
import numpy as np
import pandas
import pytest
from modin.pandas.dataframe import DataFrame
from pandas._libs.lib import NoDefault, no_default

import snowflake.snowpark.modin.plugin  # noqa: F401
import snowflake.snowpark.session
from snowflake.snowpark._internal.telemetry import TelemetryClient, TelemetryField
from snowflake.snowpark.modin.plugin._internal.telemetry import (
    _not_equal_to_default,
    _send_snowpark_pandas_telemetry_helper,
    _try_get_kwargs_telemetry,
    error_to_telemetry_type,
    snowpark_pandas_telemetry_method_decorator,
)
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import BASIC_TYPE_DATA1, BASIC_TYPE_DATA2
from tests.unit.modin.test_telemetry import snowpark_pandas_error_test_helper


def _extract_snowpark_pandas_telemetry_log_data(
    *,
    expected_func_name: str,
    session: snowflake.snowpark.session.Session,
) -> dict:
    """
    Extracts Snowpark pandas telemetry log data for a specific function name.

    Args:
        expected_func_name: The expected name of the function.
        session: Session instance.

    Returns:
        A dictionary containing the extracted telemetry log data.

    """
    for i in range(len(session._conn._telemetry_client.telemetry._log_batch)):
        try:
            if (
                session._conn._telemetry_client.telemetry._log_batch[i].to_dict()[
                    "message"
                ][TelemetryField.KEY_DATA.value]["func_name"]
                == expected_func_name
            ):
                return session._conn._telemetry_client.telemetry._log_batch[
                    i
                ].to_dict()["message"][TelemetryField.KEY_DATA.value]
        except Exception:
            # Exception usually means this message does not have attribute we want and we don't really care
            pass
    return dict()


@patch(
    "snowflake.snowpark.modin.plugin._internal.telemetry._send_snowpark_pandas_telemetry_helper"
)
@sql_count_checker(query_count=2)
def test_snowpark_pandas_telemetry_standalone_function_decorator(
    send_telemetry_mock,
    session,
    test_table_name,
):
    """
    Test one of two telemetry decorators: snowpark_pandas_telemetry_standalone_function_decorator
    """
    session.create_dataframe([BASIC_TYPE_DATA1, BASIC_TYPE_DATA2]).write.save_as_table(
        test_table_name, table_type="temp"
    )
    df = pd.read_snowflake(test_table_name)
    assert df._query_compiler.snowpark_pandas_api_calls == [
        {
            "name": "pd_extensions.read_snowflake",
        }
    ]
    send_telemetry_mock.assert_not_called()


@sql_count_checker(query_count=0)
def test_standalone_api_telemetry():
    df = pd.Series(["1", "2", "3"])
    newdf = pd.to_numeric(df)
    assert df._query_compiler.snowpark_pandas_api_calls == [
        {"name": "Series.property.name_set"}
    ]
    assert newdf._query_compiler.snowpark_pandas_api_calls == [
        {
            "name": "general.to_numeric",
        }
    ]


def test_snowpark_pandas_telemetry_method_decorator(test_table_name):
    """
    Test one of two telemetry decorators: snowpark_pandas_telemetry_method_decorator
    """
    df1 = pd.DataFrame([[1, np.nan], [3, 4]], index=[1, 0])
    # Test in place lazy API: df1 api_call_list should contain lazy.
    df1._query_compiler.snowpark_pandas_api_calls.clear()
    df1._query_compiler.snowpark_pandas_api_calls = [{"name": "TestClass.test_func"}]
    with SqlCounter(query_count=0):
        df1.dropna(inplace=True)

    df1_expected_api_calls = [
        {"name": "TestClass.test_func"},
        {"name": "DataFrame.DataFrame.dropna", "argument": ["inplace"]},
    ]
    assert df1._query_compiler.snowpark_pandas_api_calls == df1_expected_api_calls

    # Test lazy APIs that are not in place: df1 api_call_list should not contain lazy but df2 should.
    # And both should contain previous APIs
    with SqlCounter(query_count=0):
        df2 = df1.dropna(inplace=False)
    assert df1._query_compiler.snowpark_pandas_api_calls == df1_expected_api_calls
    df2_expected_api_calls = df1_expected_api_calls + [
        {
            "name": "DataFrame.DataFrame.dropna",
        },
    ]
    assert df2._query_compiler.snowpark_pandas_api_calls == df2_expected_api_calls
    # Clear connector telemetry client buffer to avoid flush triggered by the next API call, ensuring log extraction.
    df1._query_compiler._modin_frame.ordered_dataframe.session._conn._telemetry_client.telemetry.send_batch()
    with SqlCounter(query_count=1):
        df1.to_snowflake(test_table_name, index=False, if_exists="replace")

    # eager api should not be collected in api_calls
    assert df1._query_compiler.snowpark_pandas_api_calls == df1_expected_api_calls
    # eager api should be sent as telemetry
    data = _extract_snowpark_pandas_telemetry_log_data(
        expected_func_name="DataFrame.to_snowflake",
        session=df1._query_compiler._modin_frame.ordered_dataframe.session,
    )
    assert set(data.keys()) == {"category", "api_calls", "sfqids", "func_name"}
    assert data["category"] == "snowpark_pandas"
    assert data["api_calls"] == df1_expected_api_calls + [
        {
            "name": "DataFrame.to_snowflake",
            "argument": [
                "if_exists",
                "index",
            ],
        }
    ]
    assert len(data["sfqids"]) > 0
    assert data["func_name"] == "DataFrame.to_snowflake"
    # Test telemetry in python connector satisfy json format
    telemetry_client = (
        df1._query_compiler._modin_frame.ordered_dataframe.session._conn._telemetry_client.telemetry
    )
    body = {"logs": [x.to_dict() for x in telemetry_client._log_batch]}
    # If any previous REST request failed to send telemetry, telemetry_client._enabled would be set to False
    assert (
        telemetry_client._enabled
    ), "Telemetry client should be enabled, likely because the previous REST request failed to send telemetry."
    _ = json.dumps(body)


@patch.object(TelemetryClient, "send")
@sql_count_checker(query_count=0)
def test_send_snowpark_pandas_telemetry_helper(send_mock):
    session = snowflake.snowpark.session._get_active_session()
    _send_snowpark_pandas_telemetry_helper(
        session=session,
        telemetry_type="test_send_type",
        func_name="test_send_func",
        query_history=None,
        api_calls=[],
    )
    send_mock.assert_called_with(
        {
            "source": "SnowparkPandas",
            "version": ANY,
            "python_version": ANY,
            "operating_system": ANY,
            "type": "test_send_type",
            "data": {"func_name": "test_send_func", "category": "snowpark_pandas"},
        }
    )


@sql_count_checker(query_count=0)
def test_not_equal_to_default():
    # Test DataFrame type
    df_none = pd.DataFrame()
    df_empty = pd.DataFrame({})
    assert _not_equal_to_default(df_none, df_none)
    assert _not_equal_to_default(df_empty, df_empty)

    # Test NoDefault and no_default
    assert not _not_equal_to_default(NoDefault, NoDefault)
    assert not _not_equal_to_default(no_default, no_default)

    # Test different types
    assert _not_equal_to_default(df_none, False)

    # Test exception handling
    class CustomTypeWithException:
        def __eq__(self, other):
            raise Exception("Equality exception")

    assert not _not_equal_to_default(
        CustomTypeWithException(), CustomTypeWithException()
    )


@sql_count_checker(query_count=0)
def test_telemetry_args():
    def sample_function(
        arg1_no_default_int: int,
        arg2_no_default_bool: bool,
        arg3_default_optional_zero: Optional[int] = 0,
        arg4_default_none: Any = None,
        arg5_simple_default: str = "arg5_default",
        arg6_default_empty_str: str = "",
        arg7_no_default_dataframe: Optional[DataFrame] = no_default,
        arg8_nodefault_detaframe: Optional[DataFrame] = NoDefault,
    ):
        pass

    # Test that non-defaulted arguments are not collected and defaulted arguments are collected
    assert _try_get_kwargs_telemetry(
        func=sample_function,
        args=(1,),
        kwargs={"arg2_no_default_bool": True, "arg3_default_optional_zero": 2},
    ) == ["arg3_default_optional_zero"]

    # Test that defaulted arguments overridden with a passed-in value that is the same as default are not collected
    # and keyword non-defaulted arguments are not collected
    assert (
        _try_get_kwargs_telemetry(
            func=sample_function,
            args=(),
            kwargs={
                "arg1_no_default_int": 0,
                "arg2_no_default_bool": False,
                "arg3_default_optional_zero": 0,
            },
        )
        == list()
    )

    # Test that defaulted to None or "" arguments are collected
    assert _try_get_kwargs_telemetry(
        func=sample_function,
        args=(1, False, 3),
        kwargs={
            "arg5_simple_default": "test",
            "arg4_default_none": {"test_key": "test_val"},
            "arg6_default_empty_str": "test6",
        },
    ) == [
        "arg3_default_optional_zero",
        "arg4_default_none",
        "arg5_simple_default",
        "arg6_default_empty_str",
    ]

    # Test that defaulted arguments overridden with None are collected
    assert _try_get_kwargs_telemetry(
        func=sample_function,
        args=(1, False, None),
        kwargs={},
    ) == ["arg3_default_optional_zero"]

    # Test dataframe type argument with default value no_default and NoDefault are not collected
    assert (
        _try_get_kwargs_telemetry(
            func=sample_function,
            args=(1, False),
            kwargs={},
        )
        == list()
    )

    # Test passing in default values, dataframe type argument with
    # default value no_default and NoDefault are not collected
    assert (
        _try_get_kwargs_telemetry(
            func=sample_function,
            args=(1, False),
            kwargs={
                "arg7_no_default_dataframe": no_default,
                "arg8_nodefault_detaframe": NoDefault,
            },
        )
        == list()
    )

    # Test passing in non-default values, dataframe type argument with
    # default value no_default and NoDefault are not collected
    assert _try_get_kwargs_telemetry(
        func=sample_function,
        args=(1, False),
        kwargs={
            "arg7_no_default_dataframe": pd.DataFrame(),
            "arg8_nodefault_detaframe": pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),
        },
    ) == ["arg7_no_default_dataframe", "arg8_nodefault_detaframe"]


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@sql_count_checker(query_count=7, fallback_count=1, sproc_count=1)
def test_property_methods_telemetry():
    datetime_series = pd.date_range("2000-01-01", periods=3, freq="h")
    ret_series = datetime_series.dt.timetz
    assert len(ret_series._query_compiler.snowpark_pandas_api_calls) == 1
    api_call = ret_series._query_compiler.snowpark_pandas_api_calls[0]
    assert api_call["is_fallback"]
    assert api_call["name"] == "Series.<property fget:timetz>"


@sql_count_checker(query_count=1)
def test_telemetry_with_update_inplace():
    # verify api_calls have been collected correctly for APIs using _update_inplace() in base.py
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    df.insert(1, "newcol", [99, 99, 90])
    assert len(df._query_compiler.snowpark_pandas_api_calls) == 1
    assert (
        df._query_compiler.snowpark_pandas_api_calls[0]["name"]
        == "DataFrame.DataFrame.insert"
    )


@sql_count_checker(query_count=0)
def test_telemetry_with_not_implemented_error():
    # verify api_calls have been collected correctly for Resample APIs
    mock_arg = MagicMock()
    mock_arg._query_compiler.snowpark_pandas_api_calls = []
    mock_arg.__class__.__name__ = "mock_class"

    index = pandas.date_range("1/1/2000", periods=9, freq="min")
    ser = pd.Series(range(9), index=index)
    try:
        ser.resample("3T").bfill()
    except NotImplementedError:
        pass

    snowpark_pandas_error_test_helper(
        func=snowpark_pandas_telemetry_method_decorator,
        error=NotImplementedError,
        telemetry_type=error_to_telemetry_type(
            NotImplementedError("Method bfill is not implemented for Resampler!")
        ),
        loc_pref="mock_class",
        mock_arg=mock_arg,
    )


@sql_count_checker(query_count=1)
def test_telemetry_with_resample():
    # verify api_calls have been collected correctly for Resample APIs
    index = pandas.date_range("1/1/2000", periods=9, freq="min")
    ser = pd.Series(range(9), index=index)
    results = ser.resample("3T").sum()

    assert len(results._query_compiler.snowpark_pandas_api_calls) == 2
    # name_set happens in series __init__
    assert (
        results._query_compiler.snowpark_pandas_api_calls[0]["name"]
        == "Series.property.name_set"
    )
    assert (
        results._query_compiler.snowpark_pandas_api_calls[1]["name"]
        == "Resampler.Resampler.sum"
    )


@sql_count_checker(query_count=0)
def test_telemetry_with_groupby():
    # verify api_calls have been collected correctly for GroupBy APIs
    df = pd.DataFrame(
        {
            "Animal": ["Falcon", "Falcon", "Parrot", "Parrot"],
            "Max Speed": [380.0, 370.0, 24.0, 26.0],
        }
    )
    results = df.groupby(["Animal"]).mean()

    assert len(results._query_compiler.snowpark_pandas_api_calls) == 1
    assert (
        results._query_compiler.snowpark_pandas_api_calls[0]["name"]
        == "DataFrameGroupBy.DataFrameGroupBy.mean"
    )


@sql_count_checker(query_count=0)
def test_telemetry_with_rolling():
    # verify api_calls have been collected correctly for Rolling APIs
    df = pd.DataFrame({"A": ["h", "e", "l", "l", "o"], "B": [0, -1, 2.5, np.nan, 4]})
    results = df.rolling(2, min_periods=1).sum(numeric_only=True)

    assert len(results._query_compiler.snowpark_pandas_api_calls) == 1
    assert (
        results._query_compiler.snowpark_pandas_api_calls[0]["name"]
        == "Rolling.Rolling.sum"
    )


@sql_count_checker(query_count=2, join_count=2)
def test_telemetry_getitem_setitem():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    s = df["a"]
    assert len(df._query_compiler.snowpark_pandas_api_calls) == 0
    assert s._query_compiler.snowpark_pandas_api_calls == [
        {"name": "DataFrame.BasePandasDataset.__getitem__"}
    ]
    df["a"] = 0
    df["b"] = 0
    assert df._query_compiler.snowpark_pandas_api_calls == [
        {"name": "DataFrame.DataFrame.__setitem__"},
        {"name": "DataFrame.DataFrame.__setitem__"},
    ]
    # Clear connector telemetry client buffer to avoid flush triggered by the next API call, ensuring log extraction.
    s._query_compiler._modin_frame.ordered_dataframe.session._conn._telemetry_client.telemetry.send_batch()
    # This trigger eager evaluation and the messages should have been flushed to the connector, so we have to extract
    # the telemetry log from the connector to validate
    _ = s[0]
    data = _extract_snowpark_pandas_telemetry_log_data(
        expected_func_name="Series.BasePandasDataset.__getitem__",
        session=s._query_compiler._modin_frame.ordered_dataframe.session,
    )
    assert data["api_calls"] == [
        {"name": "DataFrame.BasePandasDataset.__getitem__"},
        {"name": "Series.BasePandasDataset.__getitem__"},
    ]


@pytest.mark.parametrize(
    "name, method, expected_query_count",
    [
        ["__repr__", lambda df: df.__repr__(), 1],
        ["__iter__", lambda df: df.__iter__(), 0],
    ],
)
def test_telemetry_private_method(name, method, expected_query_count):
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    # Clear connector telemetry client buffer to avoid flush triggered by the next API call, ensuring log extraction.
    df._query_compiler._modin_frame.ordered_dataframe.session._conn._telemetry_client.telemetry.send_batch()

    with SqlCounter(query_count=expected_query_count):
        method(df)
    # This trigger eager evaluation and the messages should have been flushed to the connector, so we have to extract
    # the telemetry log from the connector to validate

    data = _extract_snowpark_pandas_telemetry_log_data(
        expected_func_name=f"DataFrame.DataFrame.{name}",
        session=df._query_compiler._modin_frame.ordered_dataframe.session,
    )
    assert data["api_calls"] == [{"name": f"DataFrame.DataFrame.{name}"}]


@sql_count_checker(query_count=3)
def test_telemetry_property_index():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    df._query_compiler.snowpark_pandas_api_calls.clear()
    # Clear connector telemetry client buffer to avoid flush triggered by the next API call, ensuring log extraction.
    df._query_compiler._modin_frame.ordered_dataframe.session._conn._telemetry_client.telemetry.send_batch()
    # This trigger eager evaluation and the messages should have been flushed to the connector, so we have to extract
    # the telemetry log from the connector to validate
    idx = df.index
    data = _extract_snowpark_pandas_telemetry_log_data(
        expected_func_name="DataFrame.property.index_get",
        session=df._query_compiler._modin_frame.ordered_dataframe.session,
    )
    assert data["api_calls"] == [
        {"name": "DataFrame.property.index_get"},
    ]

    df.index = idx
    assert df._query_compiler.snowpark_pandas_api_calls == [
        {"name": "DataFrame.property.index_set"},
    ]


# TODO SNOW-996140: add telemetry for iloc/loc set
@pytest.mark.parametrize(
    "name, method, expected_query_count, expected_join_count",
    [
        ["iloc", lambda df: df.iloc[0, 0], 1, 2],
        ["loc", lambda df: df.loc[0, "a"], 2, 2],
    ],
)
def test_telemetry_property_iloc(
    name, method, expected_query_count, expected_join_count
):
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    df._query_compiler.snowpark_pandas_api_calls.clear()
    # Clear connector telemetry client buffer to avoid flush triggered by the next API call, ensuring log extraction.
    df._query_compiler._modin_frame.ordered_dataframe.session._conn._telemetry_client.telemetry.send_batch()
    # This trigger eager evaluation and the messages should have been flushed to the connector, so we have to extract
    # the telemetry log from the connector to validate
    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        _ = method(df)
    data = _extract_snowpark_pandas_telemetry_log_data(
        expected_func_name=f"DataFrame.property.{name}_get",
        session=df._query_compiler._modin_frame.ordered_dataframe.session,
    )
    assert data["api_calls"] == [
        {"name": f"DataFrame.property.{name}_get"},
    ]


@sql_count_checker(query_count=1)
def test_telemetry_repr():
    s = pd.Series([1, 2, 3, 4])
    s.__repr__()
    data = _extract_snowpark_pandas_telemetry_log_data(
        expected_func_name="Series.Series.__repr__",
        session=s._query_compiler._modin_frame.ordered_dataframe.session,
    )
    assert data["api_calls"] == [
        {"name": "Series.property.name_set"},
        {"name": "Series.Series.__repr__"},
    ]
