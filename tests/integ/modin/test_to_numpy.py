#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import logging

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from numpy.testing import assert_array_equal

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import Utils


@pytest.mark.parametrize(
    "data",
    [
        [1, 2],
        [1, 2, None],
        [1.1, 2.2],
        [1.1, 2.2, np.nan],
        ["snow", "flake"],
        ["snow", "flake", None],
        [True, False],
        [True, False, None],
        [bytes("snow", "utf-8"), bytes("flake", "utf-8")],
        [bytes("snow", "utf-8"), bytes("flake", "utf-8"), None],
        [datetime.date(2023, 1, 1), datetime.date(2023, 9, 15)],
        [datetime.date(2023, 1, 1), datetime.date(2023, 9, 15), None],
        [datetime.time(1, 2, 3, 1), datetime.time(0, 0, 0)],
        [datetime.time(1, 2, 3, 1), datetime.time(0, 0, 0), None],
        [datetime.datetime(2023, 1, 1), datetime.datetime(2023, 1, 1, 1, 2, 3)],
        [datetime.datetime(2023, 1, 1), datetime.datetime(2023, 1, 1, 1, 2, 3), None],
    ],
)
@pytest.mark.parametrize("pandas_obj", ["DataFrame", "Series", "Index"])
@pytest.mark.parametrize("func", ["to_numpy", "values"])
def test_to_numpy_basic(data, pandas_obj, func):
    if pandas_obj == "Series":
        df = pd.Series(data)
        native_df = native_pd.Series(data)
    elif pandas_obj == "Index":
        df = pd.Index(data)
        native_df = native_pd.Index(data)
    else:
        df = pd.DataFrame([data, data])
        native_df = native_pd.DataFrame([data, data])
    with SqlCounter(query_count=1):
        if func == "to_numpy":
            assert_array_equal(df.to_numpy(), native_df.to_numpy())
        else:
            assert_array_equal(df.values, native_df.values)
    if pandas_obj == "Series":
        with SqlCounter(query_count=1):
            res = df.to_numpy()
        expected_res = native_df.to_numpy()
        for r1, r2 in zip(res, expected_res):
            # native pandas series returns a list of pandas Timestamp,
            # but Snowpark pandas returns a list of integers in ms.
            # Their values are equal
            if isinstance(r2, native_pd.Timestamp):
                assert native_pd.Timestamp(r1) == r2
            # can't compare np.nan directly
            elif native_pd.isna(r2):
                assert native_pd.isna(r1)
            else:
                assert r1 == r2


@sql_count_checker(query_count=5)
def test_tz_aware_data_to_numpy(session):
    table_name = random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(
        session, table_name, "a timestamp_ltz, b timestamp_tz", is_temporary=True
    )
    session.sql(
        f"insert into {table_name} values "
        "('2023-01-01 00:00:01.001', '2023-01-01 00:00:01.001 +0000'), "
        "('2023-12-31 23:59:59.999', '2023-12-31 23:59:59.999 +1000')"
    ).collect()
    expected_result = np.array(
        [
            [
                pd.Timestamp(
                    "2023-01-01 00:00:01.001000-0800", tz="America/Los_Angeles"
                ),
                pd.Timestamp(
                    "2022-12-31 16:00:01.001000-0800", tz="America/Los_Angeles"
                ),
            ],
            [
                pd.Timestamp(
                    "2023-12-31 23:59:59.999000-0800", tz="America/Los_Angeles"
                ),
                pd.Timestamp(
                    "2023-12-31 05:59:59.999000-0800", tz="America/Los_Angeles"
                ),
            ],
        ]
    )
    df = pd.read_snowflake(table_name)
    assert_array_equal(df.to_numpy(), expected_result)


@pytest.mark.parametrize("pandas_obj", ["DataFrame", "Series", "Index"])
@sql_count_checker(query_count=1)
def test_variant_data_to_numpy(pandas_obj):
    data = [
        1,
        1.1,
        "snow",
        bytes("snow", "utf-8"),
        datetime.date(2023, 1, 1),
        datetime.time(1, 2, 3, 1),
        datetime.datetime(2023, 1, 1),
        [1, 2],
        {"snow": "flake"},
        None,
    ]
    expected_data = []
    for e in data:
        # This is how Snowflake encode these types of data in string format
        if isinstance(e, bytes):
            expected_data.append(e.hex())
        elif isinstance(e, (datetime.date, datetime.time, datetime.datetime)):
            expected_data.append(e.isoformat())
        elif isinstance(e, datetime.datetime):
            expected_data.append(e.strftime("%Y-%m-%dT%H:%M:%S"))
        else:
            expected_data.append(e)
    df = getattr(pd, pandas_obj)(data)
    expected_result = np.array(expected_data, dtype=object)
    if pandas_obj == "DataFrame":
        expected_result = expected_result.reshape(-1, 1)
    assert_array_equal(df.to_numpy(), expected_result)


@sql_count_checker(query_count=1)
def test_to_numpy_copy_true_series(caplog):
    series = pd.Series([1])

    caplog.clear()
    WarningMessage.printed_warnings.clear()
    with caplog.at_level(logging.WARNING):
        assert_array_equal(series.to_numpy(copy=True), native_pd.Series([1]).to_numpy())
        assert "has been ignored by Snowpark pandas" in caplog.text


@sql_count_checker(query_count=1)
def test_to_numpy_copy_true_index(caplog):
    idx = pd.Index([1])

    caplog.clear()
    WarningMessage.printed_warnings.clear()
    with caplog.at_level(logging.WARNING):
        assert_array_equal(idx.to_numpy(copy=True), native_pd.Index([1]).to_numpy())
        assert "has been ignored by Snowpark pandas" in caplog.text


@sql_count_checker(query_count=1)
def test_to_numpy_warning(caplog):
    series = pd.Series([1])

    caplog.clear()
    WarningMessage.printed_warnings.clear()
    with caplog.at_level(logging.WARNING):
        series.to_numpy()
        assert (
            "The current operation leads to materialization and can be slow if the data is large!"
            in caplog.text
        )
