#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import decimal
import sys
import threading
from unittest.mock import patch
import uuid
from functools import partial
from typing import Any, Dict, Tuple

import pytest
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import PlanState

try:
    import pandas as pd  # noqa: F401

    from snowflake.snowpark.types import PandasDataFrameType, PandasSeriesType

    is_pandas_available = True
except ImportError:
    is_pandas_available = False


from snowflake.snowpark import Row
from snowflake.snowpark._internal.telemetry import TelemetryField
from snowflake.snowpark._internal.utils import generate_random_alphanumeric
from snowflake.snowpark.exceptions import SnowparkColumnException
from snowflake.snowpark.functions import (
    call_udf,
    col,
    lit,
    max as max_,
    mean,
    pandas_udf,
    sproc,
    udf,
    udtf,
)
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import IntegerType
from tests.utils import TestData, TestFiles

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="This is testing inbound telemetry",
        run=False,
    )
]


class TelemetryDataTracker:
    def __init__(self, session: Session) -> None:
        self.session = session

    def extract_telemetry_log_data(self, index, partial_func) -> Tuple[Dict, str, Any]:
        """Extracts telemetry data, telemetry type from the log batch and result of running partial_func."""
        telemetry_obj = self.session._conn._telemetry_client.telemetry

        result = partial_func()
        message_log = telemetry_obj._log_batch

        if len(message_log) < abs(index):
            # if current message_log is smaller than requested index, this means that we just
            # send a batch of messages and reset message log. We will re-run our function to
            # refill our message log and extract the message. This assumes that the requested
            # index is appropriate and will be fill once the function is called again.
            result = partial_func()
            message_log = telemetry_obj._log_batch

        message = message_log[index].to_dict()["message"]
        data = message.get(TelemetryField.KEY_DATA.value, None)
        type_ = message.get("type", None)
        return data, type_, result

    def find_message_in_log_data(self, size, partial_func, expected_data) -> bool:
        telemetry_obj = self.session._conn._telemetry_client.telemetry

        partial_func()
        message_log = telemetry_obj._log_batch

        if len(message_log) < size:
            # if current message_log is smaller than requested size, this means that we just
            # send a batch of messages and reset message log. We will re-run our function to
            # refill our message log and extract the message. This assumes that the requested
            # size is appropriate and will be fill once the function is called again.
            partial_func()
            message_log = telemetry_obj._log_batch

        # we search for messages in reverse until we hit
        for message in message_log[: -(size + 1) : -1]:
            data = message.to_dict()["message"].get(TelemetryField.KEY_DATA.value, {})
            if data == expected_data:
                return True
        return False


def compare_api_calls(actual_calls, expected_calls):
    assert len(actual_calls) == len(
        expected_calls
    ), f"{actual_calls} != {expected_calls}"
    for actual, expected in zip(actual_calls, expected_calls):
        assert actual["name"] == expected["name"]
        if "subcalls" in actual and "subcalls" in expected:
            compare_api_calls(actual["subcalls"], expected["subcalls"])
        elif "subcalls" in actual or "subcalls" in expected:
            raise AssertionError(f"Subcalls mismatch: {actual} != {expected}")


def test_basic_api_calls(session):
    df = session.range(1, 10, 2)

    df_filter = df.filter(col("id") > 4)
    compare_api_calls(
        df_filter._plan.api_calls,
        [
            {"name": "Session.range"},
            {"name": "DataFrame.filter"},
        ],
    )
    # Make sure API call does not add to the api_calls list of the original dataframe
    compare_api_calls(df._plan.api_calls, [{"name": "Session.range"}])
    # Repeat API call to make sure the list stays correct even with multiple calls to the same API
    df_filter = df.filter(col("id") > 4)
    compare_api_calls(
        df_filter._plan.api_calls,
        [
            {"name": "Session.range"},
            {"name": "DataFrame.filter"},
        ],
    )

    # Repeat API call to make sure the list stays correct even with multiple calls to the same API
    df_select = df_filter.select("id")
    compare_api_calls(
        df_select._plan.api_calls,
        [
            {"name": "Session.range"},
            {"name": "DataFrame.filter"},
            {"name": "DataFrame.select"},
        ],
    )
    df_select = df_filter.select("id")
    compare_api_calls(
        df_select._plan.api_calls,
        [
            {"name": "Session.range"},
            {"name": "DataFrame.filter"},
            {"name": "DataFrame.select"},
        ],
    )


def test_describe_api_calls(session):
    df = TestData.test_data2(session)
    compare_api_calls(
        df._plan.api_calls, [{"name": "Session.create_dataframe[values]"}]
    )

    desc = df.describe("a", "b")
    compare_api_calls(
        desc._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {
                "name": "DataFrame.describe",
                "subcalls": [
                    {"name": "Session.create_dataframe[values]"},
                    {
                        "name": "DataFrame.agg",
                        "subcalls": [
                            {"name": "DataFrame.group_by"},
                            {"name": "RelationalGroupedDataFrame.agg"},
                        ],
                    },
                    {
                        "name": "DataFrame.to_df",
                        "subcalls": [{"name": "DataFrame.select"}],
                    },
                    {"name": "DataFrame.select"},
                    {"name": "Session.create_dataframe[values]"},
                    {
                        "name": "DataFrame.agg",
                        "subcalls": [
                            {"name": "DataFrame.group_by"},
                            {"name": "RelationalGroupedDataFrame.agg"},
                        ],
                    },
                    {
                        "name": "DataFrame.to_df",
                        "subcalls": [{"name": "DataFrame.select"}],
                    },
                    {"name": "DataFrame.select"},
                    {"name": "DataFrame.union"},
                    {"name": "Session.create_dataframe[values]"},
                    {
                        "name": "DataFrame.agg",
                        "subcalls": [
                            {"name": "DataFrame.group_by"},
                            {"name": "RelationalGroupedDataFrame.agg"},
                        ],
                    },
                    {
                        "name": "DataFrame.to_df",
                        "subcalls": [{"name": "DataFrame.select"}],
                    },
                    {"name": "DataFrame.select"},
                    {"name": "DataFrame.union"},
                    {"name": "Session.create_dataframe[values]"},
                    {
                        "name": "DataFrame.agg",
                        "subcalls": [
                            {"name": "DataFrame.group_by"},
                            {"name": "RelationalGroupedDataFrame.agg"},
                        ],
                    },
                    {
                        "name": "DataFrame.to_df",
                        "subcalls": [{"name": "DataFrame.select"}],
                    },
                    {"name": "DataFrame.select"},
                    {"name": "DataFrame.union"},
                    {"name": "Session.create_dataframe[values]"},
                    {
                        "name": "DataFrame.agg",
                        "subcalls": [
                            {"name": "DataFrame.group_by"},
                            {"name": "RelationalGroupedDataFrame.agg"},
                        ],
                    },
                    {
                        "name": "DataFrame.to_df",
                        "subcalls": [{"name": "DataFrame.select"}],
                    },
                    {"name": "DataFrame.select"},
                    {"name": "DataFrame.union"},
                ],
            },
        ],
    )
    # Original dataframe still has the same API calls
    compare_api_calls(
        df._plan.api_calls, [{"name": "Session.create_dataframe[values]"}]
    )
    # Repeat API calls results in same output
    desc = df.describe("a", "b")
    compare_api_calls(
        desc._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {
                "name": "DataFrame.describe",
                "subcalls": [
                    {"name": "Session.create_dataframe[values]"},
                    {
                        "name": "DataFrame.agg",
                        "subcalls": [
                            {"name": "DataFrame.group_by"},
                            {"name": "RelationalGroupedDataFrame.agg"},
                        ],
                    },
                    {
                        "name": "DataFrame.to_df",
                        "subcalls": [{"name": "DataFrame.select"}],
                    },
                    {"name": "DataFrame.select"},
                    {"name": "Session.create_dataframe[values]"},
                    {
                        "name": "DataFrame.agg",
                        "subcalls": [
                            {"name": "DataFrame.group_by"},
                            {"name": "RelationalGroupedDataFrame.agg"},
                        ],
                    },
                    {
                        "name": "DataFrame.to_df",
                        "subcalls": [{"name": "DataFrame.select"}],
                    },
                    {"name": "DataFrame.select"},
                    {"name": "DataFrame.union"},
                    {"name": "Session.create_dataframe[values]"},
                    {
                        "name": "DataFrame.agg",
                        "subcalls": [
                            {"name": "DataFrame.group_by"},
                            {"name": "RelationalGroupedDataFrame.agg"},
                        ],
                    },
                    {
                        "name": "DataFrame.to_df",
                        "subcalls": [{"name": "DataFrame.select"}],
                    },
                    {"name": "DataFrame.select"},
                    {"name": "DataFrame.union"},
                    {"name": "Session.create_dataframe[values]"},
                    {
                        "name": "DataFrame.agg",
                        "subcalls": [
                            {"name": "DataFrame.group_by"},
                            {"name": "RelationalGroupedDataFrame.agg"},
                        ],
                    },
                    {
                        "name": "DataFrame.to_df",
                        "subcalls": [{"name": "DataFrame.select"}],
                    },
                    {"name": "DataFrame.select"},
                    {"name": "DataFrame.union"},
                    {"name": "Session.create_dataframe[values]"},
                    {
                        "name": "DataFrame.agg",
                        "subcalls": [
                            {"name": "DataFrame.group_by"},
                            {"name": "RelationalGroupedDataFrame.agg"},
                        ],
                    },
                    {
                        "name": "DataFrame.to_df",
                        "subcalls": [{"name": "DataFrame.select"}],
                    },
                    {"name": "DataFrame.select"},
                    {"name": "DataFrame.union"},
                ],
            },
        ],
    )

    empty_df = TestData.timestamp1(session).describe()
    compare_api_calls(
        empty_df._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.select"},
            {
                "name": "DataFrame.describe",
                "subcalls": [{"name": "Session.create_dataframe[values]"}],
            },
        ],
    )


def test_drop_duplicates_api_calls(session):
    df = session.create_dataframe(
        [[1, 1, 1, 1], [1, 1, 1, 2], [1, 1, 2, 3], [1, 2, 3, 4], [1, 2, 3, 4]],
        schema=["a", "b", "c", "d"],
    )

    if session.conf.get("use_simplified_query_generation"):
        distinct_api_calls = {"name": "DataFrame.distinct[select]"}
    else:
        distinct_api_calls = {
            "name": "DataFrame.distinct[group_by]",
            "subcalls": [
                {"name": "DataFrame.group_by"},
                {"name": "RelationalGroupedDataFrame.agg"},
            ],
        }

    dd_df = df.drop_duplicates()
    compare_api_calls(
        dd_df._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {
                "name": "DataFrame.drop_duplicates",
                "subcalls": [distinct_api_calls],
            },
        ],
    )
    # check that original dataframe has the same API calls
    compare_api_calls(
        df._plan.api_calls, [{"name": "Session.create_dataframe[values]"}]
    )

    subset_df = df.drop_duplicates(["a", "b"])
    compare_api_calls(
        subset_df._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {
                "name": "DataFrame.drop_duplicates",
                "subcalls": [
                    {"name": "DataFrame.select"},
                    {"name": "DataFrame.filter"},
                    {"name": "DataFrame.select"},
                ],
            },
        ],
    )
    # check that original dataframe has the same API calls
    compare_api_calls(
        df._plan.api_calls, [{"name": "Session.create_dataframe[values]"}]
    )


@pytest.mark.parametrize("use_simplified_query_generation", [True, False])
def test_drop_api_calls(session, use_simplified_query_generation):
    df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
    original = session.conf.get("use_simplified_query_generation")
    try:
        session.conf.set(
            "use_simplified_query_generation", use_simplified_query_generation
        )
        if use_simplified_query_generation:
            drop_api_call = {"name": "DataFrame.drop[exclude]"}
        else:
            drop_api_call = {
                "name": "DataFrame.drop[select]",
                "subcalls": [{"name": "DataFrame.select"}],
            }

        drop_id = df.drop("id")
        compare_api_calls(
            drop_id._plan.api_calls,
            [
                {"name": "Session.range"},
                {"name": "DataFrame.select"},
                drop_api_call,
            ],
        )

        # Raise exception and make sure the new API call isn't added to the list
        with pytest.raises(SnowparkColumnException, match=" Cannot drop all columns"):
            drop_id.drop("id_prime")
        compare_api_calls(
            drop_id._plan.api_calls,
            [
                {"name": "Session.range"},
                {"name": "DataFrame.select"},
                drop_api_call,
            ],
        )

        df2 = (
            session.range(3, 8)
            .select(["id", col("id").alias("id_prime")])
            .select(["id", col("id_prime").alias("id_prime_2")])
            .select(["id", col("id_prime_2").alias("id_prime_3")])
            .select(["id", col("id_prime_3").alias("id_prime_4")])
            .drop("id_prime_4")
        )
        compare_api_calls(
            df2._plan.api_calls,
            [
                {"name": "Session.range"},
                {"name": "DataFrame.select"},
                {"name": "DataFrame.select"},
                {"name": "DataFrame.select"},
                {"name": "DataFrame.select"},
                drop_api_call,
            ],
        )
    finally:
        session.conf.set("use_simplified_query_generation", original)


def test_to_df_api_calls(session):
    df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
    compare_api_calls(
        df._plan.api_calls,
        [
            {"name": "Session.range"},
            {"name": "DataFrame.select"},
        ],
    )

    # Raise exception and make sure api call list doesn't change
    with pytest.raises(ValueError, match="The number of columns doesn't match"):
        df.to_df(["new_name"])
    compare_api_calls(
        df._plan.api_calls,
        [
            {"name": "Session.range"},
            {"name": "DataFrame.select"},
        ],
    )

    to_df = df.to_df(["rename1", "rename2"])
    compare_api_calls(
        to_df._plan.api_calls,
        [
            {"name": "Session.range"},
            {"name": "DataFrame.select"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        ],
    )


def test_select_expr_api_calls(session):
    df = session.create_dataframe([-1, 2, 3], schema=["a"])
    compare_api_calls(
        df._plan.api_calls, [{"name": "Session.create_dataframe[values]"}]
    )

    select_expr = df.select_expr("abs(a)", "a + 2", "cast(a as string)")
    compare_api_calls(
        select_expr._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {
                "name": "DataFrame.select_expr",
                "subcalls": [{"name": "DataFrame.select"}],
            },
        ],
    )
    # check to make sure that the original dataframe doesn't have the extra API call
    compare_api_calls(
        df._plan.api_calls, [{"name": "Session.create_dataframe[values]"}]
    )


def test_agg_api_calls(session):
    df = session.create_dataframe([[1, 2], [4, 5]]).to_df("col1", "col2")
    compare_api_calls(
        df._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        ],
    )

    agg_df = df.agg([max_(col("col1")), mean(col("col2"))])
    compare_api_calls(
        agg_df._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
            {
                "name": "DataFrame.agg",
                "subcalls": [
                    {"name": "DataFrame.group_by"},
                    {"name": "RelationalGroupedDataFrame.agg"},
                ],
            },
        ],
    )
    # check to make sure that the original DF is unchanged
    compare_api_calls(
        df._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        ],
    )


@pytest.mark.parametrize("use_simplified_query_generation", [True, False])
def test_distinct_api_calls(session, use_simplified_query_generation):
    original = session.conf.get("use_simplified_query_generation")
    try:
        session.conf.set(
            "use_simplified_query_generation", use_simplified_query_generation
        )
        df = session.create_dataframe(
            [
                [1, 1],
                [1, 1],
                [2, 2],
                [3, 3],
                [4, 4],
                [5, 5],
                [None, 1],
                [1, None],
                [None, None],
            ]
        ).to_df("id", "v")
        compare_api_calls(
            df._plan.api_calls,
            [
                {"name": "Session.create_dataframe[values]"},
                {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
            ],
        )

        res = df.distinct()
        if use_simplified_query_generation:
            distinct_api_call = {"name": "DataFrame.distinct[select]"}
        else:
            distinct_api_call = {
                "name": "DataFrame.distinct[group_by]",
                "subcalls": [
                    {"name": "DataFrame.group_by"},
                    {"name": "RelationalGroupedDataFrame.agg"},
                ],
            }
        compare_api_calls(
            res._plan.api_calls,
            [
                {"name": "Session.create_dataframe[values]"},
                {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
                distinct_api_call,
            ],
        )
        # check to make sure that the original DF is unchanged
        compare_api_calls(
            df._plan.api_calls,
            [
                {"name": "Session.create_dataframe[values]"},
                {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
            ],
        )

        res2 = df.select(col("id")).distinct()
        res2_with_sort = res2.sort(["id"])
        compare_api_calls(
            res2_with_sort._plan.api_calls,
            [
                {"name": "Session.create_dataframe[values]"},
                {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
                {"name": "DataFrame.select"},
                distinct_api_call,
                {"name": "DataFrame.sort"},
            ],
        )
        # check to make sure that the original DF is unchanged
        compare_api_calls(
            res2._plan.api_calls,
            [
                {"name": "Session.create_dataframe[values]"},
                {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
                {"name": "DataFrame.select"},
                distinct_api_call,
            ],
        )
    finally:
        session.conf.set("use_simplified_query_generation", original)


@pytest.mark.parametrize("n", [None, 2, -1])
def test_first_api_calls(session, n):
    telemetry_tracker = TelemetryDataTracker(session)

    df = session.create_dataframe([[1, 2], [4, 5]]).to_df("a", "b")

    first_partial = partial(df.sort("A").first, n)
    data, type_, _ = telemetry_tracker.extract_telemetry_log_data(-1, first_partial)
    if n is not None and n < 0:
        expected_first_api_call = {"name": "DataFrame.first"}
    else:
        expected_first_api_call = {
            "name": "DataFrame.first",
            "subcalls": [{"name": "DataFrame.limit"}],
        }

    compare_api_calls(
        data["api_calls"],
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
            {"name": "DataFrame.sort"},
            expected_first_api_call,
            {"name": "DataFrame._internal_collect_with_tag_no_telemetry"},
        ],
    )
    assert type_ == "snowpark_function_usage"


def test_count_api_calls(session):
    telemetry_tracker = TelemetryDataTracker(session)

    df = session.create_dataframe([[1, 2], [4, 5]], schema="a int, b int")
    count_partial = partial(df.count)
    data, type_, _ = telemetry_tracker.extract_telemetry_log_data(-1, count_partial)
    compare_api_calls(
        data["api_calls"],
        [
            {"name": "Session.create_dataframe[values]"},
            {
                "name": "DataFrame.count",
                "subcalls": [
                    {
                        "name": "DataFrame.agg",
                        "subcalls": [
                            {"name": "DataFrame.group_by"},
                            {"name": "RelationalGroupedDataFrame.agg"},
                        ],
                    }
                ],
            },
            {"name": "DataFrame._internal_collect_with_tag_no_telemetry"},
        ],
    )
    assert type_ == "snowpark_function_usage"


@pytest.mark.parametrize("use_simplified_query_generation", [True, False])
def test_random_split(session, use_simplified_query_generation):
    original = session.conf.get("use_simplified_query_generation")
    try:
        session.conf.set(
            "use_simplified_query_generation", use_simplified_query_generation
        )
        df = session.range(0, 50)
        df1, df2 = df.random_split([0.6, 0.4], seed=1234)
        # assert df1 and df2 have the same api_calls
        compare_api_calls(df1._plan.api_calls, df2._plan.api_calls)

        if use_simplified_query_generation:
            expected_api_calls = [
                {"name": "Session.range"},
                {
                    "name": "DataFrame.random_split[hash]",
                    "subcalls": [
                        {
                            "name": "DataFrame.with_column",
                            "subcalls": [
                                {
                                    "name": "DataFrame.with_columns",
                                    "subcalls": [{"name": "DataFrame.select"}],
                                }
                            ],
                        },
                        {"name": "DataFrame.filter"},
                        {"name": "DataFrame.drop[exclude]"},
                    ],
                },
            ]
        else:
            expected_api_calls = [
                {"name": "Session.range"},
                {
                    "name": "DataFrame.random_split[cache_result]",
                    "subcalls": [
                        {"name": "Table.__init__"},
                        {"name": "DataFrame.filter"},
                        {
                            "name": "DataFrame.drop[select]",
                            "subcalls": [{"name": "DataFrame.select"}],
                        },
                    ],
                },
            ]

        compare_api_calls(df1._plan.api_calls, expected_api_calls)
    finally:
        session.conf.set("use_simplified_query_generation", original)


def test_with_column_variations_api_calls(session):
    df = session.create_dataframe([Row(1, 2, 3)]).to_df(["a", "b", "c"])
    compare_api_calls(
        df._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        ],
    )

    # Test with_columns
    replaced = df.with_columns(["b", "d"], [lit(5), lit(6)])
    compare_api_calls(
        replaced._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
            {
                "name": "DataFrame.with_columns",
                "subcalls": [{"name": "DataFrame.select"}],
            },
        ],
    )
    # check to make sure that the original DF is unchanged
    compare_api_calls(
        df._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        ],
    )

    # Test with_column
    replaced = df.with_column("b", lit(7))
    compare_api_calls(
        replaced._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
            {
                "name": "DataFrame.with_column",
                "subcalls": [
                    {
                        "name": "DataFrame.with_columns",
                        "subcalls": [{"name": "DataFrame.select"}],
                    }
                ],
            },
        ],
    )
    # check to make sure that the original DF is unchanged
    compare_api_calls(
        df._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        ],
    )

    # Test with_column_renamed
    replaced = df.with_column_renamed(col("b"), "e")
    compare_api_calls(
        replaced._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
            {
                "name": "DataFrame.with_column_renamed",
                "subcalls": [{"name": "DataFrame.select"}],
            },
        ],
    )
    # check to make sure that the original DF is unchanged
    compare_api_calls(
        df._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        ],
    )

    # Test with rename, compatibility mode
    replaced = df.rename(col("b"), "e")
    compare_api_calls(
        replaced._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
            {
                "name": "DataFrame.with_column_renamed",
                "subcalls": [{"name": "DataFrame.select"}],
            },
            {"name": "DataFrame.rename"},
        ],
    )
    # check to make sure that the original DF is unchanged
    compare_api_calls(
        df._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        ],
    )

    # Test with rename, multiple columns
    replaced = df.rename({col("b"): "e", "c": "d"})
    compare_api_calls(
        replaced._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
            {"name": "DataFrame.rename"},
        ],
    )
    # check to make sure that the original DF is unchanged
    compare_api_calls(
        df._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        ],
    )


@pytest.mark.skipif(
    not is_pandas_available, reason="pandas is required to register vectorized UDFs"
)
def test_execute_queries_api_calls(session, sql_simplifier_enabled):
    df = session.range(1, 10, 2).filter(col("id") <= 4).filter(col("id") >= 0)
    compare_api_calls(
        df._plan.api_calls,
        [
            {"name": "Session.range"},
            {"name": "DataFrame.filter"},
            {"name": "DataFrame.filter"},
        ],
    )

    df.collect()
    # API calls don't change after query is executed
    thread_ident = threading.get_ident()

    compare_api_calls(
        df._plan.api_calls,
        [
            {
                "name": "Session.range",
                "sql_simplifier_enabled": session.sql_simplifier_enabled,
                "thread_ident": thread_ident,
            },
            {"name": "DataFrame.filter"},
            {"name": "DataFrame.filter"},
        ],
    )

    df._internal_collect_with_tag()
    # API calls don't change after query is executed
    compare_api_calls(
        df._plan.api_calls,
        [
            {
                "name": "Session.range",
                "sql_simplifier_enabled": session.sql_simplifier_enabled,
                "thread_ident": thread_ident,
            },
            {"name": "DataFrame.filter"},
            {"name": "DataFrame.filter"},
        ],
    )

    df.to_local_iterator()
    # API calls don't change after query is executed
    compare_api_calls(
        df._plan.api_calls,
        [
            {
                "name": "Session.range",
                "sql_simplifier_enabled": session.sql_simplifier_enabled,
                "thread_ident": thread_ident,
            },
            {"name": "DataFrame.filter"},
            {"name": "DataFrame.filter"},
        ],
    )

    df.to_pandas()
    # API calls don't change after query is executed
    compare_api_calls(
        df._plan.api_calls,
        [
            {
                "name": "Session.range",
                "sql_simplifier_enabled": session.sql_simplifier_enabled,
                "thread_ident": thread_ident,
            },
            {"name": "DataFrame.filter"},
            {"name": "DataFrame.filter"},
        ],
    )

    df.to_pandas_batches()
    # API calls don't change after query is executed
    compare_api_calls(
        df._plan.api_calls,
        [
            {
                "name": "Session.range",
                "sql_simplifier_enabled": session.sql_simplifier_enabled,
                "thread_ident": thread_ident,
            },
            {"name": "DataFrame.filter"},
            {"name": "DataFrame.filter"},
        ],
    )


def test_relational_dataframe_api_calls(session):
    df = TestData.test_data2(session)
    compare_api_calls(
        df._plan.api_calls, [{"name": "Session.create_dataframe[values]"}]
    )

    agg = df.group_by("a").agg([(col("*"), "count")])
    compare_api_calls(
        agg._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.group_by"},
            {"name": "RelationalGroupedDataFrame.agg"},
        ],
    )
    # check to make sure that the original DF is unchanged
    compare_api_calls(
        df._plan.api_calls, [{"name": "Session.create_dataframe[values]"}]
    )

    df1 = session.create_dataframe(
        [("a", 1, 0, "b"), ("b", 2, 4, "c"), ("a", 2, 3, "d")]
    ).to_df(["key", "value1", "value2", "rest"])
    compare_api_calls(
        df1._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        ],
    )

    res = df1.group_by("key").min(col("value2"))
    compare_api_calls(
        res._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
            {"name": "DataFrame.group_by"},
            {"name": "RelationalGroupedDataFrame.min"},
        ],
    )
    # check to make sure that the original DF is unchanged
    compare_api_calls(
        df1._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        ],
    )

    res = df1.group_by("key").max("value1", "value2")
    compare_api_calls(
        res._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
            {"name": "DataFrame.group_by"},
            {"name": "RelationalGroupedDataFrame.max"},
        ],
    )
    # check to make sure that the original DF is unchanged
    compare_api_calls(
        df1._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        ],
    )

    res = df1.group_by("key").sum("value1")
    compare_api_calls(
        res._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
            {"name": "DataFrame.group_by"},
            {"name": "RelationalGroupedDataFrame.sum"},
        ],
    )
    # check to make sure that the original DF is unchanged
    compare_api_calls(
        df1._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        ],
    )

    res = df1.group_by("key").avg("value1")
    compare_api_calls(
        res._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
            {"name": "DataFrame.group_by"},
            {"name": "RelationalGroupedDataFrame.avg"},
        ],
    )
    # check to make sure that the original DF is unchanged
    compare_api_calls(
        df1._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        ],
    )

    res = df1.group_by("key").median("value1")
    compare_api_calls(
        res._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
            {"name": "DataFrame.group_by"},
            {"name": "RelationalGroupedDataFrame.median"},
        ],
    )
    # check to make sure that the original DF is unchanged
    compare_api_calls(
        df1._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        ],
    )

    res = df1.group_by("key").count()
    compare_api_calls(
        res._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
            {"name": "DataFrame.group_by"},
            {"name": "RelationalGroupedDataFrame.count"},
        ],
    )
    # check to make sure that the original DF is unchanged
    compare_api_calls(
        df1._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        ],
    )


@pytest.mark.parametrize("use_simplified_query_generation", [True, False])
def test_dataframe_stat_functions_api_calls(session, use_simplified_query_generation):
    session.conf.set("use_simplified_query_generation", use_simplified_query_generation)
    df = TestData.monthly_sales(session)
    compare_api_calls(
        df._plan.api_calls, [{"name": "Session.create_dataframe[values]"}]
    )

    sample_by = df.stat.sample_by(col("empid"), {1: 0.0, 2: 1.0})
    if use_simplified_query_generation:
        sample_by_api_calls = {"name": "DataFrameStatFunctions.sample_by[percent_rank]"}
    else:
        sample_by_api_calls = {
            "name": "DataFrameStatFunctions.sample_by[union_all]",
            "subcalls": [
                {"name": "Session.create_dataframe[values]"},
                {"name": "DataFrame.filter"},
                {"name": "DataFrame.sample"},
                {"name": "Session.create_dataframe[values]"},
                {"name": "DataFrame.filter"},
                {"name": "DataFrame.sample"},
                {"name": "DataFrame.union_all"},
            ],
        }

    compare_api_calls(
        sample_by._plan.api_calls,
        [
            {"name": "Session.create_dataframe[values]"},
            sample_by_api_calls,
        ],
    )
    # check to make sure that the original DF is unchanged
    compare_api_calls(
        df._plan.api_calls, [{"name": "Session.create_dataframe[values]"}]
    )

    crosstab = df.stat.crosstab("empid", "month")
    # uuid here is generated by an intermediate dataframe in crosstab implementation
    # therefore we can't predict it. We check that the uuid for crosstab is same as
    # that for df.
    thread_ident = threading.get_ident()
    compare_api_calls(
        crosstab._plan.api_calls,
        [
            {
                "name": "Session.create_dataframe[values]",
                "sql_simplifier_enabled": session.sql_simplifier_enabled,
                "thread_ident": thread_ident,
            },
            {
                "name": "DataFrameStatFunctions.crosstab",
                "subcalls": [
                    {"name": "DataFrame.select"},
                    {"name": "DataFrame.pivot"},
                    {"name": "RelationalGroupedDataFrame.agg"},
                ],
            },
        ],
    )

    # check to make sure that the original DF is unchanged
    compare_api_calls(
        df._plan.api_calls,
        [
            {
                "name": "Session.create_dataframe[values]",
                "sql_simplifier_enabled": session.sql_simplifier_enabled,
                "thread_ident": thread_ident,
            }
        ],
    )


def test_dataframe_na_functions_api_calls(session, local_testing_mode):
    df1 = TestData.double3(session, local_testing_mode)
    compare_api_calls(df1._plan.api_calls, [{"name": "Session.sql"}])

    drop = df1.na.drop(thresh=1, subset=["a"])
    compare_api_calls(
        drop._plan.api_calls,
        [
            {"name": "Session.sql"},
            {
                "name": "DataFrameNaFunctions.drop",
                "subcalls": [{"name": "DataFrame.filter"}],
            },
        ],
    )
    # check to make sure that the original DF is unchanged
    compare_api_calls(df1._plan.api_calls, [{"name": "Session.sql"}])

    df2 = TestData.null_data3(session, local_testing_mode)
    compare_api_calls(df2._plan.api_calls, [{"name": "Session.sql"}])

    fill = df2.na.fill({"flo": 12.3, "int": 11, "boo": False, "str": "f"})
    compare_api_calls(
        fill._plan.api_calls,
        [
            {"name": "Session.sql"},
            {
                "name": "DataFrameNaFunctions.fill",
                "subcalls": [{"name": "DataFrame.select"}],
            },
        ],
    )
    # check to make sure that the original DF is unchanged
    compare_api_calls(df2._plan.api_calls, [{"name": "Session.sql"}])

    replace = df2.na.replace({2: 300, 1: 200}, subset=["flo"])
    compare_api_calls(
        replace._plan.api_calls,
        [
            {"name": "Session.sql"},
            {
                "name": "DataFrameNaFunctions.replace",
                "subcalls": [{"name": "DataFrame.select"}],
            },
        ],
    )
    # check to make sure that the original DF is unchanged
    compare_api_calls(df2._plan.api_calls, [{"name": "Session.sql"}])


@pytest.mark.skipif(
    not is_pandas_available, reason="pandas is required to register vectorized UDFs"
)
@pytest.mark.udf
def test_udf_call_and_invoke(session, resources_path):
    telemetry_tracker = TelemetryDataTracker(session)
    df = session.create_dataframe([[1, 2]], schema=["a", "b"])

    # udf register
    def minus_one(x):
        return x - 1

    minus_one_name = f"minus_one_{generate_random_alphanumeric()}"
    minus_one_udf_partial = partial(
        udf,
        minus_one,
        return_type=IntegerType(),
        input_types=[IntegerType()],
        name=minus_one_name,
        replace=True,
    )

    data, type_, minus_one_udf = telemetry_tracker.extract_telemetry_log_data(
        -1, minus_one_udf_partial
    )
    assert data == {"func_name": "UDFRegistration.register", "category": "create"}
    assert type_ == "snowpark_function_usage"

    select_partial = partial(df.select, df.a, minus_one_udf(df.b))
    data, type_, _ = telemetry_tracker.extract_telemetry_log_data(-1, select_partial)
    assert data == {"func_name": "UserDefinedFunction.__call__", "category": "usage"}
    assert type_ == "snowpark_function_usage"

    # udf register from file
    test_files = TestFiles(resources_path)
    mod5_udf_partial = partial(
        session.udf.register_from_file,
        test_files.test_udf_py_file,
        "mod5",
        return_type=IntegerType(),
        input_types=[IntegerType()],
        replace=True,
    )

    data, type_, mod5_udf = telemetry_tracker.extract_telemetry_log_data(
        -1, mod5_udf_partial
    )
    assert data == {
        "func_name": "UDFRegistration.register_from_file",
        "category": "create",
    }
    assert type_ == "snowpark_function_usage"

    select_partial = partial(df.select, mod5_udf(df.a))
    data, type_, _ = telemetry_tracker.extract_telemetry_log_data(-1, select_partial)
    assert data == {"func_name": "UserDefinedFunction.__call__", "category": "usage"}
    assert type_ == "snowpark_function_usage"

    # pandas udf register
    def add_one_df_pandas_udf(df):
        return df[0] + df[1] + 1

    pandas_udf_partial = partial(
        pandas_udf,
        add_one_df_pandas_udf,
        return_type=PandasSeriesType(IntegerType()),
        input_types=[PandasDataFrameType([IntegerType(), IntegerType()])],
        replace=True,
    )
    data, type_, add_one_df_pandas_udf = telemetry_tracker.extract_telemetry_log_data(
        -1, pandas_udf_partial
    )
    assert data == {
        "func_name": "UDFRegistration.register[pandas_udf]",
        "category": "create",
    }
    assert type_ == "snowpark_function_usage"

    select_partial = partial(df.select, add_one_df_pandas_udf("a", "b"))
    data, type_, _ = telemetry_tracker.extract_telemetry_log_data(-1, select_partial)
    assert data == {"func_name": "UserDefinedFunction.__call__", "category": "usage"}
    assert type_ == "snowpark_function_usage"

    # call using call_udf
    select_partial = partial(df.select, call_udf(minus_one_name, df.a))
    data, type_, _ = telemetry_tracker.extract_telemetry_log_data(-1, select_partial)
    assert data == {"func_name": "functions.call_udf", "category": "usage"}
    assert type_ == "snowpark_function_usage"


@pytest.mark.udf
def test_sproc_call_and_invoke(session, resources_path):
    telemetry_tracker = TelemetryDataTracker(session)

    # sproc register
    def add_one(session_, x):
        return session_.sql(f"select {x} + 1").collect()[0][0]

    add_one_partial = partial(
        sproc,
        add_one,
        return_type=IntegerType(),
        input_types=[IntegerType()],
        packages=["snowflake-snowpark-python"],
        replace=True,
    )

    data, type_, add_one_sp = telemetry_tracker.extract_telemetry_log_data(
        -1, add_one_partial
    )
    assert data == {
        "func_name": "StoredProcedureRegistration.register",
        "category": "create",
    }
    assert type_ == "snowpark_function_usage"

    invoke_partial = partial(add_one_sp, 7)
    # the 3 messages after sproc_invoke are client_time_consume_first_result, client_time_consume_last_result, and action_collect
    expected_data = {"func_name": "StoredProcedure.__call__", "category": "usage"}
    assert telemetry_tracker.find_message_in_log_data(6, invoke_partial, expected_data)

    # sproc register from file
    test_files = TestFiles(resources_path)
    mod5_sp_partial = partial(
        session.sproc.register_from_file,
        test_files.test_sp_py_file,
        "mod5",
        return_type=IntegerType(),
        input_types=[IntegerType()],
        packages=["snowflake-snowpark-python"],
        replace=True,
    )
    data, type_, mod5_sp = telemetry_tracker.extract_telemetry_log_data(
        -1, mod5_sp_partial
    )
    assert data == {
        "func_name": "StoredProcedureRegistration.register_from_file",
        "category": "create",
    }
    assert type_ == "snowpark_function_usage"

    invoke_partial = partial(mod5_sp, 3)
    expected_data = {"func_name": "StoredProcedure.__call__", "category": "usage"}
    assert telemetry_tracker.find_message_in_log_data(6, invoke_partial, expected_data)


@pytest.mark.udf
def test_udtf_call_and_invoke(session, resources_path):
    telemetry_tracker = TelemetryDataTracker(session)
    df = session.create_dataframe([[1, 2]], schema=["a", "b"])

    # udtf register
    class SumUDTF:
        def process(self, a, b) -> Iterable[Tuple[int]]:
            yield (a + b,)

    sum_udtf_partial = partial(
        udtf,
        SumUDTF,
        output_schema=["sum"],
        input_types=[IntegerType(), IntegerType()],
        replace=True,
    )

    expected_data = {"func_name": "UDTFRegistration.register", "category": "create"}
    assert telemetry_tracker.find_message_in_log_data(
        3, sum_udtf_partial, expected_data
    ), f"could not find expected message: {expected_data} in the last 3 message log entries"

    sum_udtf = sum_udtf_partial()
    select_partial = partial(df.select, sum_udtf(df.a, df.b))
    expected_data = {
        "func_name": "UserDefinedTableFunction.__call__",
        "category": "usage",
    }
    assert telemetry_tracker.find_message_in_log_data(
        3, select_partial, expected_data
    ), f"could not find expected message: {expected_data} in the last 3 message log entries"

    # udtf register from file
    test_files = TestFiles(resources_path)
    schema = ["int_", "float_", "bool_", "decimal_", "str_", "bytes_", "bytearray_"]
    my_udtf_partial = partial(
        session.udtf.register_from_file,
        test_files.test_udtf_py_file,
        "MyUDTFWithTypeHints",
        output_schema=schema,
        replace=True,
    )

    expected_data = {
        "func_name": "UDTFRegistration.register_from_file",
        "category": "create",
    }
    assert telemetry_tracker.find_message_in_log_data(
        3, my_udtf_partial, expected_data
    ), f"could not find expected message: {expected_data} in the last 3 message log entries"
    my_udtf = my_udtf_partial()

    invoke_partial = partial(
        session.table_function,
        my_udtf(
            lit(1),
            lit(2.2),
            lit(True),
            lit(decimal.Decimal("3.33")),
            lit("python"),
            lit(b"bytes"),
            lit(bytearray("bytearray", "utf-8")),
        ),
    )

    expected_data = {
        "func_name": "UserDefinedTableFunction.__call__",
        "category": "usage",
    }
    assert telemetry_tracker.find_message_in_log_data(
        3, invoke_partial, expected_data
    ), f"could not find expected message: {expected_data} in the last 3 message log entries"


@pytest.mark.skip(
    "This is broken because server doesn't have parameter PYTHON_SNOWPARK_USE_SQL_SIMPLIFIER yet. Enable it after server releases it."
)
def test_sql_simplifier_enabled(session):
    telemetry_tracker = TelemetryDataTracker(session)
    original_value = session.sql_simplifier_enabled
    try:

        def set_sql_simplifier_enabled():
            session.sql_simplifier_enabled = True

        data, _, _ = telemetry_tracker.extract_telemetry_log_data(
            -1, set_sql_simplifier_enabled
        )
        assert data == {
            TelemetryField.SESSION_ID.value: session._session_id,
            TelemetryField.SQL_SIMPLIFIER_ENABLED.value: True,
        }
    finally:
        session.sql_simplifier_enabled = original_value


def test_post_compilation_stage_telemetry(session):
    client = session._conn._telemetry_client
    uuid_str = str(uuid.uuid4())

    def send_telemetry():
        summary_value = {
            "cte_optimization_enabled": True,
            "large_query_breakdown_enabled": True,
            "complexity_score_bounds": (300, 600),
            "time_taken_for_compilation": 0.136,
            "time_taken_for_deep_copy_plan": 0.074,
            "time_taken_for_cte_optimization": 0.01,
            "time_taken_for_large_query_breakdown": 0.062,
            "complexity_score_before_compilation": 1148,
            "complexity_score_after_cte_optimization": [1148],
            "complexity_score_after_large_query_breakdown": [514, 636],
            "cte_node_created": 2,
        }
        client.send_query_compilation_summary_telemetry(
            session_id=session.session_id,
            plan_uuid=uuid_str,
            compilation_stage_summary=summary_value,
        )

    telemetry_tracker = TelemetryDataTracker(session)

    expected_data = {
        "session_id": session.session_id,
        "plan_uuid": uuid_str,
        "category": "query_compilation_stage_statistics",
        "cte_optimization_enabled": True,
        "large_query_breakdown_enabled": True,
        "complexity_score_bounds": (300, 600),
        "time_taken_for_compilation": 0.136,
        "time_taken_for_deep_copy_plan": 0.074,
        "time_taken_for_cte_optimization": 0.01,
        "time_taken_for_large_query_breakdown": 0.062,
        "complexity_score_before_compilation": 1148,
        "complexity_score_after_cte_optimization": [1148],
        "complexity_score_after_large_query_breakdown": [514, 636],
        "cte_node_created": 2,
    }

    data, type_, _ = telemetry_tracker.extract_telemetry_log_data(-1, send_telemetry)
    assert data == expected_data
    assert type_ == "snowpark_compilation_stage_statistics"


def test_temp_table_cleanup(session):
    client = session._conn._telemetry_client

    def send_telemetry():
        client.send_temp_table_cleanup_telemetry(
            session.session_id,
            temp_table_cleaner_enabled=True,
            num_temp_tables_cleaned=2,
            num_temp_tables_created=5,
        )

    telemetry_tracker = TelemetryDataTracker(session)

    expected_data = {
        "session_id": session.session_id,
        "temp_table_cleaner_enabled": True,
        "num_temp_tables_cleaned": 2,
        "num_temp_tables_created": 5,
    }

    data, type_, _ = telemetry_tracker.extract_telemetry_log_data(-1, send_telemetry)
    assert data == expected_data
    assert type_ == "snowpark_temp_table_cleanup"


def test_temp_table_cleanup_exception(session):
    client = session._conn._telemetry_client

    def send_telemetry():
        client.send_temp_table_cleanup_abnormal_exception_telemetry(
            session.session_id,
            table_name="table_name_placeholder",
            exception_message="exception_message_placeholder",
        )

    telemetry_tracker = TelemetryDataTracker(session)

    expected_data = {
        "session_id": session.session_id,
        "temp_table_cleanup_abnormal_exception_table_name": "table_name_placeholder",
        "temp_table_cleanup_abnormal_exception_message": "exception_message_placeholder",
    }

    data, type_, _ = telemetry_tracker.extract_telemetry_log_data(-1, send_telemetry)
    assert data == expected_data
    assert type_ == "snowpark_temp_table_cleanup"


def test_cursor_created_telemetry(session):
    client = session._conn._telemetry_client
    telemetry_tracker = TelemetryDataTracker(session)

    def send_telemetry():
        client.send_cursor_created_telemetry(session_id=session.session_id, thread_id=1)

    expected_data = {
        "session_id": session.session_id,
        "thread_ident": 1,
    }
    data, type_, _ = telemetry_tracker.extract_telemetry_log_data(-1, send_telemetry)
    assert data == expected_data
    assert type_ == "snowpark_cursor_created"


def test_describe_query_details(session):
    client = session._conn._telemetry_client

    def send_telemetry():
        client.send_describe_query_details(
            session.session_id,
            sql_text="select 1 as a, 2 as b",
            e2e_time=0.01,
            stack_trace=["line1", "line2"],
        )

    telemetry_tracker = TelemetryDataTracker(session)

    expected_data = {
        "session_id": session.session_id,
        "sql_text": "select 1 as a, 2 as b",
        "e2e_time": 0.01,
        "stack_trace": ["line1", "line2"],
    }

    data, type_, _ = telemetry_tracker.extract_telemetry_log_data(-1, send_telemetry)
    assert data == expected_data
    assert type_ == "snowpark_describe_query_details"


def test_plan_metrics_telemetry(session):
    client = session._conn._telemetry_client
    telemetry_data = {
        "plan_uuid": "plan_uuid_placeholder",
        "category": "snowflake_plan_metrics",
        "query_plan_height": 10,
        "query_plan_num_duplicate_nodes": 5,
        "query_plan_num_selects_with_complexity_merged": 3,
        "query_plan_duplicated_node_complexity_distribution": [1, 2, 3],
        "query_plan_complexity": {
            "filter": 1,
            "low_impact": 2,
            "function": 3,
            "column": 4,
            "literal": 5,
            "window": 6,
            "order_by": 7,
        },
    }

    def send_telemetry():
        client.send_plan_metrics_telemetry(session.session_id, data=telemetry_data)

    telemetry_tracker = TelemetryDataTracker(session)

    expected_data = {"session_id": session.session_id, **telemetry_data}

    data, type_, _ = telemetry_tracker.extract_telemetry_log_data(-1, send_telemetry)
    assert data == expected_data
    assert type_ == "snowpark_compilation_stage_statistics"


@pytest.mark.parametrize("enabled", [True, False])
def test_snowflake_plan_telemetry_sent_at_critical_path(session, enabled):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df = df.filter(df.a > 0).union(df.filter(df.b > 0))
    original_collect_telemetry_at_critical_path = (
        session._collect_snowflake_plan_telemetry_at_critical_path
    )
    try:
        plan_state = df._plan.plan_state
        query_plan_complexity = {
            key.value: value
            for key, value in df._plan.cumulative_node_complexity.items()
        }
        expected_data = {
            "session_id": session.session_id,
            "data": {
                "plan_uuid": df._plan._uuid,
                "query_plan_height": plan_state[PlanState.PLAN_HEIGHT],
                "query_plan_num_selects_with_complexity_merged": plan_state[
                    PlanState.NUM_SELECTS_WITH_COMPLEXITY_MERGED
                ],
                "query_plan_num_duplicate_nodes": plan_state[PlanState.NUM_CTE_NODES],
                "query_plan_duplicated_node_complexity_distribution": plan_state[
                    PlanState.DUPLICATED_NODE_COMPLEXITY_DISTRIBUTION
                ],
                "query_plan_complexity": query_plan_complexity,
                "complexity_score_before_compilation": 25,
            },
        }
        session._collect_snowflake_plan_telemetry_at_critical_path = enabled
        with patch.object(
            session._conn._telemetry_client, "send_plan_metrics_telemetry"
        ) as patch_send:
            df._internal_collect_with_tag_no_telemetry()

        if enabled:
            patch_send.assert_called_once()
            _, kwargs = patch_send.call_args
            assert kwargs == expected_data
        else:
            patch_send.assert_not_called()

        with patch.object(
            session._conn._telemetry_client, "send_plan_metrics_telemetry"
        ) as patch_send:
            df.collect()

        patch_send.assert_called_once()
        _, kwargs = patch_send.call_args
        assert kwargs == expected_data
    finally:
        session._collect_snowflake_plan_telemetry_at_critical_path = (
            original_collect_telemetry_at_critical_path
        )
