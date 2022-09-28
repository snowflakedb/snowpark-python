#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import decimal
from typing import Iterable, Tuple

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark._internal.telemetry import TelemetryField
from snowflake.snowpark.functions import (
    col,
    lit,
    max as max_,
    mean,
    pandas_udf,
    sproc,
    udf,
    udtf,
)
from snowflake.snowpark.types import IntegerType, PandasDataFrameType, PandasSeriesType
from tests.utils import TestData, TestFiles


def test_basic_api_calls(session):
    df = session.range(1, 10, 2)

    df_filter = df.filter(col("id") > 4)
    assert df_filter._plan.api_calls == [
        {"name": "Session.range"},
        {"name": "DataFrame.filter"},
    ]
    # Make sure API call does not add to the api_calls list of the original dataframe
    assert df._plan.api_calls == [{"name": "Session.range"}]
    # Repeat API call to make sure the list stays correct even with multiple calls to the same API
    df_filter = df.filter(col("id") > 4)
    assert df_filter._plan.api_calls == [
        {"name": "Session.range"},
        {"name": "DataFrame.filter"},
    ]

    # Repeat API call to make sure the list stays correct even with multiple calls to the same API
    df_select = df_filter.select("id")
    assert df_select._plan.api_calls == [
        {"name": "Session.range"},
        {"name": "DataFrame.filter"},
        {"name": "DataFrame.select"},
    ]
    df_select = df_filter.select("id")
    assert df_select._plan.api_calls == [
        {"name": "Session.range"},
        {"name": "DataFrame.filter"},
        {"name": "DataFrame.select"},
    ]


def test_describe_api_calls(session):
    df = TestData.test_data2(session)
    assert df._plan.api_calls == [{"name": "Session.create_dataframe[values]"}]

    desc = df.describe("a", "b")
    assert desc._plan.api_calls == [
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
                {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
                {"name": "DataFrame.select"},
                {"name": "Session.create_dataframe[values]"},
                {
                    "name": "DataFrame.agg",
                    "subcalls": [
                        {"name": "DataFrame.group_by"},
                        {"name": "RelationalGroupedDataFrame.agg"},
                    ],
                },
                {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
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
                {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
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
                {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
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
                {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
                {"name": "DataFrame.select"},
                {"name": "DataFrame.union"},
            ],
        },
    ]
    # Original dataframe still has the same API calls
    assert df._plan.api_calls == [{"name": "Session.create_dataframe[values]"}]
    # Repeat API calls results in same output
    desc = df.describe("a", "b")
    assert desc._plan.api_calls == [
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
                {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
                {"name": "DataFrame.select"},
                {"name": "Session.create_dataframe[values]"},
                {
                    "name": "DataFrame.agg",
                    "subcalls": [
                        {"name": "DataFrame.group_by"},
                        {"name": "RelationalGroupedDataFrame.agg"},
                    ],
                },
                {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
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
                {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
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
                {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
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
                {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
                {"name": "DataFrame.select"},
                {"name": "DataFrame.union"},
            ],
        },
    ]

    empty_df = TestData.timestamp1(session).describe()
    assert empty_df._plan.api_calls == [
        {"name": "Session.sql"},
        {
            "name": "DataFrame.describe",
            "subcalls": [{"name": "Session.create_dataframe[values]"}],
        },
    ]


def test_drop_duplicates_api_calls(session):
    df = session.create_dataframe(
        [[1, 1, 1, 1], [1, 1, 1, 2], [1, 1, 2, 3], [1, 2, 3, 4], [1, 2, 3, 4]],
        schema=["a", "b", "c", "d"],
    )

    dd_df = df.drop_duplicates()
    assert dd_df._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {
            "name": "DataFrame.drop_duplicates",
            "subcalls": [
                {
                    "name": "DataFrame.distinct",
                    "subcalls": [
                        {"name": "DataFrame.group_by"},
                        {"name": "RelationalGroupedDataFrame.agg"},
                    ],
                }
            ],
        },
    ]
    # check that original dataframe has the same API calls
    assert df._plan.api_calls == [{"name": "Session.create_dataframe[values]"}]

    subset_df = df.drop_duplicates(["a", "b"])
    assert subset_df._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {
            "name": "DataFrame.drop_duplicates",
            "subcalls": [
                {"name": "DataFrame.select"},
                {"name": "DataFrame.filter"},
                {"name": "DataFrame.select"},
            ],
        },
    ]
    # check that original dataframe has the same API calls
    assert df._plan.api_calls == [{"name": "Session.create_dataframe[values]"}]


def test_drop_api_calls(session):
    df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])

    drop_id = df.drop("id")
    assert drop_id._plan.api_calls == [
        {"name": "Session.range"},
        {"name": "DataFrame.select"},
        {"name": "DataFrame.drop", "subcalls": [{"name": "DataFrame.select"}]},
    ]

    # Raise exception and make sure the new API call isn't added to the list
    with pytest.raises(Exception):
        drop_id.drop("id_prime")
    assert drop_id._plan.api_calls == [
        {"name": "Session.range"},
        {"name": "DataFrame.select"},
        {"name": "DataFrame.drop", "subcalls": [{"name": "DataFrame.select"}]},
    ]

    df2 = (
        session.range(3, 8)
        .select(["id", col("id").alias("id_prime")])
        .select(["id", col("id_prime").alias("id_prime_2")])
        .select(["id", col("id_prime_2").alias("id_prime_3")])
        .select(["id", col("id_prime_3").alias("id_prime_4")])
        .drop("id_prime_4")
    )
    assert df2._plan.api_calls == [
        {"name": "Session.range"},
        {"name": "DataFrame.select"},
        {"name": "DataFrame.select"},
        {"name": "DataFrame.select"},
        {"name": "DataFrame.select"},
        {"name": "DataFrame.drop", "subcalls": [{"name": "DataFrame.select"}]},
    ]


def test_to_df_api_calls(session):
    df = session.range(3, 8).select([col("id"), col("id").alias("id_prime")])
    assert df._plan.api_calls == [
        {"name": "Session.range"},
        {"name": "DataFrame.select"},
    ]

    # Raise exception and make sure api call list doesn't change
    with pytest.raises(Exception):
        df.to_df(["new_name"])
    assert df._plan.api_calls == [
        {"name": "Session.range"},
        {"name": "DataFrame.select"},
    ]

    to_df = df.to_df(["rename1", "rename2"])
    assert to_df._plan.api_calls == [
        {"name": "Session.range"},
        {"name": "DataFrame.select"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
    ]


def test_select_expr_api_calls(session):
    df = session.create_dataframe([-1, 2, 3], schema=["a"])
    assert df._plan.api_calls == [{"name": "Session.create_dataframe[values]"}]

    select_expr = df.select_expr("abs(a)", "a + 2", "cast(a as string)")
    assert select_expr._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.select_expr", "subcalls": [{"name": "DataFrame.select"}]},
    ]
    # check to make sure that the original dataframe doesn't have the extra API call
    assert df._plan.api_calls == [{"name": "Session.create_dataframe[values]"}]


def test_agg_api_calls(session):
    df = session.create_dataframe([[1, 2], [4, 5]]).to_df("col1", "col2")
    assert df._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
    ]

    agg_df = df.agg([max_(col("col1")), mean(col("col2"))])
    assert agg_df._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        {
            "name": "DataFrame.agg",
            "subcalls": [
                {"name": "DataFrame.group_by"},
                {"name": "RelationalGroupedDataFrame.agg"},
            ],
        },
    ]
    # check to make sure that the original DF is unchanged
    assert df._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
    ]


def test_distinct_api_calls(session):
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
    assert df._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
    ]

    res = df.distinct()
    assert res._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        {
            "name": "DataFrame.distinct",
            "subcalls": [
                {"name": "DataFrame.group_by"},
                {"name": "RelationalGroupedDataFrame.agg"},
            ],
        },
    ]
    # check to make sure that the original DF is unchanged
    assert df._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
    ]

    res2 = df.select(col("id")).distinct()
    res2_with_sort = res2.sort(["id"])
    assert res2_with_sort._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        {"name": "DataFrame.select"},
        {
            "name": "DataFrame.distinct",
            "subcalls": [
                {"name": "DataFrame.group_by"},
                {"name": "RelationalGroupedDataFrame.agg"},
            ],
        },
        {"name": "DataFrame.sort"},
    ]
    # check to make sure that the original DF is unchanged
    assert res2._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        {"name": "DataFrame.select"},
        {
            "name": "DataFrame.distinct",
            "subcalls": [
                {"name": "DataFrame.group_by"},
                {"name": "RelationalGroupedDataFrame.agg"},
            ],
        },
    ]


def test_with_column_variations_api_calls(session):
    df = session.create_dataframe([Row(1, 2, 3)]).to_df(["a", "b", "c"])
    assert df._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
    ]

    # Test with_columns
    replaced = df.with_columns(["b", "d"], [lit(5), lit(6)])
    assert replaced._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        {"name": "DataFrame.with_columns", "subcalls": [{"name": "DataFrame.select"}]},
    ]
    # check to make sure that the original DF is unchanged
    assert df._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
    ]

    # Test with_column
    replaced = df.with_column("b", lit(7))
    assert replaced._plan.api_calls == [
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
    ]
    # check to make sure that the original DF is unchanged
    assert df._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
    ]

    # Test with_column_renamed
    replaced = df.with_column_renamed(col("b"), "e")
    assert replaced._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        {
            "name": "DataFrame.with_column_renamed",
            "subcalls": [{"name": "DataFrame.select"}],
        },
    ]
    # check to make sure that the original DF is unchanged
    assert df._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
    ]


def test_execute_queries_api_calls(session):
    df = session.range(1, 10, 2).filter(col("id") <= 4).filter(col("id") >= 0)
    assert df._plan.api_calls == [
        {"name": "Session.range"},
        {"name": "DataFrame.filter"},
        {"name": "DataFrame.filter"},
    ]

    df.collect()
    # API calls don't change after query is executed
    assert df._plan.api_calls == [
        {"name": "Session.range"},
        {"name": "DataFrame.filter"},
        {"name": "DataFrame.filter"},
    ]

    df._internal_collect_with_tag()
    # API calls don't change after query is executed
    assert df._plan.api_calls == [
        {"name": "Session.range"},
        {"name": "DataFrame.filter"},
        {"name": "DataFrame.filter"},
    ]

    df.to_local_iterator()
    # API calls don't change after query is executed
    assert df._plan.api_calls == [
        {"name": "Session.range"},
        {"name": "DataFrame.filter"},
        {"name": "DataFrame.filter"},
    ]

    df.to_pandas()
    # API calls don't change after query is executed
    assert df._plan.api_calls == [
        {"name": "Session.range"},
        {"name": "DataFrame.filter"},
        {"name": "DataFrame.filter"},
    ]

    df.to_pandas_batches()
    # API calls don't change after query is executed
    assert df._plan.api_calls == [
        {"name": "Session.range"},
        {"name": "DataFrame.filter"},
        {"name": "DataFrame.filter"},
    ]


def test_relational_dataframe_api_calls(session):
    df = TestData.test_data2(session)
    assert df._plan.api_calls == [{"name": "Session.create_dataframe[values]"}]

    agg = df.group_by("a").agg([(col("*"), "count")])
    assert agg._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.group_by"},
        {"name": "RelationalGroupedDataFrame.agg"},
    ]
    # check to make sure that the original DF is unchanged
    assert df._plan.api_calls == [{"name": "Session.create_dataframe[values]"}]

    df1 = session.create_dataframe(
        [("a", 1, 0, "b"), ("b", 2, 4, "c"), ("a", 2, 3, "d")]
    ).to_df(["key", "value1", "value2", "rest"])
    assert df1._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
    ]

    res = df1.group_by("key").min(col("value2"))
    assert res._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        {"name": "DataFrame.group_by"},
        {"name": "RelationalGroupedDataFrame.min"},
    ]
    # check to make sure that the original DF is unchanged
    assert df1._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
    ]

    res = df1.group_by("key").max("value1", "value2")
    assert res._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        {"name": "DataFrame.group_by"},
        {"name": "RelationalGroupedDataFrame.max"},
    ]
    # check to make sure that the original DF is unchanged
    assert df1._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
    ]

    res = df1.group_by("key").sum("value1")
    assert res._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        {"name": "DataFrame.group_by"},
        {"name": "RelationalGroupedDataFrame.sum"},
    ]
    # check to make sure that the original DF is unchanged
    assert df1._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
    ]

    res = df1.group_by("key").avg("value1")
    assert res._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        {"name": "DataFrame.group_by"},
        {"name": "RelationalGroupedDataFrame.avg"},
    ]
    # check to make sure that the original DF is unchanged
    assert df1._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
    ]

    res = df1.group_by("key").median("value1")
    assert res._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        {"name": "DataFrame.group_by"},
        {"name": "RelationalGroupedDataFrame.median"},
    ]
    # check to make sure that the original DF is unchanged
    assert df1._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
    ]

    res = df1.group_by("key").count()
    assert res._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
        {"name": "DataFrame.group_by"},
        {"name": "RelationalGroupedDataFrame.count"},
    ]
    # check to make sure that the original DF is unchanged
    assert df1._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
    ]


def test_dataframe_stat_functions_api_calls(session):
    df = TestData.monthly_sales(session)
    assert df._plan.api_calls == [{"name": "Session.create_dataframe[values]"}]

    sample_by = df.stat.sample_by(col("empid"), {1: 0.0, 2: 1.0})
    assert sample_by._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {
            "name": "DataFrameStatFunctions.sample_by",
            "subcalls": [
                {"name": "Session.create_dataframe[values]"},
                {"name": "DataFrame.filter"},
                {"name": "DataFrame.sample"},
                {"name": "Session.create_dataframe[values]"},
                {"name": "DataFrame.filter"},
                {"name": "DataFrame.sample"},
                {"name": "DataFrame.union_all"},
            ],
        },
    ]
    # check to make sure that the original DF is unchanged
    assert df._plan.api_calls == [{"name": "Session.create_dataframe[values]"}]

    crosstab = df.stat.crosstab("empid", "month")
    assert crosstab._plan.api_calls == [
        {"name": "Session.create_dataframe[values]"},
        {
            "name": "DataFrameStatFunctions.crosstab",
            "subcalls": [
                {"name": "DataFrame.select"},
                {"name": "DataFrame.pivot"},
                {"name": "RelationalGroupedDataFrame.agg"},
            ],
        },
    ]
    # check to make sure that the original DF is unchanged
    assert df._plan.api_calls == [{"name": "Session.create_dataframe[values]"}]


def test_dataframe_na_functions_api_calls(session):
    df1 = TestData.double3(session)
    assert df1._plan.api_calls == [{"name": "Session.sql"}]

    drop = df1.na.drop(thresh=1, subset=["a"])
    assert drop._plan.api_calls == [
        {"name": "Session.sql"},
        {
            "name": "DataFrameNaFunctions.drop",
            "subcalls": [{"name": "DataFrame.filter"}],
        },
    ]
    # check to make sure that the original DF is unchanged
    assert df1._plan.api_calls == [{"name": "Session.sql"}]

    df2 = TestData.null_data3(session)
    assert df2._plan.api_calls == [{"name": "Session.sql"}]

    fill = df2.na.fill({"flo": 12.3, "int": 11, "boo": False, "str": "f"})
    assert fill._plan.api_calls == [
        {"name": "Session.sql"},
        {
            "name": "DataFrameNaFunctions.fill",
            "subcalls": [{"name": "DataFrame.select"}],
        },
    ]
    # check to make sure that the original DF is unchanged
    assert df2._plan.api_calls == [{"name": "Session.sql"}]

    replace = df2.na.replace({2: 300, 1: 200}, subset=["flo"])
    assert replace._plan.api_calls == [
        {"name": "Session.sql"},
        {
            "name": "DataFrameNaFunctions.replace",
            "subcalls": [{"name": "DataFrame.select"}],
        },
    ]
    # check to make sure that the original DF is unchanged
    assert df2._plan.api_calls == [{"name": "Session.sql"}]


def test_udf_call_and_invoke(session, resources_path):
    telemetry_obj = session._conn._telemetry_client.telemetry
    df = session.create_dataframe([[1, 2]], schema=["a", "b"])

    # udf register
    def minus_one(x):
        return x - 1

    minus_one_udf = udf(
        minus_one, return_type=IntegerType(), input_types=[IntegerType()]
    )

    data = telemetry_obj._log_batch[-1].to_dict()["message"][
        TelemetryField.KEY_DATA.value
    ]
    assert data == {"func_name": "UDFRegistration.register", "category": "create"}

    df.select(df.a, minus_one_udf(df.b))
    data = telemetry_obj._log_batch[-1].to_dict()["message"][
        TelemetryField.KEY_DATA.value
    ]
    assert data == {"func_name": "UserDefinedFunction.__call__", "category": "usage"}

    # udf register from file
    test_files = TestFiles(resources_path)
    mod5_udf = session.udf.register_from_file(
        test_files.test_udf_py_file,
        "mod5",
        return_type=IntegerType(),
        input_types=[IntegerType()],
    )

    data = telemetry_obj._log_batch[-1].to_dict()["message"][
        TelemetryField.KEY_DATA.value
    ]
    assert data == {
        "func_name": "UDFRegistration.register_from_file",
        "category": "create",
    }

    df.select(mod5_udf(df.a))
    data = telemetry_obj._log_batch[-1].to_dict()["message"][
        TelemetryField.KEY_DATA.value
    ]
    assert data == {"func_name": "UserDefinedFunction.__call__", "category": "usage"}

    # pandas udf register
    @pandas_udf(
        return_type=PandasSeriesType(IntegerType()),
        input_types=[PandasDataFrameType([IntegerType(), IntegerType()])],
    )
    def add_one_df_pandas_udf(df):
        return df[0] + df[1] + 1

    data = telemetry_obj._log_batch[-1].to_dict()["message"][
        TelemetryField.KEY_DATA.value
    ]
    assert data == {
        "func_name": "UDFRegistration.register[pandas_udf]",
        "category": "create",
    }

    df.select(add_one_df_pandas_udf("a", "b"))
    data = telemetry_obj._log_batch[-1].to_dict()["message"][
        TelemetryField.KEY_DATA.value
    ]
    assert data == {"func_name": "UserDefinedFunction.__call__", "category": "usage"}


def test_sproc_call_and_invoke(session, resources_path):
    telemetry_obj = session._conn._telemetry_client.telemetry

    # sproc register
    def add_one(session_, x):
        return session_.sql(f"select {x} + 1").collect()[0][0]

    add_one_sp = sproc(
        add_one,
        return_type=IntegerType(),
        input_types=[IntegerType()],
        packages=["snowflake-snowpark-python"],
    )

    data = telemetry_obj._log_batch[-1].to_dict()["message"][
        TelemetryField.KEY_DATA.value
    ]
    assert data == {
        "func_name": "StoredProcedureRegistration.register",
        "category": "create",
    }

    add_one_sp(7)
    # the 3 messages after sproc_invoke are client_time_consume_first_result, client_time_consume_last_result, and action_collect
    print(telemetry_obj._log_batch)
    data = telemetry_obj._log_batch[-4].to_dict()["message"][
        TelemetryField.KEY_DATA.value
    ]
    assert data == {"func_name": "StoredProcedure.__call__", "category": "usage"}

    # sproc register from file
    test_files = TestFiles(resources_path)
    mod5_sp = session.sproc.register_from_file(
        test_files.test_sp_py_file,
        "mod5",
        return_type=IntegerType(),
        input_types=[IntegerType()],
        packages=["snowflake-snowpark-python"],
    )
    data = telemetry_obj._log_batch[-1].to_dict()["message"][
        TelemetryField.KEY_DATA.value
    ]
    assert data == {
        "func_name": "StoredProcedureRegistration.register_from_file",
        "category": "create",
    }

    mod5_sp(3)
    data = telemetry_obj._log_batch[-4].to_dict()["message"][
        TelemetryField.KEY_DATA.value
    ]
    assert data == {"func_name": "StoredProcedure.__call__", "category": "usage"}


def test_udtf_call_and_invoke(session, resources_path):
    telemetry_obj = session._conn._telemetry_client.telemetry
    df = session.create_dataframe([[1, 2]], schema=["a", "b"])

    # udtf register
    @udtf(output_schema=["sum"])
    class sum_udtf:
        def process(self, a: int, b: int) -> Iterable[Tuple[int]]:
            return (a + b,)

    data = telemetry_obj._log_batch[-1].to_dict()["message"][
        TelemetryField.KEY_DATA.value
    ]
    assert data == {"func_name": "UDTFRegistration.register", "category": "create"}

    df.select(sum_udtf(df.a, df.b))
    data = telemetry_obj._log_batch[-2].to_dict()["message"][
        TelemetryField.KEY_DATA.value
    ]
    assert data == {
        "func_name": "UserDefinedTableFunction.__call__",
        "category": "usage",
    }

    # udtf register from file
    test_files = TestFiles(resources_path)
    schema = ["int_", "float_", "bool_", "decimal_", "str_", "bytes_", "bytearray_"]
    my_udtf = session.udtf.register_from_file(
        test_files.test_udtf_py_file,
        "MyUDTFWithTypeHints",
        output_schema=schema,
    )

    data = telemetry_obj._log_batch[-1].to_dict()["message"][
        TelemetryField.KEY_DATA.value
    ]
    assert data == {
        "func_name": "UDTFRegistration.register_from_file",
        "category": "create",
    }

    df = session.table_function(
        my_udtf(
            lit(1),
            lit(2.2),
            lit(True),
            lit(decimal.Decimal("3.33")),
            lit("python"),
            lit(b"bytes"),
            lit(bytearray("bytearray", "utf-8")),
        )
    )

    data = telemetry_obj._log_batch[-1].to_dict()["message"][
        TelemetryField.KEY_DATA.value
    ]
    assert data == {
        "func_name": "UserDefinedTableFunction.__call__",
        "category": "usage",
    }
