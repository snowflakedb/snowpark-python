#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from decimal import Decimal
from math import sqrt
import re
from typing import NamedTuple

import pytest

from snowflake.snowpark import GroupingSets, Row
from snowflake.snowpark._internal.utils import (
    TempObjectType,
)
from snowflake.snowpark.column import Column
from snowflake.snowpark.exceptions import (
    SnowparkDataframeException,
    SnowparkSQLException,
)
from snowflake.snowpark.functions import (
    avg,
    col,
    count,
    count_distinct,
    kurtosis,
    listagg,
    lit,
    max,
    mean,
    median,
    min,
    skew,
    sql_expr,
    stddev,
    stddev_pop,
    stddev_samp,
    sum,
    sum_distinct,
    var_pop,
    var_samp,
    variance,
)
from tests.utils import IS_IN_STORED_PROC, TestData, Utils, multithreaded_run


def test_pivot(session):
    Utils.check_answer(
        TestData.monthly_sales(session)
        .pivot("month", ["JAN", "FEB", "MAR", "APR"])
        .agg(sum(col("amount")))
        .sort(col("empid")),
        [Row(1, 10400, 8000, 11000, 18000), Row(2, 39500, 90700, 12000, 5300)],
        sort=False,
    )


def test_pivot_snow_1869802_repro(session):
    df = session.create_dataframe(
        [[1, "A", 10], [1, "B", 0], [2, "A", 11], [2, "B", 12]]
    )
    Utils.check_answer(
        df.pivot(pivot_col="_2", values=["A", "B"]).function("min")("_3"),
        [Row(1, 10, 0), Row(2, 11, 12)],
        False,
    )


@pytest.mark.parametrize(
    "func,expected",
    [
        (
            avg,
            [
                Row(
                    1,
                    "A",
                    Decimal("10000.000000"),
                    None,
                    Decimal("5200.000000"),
                    Decimal("5000.000000"),
                ),
                Row(
                    1,
                    "B",
                    Decimal("8000.000000"),
                    Decimal("4000.000000"),
                    None,
                    Decimal("6000.000000"),
                ),
                Row(
                    2,
                    "A",
                    Decimal("2650.000000"),
                    Decimal("45350.000000"),
                    Decimal("4500.000000"),
                    None,
                ),
                Row(
                    2, "B", None, None, Decimal("35000.000000"), Decimal("6000.000000")
                ),
            ],
        ),
        (
            count,
            [
                Row(1, "A", 1, 0, 2, 1),
                Row(1, "B", 1, 2, 0, 1),
                Row(2, "A", 2, 2, 1, 0),
                Row(2, "B", 0, 0, 1, 2),
            ],
        ),
        (
            max,
            [
                Row(1, "A", 10000, None, 10000, 5000),
                Row(1, "B", 8000, 5000, None, 6000),
                Row(2, "A", 4500, 90500, 4500, None),
                Row(2, "B", None, None, 35000, 9500),
            ],
        ),
        (
            min,
            [
                Row(1, "A", 10000, None, 400, 5000),
                Row(1, "B", 8000, 3000, None, 6000),
                Row(2, "A", 800, 200, 4500, None),
                Row(2, "B", None, None, 35000, 2500),
            ],
        ),
        (
            sum,
            [
                Row(1, "A", 10000, None, 10400, 5000),
                Row(1, "B", 8000, 8000, None, 6000),
                Row(2, "A", 5300, 90700, 4500, None),
                Row(2, "B", None, None, 35000, 12000),
            ],
        ),
    ],
)
def test_pivot_agg_functions(session, func, expected):
    Utils.check_answer(
        TestData.monthly_sales_with_team(session)
        .group_by(["empid", "team"])
        .pivot("month")
        .agg(func(col("amount")))
        .sort(col("empid"), col("team")),
        expected,
        sort=False,
    )


def test_group_by_pivot(session):
    Utils.check_answer(
        TestData.monthly_sales_with_team(session)
        .group_by("empid")
        .pivot("month", ["JAN", "FEB", "MAR", "APR"])
        .agg(sum(col("amount")))
        .sort(col("empid")),
        [Row(1, 10400, 8000, 11000, 18000), Row(2, 39500, 90700, 12000, 5300)],
        sort=False,
    )

    Utils.check_answer(
        TestData.monthly_sales_with_team(session)
        .group_by(["empid", "team"])
        .pivot("month", ["JAN", "FEB", "MAR", "APR"])
        .agg(sum(col("amount")))
        .sort(col("empid"), col("team")),
        [
            Row(1, "A", 10400, None, 5000, 10000),
            Row(1, "B", None, 8000, 6000, 8000),
            Row(2, "A", 4500, 90700, None, 5300),
            Row(2, "B", 35000, None, 12000, None),
        ],
        sort=False,
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="not supported in local testing when pivot column and aggregate column are the same.",
)
def test_group_by_pivot_agg_same_column(session):
    Utils.check_answer(
        TestData.monthly_sales_with_team(session)
        .group_by("empid")
        .pivot("month", ["JAN", "FEB", "MAR", "APR"])
        .agg(count(col("month"))),
        [
            Row(EMPID=1, JAN=2, FEB=2, MAR=2, APR=2),
            Row(EMPID=2, JAN=2, FEB=2, MAR=2, APR=2),
        ],
    )


@multithreaded_run()
def test_group_by_pivot_dynamic_any(session, caplog):
    Utils.check_answer(
        TestData.monthly_sales_with_team(session)
        .group_by("empid")
        .pivot("month")
        .agg(sum(col("amount")))
        .sort(col("empid")),
        [
            Row(1, 18000, 8000, 10400, 11000),
            Row(2, 5300, 90700, 39500, 12000),
        ],
        sort=False,
    )

    Utils.check_answer(
        TestData.monthly_sales_with_team(session)
        .group_by(["empid", "team"])
        .pivot("month")
        .agg(sum(col("amount")))
        .sort(col("empid"), col("team")),
        [
            Row(1, "A", 10000, None, 10400, 5000),
            Row(1, "B", 8000, 8000, None, 6000),
            Row(2, "A", 5300, 90700, 4500, None),
            Row(2, "B", None, None, 35000, 12000),
        ],
        sort=False,
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Pivot does not support values from subqueries yet.",
)
def test_group_by_pivot_dynamic_subquery(session):
    src = TestData.monthly_sales(session)
    subquery_df = src.select(col("month")).filter(col("month") == "JAN")

    Utils.check_answer(
        TestData.monthly_sales_with_team(session)
        .group_by("empid")
        .pivot("month", subquery_df)
        .agg(sum(col("amount")))
        .sort(col("empid")),
        [Row(1, 10400), Row(2, 39500)],
        sort=False,
    )

    Utils.check_answer(
        TestData.monthly_sales_with_team(session)
        .group_by(["empid", "team"])
        .pivot("month", subquery_df, 999)
        .agg(sum(col("amount")))
        .sort(col("empid"), col("team")),
        [Row(1, "A", 10400), Row(1, "B", 999), Row(2, "A", 4500), Row(2, "B", 35000)],
        sort=False,
    )


def test_join_on_pivot(session):
    df1 = (
        TestData.monthly_sales(session)
        .pivot("month", ["JAN", "FEB", "MAR", "APR"])
        .agg(sum(col("amount")))
        .sort(col("empid"))
    )

    df2 = session.create_dataframe([[1, 12345], [2, 67890]]).to_df("empid", "may")

    Utils.check_answer(
        df1.join(df2, "empid"),
        [
            Row(1, 10400, 8000, 11000, 18000, 12345),
            Row(2, 39500, 90700, 12000, 5300, 67890),
        ],
        sort=False,
    )


def test_pivot_on_join(session):
    df = session.create_dataframe([[1, "One"], [2, "Two"]]).to_df("empid", "name")

    Utils.check_answer(
        TestData.monthly_sales(session)
        .join(df, "empid")
        .pivot("month", ["JAN", "FEB", "MAR", "APR"])
        .agg(sum(col("amount")))
        .sort(col("name")),
        [
            Row(1, "One", 10400, 8000, 11000, 18000),
            Row(2, "Two", 39500, 90700, 12000, 5300),
        ],
        sort=False,
    )


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="pivot does not work in stored proc")
def test_pivot_dynamic_any_with_temp_table_inlined_data(session, local_testing_mode):
    original_df = session.create_dataframe(
        [tuple(range(26)) for r in range(20)], schema=list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    )

    if not local_testing_mode:
        # Validate the data is backed by a temporary table
        assert len(original_df.queries.get("post_actions", [])) > 0

    pivot_op_df = original_df.pivot("a").agg(sum(col("b"))).sort(col("c"))

    # Query and ensure the schema matches as expected, this would fail with an exception if the data is not
    # materialized (happens internally) first.
    assert {f.column_identifier.name for f in pivot_op_df.schema.fields} == set(
        list("CDEFGHIJKLMNOPQRSTUVWXYZ") + ['"0"']
    )

    assert pivot_op_df.count() == 1


def test_pivot_dynamic_any(session):
    Utils.check_answer(
        TestData.monthly_sales(session)
        .pivot("month")
        .agg(sum(col("amount")))
        .sort(col("empid")),
        [
            Row(1, 18000, 8000, 10400, 11000),
            Row(2, 5300, 90700, 39500, 12000),
        ],
        sort=False,
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="BUG: SNOW-1370114 pivot should raise not implemented error but get AttributeError: DataFrame object has no attribute queries",
)
def test_pivot_dynamic_subquery(session):
    src = TestData.monthly_sales(session)
    subquery_df = src.select(col("month")).filter(col("month") == "JAN")

    Utils.check_answer(
        TestData.monthly_sales(session)
        .pivot("month", subquery_df)
        .agg(sum(col("amount")))
        .sort(col("empid")),
        [Row(1, 10400), Row(2, 39500)],
        sort=False,
    )


@pytest.mark.skip(
    "SNOW-847500: Currently fails because of snowpark is not using DISTINCT keyword expected by server"
)
@pytest.mark.parametrize("is_ascending", [True, False])
def test_pivot_dynamic_subquery_with_sort(session, is_ascending):
    src = TestData.monthly_sales(session)
    subquery_df = src.select(col("month")).filter(
        (col("month") == "JAN") | (col("month") == "APR")
    )

    Utils.check_answer(
        TestData.monthly_sales(session)
        .pivot(
            "month",
            subquery_df.select("month")
            .distinct()
            .sort("month", ascending=is_ascending),
        )
        .agg(sum(col("amount")))
        .sort(col("empid")),
        [
            Row(1, 18000, 10400) if is_ascending else Row(1, 10400, 18000),
            Row(2, 5300, 39500) if is_ascending else Row(2, 39500, 5300),
        ],
        sort=False,
    )


@pytest.mark.skip(
    "SNOW-848987: Requires server changes in 7.22 so can unskip once sfctest0 is on >= 7.22"
)
def test_pivot_dynamic_subquery_with_bad_subquery(session):
    src = TestData.monthly_sales(session)
    subquery_df = src.select(col("month")).filter(
        (col("month") == "JAN") | (col("month") == "APR")
    )

    with pytest.raises(SnowparkSQLException) as ex_info:
        TestData.monthly_sales(session).pivot(
            "month", subquery_df.select("month").sort("month")
        ).agg(sum(col("amount"))).collect()

    assert "Invalid subquery pivot order by must be distinct query" in str(ex_info)

    with pytest.raises(SnowparkSQLException) as ex_info:
        TestData.monthly_sales(session).pivot(
            "month", subquery_df.select(["month", "empid"])
        ).agg(sum(col("amount"))).collect()

    assert "Pivot subquery must select single column" in str(ex_info.value)


def test_pivot_default_on_none(session, caplog):
    class MonthlySales(NamedTuple):
        empid: int
        amount: int
        month: str

    src = session.create_dataframe(
        [
            MonthlySales(1, 10000, "JAN"),
            MonthlySales(1, 400, "JAN"),
            MonthlySales(1, None, "FEB"),
            MonthlySales(1, 6000, "MAR"),
            MonthlySales(2, 9000, "MAR"),
            MonthlySales(2, None, "MAR"),
        ]
    )

    for default_on_null in [Decimal(1.5), lit(9999), 9999, 0, None]:
        default_value = (
            default_on_null._expression.value
            if isinstance(default_on_null, Column)
            else default_on_null
        )
        Utils.check_answer(
            src.pivot("month", ["JAN", "FEB", "MAR"], default_on_null=default_on_null)
            .agg(sum(col("amount")))
            .sort(col("empid")),
            [
                Row(1, 10400, default_value, 6000),
                Row(2, default_value, default_value, 9000),
            ],
            sort=False,
        )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Multiple aggregations are not supported in local testing mode",
)
def test_pivot_multiple_aggs(session):
    with pytest.raises(
        SnowparkDataframeException,
        match=re.escape(
            "You can apply only one aggregate expression to a RelationalGroupedDataFrame returned by the pivot() method unless the pivot is applied with a groupby clause."
        ),
    ):
        TestData.monthly_sales(session).pivot(
            "month", ["JAN", "FEB", "MAR", "APR"]
        ).agg([sum(col("amount")), avg(col("amount"))]).sort(col("empid"))

    df = (
        TestData.monthly_sales(session)
        .groupBy(col("empid"))
        .pivot("month", ["JAN", "FEB", "MAR", "APR"])
        .agg([sum(col("amount")), avg(col("amount"))])
        .sort(col("empid"))
    )

    assert [f.name for f in df.schema.fields] == [
        "EMPID",
        '"JAN_sum(""AMOUNT"")"',
        '"FEB_sum(""AMOUNT"")"',
        '"MAR_sum(""AMOUNT"")"',
        '"APR_sum(""AMOUNT"")"',
        '"JAN_avg(""AMOUNT"")"',
        '"FEB_avg(""AMOUNT"")"',
        '"MAR_avg(""AMOUNT"")"',
        '"APR_avg(""AMOUNT"")"',
    ]

    Utils.check_answer(
        df,
        [
            Row(1, 10400, 8000, 11000, 18000, 5200.0, 4000.0, 5500.0, 9000.0),
            Row(
                2,
                39500,
                90700,
                12000,
                5300,
                19750.0,
                45350.0,
                6000.0,
                2650.0,
            ),
        ],
    )

    df = (
        TestData.monthly_sales(session)
        .groupBy(col("empid"))
        .pivot("month", ["JAN", "FEB", "MAR", "APR"])
        .agg([min(col("amount")), max(col("amount"))])
        .sort(col("empid"))
    )

    assert [f.name for f in df.schema.fields] == [
        "EMPID",
        '"JAN_min(""AMOUNT"")"',
        '"FEB_min(""AMOUNT"")"',
        '"MAR_min(""AMOUNT"")"',
        '"APR_min(""AMOUNT"")"',
        '"JAN_max(""AMOUNT"")"',
        '"FEB_max(""AMOUNT"")"',
        '"MAR_max(""AMOUNT"")"',
        '"APR_max(""AMOUNT"")"',
    ]

    # 2) MIN and MAX
    Utils.check_answer(
        df,
        [
            Row(1, 400, 3000, 5000, 8000, 10000, 5000, 6000, 10000),
            Row(2, 4500, 200, 2500, 800, 35000, 90500, 9500, 4500),
        ],
    )


def test_rel_grouped_dataframe_agg(session):
    df = (
        session.create_dataframe([[1, "One"], [2, "Two"], [3, "Three"]])
        .to_df(["empid", "name"])
        .group_by()
    )

    # Agg() on 1 column
    assert df.agg(max(col("empid"))).collect() == [Row(3)]
    assert df.agg([min(col("empid"))]).collect() == [Row(1)]
    assert df.agg([(col("empid"), "max")]).collect() == [Row(3)]
    assert df.agg([(col("empid"), "avg")]).collect() == [Row(2.0)]

    # Agg() on 2 columns
    assert df.agg([max(col("empid")), max(col("name"))]).collect() == [Row(3, "Two")]
    assert df.agg([min(col("empid")), min(col("name"))]).collect() == [Row(1, "One")]
    assert df.agg([(col("empid"), "max"), (col("name"), "max")]).collect() == [
        Row(3, "Two")
    ]
    assert df.agg([(col("empid"), "min"), (col("name"), "min")]).collect() == [
        Row(1, "One")
    ]


def test_group_by(session):
    result = (
        TestData.nurse(session)
        .group_by("medical_license")
        .agg(count(col("*")).as_("count"))
        .with_column("radio_license", lit(None))
        .select("medical_license", "radio_license", "count")
        .union_all(
            TestData.nurse(session)
            .group_by("radio_license")
            .agg(count(col("*")).as_("count"))
            .with_column("medical_license", lit(None))
            .select("medical_license", "radio_license", "count")
        )
        .sort(col("count"))
        .collect()
    )
    Utils.check_answer(
        result,
        [
            Row(None, "General", 1),
            Row(None, "Amateur Extra", 1),
            Row("RN", None, 2),
            Row(None, "Technician", 2),
            Row(None, None, 3),
            Row("LVN", None, 5),
        ],
        sort=False,
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: SNOW-977749 grouping by grouping sets not supported",
)
def test_group_by_grouping_sets(session):
    result = (
        TestData.nurse(session)
        .group_by("medical_license")
        .agg(count(col("*")).as_("count"))
        .with_column("radio_license", lit(None))
        .select("medical_license", "radio_license", "count")
        .union_all(
            TestData.nurse(session)
            .group_by("radio_license")
            .agg(count(col("*")).as_("count"))
            .with_column("medical_license", lit(None))
            .select("medical_license", "radio_license", "count")
        )
        .sort(col("count"))
        .collect()
    )

    grouping_sets = (
        TestData.nurse(session)
        .group_by_grouping_sets(
            GroupingSets([col("medical_license")], [col("radio_license")])
        )
        .agg(count(col("*")).as_("count"))
        .sort(col("count"))
    )

    Utils.check_answer(grouping_sets, result, sort=False)

    Utils.check_answer(
        grouping_sets,
        [
            Row(None, "General", 1),
            Row(None, "Amateur Extra", 1),
            Row("RN", None, 2),
            Row(None, "Technician", 2),
            Row(None, None, 3),
            Row("LVN", None, 5),
        ],
        sort=False,
    )

    # comparing with group_by
    Utils.check_answer(
        TestData.nurse(session)
        .group_by("medical_license", "radio_license")
        .agg(count(col("*")).as_("count"))
        .sort(col("count"), col("medical_license"), col("radio_license"))
        .select("count", "medical_license", "radio_license"),
        [
            Row(1, "LVN", "General"),
            Row(1, "RN", None),
            Row(1, "RN", "Amateur Extra"),
            Row(2, "LVN", None),
            Row(2, "LVN", "Technician"),
        ],
        sort=False,
    )

    # mixed grouping expression
    Utils.check_answer(
        TestData.nurse(session)
        .group_by_grouping_sets(
            GroupingSets([col("medical_license"), col("radio_license")]),
            GroupingSets([col("radio_license")]),
        )  # duplicated column is removed in the result
        .agg(col("radio_license"))
        .sort(col("medical_license"), col("radio_license")),
        [
            Row("LVN", None),
            Row("LVN", "General"),
            Row("LVN", "Technician"),
            Row("RN", None),
            Row("RN", "Amateur Extra"),
        ],
        sort=False,
    )

    # default constructor
    Utils.check_answer(
        TestData.nurse(session)
        .group_by_grouping_sets(
            [
                GroupingSets([col("medical_license"), col("radio_license")]),
                GroupingSets([col("radio_license")]),
            ]
        )  # duplicated column is removed in the result
        .agg(col("radio_license").as_("rl"))
        .sort(col("medical_license"), col("radio_license"))
        .select("medical_license", "rl"),
        [
            Row("LVN", None),
            Row("LVN", "General"),
            Row("LVN", "Technician"),
            Row("RN", None),
            Row("RN", "Amateur Extra"),
        ],
        sort=False,
    )


def test_rel_grouped_dataframe_max(session):
    df1 = session.create_dataframe(
        [("a", 1, 11, "b"), ("b", 2, 22, "c"), ("a", 3, 33, "d"), ("b", 4, 44, "e")]
    ).to_df(["key", "value1", "value2", "rest"])

    # below 2 ways to call max() must return the same result.
    expected = [Row("a", 3, 33), Row("b", 4, 44)]
    Utils.check_answer(
        df1.group_by("key").max(col("value1"), col("value2")).collect(), expected
    )
    Utils.check_answer(
        df1.group_by("key").agg([max(col("value1")), max(col("value2"))]).collect(),
        expected,
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(df1.group_by("key").max("value1", "value2").collect(), expected)
    Utils.check_answer(
        df1.group_by("key").agg([max("value1"), max("value2")]).collect(), expected
    )


def test_rel_grouped_dataframe_avg_mean(session):
    df1 = session.create_dataframe(
        [("a", 1, 11, "b"), ("b", 2, 22, "c"), ("a", 3, 33, "d"), ("b", 4, 44, "e")]
    ).to_df(["key", "value1", "value2", "rest"])

    expected = [Row("a", 2.0, 22.0), Row("b", 3, 33.0)]
    Utils.check_answer(df1.group_by("key").avg(col("value1"), col("value2")), expected)
    Utils.check_answer(
        df1.group_by("key").agg([avg(col("value1")), avg(col("value2"))]), expected
    )
    # Same results for mean()
    Utils.check_answer(df1.group_by("key").mean(col("value1"), col("value2")), expected)
    Utils.check_answer(
        df1.group_by("key").agg([mean(col("value1")), mean(col("value2"))]), expected
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(df1.group_by("key").avg("value1", "value2"), expected)
    Utils.check_answer(
        df1.group_by("key").agg([avg("value1"), avg("value2")]), expected
    )
    # Same results for mean()
    Utils.check_answer(df1.group_by("key").mean("value1", "value2"), expected)
    Utils.check_answer(
        df1.group_by("key").agg([mean("value1"), mean("value2")]), expected
    )


def test_rel_grouped_dataframe_median(session):
    df1 = session.create_dataframe(
        [
            ("a", 1, 11, "b"),
            ("b", 2, 22, "c"),
            ("a", 3, 33, "d"),
            ("b", 4, 44, "e"),
            ("b", 4, 44, "f"),
        ]
    ).to_df(["key", "value1", "value2", "rest"])

    # call median without groupb-y
    Utils.check_answer(
        df1.select(median(col("value1")), median(col("value2"))), [Row(3.0, 33.0)]
    )

    expected = [Row("a", 2.0, 22.0), Row("b", 4, 44.0)]
    assert (
        df1.group_by("key")
        .median(col("value1"), col("value2"))
        .sort(col("key"))
        .collect()
        == expected
    )
    assert (
        df1.group_by("key")
        .agg([median(col("value1")), median(col("value2"))])
        .sort(col("key"))
        .collect()
        == expected
    )
    # same as above, but pass str instead of Column
    assert (
        df1.group_by("key").median("value1", "value2").sort("key").collect() == expected
    )
    assert (
        df1.group_by("key")
        .agg([median("value1"), median("value2")])
        .sort("key")
        .collect()
        == expected
    )


def test_builtin_functions(session):
    df = session.create_dataframe([(1, 11), (2, 12), (1, 13)]).to_df(["a", "b"])

    assert df.group_by("a").builtin("max")(col("a"), col("b")).collect() == [
        Row(1, 1, 13),
        Row(2, 2, 12),
    ]
    assert df.group_by("a").builtin("max")(col("b")).collect() == [
        Row(1, 13),
        Row(2, 12),
    ]


def test_non_empty_arg_functions(session):
    func_name = "avg"
    with pytest.raises(ValueError) as ex_info:
        TestData.integer1(session).group_by("a").avg()
    assert (
        f"You must pass a list of one or more Columns to function: {func_name}"
        in str(ex_info)
    )

    func_name = "sum"
    with pytest.raises(ValueError) as ex_info:
        TestData.integer1(session).group_by("a").sum()
    assert (
        f"You must pass a list of one or more Columns to function: {func_name}"
        in str(ex_info)
    )

    func_name = "median"
    with pytest.raises(ValueError) as ex_info:
        TestData.integer1(session).group_by("a").median()
    assert (
        f"You must pass a list of one or more Columns to function: {func_name}"
        in str(ex_info)
    )

    func_name = "min"
    with pytest.raises(ValueError) as ex_info:
        TestData.integer1(session).group_by("a").min()
    assert (
        f"You must pass a list of one or more Columns to function: {func_name}"
        in str(ex_info)
    )

    func_name = "max"
    with pytest.raises(ValueError) as ex_info:
        TestData.integer1(session).group_by("a").max()
    assert (
        f"You must pass a list of one or more Columns to function: {func_name}"
        in str(ex_info)
    )


def test_null_count(session):
    assert TestData.test_data3(session).group_by("a").agg(
        count(col("b"))
    ).collect() == [
        Row(1, 0),
        Row(2, 1),
    ]

    assert TestData.test_data3(session).group_by("a").agg(
        count(col("a") + col("b"))
    ).collect() == [Row(1, 0), Row(2, 1)]

    assert TestData.test_data3(session).agg(
        [
            count(col("a")),
            count(col("b")),
            count(lit(1)),
            count_distinct(col("a")),
            count_distinct(col("b")),
        ]
    ).collect() == [Row(2, 1, 2, 2, 1)]

    assert TestData.test_data3(session).agg(
        [count(col("b")), count_distinct(col("b")), sum_distinct(col("b"))]
    ).collect() == [Row(1, 1, 2)]


def test_distinct(session):
    df = session.create_dataframe(
        [(1, "one", 1.0), (2, "one", 2.0), (2, "two", 1.0)]
    ).to_df("i", "s", '"i"')

    Utils.check_answer(
        df.distinct(),
        [
            Row(1, "one", 1.0),
            Row(2, "one", 2.0),
            Row(2, "two", 1.0),
        ],
    )
    Utils.check_answer(df.select("i").distinct(), [Row(1), Row(2)])
    Utils.check_answer(df.select('"i"').distinct(), [Row(1), Row(2)])
    Utils.check_answer(df.select("s").distinct(), [Row("one"), Row("two")])

    res = df.select(["i", '"i"']).distinct()
    Utils.check_answer(res, [Row(1, 1.0), Row(2, 1.0), Row(2, 2.0)])

    res = df.select(["s", '"i"']).distinct()
    Utils.check_answer(res, [Row("one", 1.0), Row("two", 1.0), Row("one", 2.0)])
    Utils.check_answer(df.filter(col("i") < 0).distinct(), [])


def test_distinct_and_joins(session):
    lhs = session.create_dataframe([(1, "one", 1.0), (2, "one", 2.0)]).to_df(
        "i", "s", '"i"'
    )
    rhs = session.create_dataframe([(1, "one", 1.0), (2, "one", 2.0)]).to_df(
        "i", "s", '"i"'
    )

    res = lhs.join(rhs, lhs["i"] == rhs["i"]).distinct().collect()
    res.sort(key=lambda x: x[0])
    assert res == [
        Row(1, "one", 1.0, 1, "one", 1.0),
        Row(2, "one", 2.0, 2, "one", 2.0),
    ]

    lhsD = lhs.select(col("s")).distinct()
    res = lhsD.join(rhs, lhsD["s"] == rhs["s"]).collect()
    res.sort(key=lambda x: x[0])
    assert res == [Row("one", 1, "one", 1.0), Row("one", 2, "one", 2.0)]

    rhsD = rhs.select(col("s"))
    res = lhsD.join(rhsD, lhsD["s"] == rhsD["s"]).collect()
    assert res == [Row("one", "one"), Row("one", "one")]

    rhsD = rhs.select(col("s")).distinct()
    res = lhsD.join(rhsD, lhsD["s"] == rhsD["s"]).collect()
    assert res == [Row("one", "one")]


def test_groupBy(session):
    Utils.check_answer(
        TestData.test_data2(session).group_by("a").agg(sum(col("b"))).collect(),
        [
            Row(1, 3),
            Row(2, 3),
            Row(3, 3),
        ],
    )

    Utils.check_answer(
        TestData.test_data2(session)
        .group_by("a")
        .agg(sum(col("b")).as_("totB"))
        .agg(sum(col("totB"))),
        [Row(9)],
    )

    Utils.check_answer(
        TestData.test_data2(session).group_by("a").agg(count(col("*"))),
        [
            Row(1, 2),
            Row(2, 2),
            Row(3, 2),
        ],
    )

    Utils.check_answer(
        TestData.test_data2(session).group_by("a").agg([(col("*"), "count")]),
        [Row(1, 2), Row(2, 2), Row(3, 2)],
    )

    Utils.check_answer(
        TestData.test_data2(session).group_by("a").agg([(col("b"), "sum")]),
        [Row(1, 3), Row(2, 3), Row(3, 3)],
    )

    df1 = session.create_dataframe(
        [("a", 1, 0, "b"), ("b", 2, 4, "c"), ("a", 2, 3, "d")]
    ).to_df(["key", "value1", "value2", "rest"])

    Utils.check_answer(
        df1.group_by("key").min(col("value2")),
        [
            Row("a", 0),
            Row("b", 4),
        ],
    )

    # same as above, but pass str instead of Column to min()
    Utils.check_answer(
        df1.group_by("key").min("value2"),
        [
            Row("a", 0),
            Row("b", 4),
        ],
    )

    Utils.check_answer(
        TestData.decimal_data(session).group_by("a").agg(sum(col("b"))),
        [
            Row(Decimal(1), Decimal(3)),
            Row(Decimal(2), Decimal(3)),
            Row(Decimal(3), Decimal(3)),
        ],
    )


def test_agg_should_be_order_preserving(session):
    df = (
        session.range(2)
        .group_by("id")
        .agg([(col("id"), "sum"), (col("id"), "count"), (col("id"), "min")])
    )

    assert [f.name for f in df.schema.fields] == [
        "ID",
        '"SUM(ID)"',
        '"COUNT(ID)"',
        '"MIN(ID)"',
    ]
    Utils.check_answer(df, [Row(0, 0, 1, 0), Row(1, 1, 1, 1)])


def test_count(session):
    assert TestData.test_data2(session).agg(
        [count(col("a")), sum_distinct(col("a"))]
    ).collect() == [Row(6, 6.0)]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: stddev not supported",
)
def test_stddev(session):
    test_data_dev = sqrt(4 / 5)

    Utils.check_answer(
        TestData.test_data2(session)
        .agg([stddev(col("a")), stddev_pop(col("a")), stddev_samp(col("a"))])
        .collect(),
        [Row(test_data_dev, 0.8164967850518458, test_data_dev)],
    )

    # same as above, but pass str instead of Column
    Utils.check_answer(
        TestData.test_data2(session)
        .agg([stddev("a"), stddev_pop("a"), stddev_samp("a")])
        .collect(),
        [Row(test_data_dev, 0.8164967850518458, test_data_dev)],
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: variance not supported",
)
def test_sn_moments(session):
    test_data2 = TestData.test_data2(session)
    var = test_data2.agg(variance(col("a")))
    Utils.check_answer(var, [Row(Decimal("0.8"))])

    Utils.check_answer(
        test_data2.group_by(col("a")).agg(variance(col("b"))),
        [Row(1, 0.50000), Row(2, 0.50000), Row(3, 0.500000)],
    )

    variance_result = session.sql(
        "select variance(a) from values(1,1),(1,2),(2,1),(2,2),(3,1),(3,2) as T(a,b);"
    ).collect()

    Utils.check_answer(var, variance_result[0])

    variance_pop = test_data2.agg(var_pop(col("a")))
    Utils.check_answer(variance_pop, [Row(Decimal("0.666667"))])

    variance_samp = test_data2.agg(var_samp(col("a")))
    Utils.check_answer(variance_samp, [Row(Decimal("0.8"))])

    kurtosis_ = test_data2.agg(kurtosis(col("a")))
    Utils.check_answer(kurtosis_, [Row(Decimal("-1.8750"))])

    # add SQL test
    agg_kurtosis_result = session.sql(
        "select kurtosis(a) from values(1,1),(1,2),(2,1),(2,2),(3,1),(3,2) as T(a,b);"
    ).collect()
    Utils.check_answer(kurtosis_, agg_kurtosis_result[0])


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: stddev not supportedg",
)
def test_sn_zero_moments(session):
    input = session.create_dataframe([[1, 2]]).to_df("a", "b")
    Utils.check_answer(
        input.agg(
            [
                stddev(col("a")),
                stddev_samp(col("a")),
                stddev_pop(col("a")),
                variance(col("a")),
                var_samp(col("a")),
                var_pop(col("a")),
                skew(col("a")),
                kurtosis(col("a")),
            ]
        ),
        [Row(None, None, 0.0, None, None, 0.0000, None, None)],
    )

    Utils.check_answer(
        input.agg(
            [
                sql_expr("stddev(a)"),
                sql_expr("stddev_samp(a)"),
                sql_expr("stddev_pop(a)"),
                sql_expr("variance(a)"),
                sql_expr("var_samp(a)"),
                sql_expr("var_pop(a)"),
                sql_expr("skew(a)"),
                sql_expr("kurtosis(a)"),
            ]
        ),
        [Row(None, None, 0.0, None, None, 0.0000, None, None)],
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: variance not supported",
)
def test_sn_null_moments(session):
    empty_table_data = session.create_dataframe([[]]).to_df("a")

    Utils.check_answer(
        empty_table_data.agg(
            [
                variance(col("a")),
                var_samp(col("a")),
                var_pop(col("a")),
                skew(col("a")),
                kurtosis(col("a")),
            ]
        ),
        [Row(None, None, None, None, None)],
    )

    Utils.check_answer(
        empty_table_data.agg(
            [
                sql_expr("variance(a)"),
                sql_expr("var_samp(a)"),
                sql_expr("var_pop(a)"),
                sql_expr("skew(a)"),
                sql_expr("kurtosis(a)"),
            ]
        ),
        [Row(None, None, None, None, None)],
    )


def test_decimal_sum_over_window_should_work(session):
    df = session.create_dataframe([1.0, 2.0, 3.0], schema=["a"])
    assert df.select(sum("a").over()).collect() == [Row(6.0), Row(6.0), Row(6.0)]
    assert df.select(avg("a").over()).collect() == [Row(2.0), Row(2.0), Row(2.0)]


def test_aggregate_function_in_groupby(session):
    with pytest.raises(SnowparkSQLException) as ex_info:
        TestData.test_data4(session).group_by(sum(col('"KEY"'))).count().collect()
    assert "is not a valid group by expression" in str(ex_info)


def test_ints_in_agg_exprs_are_taken_as_groupby_ordinal(session):
    assert TestData.test_data2(session).group_by(lit(3), lit(4)).agg(
        [lit(6), lit(7), sum(col("b"))]
    ).collect() == [Row(3, 4, 6, 7, 9)]

    assert TestData.test_data2(session).group_by([lit(3), lit(4)]).agg(
        [lit(6), col("b"), sum(col("b"))]
    ).collect() == [Row(3, 4, 6, 1, 3), Row(3, 4, 6, 2, 6)]


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL query not supported",
    run=False,
)
def test_ints_in_agg_exprs_are_taken_as_groupby_ordinal_sql(session):

    testdata2str = "(SELECT * FROM VALUES (1,1),(1,2),(2,1),(2,2),(3,1),(3,2) T(a, b) )"
    assert session.sql(
        f"SELECT 3, 4, SUM(b) FROM {testdata2str} GROUP BY 1, 2"
    ).collect() == [Row(3, 4, 9)]

    assert session.sql(
        f"SELECT 3 AS c, 4 AS d, SUM(b) FROM {testdata2str} GROUP BY c, d"
    ).collect() == [Row(3, 4, 9)]


def test_distinct_and_unions(session: object) -> object:
    lhs = session.create_dataframe([(1, "one", 1.0), (2, "one", 2.0)]).to_df(
        "i", "s", '"i"'
    )
    rhs = session.create_dataframe([(1, "one", 1.0), (2, "one", 2.0)]).to_df(
        "i", "s", '"i"'
    )

    res = lhs.union(rhs).distinct().collect()
    res.sort(key=lambda x: x[0])
    assert res == [Row(1, "one", 1.0), Row(2, "one", 2.0)]

    lhsD = lhs.select(col("s")).distinct()
    rhs = rhs.select(col("s"))
    res = lhsD.union(rhs).collect()
    assert res == [Row("one")]

    lhs = lhs.select(col("s"))
    rhsD = rhs.select("s").distinct()

    res = lhs.union(rhsD).collect()
    assert res == [Row("one")]

    res = lhsD.union(rhsD).collect()
    assert res == [Row("one")]


def test_distinct_and_unionall(session):
    lhs = session.create_dataframe([(1, "one", 1.0), (2, "one", 2.0)]).to_df(
        "i", "s", '"i"'
    )
    rhs = session.create_dataframe([(1, "one", 1.0), (2, "one", 2.0)]).to_df(
        "i", "s", '"i"'
    )

    res = lhs.union_all(rhs).distinct().collect()
    res.sort(key=lambda x: x[0])
    assert res == [Row(1, "one", 1.0), Row(2, "one", 2.0)]

    lhsD = lhs.select(col("s")).distinct()
    rhs = rhs.select(col("s"))
    res = lhsD.union_all(rhs).collect()
    assert res == [Row("one"), Row("one"), Row("one")]

    lhs = lhs.select(col("s"))
    rhsD = rhs.select("s").distinct()

    res = lhs.union_all(rhsD).collect()
    assert res == [Row("one"), Row("one"), Row("one")]

    res = lhsD.union_all(rhsD).collect()
    assert res == [Row("one"), Row("one")]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: count_if not suppored in snowpark python",
)
def test_count_if(session):
    temp_view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    session.create_dataframe(
        [
            ["a", None],
            ["a", 1],
            ["a", 2],
            ["a", 3],
            ["b", None],
            ["b", 4],
            ["b", 5],
            ["b", 6],
        ]
    ).toDF("x", "y").create_or_replace_temp_view(temp_view_name)

    res = session.sql(
        f"SELECT COUNT_IF(NULL), COUNT_IF(y % 2 = 0), COUNT_IF(y % 2 <> 0), COUNT_IF(y IS NULL) FROM {temp_view_name}"
    ).collect()
    assert res == [Row(0, 3, 3, 2)]

    res = session.sql(
        f"SELECT x, COUNT_IF(NULL), COUNT_IF(y % 2 = 0), COUNT_IF(y % 2 <> 0), COUNT_IF(y IS NULL) FROM {temp_view_name} GROUP BY x"
    ).collect()
    res.sort(key=lambda x: x[0])
    assert res == [Row("a", 0, 1, 2, 1), Row("b", 0, 2, 1, 1)]

    res = session.sql(
        f"SELECT x FROM {temp_view_name} GROUP BY x HAVING COUNT_IF(y % 2 = 0) = 1"
    ).collect()
    assert res == [Row("a")]

    res = session.sql(
        f"SELECT x FROM {temp_view_name} GROUP BY x HAVING COUNT_IF(y % 2 = 0) = 2"
    ).collect()
    assert res == [Row("b")]

    res = session.sql(
        f"SELECT x FROM {temp_view_name} GROUP BY x HAVING COUNT_IF(y IS NULL) > 0"
    ).collect()
    res.sort(key=lambda x: x[0])
    assert res == [Row("a"), Row("b")]

    res = session.sql(
        f"SELECT x FROM {temp_view_name} GROUP BY x HAVING COUNT_IF(NULL) > 0"
    ).collect()
    assert res == []

    with pytest.raises(SnowparkSQLException):
        session.sql(f"SELECT COUNT_IF(x) FROM {temp_view_name}").collect()


def test_agg_without_groups(session):
    assert TestData.test_data2(session).agg(sum(col("b"))).collect() == [Row(9)]


def test_agg_without_groups_and_functions(session):
    assert TestData.test_data2(session).agg(lit(1)).collect() == [Row(1)]


def test_null_average(session):
    assert TestData.test_data3(session).agg(avg(col("b"))).collect() == [Row(2.0)]

    assert TestData.test_data3(session).agg(
        [avg(col("b")), count_distinct(col("b"))]
    ).collect() == [Row(2.0, 1)]

    assert TestData.test_data3(session).agg(
        [avg(col("b")), sum_distinct(col("b"))]
    ).collect() == [Row(2.0, 2.0)]


def test_zero_average(session):
    df = session.create_dataframe([[]]).to_df(["a"])
    assert df.agg(avg(col("a"))).collect() == [Row(None)]

    assert df.agg([avg(col("a")), sum_distinct(col("a"))]).collect() == [
        Row(None, None)
    ]


def test_multiple_column_distinct_count(session):
    df1 = session.create_dataframe(
        [
            ("a", "b", "c"),
            ("a", "b", "c"),
            ("a", "b", "d"),
            ("x", "y", "z"),
            ("x", "q", None),
        ]
    ).to_df("key1", "key2", "key3")

    res = df1.agg(count_distinct(col("key1"), col("key2"))).collect()
    assert res == [Row(3)]

    res = df1.agg(count_distinct(col("key1"), col("key2"), col("key3"))).collect()
    assert res == [Row(3)]

    res = (
        df1.group_by(col("key1"))
        .agg(count_distinct(col("key2"), col("key3")))
        .collect()
    )
    res.sort(key=lambda x: x[0])
    assert res == [Row("a", 2), Row("x", 1)]

    aa = [
        "a1",
        "a1",
        "a1",
        "a1",
        "a2",
        "a2",
        "a2",
        "a3",
        "a3",
        "a4",
    ]

    bb = [
        "b1",
        "b2",
        "b2",
        "b3",
        "b1",
        "b2",
        "b5",
        "b1",
        "b4",
        "b1",
    ]

    df = session.create_dataframe(list(zip(aa, bb)), ["a", "b"])

    assert df.group_by("a").agg(count_distinct("b").alias("C")).select(
        avg("C").alias("C")
    ).collect() == [Row(2.25)]


def test_zero_count(session):
    empty_table = session.create_dataframe([[]]).to_df(["a"])
    assert empty_table.agg([count(col("a")), sum_distinct(col("a"))]).collect() == [
        Row(0, None)
    ]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: stddev not supported",
)
def test_zero_stddev(session):
    df = session.create_dataframe([[]]).to_df(["a"])
    assert df.agg(
        [stddev(col("a")), stddev_pop(col("a")), stddev_samp(col("a"))]
    ).collect() == [Row(None, None, None)]


def test_zero_sum(session):
    df = session.create_dataframe([[]]).to_df(["a"])
    assert df.agg([sum(col("a"))]).collect() == [Row(None)]


def test_zero_sum_distinct(session):
    df = session.create_dataframe([[]]).to_df(["a"])
    assert df.agg([sum_distinct(col("a"))]).collect() == [Row(None)]


def test_limit_and_aggregates(session):
    df = session.create_dataframe([("a", 1), ("b", 2), ("c", 1), ("d", 5)]).to_df(
        "id", "value"
    )
    limit_2df = df.limit(2)
    Utils.check_answer(
        limit_2df.group_by("id").count().select(col("id")), limit_2df.select("id"), True
    )


def test_listagg(session):
    df = session.create_dataframe(
        [
            (2, 1, 35, "red", 99),
            (7, 2, 24, "red", 99),
            (7, 9, 77, "green", 99),
            (8, 5, 11, "green", 99),
            (8, 4, 14, "blue", 99),
            (8, 3, 21, "red", 99),
            (9, 9, 12, "orange", 99),
        ],
        schema=["v1", "v2", "length", "color", "unused"],
    )

    result = df.group_by("color").agg(listagg("length", ",")).collect()
    # result is unpredictable without within group
    assert len(result) == 4


def test_listagg_within_group(session):
    df = session.create_dataframe(
        [
            (2, 1, 35, "red", 99),
            (7, 2, 24, "red", 99),
            (7, 9, 77, "green", 99),
            (8, 5, 11, "green", 99),
            (8, 4, 14, "blue", 99),
            (8, 3, 21, "red", 99),
            (9, 9, 12, "orange", 99),
        ],
        schema=["v1", "v2", "length", "color", "unused"],
    )
    Utils.check_answer(
        df.group_by("color")
        .agg(listagg("length", ",").within_group(df.length))
        .sort("color"),
        [
            Row("blue", "14"),
            Row("green", "11,77"),
            Row("orange", "12"),
            Row("red", "21,24,35"),
        ],
        sort=False,
    )
