#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Corpus of workloads for the lightweight DataFrame trace tests.

Each workload builds a DataFrame whose action fails on the server. The operation under test is
in the middle of the chain and a final ``select`` of a missing column makes the plan fail, so the
operation appears in the lineage of the error.

The expected debug output of a workload refers to its calls by label. A comment line
``# @label`` (or ``# @label=method`` when the label is not the method name, ``method#2`` for the
second call of that name) above a line labels the call of that method named on the line. The
labels of ``_base``, ``_other`` and ``_fail`` are shared by all workloads. ``test_df_trace_output``
computes the source position of each labeled call from the parsed source of this file.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, Tuple

from snowflake.snowpark import Window
from snowflake.snowpark.functions import (
    col,
    lit,
    parse_json,
    seq4,
    sum as sum_,
    udf,
)
from snowflake.snowpark.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

BAD = "DOES_NOT_EXIST"

# The context of the running test, for actions that need an object of it.
CURRENT_CTX = [None]


@dataclass
class Ctx:
    """Objects that workloads can use."""

    table: str
    stage: str

    def new_name(self, prefix: str = "DF_TRACE_OBJ") -> str:
        import uuid

        return f"{prefix}_{uuid.uuid4().hex[:12].upper()}"


@dataclass
class Workload:
    name: str
    build: Callable[[Any, Ctx], Any]
    # How the workload is executed: a function taking the built object.
    action: Callable[[Any], Any] = lambda df: df.collect()
    error_kind: str = "invalid_identifier"
    # False for workloads whose failure is raised while the dataframe is being built.
    fails_at_action: bool = True
    needs_simplifier: bool = False
    notes: str = ""
    # The expected debug output, as labels of calls (see the module docstring).
    # The lineage trace, most recent operation first.
    trace: List[str] = field(default_factory=list)
    # The operations the SQL error is mapped to, with the SQL simplifier enabled.
    mapped: List[str] = field(default_factory=list)
    # Where CTE optimization maps the error differently.
    mapped_cte: Optional[List[str]] = None
    # Without the SQL simplifier every operation is a query of its own: an error mapped to the
    # final ``_fail`` is mapped to it alone, others are mapped as with the simplifier.
    trace_nosimplifier: Optional[List[str]] = None
    mapped_nosimplifier: Optional[List[str]] = None
    # (object name, label of the call) of the missing or existing object context.
    missing_object: Optional[Tuple[str, str]] = None
    existing_object: Optional[Tuple[str, str]] = None


def _base(s):
    # @base=create_dataframe
    return s.create_dataframe(
        [(1, "a", 1.0), (2, "b", 2.0), (3, "c", 3.0)], schema=["ID", "NAME", "V"]
    )


def _other(s):
    # @other=create_dataframe
    return s.create_dataframe([(1, "x"), (2, "y")], schema=["ID", "TAG"])


def _fail(df):
    # @fail=select
    return df.select(BAD)


WORKLOADS: List[Workload] = []


def workload(name, **kwargs):
    def deco(fn):
        WORKLOADS.append(Workload(name=name, build=fn, **kwargs))
        return fn

    return deco


# ----------------------------------------------------------------------------- sources
@workload("src_table", trace=["fail", "table"], mapped=["table", "fail"])
def _(s, c):
    # @table
    return _fail(s.table(c.table))


@workload("src_read_table", trace=["fail", "table"], mapped=["table", "fail"])
def _(s, c):
    # @table
    return _fail(s.read.table(c.table))


@workload("src_sql_select", trace=["fail", "sql"], mapped=["sql", "fail"])
def _(s, c):
    # @sql
    return _fail(s.sql("select 1 as id"))


@workload("src_sql_with_params", trace=["fail", "sql"], mapped=["sql", "fail"])
def _(s, c):
    # @sql
    return _fail(s.sql("select ? as id", params=[1]))


@workload("src_sql_non_select", trace=["fail", "sql"], mapped=["sql", "fail"])
def _(s, c):
    # @sql
    return _fail(s.sql("show parameters like 'TIMEZONE'"))


@workload("src_create_dataframe", trace=["fail", "base"], mapped=["base", "fail"])
def _(s, c):
    return _fail(_base(s))


@workload(
    "src_create_dataframe_large",
    trace=["fail", "create_dataframe"],
    mapped=["create_dataframe", "fail"],
)
def _(s, c):
    return _fail(
        # @create_dataframe
        s.create_dataframe([(i, str(i)) for i in range(20000)], schema=["ID", "NAME"])
    )


@workload(
    "src_create_dataframe_dicts_and_createDataFrame",
    trace=["fail", "createDataFrame"],
    mapped=["createDataFrame", "fail"],
)
def _(s, c):
    # @createDataFrame
    return _fail(s.createDataFrame([{"ID": 1, "NAME": "a"}]))


@workload("src_range", trace=["fail", "range"], mapped=["range", "fail"])
def _(s, c):
    # @range
    return _fail(s.range(5))


@workload("src_generator", trace=["fail", "generator"], mapped=["generator", "fail"])
def _(s, c):
    # @generator
    return _fail(s.generator(seq4().alias("A"), rowcount=3))


@workload(
    "src_table_function",
    trace=["fail", "table_function"],
    mapped=["table_function", "fail"],
)
def _(s, c):
    # @table_function
    return _fail(s.table_function("split_to_table", lit("a b"), lit(" ")))


@workload("src_flatten", trace=["fail", "flatten"], mapped=["fail"])
def _(s, c):
    # @flatten
    return _fail(s.flatten(parse_json(lit("[1,2]"))))


_csv_schema = StructType(
    [StructField("ID", IntegerType()), StructField("NAME", StringType())]
)


@workload("src_read_csv", trace=["fail", "csv"], mapped=["csv", "fail"])
def _(s, c):
    # @csv
    return _fail(s.read.schema(_csv_schema).csv(f"@{c.stage}/a.csv"))


@workload("src_read_json_options", trace=["fail", "json"], mapped=["json", "fail"])
def _(s, c):
    return _fail(
        s.read.option("strip_outer_array", True).schema(_csv_schema)
        # @json
        .json(f"@{c.stage}/a.json")
    )


@workload("src_read_parquet", trace=["fail", "parquet"], mapped=["parquet", "fail"])
def _(s, c):
    # @parquet
    return _fail(s.read.schema(_csv_schema).parquet(f"@{c.stage}/a.parquet"))


@workload(
    "src_read_format_load_options", trace=["fail", "load"], mapped=["load", "fail"]
)
def _(s, c):
    return _fail(
        s.read.format("csv")
        .options({"field_delimiter": ","})
        .schema(_csv_schema)
        # @load
        .load(f"@{c.stage}/a.csv")
    )


# ----------------------------------------------------------------------------- projections
@workload(
    "op_select", trace=["fail", "select", "base"], mapped=["base", "select", "fail"]
)
def _(s, c):
    # @select
    return _fail(_base(s).select("ID", col("V") + 1))


@workload(
    "op_select_expr",
    trace=["fail", "selectExpr", "select_expr", "base"],
    mapped=["base", "selectExpr", "fail"],
)
def _(s, c):
    # @selectExpr @select_expr
    return _fail(_base(s).select_expr("ID", "V + 1").selectExpr("ID"))


@workload(
    "op_with_column",
    trace=["fail", "withColumn", "with_column", "base"],
    mapped=["base", "withColumn", "fail"],
)
def _(s, c):
    # @withColumn @with_column
    return _fail(_base(s).with_column("X", col("ID") + 1).withColumn("Y", col("X") * 2))


@workload(
    "op_with_columns",
    trace=["fail", "with_columns", "base"],
    mapped=["base", "with_columns", "fail"],
)
def _(s, c):
    # @with_columns
    return _fail(_base(s).with_columns(["X", "Y"], [col("ID") + 1, col("V") * 2]))


@workload(
    "op_with_column_renamed",
    trace=["fail", "withColumnRenamed", "with_column_renamed", "base"],
    mapped=["base", "withColumnRenamed", "fail"],
)
def _(s, c):
    # @withColumnRenamed @with_column_renamed
    return _fail(_base(s).with_column_renamed("ID", "I2").withColumnRenamed("I2", "I3"))


@workload(
    "op_rename_to_df",
    trace=["fail", "toDF", "to_df", "base"],
    mapped=["base", "toDF", "fail"],
)
def _(s, c):
    # @toDF @to_df
    return _fail(_base(s).to_df(["A", "B", "C"]).toDF("X", "Y", "Z"))


@workload(
    "op_rename", trace=["fail", "rename", "base"], mapped=["base", "rename", "fail"]
)
def _(s, c):
    # @rename
    return _fail(_base(s).rename("V", "W"))


@workload("op_drop", trace=["fail", "drop", "base"], mapped=["base", "drop", "fail"])
def _(s, c):
    # @drop
    return _fail(_base(s).drop("V"))


@workload(
    "op_alias",
    trace=["fail", "select", "alias", "base"],
    mapped=["base", "select"],
    mapped_nosimplifier=["select"],
)
def _(s, c):
    # @select @alias
    return _fail(_base(s).alias("T").select("T.ID"))


@workload(
    "op_col_ilike",
    needs_simplifier=True,
    trace=["fail", "col_ilike", "base"],
    mapped=["base", "col_ilike", "fail"],
)
def _(s, c):
    # @col_ilike
    return _fail(_base(s).col_ilike("i%"))


# ----------------------------------------------------------------------------- row operations
@workload(
    "op_filter_where",
    trace=["fail", "where", "filter", "base"],
    mapped=["base", "where", "fail"],
)
def _(s, c):
    # @where @filter
    return _fail(_base(s).filter(col("ID") > 1).where("V > 1"))


@workload(
    "op_sort",
    trace=["fail", "order_by", "orderBy", "sort", "base"],
    mapped=["base", "order_by", "fail"],
)
def _(s, c):
    # @order_by @orderBy @sort
    return _fail(_base(s).sort("ID").orderBy("V").order_by(col("NAME")))


@workload("op_limit", trace=["fail", "limit", "base"], mapped=["base", "limit", "fail"])
def _(s, c):
    # @limit
    return _fail(_base(s).limit(2, offset=1))


@workload(
    "op_table_sample", trace=["fail", "sample", "table"], mapped=["sample", "fail"]
)
def _(s, c):
    # @sample @table
    return _fail(s.table(c.table).sample(0.5))


@workload(
    "op_distinct", trace=["fail", "distinct", "base"], mapped=["distinct", "fail"]
)
def _(s, c):
    # @distinct
    return _fail(_base(s).distinct())


@workload(
    "op_drop_duplicates",
    trace=["fail", "dropDuplicates", "drop_duplicates", "base"],
    mapped=["base", "dropDuplicates", "fail"],
)
def _(s, c):
    # @dropDuplicates @drop_duplicates
    return _fail(_base(s).drop_duplicates("ID").dropDuplicates("NAME"))


@workload("op_sample", trace=["fail", "sample", "base"], mapped=["sample", "fail"])
def _(s, c):
    # @sample
    return _fail(_base(s).sample(0.5))


@workload(
    "op_sample_by",
    trace=["fail", "sampleBy", "sample_by", "base"],
    mapped=["sampleBy", "fail"],
)
def _(s, c):
    # @sample_by @sampleBy
    d = _base(s).sample_by(col("NAME"), {"a": 0.5}).sampleBy("NAME", {"b": 0.5})
    return _fail(d)


@workload(
    "op_dropna_fillna_replace",
    trace=["fail", "replace", "fillna", "dropna", "base"],
    mapped=["base", "replace", "fail"],
)
def _(s, c):
    # @replace @fillna @dropna
    return _fail(_base(s).dropna().fillna(0).replace(1, 5))


@workload(
    "op_na_stat_functions",
    trace=["fail", "sample_by", "replace", "fill", "drop", "base"],
    mapped=["sample_by", "fail"],
)
def _(s, c):
    return _fail(
        _base(s)
        # @drop
        .na.drop()
        # @fill
        .na.fill(0)
        # @replace
        .na.replace(1, 5)
        # @sample_by
        .stat.sample_by("NAME", {"a": 0.5})
    )


@workload(
    "op_crosstab", trace=["fail", "crosstab", "base"], mapped=["crosstab", "fail"]
)
def _(s, c):
    # @crosstab
    return _fail(_base(s).stat.crosstab("ID", "NAME"))


@workload(
    "op_describe", trace=["fail", "describe", "base"], mapped=["describe", "fail"]
)
def _(s, c):
    # @describe
    return _fail(_base(s).describe())


@workload(
    "op_cache_result",
    trace=["fail", "select", "cache_result", "filter", "base"],
    mapped=["select", "fail"],
)
def _(s, c):
    # @select @cache_result @filter
    return _fail(_base(s).filter(col("ID") > 0).cache_result().select("ID"))


# ----------------------------------------------------------------------------- aggregation
@workload(
    "op_group_by_agg", trace=["fail", "agg", "group_by", "base"], mapped=["agg", "fail"]
)
def _(s, c):
    # @agg @group_by
    return _fail(_base(s).group_by("NAME").agg(sum_("V").alias("S")))


@workload(
    "op_groupBy_functions",
    trace=["fail", "select", "union_all", "count", "select2", "groupBy", "sum", "base"],
    mapped=["select", "fail"],
    mapped_cte=["select"],
)
def _(s, c):
    # @groupBy
    g = _base(s).groupBy("NAME")
    # @count @union_all @sum @select2=select#1 @select=select#2
    d = g.count().union_all(g.sum("V").select("NAME", "SUM(V)")).select("NAME", "COUNT")
    return _fail(d)


@workload(
    "op_grouped_min_max_avg_median",
    trace=[
        "fail",
        "join",
        "join2",
        "median",
        "join3",
        "mean",
        "group_by",
        "join4",
        "avg",
        "base",
        "min",
        "max",
    ],
    mapped=["join", "fail"],
    mapped_cte=["join3"],
)
def _(s, c):
    # @group_by
    g = _base(s).group_by("NAME")
    return _fail(
        # @min
        g.min("V")
        # @join4=join @max
        .join(g.max("V"), on="NAME")
        # @join3=join @avg
        .join(g.avg("V"), on="NAME")
        # @join2=join @mean
        .join(g.mean("V"), on="NAME")
        # @join @median
        .join(g.median("V"), on="NAME")
    )


@workload("op_rollup", trace=["fail", "agg", "rollup", "base"], mapped=["agg", "fail"])
def _(s, c):
    # @agg @rollup
    return _fail(_base(s).rollup("NAME").agg(sum_("V")))


@workload("op_cube", trace=["fail", "agg", "cube", "base"], mapped=["agg", "fail"])
def _(s, c):
    # @agg @cube
    return _fail(_base(s).cube("NAME", "ID").agg(sum_("V")))


@workload(
    "op_grouping_sets",
    trace=["fail", "agg", "group_by_grouping_sets", "base"],
    mapped=["agg", "fail"],
)
def _(s, c):
    from snowflake.snowpark import GroupingSets

    return _fail(
        _base(s)
        # @group_by_grouping_sets
        .group_by_grouping_sets(GroupingSets([col("NAME")], [col("ID")]))
        # @agg
        .agg(sum_("V"))
    )


@workload("op_df_agg", trace=["fail", "agg", "base"], mapped=["agg", "fail"])
def _(s, c):
    # @agg
    return _fail(_base(s).agg(sum_("V")))


@workload(
    "op_pivot", trace=["fail", "sum", "pivot", "select", "base"], mapped=["sum", "fail"]
)
def _(s, c):
    # @sum @pivot @select
    return _fail(_base(s).select("ID", "NAME", "V").pivot("NAME", ["a", "b"]).sum("V"))


@workload(
    "op_grouped_pivot",
    trace=["fail", "agg", "pivot", "group_by", "base"],
    mapped=["agg", "fail"],
)
def _(s, c):
    # @agg @pivot @group_by
    return _fail(_base(s).group_by("ID").pivot("NAME", ["a", "b"]).agg(sum_("V")))


@workload(
    "op_unpivot",
    trace=["unpivot", "base"],
    trace_nosimplifier=["fail", "unpivot", "base"],
)
def _(s, c):
    # @unpivot
    return _fail(_base(s).unpivot("VAL", "COL", ["ID", "V"]))


@workload(
    "op_window",
    trace=["fail", "with_column", "base"],
    mapped=["base", "with_column", "fail"],
)
def _(s, c):
    w = Window.partition_by("NAME").order_by("ID")
    # @with_column
    return _fail(_base(s).with_column("RN", sum_("V").over(w)))


# ----------------------------------------------------------------------------- joins and set ops
@workload(
    "op_join_inner", trace=["fail", "join", "base", "other"], mapped=["join", "fail"]
)
def _(s, c):
    # @join
    return _fail(_base(s).join(_other(s), on="ID", how="inner"))


@workload(
    "op_join_condition",
    trace=["fail", "join", "base", "other"],
    mapped=["join", "fail"],
)
def _(s, c):
    a, b = _base(s), _other(s)
    # @join
    return _fail(a.join(b, a["ID"] == b["ID"], "left"))


@workload(
    "op_join_variants",
    trace=["fail", "join", "other", "join2", "join3", "base"],
    mapped=["join", "fail"],
)
def _(s, c):
    a, b = _base(s), _other(s)
    return _fail(
        # @join3=join
        a.join(b, on="ID", how="right")
        # @join2=join
        .join(b, on="ID", how="full")
        # @join
        .join(b, on="ID", how="semi")
    )


@workload(
    "op_cross_join",
    trace=["fail", "crossJoin", "cross_join", "select", "base", "select2", "other"],
    mapped=["crossJoin", "fail"],
)
def _(s, c):
    return _fail(
        _base(s)
        # @cross_join @select2=select
        .cross_join(_other(s).select(col("TAG").alias("T2")))
        # @crossJoin @select
        .crossJoin(_other(s).select(col("TAG").alias("T3")))
    )


@workload(
    "op_natural_join",
    trace=["fail", "natural_join", "base", "other"],
    mapped=["natural_join", "fail"],
)
def _(s, c):
    # @natural_join
    return _fail(_base(s).natural_join(_other(s)))


@workload(
    "op_self_join", trace=["fail", "join", "base", "select"], mapped=["join", "fail"]
)
def _(s, c):
    a = _base(s)
    # @join @select
    return _fail(a.join(a.select(col("ID").alias("ID2")), col("ID") == col("ID2")))


@workload(
    "op_join_table_function",
    trace=["fail", "join_table_function", "create_dataframe"],
    mapped=["join_table_function", "fail"],
)
def _(s, c):
    # @create_dataframe
    df = s.create_dataframe([("a b",)], schema=["T"])
    # @join_table_function
    return _fail(df.join_table_function("split_to_table", col("T"), lit(" ")))


@workload(
    "op_lateral_join",
    trace=["fail", "lateral_join", "base", "select", "other"],
    mapped=["lateral_join", "fail"],
)
def _(s, c):
    # @select
    a, b = _base(s), _other(s).select(col("TAG"))
    # @lateral_join
    return _fail(a.lateral_join(b))


@workload(
    "op_df_flatten",
    trace=["fail", "flatten", "select", "create_dataframe"],
    mapped=["fail"],
)
def _(s, c):
    # @select @create_dataframe
    df = s.create_dataframe([("[1,2]",)], schema=["T"]).select(
        parse_json(col("T")).alias("J")
    )
    # @flatten
    return _fail(df.flatten(col("J")))


@workload(
    "op_union_family",
    trace=["fail", "unionAll", "other", "union_all", "union"],
    mapped=["unionAll", "fail"],
    mapped_cte=["other"],
)
def _(s, c):
    a, b = _other(s), _other(s)
    # @unionAll @union_all @union
    return _fail(a.union(b).union_all(b).unionAll(b))


@workload(
    "op_union_by_name_family",
    trace=[
        "fail",
        "unionAllByName",
        "other",
        "union_all_by_name",
        "unionByName",
        "union_by_name",
    ],
    mapped=["unionAllByName", "fail"],
    mapped_cte=["other"],
)
def _(s, c):
    a, b = _other(s), _other(s)
    # @union_by_name @unionByName @union_all_by_name @unionAllByName
    d = a.union_by_name(b).unionByName(b).union_all_by_name(b).unionAllByName(b)
    return _fail(d)


@workload(
    "op_intersect_except",
    trace=["fail", "subtract", "other", "minus", "except_", "intersect"],
    mapped=["subtract", "fail"],
    mapped_cte=["other"],
)
def _(s, c):
    a, b = _other(s), _other(s)
    # @subtract @minus @except_ @intersect
    return _fail(a.intersect(b).except_(b).minus(b).subtract(b))


@workload(
    "op_random_split_first",
    trace=["fail", "random_split", "base"],
    mapped=["random_split", "fail"],
)
def _(s, c):
    # @random_split
    parts = _base(s).random_split([0.5, 0.5])
    return _fail(parts[0])


@workload(
    "op_to_df_then_union_with_self",
    trace=["fail", "union_all", "filter", "other"],
    mapped=["union_all", "fail"],
    mapped_cte=["other", "filter"],
)
def _(s, c):
    # @filter
    a = _other(s).filter(col("ID") > 0)
    # @union_all
    return _fail(a.union_all(a))


# ----------------------------------------------------------------------------- shapes
@workload(
    "shape_chain_50",
    trace=["fail", "with_column", "base"],
    mapped=["base", "with_column", "fail"],
)
def _(s, c):
    df = _base(s)
    for i in range(50):
        # @with_column
        df = df.with_column(f"C{i}", col("ID") + i)
    return _fail(df)


@workload(
    "shape_loop_reassign",
    trace=["fail", "with_column", "base"],
    mapped=["base", "with_column", "fail"],
)
def _(s, c):
    df = _base(s)
    for i in range(5):
        # @with_column
        df = df.with_column("NEW", df["ID"] + i)
    return _fail(df)


@workload(
    "shape_diamond",
    trace=["fail", "join", "select", "select2", "filter", "base"],
    mapped=["join", "fail"],
)
def _(s, c):
    # @filter
    base = _base(s).filter(col("ID") > 0)
    # @select
    left = base.select("ID", "NAME")
    # @select2=select
    right = base.select(col("ID").alias("ID2"), "V")
    # @join
    return _fail(left.join(right, col("ID") == col("ID2")))


@workload(
    "shape_same_df_twice_in_expression",
    trace=["fail", "filter", "base", "filter2", "select"],
    mapped=["fail"],
)
def _(s, c):
    a = _base(s)
    # @filter=filter#1 @filter2=filter#2 @select
    return _fail(a.filter(col("ID").isin(a.select("ID").filter(col("ID") > 1))))


@workload(
    "shape_multiline_call",
    trace=["fail", "with_column", "select", "base"],
    mapped=["base", "select", "with_column", "fail"],
)
def _(s, c):
    df = (
        _base(s)
        # @select
        .select(
            "ID",
            "NAME",
        )
        # @with_column
        .with_column(
            "X",
            col("ID") + 1,
        )
    )
    return _fail(df)


@workload(
    "shape_many_calls_one_line",
    trace=["fail", "limit", "sort", "select", "filter", "base"],
    mapped=["base", "limit", "fail"],
)
def _(s, c):
    # @filter @select @sort @limit
    d = _base(s).filter(col("ID") > 0).select("ID", "NAME").sort("ID").limit(2)
    return _fail(d)


@workload(
    "shape_lambda_call_site",
    trace=["fail", "select", "filter", "base"],
    mapped=["base", "select", "fail"],
)
def _(s, c):
    # @select @filter
    f = lambda d: d.filter(col("ID") > 0).select("ID")  # noqa: E731
    return _fail(f(_base(s)))


@workload(
    "shape_helper_functions",
    trace=["fail", "filter", "with_column", "base"],
    mapped=["base", "with_column", "filter", "fail"],
)
def _(s, c):
    def add(d):
        # @with_column
        return d.with_column("X", col("ID") + 1)

    def mid(d):
        # @filter
        return add(add(d)).filter(col("X") > 0)

    return _fail(mid(_base(s)))


@workload(
    "shape_comprehension_call_site",
    trace=["fail", "union_all", "with_column", "base"],
    mapped=["union_all", "fail"],
)
def _(s, c):
    # @with_column
    dfs = [_base(s).with_column("K", lit(i)) for i in range(3)]
    out = dfs[0]
    for d in dfs[1:]:
        # @union_all
        out = out.union_all(d)
    return _fail(out)


@workload(
    "shape_wide_projection",
    trace=["fail", "select", "base"],
    mapped=["base", "select", "fail"],
)
def _(s, c):
    df = _base(s)
    # @select
    return _fail(df.select(*[(col("ID") + i).alias(f"W{i}") for i in range(200)]))


@workload(
    "shape_udf_in_pipeline",
    trace=["fail", "with_column", "udf", "base"],
    mapped=["base", "with_column", "fail"],
)
def _(s, c):
    # @udf
    f = udf(lambda x: x + 1, return_type=IntegerType(), input_types=[IntegerType()])
    # @with_column
    return _fail(_base(s).with_column("U", f(col("ID"))))


# ----------------------------------------------------------------------------- error kinds
@workload(
    "err_typing_with_error_line",
    error_kind="typing",
    trace=["filter", "with_column", "base"],
)
def _(s, c):
    # @filter @with_column
    return _base(s).with_column("BAD", col("NAME") + lit(1)).filter(col("ID") > 0)


@workload(
    "err_typing_in_flattened_select",
    error_kind="typing",
    trace=["select", "select2", "base"],
)
def _(s, c):
    # @select=select#2 @select2=select#1
    return _base(s).select("ID", (col("NAME") + lit(1)).alias("X")).select("X")


@workload(
    "err_invalid_identifier_after_rename",
    error_kind="invalid_identifier",
    trace=["select", "select2", "base"],
    mapped=["base", "select"],
    mapped_nosimplifier=["select"],
)
def _(s, c):
    # @select=select#2 @select2=select#1
    return _base(s).select(col("ID").alias("RENAMED")).select("ID")


@workload(
    "err_missing_table",
    error_kind="missing_object",
    trace=["table"],
    trace_nosimplifier=["select", "table"],
    missing_object=["DF_TRACE_NO_SUCH_TABLE", "table"],
)
def _(s, c):
    # @table @select
    return s.table("DF_TRACE_NO_SUCH_TABLE").select("A")


@workload(
    "err_missing_table_in_join",
    error_kind="missing_object",
    trace=["table"],
    missing_object=["DF_TRACE_NO_SUCH_TABLE", "table"],
)
def _(s, c):
    # @table
    return _base(s).join(s.table("DF_TRACE_NO_SUCH_TABLE"), on="ID")


def _name(prefix):
    return Ctx(table="", stage="").new_name(prefix)


# ----------------------------------------------------------------------------- actions
def _action_workload(name, action, **kw):
    WORKLOADS.append(
        Workload(name=name, build=lambda s, c: _fail(_base(s)), action=action, **kw)
    )


_action_workload(
    "act_count",
    # @count
    lambda d: d.count(),
    trace=["count", "fail", "base"],
    mapped=["base", "fail"],
)
_action_workload(
    "act_show",
    # @show
    lambda d: d.show(),
    trace=["show", "fail", "base"],
    mapped=["base", "fail", "show"],
)
_action_workload(
    "act_first",
    # @first
    lambda d: d.first(),
    trace=["first", "fail", "base"],
    mapped=["base", "fail", "first"],
)
_action_workload(
    "act_to_pandas",
    lambda d: d.to_pandas(),
    trace=["fail", "base"],
    mapped=["base", "fail"],
)
_action_workload(
    "act_to_pandas_batches",
    lambda d: list(d.to_pandas_batches()),
    trace=["fail", "base"],
    mapped=["base", "fail"],
)
_action_workload(
    "act_to_local_iterator",
    lambda d: list(d.to_local_iterator()),
    trace=["fail", "base"],
    mapped=["base", "fail"],
)
_action_workload(
    "act_cache_result",
    lambda d: d.cache_result(),
    trace=["fail", "base"],
    mapped=["base", "fail"],
)
_action_workload(
    "act_print_schema",
    lambda d: d.print_schema(),
    trace=["fail", "base"],
    mapped=["base", "fail"],
)
_action_workload(
    "act_stat_corr_cov",
    # @corr
    lambda d: d.stat.corr("ID", "V"),
    trace=["corr", "fail", "base"],
    mapped=["base", "fail"],
)
_action_workload(
    "act_stat_approx_quantile",
    # @approx_quantile
    lambda d: d.stat.approx_quantile("ID", [0.5]),
    trace=["approx_quantile", "fail", "base"],
    mapped=["base", "fail", "approx_quantile"],
)
_action_workload(
    "act_create_or_replace_view",
    lambda d: d.create_or_replace_view(_name("DF_TRACE_V")),
    trace=[],
)
_action_workload(
    "act_create_or_replace_temp_view",
    lambda d: d.create_or_replace_temp_view(_name("DF_TRACE_V")),
    trace=[],
)
_action_workload(
    "act_create_temp_view", lambda d: d.create_temp_view(_name("DF_TRACE_V")), trace=[]
)
_action_workload(
    "act_write_save_as_table",
    lambda d: d.write.mode("overwrite").save_as_table(_name("DF_TRACE_T")),
    trace=["fail", "base"],
    mapped=["base", "fail"],
)
_action_workload(
    "act_write_copy_into_location",
    # @copy_into_location
    lambda d: d.write.copy_into_location(
        "@~/df_trace_out/", file_format_type="csv", overwrite=True
    ),
    trace=["copy_into_location", "fail", "base"],
    mapped=["copy_into_location"],
)
_action_workload(
    "act_write_csv_json_parquet",
    # @csv
    lambda d: d.write.option("overwrite", True).csv("@~/df_trace_out_csv/"),
    trace=["csv", "fail", "base"],
    mapped=["csv"],
)
_action_workload(
    "act_write_json",
    # @json
    lambda d: d.write.json("@~/df_trace_out_json/", overwrite=True),
    trace=["json", "fail", "base"],
    mapped=["json"],
)
_action_workload(
    "act_write_parquet",
    # @parquet
    lambda d: d.write.parquet("@~/df_trace_out_pq/", overwrite=True),
    trace=["parquet", "fail", "base"],
    mapped=["parquet"],
)
_action_workload(
    "act_write_partition_by_format_save",
    lambda d: d.write.format("csv").partition_by("ID")
    # @save
    .save("@~/df_trace_out_p/", mode="overwrite"),
    trace=["save", "fail", "base"],
)
_action_workload(
    "act_write_options",
    # @csv
    lambda d: d.write.options({"overwrite": True}).csv("@~/df_trace_out_opts/"),
    trace=["csv", "fail", "base"],
    mapped=["csv"],
)
_action_workload(
    "act_write_insert_into",
    lambda d: d.write.insert_into(CURRENT_CTX[0].table),
    trace=["fail", "base"],
    mapped=["base", "fail"],
)


# ----------------------------------------------------------------------------- already exists
@workload(
    "err_exists_sql",
    error_kind="already_exists",
    trace=["sql"],
    existing_object=["DF_TRACE_EX_<RANDOM>", "sql2"],
)
def _(s, c):
    name = c.new_name("DF_TRACE_EX")
    # @sql2=sql
    s.sql(f"create or replace temp table {name} (a int)").collect()
    # @sql
    return s.sql(f"create temp table {name} (a int)")


@workload(
    "err_exists_temp_view_twice",
    error_kind="already_exists",
    trace=[],
    existing_object=["DF_TRACE_EXV_<RANDOM>", "create_temp_view"],
)
def _(s, c):
    name = c.new_name("DF_TRACE_EXV")
    d = _base(s).select("ID")
    # @create_temp_view
    d.create_temp_view(name)
    d.create_temp_view(name)
    return d


@workload(
    "err_exists_save_as_table_errorifexists",
    error_kind="already_exists",
    trace=[],
    existing_object=["DF_TRACE_EXT_<RANDOM>", "save_as_table"],
)
def _(s, c):
    name = c.new_name("DF_TRACE_EXT")
    d = _base(s).select("ID")
    # @save_as_table
    d.write.mode("overwrite").save_as_table(name, table_type="temp")
    d.write.mode("errorifexists").save_as_table(name, table_type="temp")
    return d


# ----------------------------------------------------------------------------- dataframes inside expressions
@workload(
    "expr_isin_subquery_in_filter",
    trace=["fail", "filter", "base", "select", "other"],
    mapped=["fail"],
)
def _(s, c):
    # @select
    a, b = _base(s), _other(s).select("ID")
    # @filter
    return _fail(a.filter(col("ID").isin(b)))


@workload(
    "expr_isin_nested_in_boolean",
    trace=["fail", "filter", "base", "select", "other"],
    mapped=["fail"],
)
def _(s, c):
    # @select
    a, b = _base(s), _other(s).select("ID")
    # @filter
    return _fail(a.filter((col("ID").isin(b)) & (col("V") > 0) | (col("ID") == 3)))


@workload(
    "expr_isin_in_when_otherwise",
    trace=["fail", "with_column", "base", "select", "other"],
    mapped=["base", "with_column", "fail"],
)
def _(s, c):
    from snowflake.snowpark.functions import when

    # @select
    a, b = _base(s), _other(s).select("ID")
    # @with_column
    return _fail(a.with_column("F", when(col("ID").isin(b), lit(1)).otherwise(lit(0))))


@workload(
    "expr_isin_in_select",
    trace=["fail", "select", "base", "select2", "other"],
    mapped=["base", "select", "fail"],
)
def _(s, c):
    # @select2=select
    a, b = _base(s), _other(s).select("ID")
    # @select
    return _fail(a.select("ID", col("ID").isin(b).alias("IN_B")))


@workload(
    "expr_isin_in_join_condition",
    trace=["fail", "join", "base", "other", "select"],
    mapped=["join", "fail"],
)
def _(s, c):
    # @select
    a, b, sub = _base(s), _other(s), _other(s).select(col("ID").alias("X"))
    # @join
    return _fail(a.join(b, (a["ID"] == b["ID"]) & a["ID"].isin(sub)))


@workload(
    "expr_subquery_reused_in_later_operation",
    trace=["fail", "sort", "select", "filter", "base", "select2", "other"],
    mapped=["base", "filter", "sort", "fail"],
)
def _(s, c):
    # @select2=select
    a, b = _base(s), _other(s).select("ID")
    # @filter
    filtered = a.filter(col("ID").isin(b))
    # @sort @select
    return _fail(filtered.select("ID", "NAME").sort("ID"))


@workload(
    "expr_same_dataframe_as_receiver_and_subquery",
    trace=["fail", "filter", "base", "select"],
    mapped=["fail"],
)
def _(s, c):
    a = _base(s)
    # @filter @select
    return _fail(a.filter(col("ID").isin(a.select("ID"))))


@workload(
    "expr_two_subqueries_one_call",
    trace=["fail", "filter", "base", "select", "select2", "other"],
    mapped=["fail"],
)
def _(s, c):
    a = _base(s)
    # @select=select#1 @select2=select#2
    b, d = _other(s).select("ID"), _other(s).select(col("ID").alias("ID"))
    # @filter
    return _fail(a.filter(col("ID").isin(b) | col("ID").isin(d)))


@workload(
    "expr_isin_in_sort",
    trace=["fail", "sort", "base", "select", "other"],
    mapped=["fail"],
)
def _(s, c):
    # @select
    a, b = _base(s), _other(s).select("ID")
    # @sort
    return _fail(a.sort(col("ID").isin(b)))


# ----------------------------------------------------------------------------- functions applied by the operation
@workload(
    "fn_udf_lambda",
    trace=["fail", "with_column", "udf", "base"],
    mapped=["base", "with_column", "fail"],
)
def _(s, c):
    # @udf
    f = udf(lambda x: x + 1, return_type=IntegerType(), input_types=[IntegerType()])
    # @with_column
    return _fail(_base(s).with_column("U", f(col("ID"))))


@workload(
    "fn_udf_decorator",
    trace=["fail", "select", "udf", "base"],
    mapped=["base", "select", "fail"],
)
def _(s, c):
    # @udf
    @udf(return_type=IntegerType(), input_types=[IntegerType()])
    def plus_one(x):
        return x + 1

    # @select
    return _fail(_base(s).select(plus_one(col("ID")).alias("U")))


@workload(
    "fn_udf_register_named",
    trace=["fail", "filter", "register", "base"],
    mapped=["base", "filter", "fail"],
)
def _(s, c):
    # @register
    f = s.udf.register(
        lambda x: x * 2,
        return_type=IntegerType(),
        input_types=[IntegerType()],
        name=_name("DF_TRACE_UDF"),
        is_permanent=False,
        replace=True,
    )
    # @filter
    return _fail(_base(s).filter(f(col("ID")) > 0))


@workload(
    "fn_udf_vectorized",
    trace=["fail", "with_column", "pandas_udf", "base"],
    mapped=["base", "with_column", "fail"],
)
def _(s, c):
    from snowflake.snowpark.functions import pandas_udf
    from snowflake.snowpark.types import PandasSeriesType

    # @pandas_udf
    f = pandas_udf(
        lambda x: x + 1,
        return_type=PandasSeriesType(IntegerType()),
        input_types=[PandasSeriesType(IntegerType())],
    )
    # @with_column
    return _fail(_base(s).with_column("U", f(col("ID"))))


@workload(
    "fn_udf_used_twice",
    trace=["fail", "filter", "udf", "with_column", "base"],
    mapped=["base", "with_column", "filter", "fail"],
)
def _(s, c):
    # @udf
    f = udf(lambda x: x + 1, return_type=IntegerType(), input_types=[IntegerType()])
    # @with_column
    df = _base(s).with_column("U", f(col("ID")))
    # @filter
    return _fail(df.filter(f(col("U")) > 0))


@workload(
    "fn_udf_nested_in_expression",
    trace=["fail", "with_column", "udf", "base"],
    mapped=["base", "with_column", "fail"],
)
def _(s, c):
    # @udf
    f = udf(lambda x: x + 1, return_type=IntegerType(), input_types=[IntegerType()])
    # @with_column
    return _fail(_base(s).with_column("U", (f(col("ID")) * 2 + 1).cast("int")))


@workload(
    "fn_udtf_join_table_function",
    trace=["fail", "join_table_function", "base", "t", "udtf"],
    mapped=["join_table_function", "fail"],
)
def _(s, c):
    from snowflake.snowpark.functions import udtf
    from snowflake.snowpark.types import StructField, StructType

    class Echo:
        def process(self, x):
            yield (x,)

    # @udtf
    t = udtf(
        Echo,
        output_schema=StructType([StructField("E", IntegerType())]),
        input_types=[IntegerType()],
    )
    # @join_table_function @t
    return _fail(_base(s).join_table_function(t(col("ID"))))


@workload(
    "fn_udtf_session_table_function",
    trace=["fail", "table_function", "t", "udtf"],
    mapped=["table_function", "fail"],
)
def _(s, c):
    from snowflake.snowpark.functions import udtf
    from snowflake.snowpark.types import StructField, StructType

    class Count:
        def process(self, n):
            for i in range(n):
                yield (i,)

    # @udtf
    t = udtf(
        Count,
        output_schema=StructType([StructField("I", IntegerType())]),
        input_types=[IntegerType()],
    )
    # @table_function @t
    return _fail(s.table_function(t(lit(3))))


@workload(
    "fn_udaf_in_agg",
    trace=["fail", "agg", "udaf", "group_by", "base"],
    mapped=["agg", "fail"],
)
def _(s, c):
    from snowflake.snowpark.functions import udaf

    class Summer:
        def __init__(self) -> None:
            self._s = 0

        @property
        def aggregate_state(self):
            return self._s

        def accumulate(self, x):
            self._s += x

        def merge(self, other):
            self._s += other

        def finish(self):
            return self._s

    # @udaf
    agg = udaf(Summer, return_type=IntegerType(), input_types=[IntegerType()])
    # @agg=agg#1 @group_by
    return _fail(_base(s).group_by("NAME").agg(agg(col("ID")).alias("S")))


# ----------------------------------------------------------------------------- reused plans under CTE
@workload(
    "cte_union_all_self",
    trace=["fail", "union_all", "filter", "other"],
    mapped=["union_all", "fail"],
    mapped_cte=["other", "filter"],
)
def _(s, c):
    # @filter
    a = _other(s).filter(col("ID") > 0)
    # @union_all
    return _fail(a.union_all(a))


@workload(
    "cte_self_join_reused",
    trace=["fail", "join", "filter", "select", "base"],
    mapped=["join", "fail"],
)
def _(s, c):
    # @filter
    a = _base(s).filter(col("ID") > 0)
    # @join @select
    return _fail(a.join(a.select(col("ID").alias("I2")), col("ID") == col("I2")))


@workload(
    "cte_describe_reused",
    trace=["fail", "union_all", "describe", "base"],
    mapped=["union_all", "fail"],
)
def _(s, c):
    # @describe
    d = _base(s).describe()
    # @union_all
    return _fail(d.union_all(d))


@workload(
    "cte_large_create_dataframe_reused",
    trace=["fail", "union_all", "create_dataframe", "filter"],
    mapped=["union_all", "fail"],
    mapped_cte=["create_dataframe"],
)
def _(s, c):
    # @create_dataframe
    big = s.create_dataframe([(i, str(i)) for i in range(20000)], schema=["ID", "NAME"])
    # @union_all @filter
    return _fail(big.union_all(big.filter(col("ID") > 5)))


@workload(
    "cte_three_way_reuse",
    trace=["fail", "union_all", "with_column", "union_all2", "other"],
    mapped=["union_all", "fail"],
    mapped_cte=["other", "with_column"],
)
def _(s, c):
    # @with_column
    a = _other(s).with_column("K", lit(1))
    # @union_all=union_all#2 @union_all2=union_all#1
    return _fail(a.union_all(a).union_all(a))


@workload(
    "cte_crosstab_reused",
    trace=["fail", "union_all", "crosstab", "base"],
    mapped=["union_all", "fail"],
    mapped_cte=["crosstab"],
)
def _(s, c):
    # @crosstab
    x = _base(s).stat.crosstab("ID", "NAME")
    # @union_all
    return _fail(x.union_all(x))


@workload(
    "fn_udf_register_from_file",
    trace=["fail", "with_column", "register_from_file", "base"],
    mapped=["base", "with_column", "fail"],
)
def _(s, c):
    import os
    import tempfile

    path = os.path.join(tempfile.mkdtemp(), "df_trace_udf.py")
    with open(path, "w") as f:
        f.write("def plus_one(x):\n    return x + 1\n")
    # @register_from_file
    f = s.udf.register_from_file(
        path, "plus_one", return_type=IntegerType(), input_types=[IntegerType()]
    )
    # @with_column
    return _fail(_base(s).with_column("U", f(col("ID"))))


@workload(
    "fn_udf_registration_describe",
    trace=["fail", "describe"],
    mapped=["describe", "fail"],
)
def _(s, c):
    f = s.udf.register(
        lambda x: x + 1,
        return_type=IntegerType(),
        input_types=[IntegerType()],
        name=_name("DF_TRACE_UDF_D"),
        is_permanent=False,
        replace=True,
    )
    # @describe
    return _fail(s.udf.describe(f))


_UDTF_SOURCE = "class Echo:\n    def process(self, x):\n        yield (x,)\n"
_UDAF_SOURCE = (
    "class Summer:\n"
    "    def __init__(self):\n        self._s = 0\n"
    "    @property\n    def aggregate_state(self):\n        return self._s\n"
    "    def accumulate(self, x):\n        self._s += x\n"
    "    def merge(self, other):\n        self._s += other\n"
    "    def finish(self):\n        return self._s\n"
)


def _source_file(name, text):
    import os
    import tempfile

    path = os.path.join(tempfile.mkdtemp(), name)
    with open(path, "w") as f:
        f.write(text)
    return path


@workload(
    "fn_udtf_register_from_file_and_describe",
    trace=["fail", "join_table_function", "base", "t", "register_from_file"],
    mapped=["join_table_function", "fail"],
)
def _(s, c):
    from snowflake.snowpark.types import StructField, StructType

    # @register_from_file
    t = s.udtf.register_from_file(
        _source_file("df_trace_udtf.py", _UDTF_SOURCE),
        "Echo",
        output_schema=StructType([StructField("E", IntegerType())]),
        input_types=[IntegerType()],
        name=_name("DF_TRACE_UDTF"),
        is_permanent=False,
        replace=True,
    )
    # @join_table_function @t
    return _fail(_base(s).join_table_function(t(col("ID"))))


@workload(
    "fn_udaf_register_from_file_and_describe",
    trace=["fail", "agg", "register_from_file", "group_by", "base"],
    mapped=["agg", "fail"],
)
def _(s, c):
    # @register_from_file
    agg = s.udaf.register_from_file(
        _source_file("df_trace_udaf.py", _UDAF_SOURCE),
        "Summer",
        return_type=IntegerType(),
        input_types=[IntegerType()],
        name=_name("DF_TRACE_UDAF"),
        is_permanent=False,
        replace=True,
    )
    s.udaf.describe(agg).collect()
    # @agg=agg#1 @group_by
    return _fail(_base(s).group_by("NAME").agg(agg(col("ID")).alias("S")))
