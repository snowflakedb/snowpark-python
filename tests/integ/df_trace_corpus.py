#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Corpus of workloads for the lightweight DataFrame trace tests.

Each workload builds a DataFrame whose action fails on the server. The operation under test is
in the middle of the chain and a final ``select`` of a missing column makes the plan fail, so the
operation appears in the lineage of the error.
"""

from dataclasses import dataclass
from typing import Any, Callable, List

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
    # Set when the AST backend cannot run the workload, so there is no oracle to compare with.
    no_ast_oracle: str = ""
    notes: str = ""


def _base(s):
    return s.create_dataframe(
        [(1, "a", 1.0), (2, "b", 2.0), (3, "c", 3.0)], schema=["ID", "NAME", "V"]
    )


def _other(s):
    return s.create_dataframe([(1, "x"), (2, "y")], schema=["ID", "TAG"])


def _fail(df):
    return df.select(BAD)


WORKLOADS: List[Workload] = []


def workload(name, **kwargs):
    def deco(fn):
        WORKLOADS.append(Workload(name=name, build=fn, **kwargs))
        return fn

    return deco


# ----------------------------------------------------------------------------- sources
@workload("src_table")
def _(s, c):
    return _fail(s.table(c.table))


@workload("src_read_table")
def _(s, c):
    return _fail(s.read.table(c.table))


@workload("src_sql_select")
def _(s, c):
    return _fail(s.sql("select 1 as id"))


@workload("src_sql_with_params")
def _(s, c):
    return _fail(s.sql("select ? as id", params=[1]))


@workload("src_sql_non_select")
def _(s, c):
    return _fail(s.sql("show parameters like 'TIMEZONE'"))


@workload("src_create_dataframe")
def _(s, c):
    return _fail(_base(s))


@workload("src_create_dataframe_large")
def _(s, c):
    return _fail(
        s.create_dataframe([(i, str(i)) for i in range(20000)], schema=["ID", "NAME"])
    )


@workload("src_create_dataframe_dicts_and_createDataFrame")
def _(s, c):
    return _fail(s.createDataFrame([{"ID": 1, "NAME": "a"}]))


@workload("src_range")
def _(s, c):
    return _fail(s.range(5))


@workload("src_generator")
def _(s, c):
    return _fail(s.generator(seq4().alias("A"), rowcount=3))


@workload("src_table_function")
def _(s, c):
    return _fail(s.table_function("split_to_table", lit("a b"), lit(" ")))


@workload("src_flatten")
def _(s, c):
    return _fail(s.flatten(parse_json(lit("[1,2]"))))


_csv_schema = StructType(
    [StructField("ID", IntegerType()), StructField("NAME", StringType())]
)


@workload("src_read_csv")
def _(s, c):
    return _fail(s.read.schema(_csv_schema).csv(f"@{c.stage}/a.csv"))


@workload("src_read_json_options")
def _(s, c):
    return _fail(
        s.read.option("strip_outer_array", True)
        .schema(_csv_schema)
        .json(f"@{c.stage}/a.json")
    )


@workload("src_read_parquet")
def _(s, c):
    return _fail(s.read.schema(_csv_schema).parquet(f"@{c.stage}/a.parquet"))


@workload("src_read_format_load_options")
def _(s, c):
    return _fail(
        s.read.format("csv")
        .options({"field_delimiter": ","})
        .schema(_csv_schema)
        .load(f"@{c.stage}/a.csv")
    )


# ----------------------------------------------------------------------------- projections
@workload("op_select")
def _(s, c):
    return _fail(_base(s).select("ID", col("V") + 1))


@workload("op_select_expr")
def _(s, c):
    return _fail(_base(s).select_expr("ID", "V + 1").selectExpr("ID"))


@workload("op_with_column")
def _(s, c):
    return _fail(_base(s).with_column("X", col("ID") + 1).withColumn("Y", col("X") * 2))


@workload("op_with_columns")
def _(s, c):
    return _fail(_base(s).with_columns(["X", "Y"], [col("ID") + 1, col("V") * 2]))


@workload("op_with_column_renamed")
def _(s, c):
    return _fail(_base(s).with_column_renamed("ID", "I2").withColumnRenamed("I2", "I3"))


@workload("op_rename_to_df")
def _(s, c):
    return _fail(_base(s).to_df(["A", "B", "C"]).toDF("X", "Y", "Z"))


@workload("op_rename")
def _(s, c):
    return _fail(_base(s).rename("V", "W"))


@workload("op_drop")
def _(s, c):
    return _fail(_base(s).drop("V"))


@workload("op_alias")
def _(s, c):
    return _fail(_base(s).alias("T").select("T.ID"))


@workload("op_col_ilike", needs_simplifier=True)
def _(s, c):
    return _fail(_base(s).col_ilike("i%"))


# ----------------------------------------------------------------------------- row operations
@workload("op_filter_where")
def _(s, c):
    return _fail(_base(s).filter(col("ID") > 1).where("V > 1"))


@workload("op_sort")
def _(s, c):
    return _fail(_base(s).sort("ID").orderBy("V").order_by(col("NAME")))


@workload("op_limit")
def _(s, c):
    return _fail(_base(s).limit(2, offset=1))


@workload("op_table_sample")
def _(s, c):
    return _fail(s.table(c.table).sample(0.5))


@workload("op_distinct")
def _(s, c):
    return _fail(_base(s).distinct())


@workload("op_drop_duplicates")
def _(s, c):
    return _fail(_base(s).drop_duplicates("ID").dropDuplicates("NAME"))


@workload("op_sample")
def _(s, c):
    return _fail(_base(s).sample(0.5))


@workload("op_sample_by")
def _(s, c):
    return _fail(
        _base(s).sample_by(col("NAME"), {"a": 0.5}).sampleBy("NAME", {"b": 0.5})
    )


@workload("op_dropna_fillna_replace")
def _(s, c):
    return _fail(_base(s).dropna().fillna(0).replace(1, 5))


@workload("op_na_stat_functions")
def _(s, c):
    return _fail(
        _base(s)
        .na.drop()
        .na.fill(0)
        .na.replace(1, 5)
        .stat.sample_by("NAME", {"a": 0.5})
    )


@workload("op_crosstab")
def _(s, c):
    return _fail(_base(s).stat.crosstab("ID", "NAME"))


@workload("op_describe")
def _(s, c):
    return _fail(_base(s).describe())


@workload("op_cache_result")
def _(s, c):
    return _fail(_base(s).filter(col("ID") > 0).cache_result().select("ID"))


# ----------------------------------------------------------------------------- aggregation
@workload("op_group_by_agg")
def _(s, c):
    return _fail(_base(s).group_by("NAME").agg(sum_("V").alias("S")))


@workload("op_groupBy_functions")
def _(s, c):
    g = _base(s).groupBy("NAME")
    return _fail(
        g.count().union_all(g.sum("V").select("NAME", "SUM(V)")).select("NAME", "COUNT")
    )


@workload("op_grouped_min_max_avg_median")
def _(s, c):
    g = _base(s).group_by("NAME")
    return _fail(
        g.min("V")
        .join(g.max("V"), on="NAME")
        .join(g.avg("V"), on="NAME")
        .join(g.mean("V"), on="NAME")
        .join(g.median("V"), on="NAME")
    )


@workload("op_rollup")
def _(s, c):
    return _fail(_base(s).rollup("NAME").agg(sum_("V")))


@workload("op_cube")
def _(s, c):
    return _fail(_base(s).cube("NAME", "ID").agg(sum_("V")))


@workload("op_grouping_sets")
def _(s, c):
    from snowflake.snowpark import GroupingSets

    return _fail(
        _base(s)
        .group_by_grouping_sets(GroupingSets([col("NAME")], [col("ID")]))
        .agg(sum_("V"))
    )


@workload("op_df_agg")
def _(s, c):
    return _fail(_base(s).agg(sum_("V")))


@workload("op_pivot")
def _(s, c):
    return _fail(_base(s).select("ID", "NAME", "V").pivot("NAME", ["a", "b"]).sum("V"))


@workload("op_grouped_pivot")
def _(s, c):
    return _fail(_base(s).group_by("ID").pivot("NAME", ["a", "b"]).agg(sum_("V")))


@workload("op_unpivot")
def _(s, c):
    return _fail(_base(s).unpivot("VAL", "COL", ["ID", "V"]))


@workload("op_window")
def _(s, c):
    w = Window.partition_by("NAME").order_by("ID")
    return _fail(_base(s).with_column("RN", sum_("V").over(w)))


# ----------------------------------------------------------------------------- joins and set ops
@workload("op_join_inner")
def _(s, c):
    return _fail(_base(s).join(_other(s), on="ID", how="inner"))


@workload("op_join_condition")
def _(s, c):
    a, b = _base(s), _other(s)
    return _fail(a.join(b, a["ID"] == b["ID"], "left"))


@workload("op_join_variants")
def _(s, c):
    a, b = _base(s), _other(s)
    return _fail(
        a.join(b, on="ID", how="right")
        .join(b, on="ID", how="full")
        .join(b, on="ID", how="semi")
    )


@workload("op_cross_join")
def _(s, c):
    return _fail(
        _base(s)
        .cross_join(_other(s).select(col("TAG").alias("T2")))
        .crossJoin(_other(s).select(col("TAG").alias("T3")))
    )


@workload("op_natural_join")
def _(s, c):
    return _fail(_base(s).natural_join(_other(s)))


@workload("op_self_join")
def _(s, c):
    a = _base(s)
    return _fail(a.join(a.select(col("ID").alias("ID2")), col("ID") == col("ID2")))


@workload("op_join_table_function")
def _(s, c):
    df = s.create_dataframe([("a b",)], schema=["T"])
    return _fail(df.join_table_function("split_to_table", col("T"), lit(" ")))


@workload("op_lateral_join")
def _(s, c):
    a, b = _base(s), _other(s).select(col("TAG"))
    return _fail(a.lateral_join(b))


@workload("op_df_flatten")
def _(s, c):
    df = s.create_dataframe([("[1,2]",)], schema=["T"]).select(
        parse_json(col("T")).alias("J")
    )
    return _fail(df.flatten(col("J")))


@workload("op_union_family")
def _(s, c):
    a, b = _other(s), _other(s)
    return _fail(a.union(b).union_all(b).unionAll(b))


@workload("op_union_by_name_family")
def _(s, c):
    a, b = _other(s), _other(s)
    return _fail(
        a.union_by_name(b).unionByName(b).union_all_by_name(b).unionAllByName(b)
    )


@workload("op_intersect_except")
def _(s, c):
    a, b = _other(s), _other(s)
    return _fail(a.intersect(b).except_(b).minus(b).subtract(b))


@workload("op_random_split_first")
def _(s, c):
    parts = _base(s).random_split([0.5, 0.5])
    return _fail(parts[0])


@workload("op_to_df_then_union_with_self")
def _(s, c):
    a = _other(s).filter(col("ID") > 0)
    return _fail(a.union_all(a))


# ----------------------------------------------------------------------------- shapes
@workload("shape_chain_50")
def _(s, c):
    df = _base(s)
    for i in range(50):
        df = df.with_column(f"C{i}", col("ID") + i)
    return _fail(df)


@workload("shape_loop_reassign")
def _(s, c):
    df = _base(s)
    for i in range(5):
        df = df.with_column("NEW", df["ID"] + i)
    return _fail(df)


@workload("shape_diamond")
def _(s, c):
    base = _base(s).filter(col("ID") > 0)
    left = base.select("ID", "NAME")
    right = base.select(col("ID").alias("ID2"), "V")
    return _fail(left.join(right, col("ID") == col("ID2")))


@workload("shape_same_df_twice_in_expression")
def _(s, c):
    a = _base(s)
    return _fail(a.filter(col("ID").isin(a.select("ID").filter(col("ID") > 1))))


@workload("shape_multiline_call")
def _(s, c):
    df = (
        _base(s)
        .select(
            "ID",
            "NAME",
        )
        .with_column(
            "X",
            col("ID") + 1,
        )
    )
    return _fail(df)


@workload("shape_many_calls_one_line")
def _(s, c):
    return _fail(
        _base(s).filter(col("ID") > 0).select("ID", "NAME").sort("ID").limit(2)
    )


@workload("shape_lambda_call_site")
def _(s, c):
    f = lambda d: d.filter(col("ID") > 0).select("ID")  # noqa: E731
    return _fail(f(_base(s)))


@workload("shape_helper_functions")
def _(s, c):
    def add(d):
        return d.with_column("X", col("ID") + 1)

    def mid(d):
        return add(add(d)).filter(col("X") > 0)

    return _fail(mid(_base(s)))


@workload("shape_comprehension_call_site")
def _(s, c):
    dfs = [_base(s).with_column("K", lit(i)) for i in range(3)]
    out = dfs[0]
    for d in dfs[1:]:
        out = out.union_all(d)
    return _fail(out)


@workload("shape_wide_projection")
def _(s, c):
    df = _base(s)
    return _fail(df.select(*[(col("ID") + i).alias(f"W{i}") for i in range(200)]))


@workload("shape_udf_in_pipeline")
def _(s, c):
    f = udf(lambda x: x + 1, return_type=IntegerType(), input_types=[IntegerType()])
    return _fail(_base(s).with_column("U", f(col("ID"))))


# ----------------------------------------------------------------------------- error kinds
@workload("err_typing_with_error_line", error_kind="typing")
def _(s, c):
    return _base(s).with_column("BAD", col("NAME") + lit(1)).filter(col("ID") > 0)


@workload("err_typing_in_flattened_select", error_kind="typing")
def _(s, c):
    return _base(s).select("ID", (col("NAME") + lit(1)).alias("X")).select("X")


@workload("err_invalid_identifier_after_rename", error_kind="invalid_identifier")
def _(s, c):
    return _base(s).select(col("ID").alias("RENAMED")).select("ID")


@workload("err_missing_table", error_kind="missing_object")
def _(s, c):
    return s.table("DF_TRACE_NO_SUCH_TABLE").select("A")


@workload("err_missing_table_in_join", error_kind="missing_object")
def _(s, c):
    return _base(s).join(s.table("DF_TRACE_NO_SUCH_TABLE"), on="ID")


def _name(prefix):
    return Ctx(table="", stage="").new_name(prefix)


# ----------------------------------------------------------------------------- actions
def _action_workload(name, action, **kw):
    WORKLOADS.append(
        Workload(name=name, build=lambda s, c: _fail(_base(s)), action=action, **kw)
    )


_action_workload("act_count", lambda d: d.count())
_action_workload("act_show", lambda d: d.show())
_action_workload("act_first", lambda d: d.first())
_action_workload("act_to_pandas", lambda d: d.to_pandas())
_action_workload("act_to_pandas_batches", lambda d: list(d.to_pandas_batches()))
_action_workload("act_to_local_iterator", lambda d: list(d.to_local_iterator()))
_action_workload("act_cache_result", lambda d: d.cache_result())
_action_workload("act_print_schema", lambda d: d.print_schema())
_action_workload("act_stat_corr_cov", lambda d: d.stat.corr("ID", "V"))
_action_workload(
    "act_stat_approx_quantile", lambda d: d.stat.approx_quantile("ID", [0.5])
)
_action_workload(
    "act_create_or_replace_view",
    lambda d: d.create_or_replace_view(_name("DF_TRACE_V")),
)
_action_workload(
    "act_create_or_replace_temp_view",
    lambda d: d.create_or_replace_temp_view(_name("DF_TRACE_V")),
)
_action_workload(
    "act_create_temp_view", lambda d: d.create_temp_view(_name("DF_TRACE_V"))
)
_action_workload(
    "act_write_save_as_table",
    lambda d: d.write.mode("overwrite").save_as_table(_name("DF_TRACE_T")),
)
_action_workload(
    "act_write_copy_into_location",
    lambda d: d.write.copy_into_location(
        "@~/df_trace_out/", file_format_type="csv", overwrite=True
    ),
)
_action_workload(
    "act_write_csv_json_parquet",
    lambda d: d.write.option("overwrite", True).csv("@~/df_trace_out_csv/"),
)
_action_workload(
    "act_write_json", lambda d: d.write.json("@~/df_trace_out_json/", overwrite=True)
)
_action_workload(
    "act_write_parquet",
    lambda d: d.write.parquet("@~/df_trace_out_pq/", overwrite=True),
)
_action_workload(
    "act_write_partition_by_format_save",
    lambda d: d.write.format("csv")
    .partition_by("ID")
    .save("@~/df_trace_out_p/", mode="overwrite"),
)
_action_workload(
    "act_write_options",
    lambda d: d.write.options({"overwrite": True}).csv("@~/df_trace_out_opts/"),
)
_action_workload(
    "act_write_insert_into", lambda d: d.write.insert_into(CURRENT_CTX[0].table)
)


# ----------------------------------------------------------------------------- already exists
@workload("err_exists_sql", error_kind="already_exists")
def _(s, c):
    name = c.new_name("DF_TRACE_EX")
    s.sql(f"create or replace temp table {name} (a int)").collect()
    return s.sql(f"create temp table {name} (a int)")


@workload("err_exists_temp_view_twice", error_kind="already_exists")
def _(s, c):
    name = c.new_name("DF_TRACE_EXV")
    d = _base(s).select("ID")
    d.create_temp_view(name)
    d.create_temp_view(name)
    return d


@workload("err_exists_save_as_table_errorifexists", error_kind="already_exists")
def _(s, c):
    name = c.new_name("DF_TRACE_EXT")
    d = _base(s).select("ID")
    d.write.mode("overwrite").save_as_table(name, table_type="temp")
    d.write.mode("errorifexists").save_as_table(name, table_type="temp")
    return d


# ----------------------------------------------------------------------------- GAP-1: dataframes inside expressions
@workload("expr_isin_subquery_in_filter")
def _(s, c):
    a, b = _base(s), _other(s).select("ID")
    return _fail(a.filter(col("ID").isin(b)))


@workload("expr_isin_nested_in_boolean")
def _(s, c):
    a, b = _base(s), _other(s).select("ID")
    return _fail(a.filter((col("ID").isin(b)) & (col("V") > 0) | (col("ID") == 3)))


@workload("expr_isin_in_when_otherwise")
def _(s, c):
    from snowflake.snowpark.functions import when

    a, b = _base(s), _other(s).select("ID")
    return _fail(a.with_column("F", when(col("ID").isin(b), lit(1)).otherwise(lit(0))))


@workload("expr_isin_in_select")
def _(s, c):
    a, b = _base(s), _other(s).select("ID")
    return _fail(a.select("ID", col("ID").isin(b).alias("IN_B")))


@workload("expr_isin_in_join_condition")
def _(s, c):
    a, b, sub = _base(s), _other(s), _other(s).select(col("ID").alias("X"))
    return _fail(a.join(b, (a["ID"] == b["ID"]) & a["ID"].isin(sub)))


@workload("expr_subquery_reused_in_later_operation")
def _(s, c):
    a, b = _base(s), _other(s).select("ID")
    filtered = a.filter(col("ID").isin(b))
    return _fail(filtered.select("ID", "NAME").sort("ID"))


@workload("expr_same_dataframe_as_receiver_and_subquery")
def _(s, c):
    a = _base(s)
    return _fail(a.filter(col("ID").isin(a.select("ID"))))


@workload("expr_two_subqueries_one_call")
def _(s, c):
    a = _base(s)
    b, d = _other(s).select("ID"), _other(s).select(col("ID").alias("ID"))
    return _fail(a.filter(col("ID").isin(b) | col("ID").isin(d)))


@workload("expr_isin_in_sort")
def _(s, c):
    a, b = _base(s), _other(s).select("ID")
    return _fail(a.sort(col("ID").isin(b)))


# ----------------------------------------------------------------------------- GAP-2: functions applied by the operation
@workload("fn_udf_lambda")
def _(s, c):
    f = udf(lambda x: x + 1, return_type=IntegerType(), input_types=[IntegerType()])
    return _fail(_base(s).with_column("U", f(col("ID"))))


@workload("fn_udf_decorator")
def _(s, c):
    @udf(return_type=IntegerType(), input_types=[IntegerType()])
    def plus_one(x):
        return x + 1

    return _fail(_base(s).select(plus_one(col("ID")).alias("U")))


@workload("fn_udf_register_named")
def _(s, c):
    f = s.udf.register(
        lambda x: x * 2,
        return_type=IntegerType(),
        input_types=[IntegerType()],
        name=_name("DF_TRACE_UDF"),
        is_permanent=False,
        replace=True,
    )
    return _fail(_base(s).filter(f(col("ID")) > 0))


@workload("fn_udf_vectorized")
def _(s, c):
    from snowflake.snowpark.functions import pandas_udf
    from snowflake.snowpark.types import PandasSeriesType

    f = pandas_udf(
        lambda x: x + 1,
        return_type=PandasSeriesType(IntegerType()),
        input_types=[PandasSeriesType(IntegerType())],
    )
    return _fail(_base(s).with_column("U", f(col("ID"))))


@workload("fn_udf_used_twice")
def _(s, c):
    f = udf(lambda x: x + 1, return_type=IntegerType(), input_types=[IntegerType()])
    df = _base(s).with_column("U", f(col("ID")))
    return _fail(df.filter(f(col("U")) > 0))


@workload("fn_udf_nested_in_expression")
def _(s, c):
    f = udf(lambda x: x + 1, return_type=IntegerType(), input_types=[IntegerType()])
    return _fail(_base(s).with_column("U", (f(col("ID")) * 2 + 1).cast("int")))


@workload("fn_udtf_join_table_function")
def _(s, c):
    from snowflake.snowpark.functions import udtf
    from snowflake.snowpark.types import StructField, StructType

    class Echo:
        def process(self, x):
            yield (x,)

    t = udtf(
        Echo,
        output_schema=StructType([StructField("E", IntegerType())]),
        input_types=[IntegerType()],
    )
    return _fail(_base(s).join_table_function(t(col("ID"))))


@workload("fn_udtf_session_table_function")
def _(s, c):
    from snowflake.snowpark.functions import udtf
    from snowflake.snowpark.types import StructField, StructType

    class Count:
        def process(self, n):
            for i in range(n):
                yield (i,)

    t = udtf(
        Count,
        output_schema=StructType([StructField("I", IntegerType())]),
        input_types=[IntegerType()],
    )
    return _fail(s.table_function(t(lit(3))))


@workload("fn_udaf_in_agg")
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

    agg = udaf(Summer, return_type=IntegerType(), input_types=[IntegerType()])
    return _fail(_base(s).group_by("NAME").agg(agg(col("ID")).alias("S")))


# ----------------------------------------------------------------------------- GAP-3: reused plans under CTE
@workload("cte_union_all_self")
def _(s, c):
    a = _other(s).filter(col("ID") > 0)
    return _fail(a.union_all(a))


@workload("cte_self_join_reused")
def _(s, c):
    a = _base(s).filter(col("ID") > 0)
    return _fail(a.join(a.select(col("ID").alias("I2")), col("ID") == col("I2")))


@workload("cte_describe_reused")
def _(s, c):
    d = _base(s).describe()
    return _fail(d.union_all(d))


@workload("cte_large_create_dataframe_reused")
def _(s, c):
    big = s.create_dataframe([(i, str(i)) for i in range(20000)], schema=["ID", "NAME"])
    return _fail(big.union_all(big.filter(col("ID") > 5)))


@workload("cte_three_way_reuse")
def _(s, c):
    a = _other(s).with_column("K", lit(1))
    return _fail(a.union_all(a).union_all(a))


@workload("cte_crosstab_reused")
def _(s, c):
    x = _base(s).stat.crosstab("ID", "NAME")
    return _fail(x.union_all(x))


@workload("fn_udf_register_from_file")
def _(s, c):
    import os
    import tempfile

    path = os.path.join(tempfile.mkdtemp(), "df_trace_udf.py")
    with open(path, "w") as f:
        f.write("def plus_one(x):\n    return x + 1\n")
    f = s.udf.register_from_file(
        path, "plus_one", return_type=IntegerType(), input_types=[IntegerType()]
    )
    return _fail(_base(s).with_column("U", f(col("ID"))))


@workload(
    "fn_udf_registration_describe",
    no_ast_oracle="udf.describe raises missing-AST errors when the AST is enabled",
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


@workload("fn_udtf_register_from_file_and_describe")
def _(s, c):
    from snowflake.snowpark.types import StructField, StructType

    t = s.udtf.register_from_file(
        _source_file("df_trace_udtf.py", _UDTF_SOURCE),
        "Echo",
        output_schema=StructType([StructField("E", IntegerType())]),
        input_types=[IntegerType()],
        name=_name("DF_TRACE_UDTF"),
        is_permanent=False,
        replace=True,
    )
    return _fail(_base(s).join_table_function(t(col("ID"))))


@workload(
    "fn_udaf_register_from_file_and_describe",
    no_ast_oracle="udaf.describe raises missing-AST errors when the AST is enabled",
)
def _(s, c):
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
    return _fail(_base(s).group_by("NAME").agg(agg(col("ID")).alias("S")))
