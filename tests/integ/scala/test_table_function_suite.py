#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark import Row
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.functions import array_agg, col, lit, parse_json
from snowflake.snowpark.types import StructField, StructType, VariantType
from tests.utils import Utils


def test_dataframe_join_table_function(session):
    df = session.create_dataframe(["[1,2]", "[3,4]"], schema=["a"])
    Utils.check_answer(
        df.join_table_function("flatten", input=parse_json(df["a"])).select("value"),
        [Row("1"), Row("2"), Row("3"), Row("4")],
        sort=False,
    )

    Utils.check_answer(
        [Row("[1"), Row("2]"), Row("[3"), Row("4]")],
        df.join_table_function("split_to_table", df["a"], lit(",")).select("value"),
        sort=False,
    )


def test_session_table_function(session):
    Utils.check_answer(
        session.table_function("flatten", input=parse_json(lit("[1,2]"))).select(
            "value"
        ),
        [Row("1"), Row("2")],
        sort=False,
    )

    Utils.check_answer(
        session.table_function(
            "split_to_table", lit("split by space"), lit(" ")
        ).select("value"),
        [Row("split"), Row("by"), Row("space")],
        sort=False,
    )


def test_schema_string_lateral_join_flatten_function_array(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        df = session.create_dataframe(["a", "b"], schema=["values"])
        agg_df = df.agg(array_agg(col("values")).alias("value"))
        agg_df.write.mode("Overwrite").save_as_table(table_name)
        table = session.table(table_name)
        flattened = table.flatten(table["value"])
        Utils.check_answer(
            flattened.select(table["value"], flattened["value"].as_("newValue")),
            [Row('[\n  "a",\n  "b"\n]', '"a"'), Row('[\n  "a",\n  "b"\n]', '"b"')],
        )
    finally:
        Utils.drop_table(session, table_name)


def test_schema_string_lateral_join_flatten_function_object(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        df = session.create_dataframe([Row(value={"a": "b"})])
        df.write.mode("Overwrite").save_as_table(table_name)
        table = session.table(table_name)
        flattened = table.flatten(table["value"])
        Utils.check_answer(
            flattened.select(table["value"], flattened["value"].as_("newValue")),
            [Row('{\n  "a": "b"\n}', '"b"')],
        )
    finally:
        Utils.drop_table(session, table_name)


def test_schema_string_lateral_join_flatten_function_variant(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        df = session.create_dataframe(
            [Row(value={"a": "b"})],
            schema=StructType([StructField("value", VariantType())]),
        )
        df.write.mode("Overwrite").save_as_table(table_name)
        table = session.table(table_name)
        flattened = table.flatten(table["value"])
        Utils.check_answer(
            flattened.select(table["value"], flattened["value"].as_("newValue")),
            [Row('{\n  "a": "b"\n}', '"b"')],
        )
    finally:
        Utils.drop_table(session, table_name)
