#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
# TODO: add more table function tests here after adding table functions other than df.flatten and session.flatten.
from test.utils import Utils

from snowflake.snowpark import Row
from snowflake.snowpark.functions import array_agg, col


def test_schema_string_lateral_join_flatten_function(session):
    table_name = Utils.random_name()
    try:
        df = session.createDataFrame(["a", "b"], schema=["values"])
        agg_df = df.agg(array_agg(col("values")).alias("value"))
        agg_df.write.mode("Overwrite").saveAsTable(table_name)
        table = session.table(table_name)
        flattened = table.flatten(table["value"])
        Utils.check_answer(
            flattened.select(table["value"], flattened["value"].as_("newValue")),
            [Row('[\n  "a",\n  "b"\n]', '"a"'), Row('[\n  "a",\n  "b"\n]', '"b"')],
        )
    finally:
        Utils.drop_table(session, table_name)
