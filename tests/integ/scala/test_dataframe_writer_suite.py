#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import copy

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark._internal.analyzer.analyzer_utils import quote_name
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import lit, object_construct
from snowflake.snowpark.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from tests.utils import TestData, TestFiles, Utils


def test_write_to_csv(session):
    target_stage_name = Utils.random_stage_name()
    Utils.create_stage(session, target_stage_name)
    df = TestData.test_data1(session)
    df.write.csv(f"@{target_stage_name}/test1.csv")
    res = (
        session.read.schema(df.schema).csv(f"@{target_stage_name}/test1.csv").collect()
    )
    assert res == [Row(1, True, "a"), Row(2, False, "b")]


def test_write_to_csv_using_save(session):
    target_stage_name = Utils.random_stage_name()
    Utils.create_stage(session, target_stage_name)
    df = TestData.test_data1(session)
    df.write.save(
        f"@{target_stage_name}/test1.csv",
        format="csv",
        mode="overwrite",
        partitionBy=df.schema.fields[0].name,
        block=True,
        sep=",",
    )
    res = (
        session.read.schema(df.schema).csv(f"@{target_stage_name}/test1.csv").collect()
    )
    assert res == [Row(NUM=1, BOOL=True, STR="a"), Row(NUM=2, BOOL=False, STR="b")]


def test_write_to_csv_using_save_options(session):
    target_stage_name = Utils.random_stage_name()
    Utils.create_stage(session, target_stage_name)
    df = TestData.test_data1(session)
    df.write.options(
        {
            "format": "csv",
            "mode": "overwrite",
            "partitionBy": df.schema.fields[0].name,
            "sep": ",",
        }
    ).save(f"@{target_stage_name}/test1.csv")
    res = (
        session.read.schema(df.schema).csv(f"@{target_stage_name}/test1.csv").collect()
    )
    assert res == [Row(1, True, "a"), Row(2, False, "b")]


def test_write_to_format_csv(session):
    target_stage_name = Utils.random_stage_name()
    Utils.create_stage(session, target_stage_name)
    df = TestData.test_data1(session)
    df.write.format("csv").csv(f"@{target_stage_name}/test1.csv")
    res = (
        session.read.schema(df.schema).csv(f"@{target_stage_name}/test1.csv").collect()
    )
    assert res == [Row(1, True, "a"), Row(2, False, "b")]


def test_write_to_json(session):
    target_stage_name = Utils.random_stage_name()
    target_stage_name = "stage1"
    Utils.create_stage(session, target_stage_name, is_temporary=False)
    df = TestData.test_data1(session)
    df.select(
        object_construct(lit("NUM"), "NUM", lit("BOOL"), "BOOL", lit("STR"), "STR")
    ).write.json(f"@{target_stage_name}/test1.json")
    res = session.read.json(f"@{target_stage_name}/test1.json").collect()
    assert res == [
        Row('{\n  "BOOL": true,\n  "NUM": 1,\n  "STR": "a"\n}'),
        Row('{\n  "BOOL": false,\n  "NUM": 2,\n  "STR": "b"\n}'),
    ]


def test_write_to_parquet(session):
    target_stage_name = Utils.random_stage_name()
    Utils.create_stage(session, target_stage_name)
    df = TestData.test_data1(session)
    df.write.parquet(f"@{target_stage_name}/test1.parquet")
    res = session.read.parquet(f"@{target_stage_name}/test1.parquet").collect()
    assert res == [Row(1, True, "a"), Row(2, False, "b")]


def test_write_with_target_column_name_order(session):
    table_name = Utils.random_table_name()
    Utils.create_table(session, table_name, "a int, b int", is_temporary=True)
    try:
        df1 = session.create_dataframe([[1, 2]], schema=["b", "a"])

        # By default, it is by index
        df1.write.save_as_table(table_name, mode="append", table_type="temp")
        Utils.check_answer(session.table(table_name), [Row(1, 2)])

        # Explicitly use "index"
        session._conn.run_query(f"truncate table {table_name}", log_on_exception=True)
        df1.write.save_as_table(
            table_name, mode="append", column_order="index", table_type="temp"
        )
        Utils.check_answer(session.table(table_name), [Row(1, 2)])

        # use order by "name"
        session._conn.run_query(f"truncate table {table_name}", log_on_exception=True)
        df1.write.save_as_table(
            table_name, mode="append", column_order="name", table_type="temp"
        )
        Utils.check_answer(session.table(table_name), [Row(2, 1)])

        # If target table doesn't exists, "order by name" is not actually used.
        Utils.drop_table(session, table_name)
        df1.write.saveAsTable(table_name, mode="append", column_order="name")
        Utils.check_answer(session.table(table_name), [Row(1, 2)])
    finally:
        Utils.drop_table(session, table_name)

    # column name and table name with special characters
    special_table_name = '"test table name"'
    Utils.create_table(
        session, special_table_name, '"a a" int, "b b" int', is_temporary=True
    )
    try:
        df2 = session.create_dataframe([(1, 2)]).to_df("b b", "a a")
        df2.write.save_as_table(
            special_table_name, mode="append", column_order="name", table_type="temp"
        )
        Utils.check_answer(session.table(special_table_name), [Row(2, 1)])
    finally:
        Utils.drop_table(session, special_table_name)


def test_write_with_target_table_autoincrement(
    session,
):  # Scala doesn't support this yet.
    table_name = Utils.random_table_name()
    Utils.create_table(
        session, table_name, "a int, b int, c int autoincrement", is_temporary=True
    )
    try:
        df1 = session.create_dataframe([[1, 2]], schema=["b", "a"])
        df1.write.save_as_table(
            table_name, mode="append", column_order="name", table_type="temp"
        )
        Utils.check_answer(session.table(table_name), [Row(2, 1, 1)])
    finally:
        Utils.drop_table(session, table_name)


def test_negative_write_with_target_column_name_order(session):
    table_name = Utils.random_table_name()
    Utils.create_table(session, table_name, "a int, b int", is_temporary=True)
    try:
        df1 = session.create_dataframe([[1, 2]], schema=["a", "c"])
        # The "columnOrder = name" needs the DataFrame has the same column name set
        with pytest.raises(SnowparkSQLException, match="invalid identifier 'C'"):
            df1.write.save_as_table(
                table_name, mode="append", column_order="name", table_type="temp"
            )

        for column_order in ("any_value", "", None):
            with pytest.raises(
                ValueError, match="'column_order' must be either 'name' or 'index'"
            ):
                df1.write.save_as_table(
                    table_name,
                    mode="append",
                    column_order=column_order,
                    table_type="temp",
                )
    finally:
        Utils.drop_table(session, table_name)


def test_write_with_target_column_name_order_all_kinds_of_dataframes(
    session, resources_path
):
    table_name = Utils.random_table_name()
    Utils.create_table(session, table_name, "a int, b int", is_temporary=True)
    try:
        df1 = session.create_dataframe([[1, 2]], schema=["b", "a"])
        # DataFrame.cache_result()
        df_cached = df1.cache_result()
        df_cached.write.save_as_table(
            table_name, mode="append", column_order="name", table_type="temp"
        )
        Utils.check_answer(session.table(table_name), [Row(2, 1)])

        # copy DataFrame
        session._conn.run_query(f"truncate table {table_name}", log_on_exception=True)
        df_cloned = copy.copy(df1)
        df_cloned.write.save_as_table(
            table_name, mode="append", column_order="name", table_type="temp"
        )
        Utils.check_answer(session.table(table_name), [Row(2, 1)])

        # large local relation
        session._conn.run_query(f"truncate table {table_name}", log_on_exception=True)
        large_df = session.create_dataframe([[1, 2]] * 1024, schema=["b", "a"])
        large_df.write.save_as_table(
            table_name, mode="append", column_order="name", table_type="temp"
        )
        rows = session.table(table_name).collect()
        assert len(rows) == 1024
        for row in rows:
            assert row["B"] == 1 and row["A"] == 2
    finally:
        Utils.drop_table(session, table_name)

    # show tables
    # Create a DataFrame from SQL `show tables` and then filter on it not supported yet. Enable the following test after it's supported.
    # try:
    #     show_table_fields = session.sql("show tables").schema.fields
    #     columns = ", ".join(f"{analyzer_utils.quote_name(f.name)} {type_utils.convert_sp_to_sf_type(f.datatype)}" for f in show_table_fields)
    #     # exchange column orders: "name"(1) <-> "kind"(4)
    #     schema_string = columns \
    #         .replace("\"kind\"", "test_place_holder") \
    #         .replace("\"name\"", "\"kind\"") \
    #         .replace("test_place_holder", "\"name\"")
    #     Utils.create_table(session, table_name, schema_string, is_temporary=True)
    #     session.sql("show tables").write.save_as_table(table_name, mode="append", column_order="name", table_type="temp")
    #     # In "show tables" result, "name" is 2nd column.
    #     # In the target table, "name" is the 4th column.
    #     assert(session.table(table_name).collect()[0][4].contains(table_name))
    # finally:
    #     Utils.drop_table(session, table_name)

    # Read file, table columns are in reverse order
    source_stage_name = Utils.random_stage_name()
    target_stage_name = Utils.random_stage_name()
    Utils.create_stage(session, source_stage_name)
    Utils.create_stage(session, target_stage_name)
    Utils.create_table(
        session, table_name, "c double, b string, a int", is_temporary=True
    )
    try:
        test_files = TestFiles(resources_path)
        test_file_on_stage = f"@{source_stage_name}/testCSV.csv"
        Utils.upload_to_stage(
            session, source_stage_name, test_files.test_file_csv, compress=False
        )
        user_schema = StructType(
            [
                StructField("a", IntegerType()),
                StructField("b", StringType()),
                StructField("c", DoubleType()),
            ]
        )
        df_readfile = session.read.schema(user_schema).csv(test_file_on_stage)
        Utils.check_answer(df_readfile, [Row(1, "one", 1.2), Row(2, "two", 2.2)])
        df_readfile.write.save_as_table(
            table_name, mode="append", column_order="name", table_type="temp"
        )
        Utils.check_answer(
            session.table(table_name), [Row(1.2, "one", 1), Row(2.2, "two", 2)]
        )
        # read with copy options
        df_readcopy = (
            session.read.schema(user_schema)
            .option("PURGE", False)
            .csv(test_file_on_stage)
        )
        session._conn.run_query(f"truncate table {table_name}", log_on_exception=True)
        df_readcopy.write.save_as_table(
            table_name, mode="append", column_order="name", table_type="temp"
        )
        Utils.check_answer(
            session.table(table_name), [Row(1.2, "one", 1), Row(2.2, "two", 2)]
        )
    finally:
        Utils.drop_table(session, table_name)
        Utils.drop_stage(session, source_stage_name)
        Utils.drop_stage(session, target_stage_name)


def test_write_table_names(session, db_parameters):
    schema = quote_name(db_parameters["schema"])

    def create_and_append_check_answer(table_name, full_table_name=None):
        try:
            assert session._table_exists(table_name) is False
            Utils.create_table(session, full_table_name or table_name, "a int, b int")
            assert session._table_exists(table_name) is True

            df = session.create_dataframe([[1, 2]], schema=["a", "b"])
            df.write.save_as_table(table_name, mode="append", table_type="temp")
            Utils.check_answer(session.table(table_name), [Row(1, 2)])
        finally:
            session.sql(f"drop table if exists {full_table_name or table_name}")

    # basic scenario
    table_name = f"{Utils.random_table_name()}"
    create_and_append_check_answer(table_name)

    # table name containing dot (.)
    table_name = f'"{Utils.random_table_name()}.{Utils.random_table_name()}"'
    create_and_append_check_answer(table_name)

    # table name containing quotes
    table_name = f'"""{Utils.random_table_name()}"""'
    create_and_append_check_answer(table_name)

    # table name containing quotes and dot
    table_name = f'"""{Utils.random_table_name()}...{Utils.random_table_name()}"""'
    create_and_append_check_answer(table_name)

    # schema + basic table name
    table_name = f"{Utils.random_table_name()}"
    full_table_name = f"{schema}.{table_name}"
    raw_table_name = [schema, table_name]
    create_and_append_check_answer(raw_table_name, full_table_name)

    # schema + table naming containg dots
    table_name = f'"{Utils.random_table_name()}.{Utils.random_table_name()}"'
    full_table_name = f"{schema}.{table_name}"
    raw_table_name = [schema, table_name]
    create_and_append_check_answer(raw_table_name, full_table_name)

    # schema + table name containing dots and quotes
    table_name = f'"""{Utils.random_table_name()}...{Utils.random_table_name()}"""'
    full_table_name = f"{schema}.{table_name}"
    raw_table_name = [schema, table_name]
    create_and_append_check_answer(raw_table_name, full_table_name)
