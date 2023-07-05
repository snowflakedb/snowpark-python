#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import copy

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark._internal.utils import parse_table_name
from snowflake.snowpark.exceptions import (
    SnowparkDataframeWriterException,
    SnowparkSQLException,
)
from snowflake.snowpark.functions import col, lit, object_construct
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
        session.read.schema(df.schema)
        .csv(f"@{target_stage_name}/test1.csv")
        .orderBy("NUM")
        .collect()
    )
    assert res == [Row(1, True, "a"), Row(2, False, "b")]


def test_write_to_csv_ignore_overwrite_when_non_bool_non_str(session):
    target_stage_name = Utils.random_stage_name()
    Utils.create_stage(session, target_stage_name)
    df = TestData.test_data1(session)
    df.write.csv(f"@{target_stage_name}/test1.csv", overwrite={})
    res = (
        session.read.schema(df.schema)
        .csv(f"@{target_stage_name}/test1.csv")
        .orderBy("NUM")
        .collect()
    )
    assert res == [Row(1, True, "a"), Row(2, False, "b")]


def test_write_to_csv_normal_options_non_aliases(session):
    target_stage_name = Utils.random_stage_name()
    Utils.create_stage(session, target_stage_name)
    df = TestData.test_data1(session)
    df.write.option("FIELD_DELIMITER", ";").option("RECORD_DELIMITER", "\n").option(
        "FIELD_OPTIONALLY_ENCLOSED_BY", "'"
    ).option("NULL_IF", "b").option("DATE_FORMAT", "YYYY-MM-DD").csv(
        f"@{target_stage_name}/test_csv_normal_options_non_aliases.csv", overwrite={}
    )
    res = (
        session.read.schema(df.schema)
        .option("FIELD_DELIMITER", ";")
        .option("RECORD_DELIMITER", "\n")
        .option("FIELD_OPTIONALLY_ENCLOSED_BY", "'")
        .option("NULL_IF", "b")
        .csv(f"@{target_stage_name}/test_csv_normal_options_non_aliases.csv")
        .orderBy("NUM")
        .collect()
    )
    assert res == [Row(1, True, "a"), Row(2, False, None)]


def test_write_to_csv_with_aliases(session):
    target_stage_name = Utils.random_stage_name()
    Utils.create_stage(session, target_stage_name)
    df = TestData.test_data1(session)
    df.write.option("DELIMITER", "x").option("SEP", ";").option("LINESEP", "\n").option(
        "QUOTE", "'"
    ).option("NULLVALUE", "b").option("DATEFORMAT", "YYYY-MM-DD").csv(
        f"@{target_stage_name}/test1.csv", mode="overwrite"
    )
    res = (
        session.read.schema(df.schema)
        .option("SEP", ";")
        .option("LINESEP", "\n")
        .option("QUOTE", "'")
        .option("NULLVALUE", "b")
        .option("DATEFORMAT", "YYYY-MM-DD")
        .csv(f"@{target_stage_name}/test1.csv")
        .orderBy("NUM")
        .collect()
    )
    assert res == [Row(1, True, "a"), Row(2, False, None)]


def test_write_to_csv_with_options(session):
    target_stage_name = Utils.random_stage_name()
    Utils.create_stage(session, target_stage_name)
    df = TestData.test_data1(session)
    df.write.option("path", f"@{target_stage_name}/test1.csv").option(
        "overwrite", True
    ).option("single", True).option("header", True).option(
        "MAX_FILE_SIZE", 5000
    ).option(
        "DETAILED_OUTPUT", True
    ).csv(
        f"@{target_stage_name}/test2.csv"
    )
    df.write.option("path", f"@{target_stage_name}/test1.csv").option(
        "overwrite", True
    ).option("overwrite", False).option("overwrite", "True").option(
        "overwrite", "False"
    ).option(
        "single", False
    ).option(
        "header", True
    ).csv(
        f"@{target_stage_name}/test1.csv",
        format="csv",
        mode="overwrite",
        partitionBy=col(df.schema.fields[0].name),
        block=True,
        sep=",",
    )
    res = (
        session.read.schema(df.schema)
        .option("header", True)
        .csv(f"@{target_stage_name}/test1.csv")
        .orderBy("NUM")
        .collect()
    )
    assert res == [Row(1, True, "a"), Row(2, False, "b")]


def test_write_to_csv_using_save(session):
    target_stage_name = Utils.random_stage_name()
    Utils.create_stage(session, target_stage_name)
    df = TestData.test_data1(session)
    df.write.option("path", f"@{target_stage_name}/test1.csv").save(
        format="csv",
        mode="overwrite",
        partitionBy=df.schema.fields[0].name,
        block=True,
        sep=",",
        overwrite=True,
    )
    res = (
        session.read.schema(df.schema)
        .csv(f"@{target_stage_name}/test1.csv")
        .orderBy("NUM")
        .collect()
    )
    assert res == [Row(NUM=1, BOOL=True, STR="a"), Row(NUM=2, BOOL=False, STR="b")]


def test_write_to_csv_using_save_without_fileformat_should_fail(session):
    with pytest.raises(
        SnowparkDataframeWriterException,
        match="DataFrameWriter.format()",
    ):
        target_stage_name = Utils.random_stage_name()
        Utils.create_stage(session, target_stage_name)
        df = TestData.test_data1(session)
        df.write.option("path", f"@{target_stage_name}/test1.csv").save(
            mode="overwrite",
            partitionBy=df.schema.fields[0].name,
            block=True,
            sep=",",
            overwrite=True,
        )


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
        session.read.schema(df.schema)
        .csv(f"@{target_stage_name}/test1.csv")
        .orderBy("NUM")
        .collect()
    )
    assert res == [Row(1, True, "a"), Row(2, False, "b")]


def test_write_to_format_csv(session):
    target_stage_name = Utils.random_stage_name()
    Utils.create_stage(session, target_stage_name)
    df = TestData.test_data1(session)
    df.write.format("csv").csv(f"@{target_stage_name}/test1.csv")
    res = (
        session.read.schema(df.schema)
        .csv(f"@{target_stage_name}/test1.csv")
        .orderBy("NUM")
        .collect()
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
    res = session.read.json(f"@{target_stage_name}/test1.json")
    res = (
        session.read.json(f"@{target_stage_name}/test1.json")
        .orderBy(col("$1")["NUM"])
        .collect()
    )
    assert res == [
        Row('{\n  "BOOL": true,\n  "NUM": 1,\n  "STR": "a"\n}'),
        Row('{\n  "BOOL": false,\n  "NUM": 2,\n  "STR": "b"\n}'),
    ]


def test_write_to_parquet(session):
    target_stage_name = Utils.random_stage_name()
    Utils.create_stage(session, target_stage_name)
    df = TestData.test_data1(session)
    df.write.parquet(f"@{target_stage_name}/test1.parquet")
    res = session.read.parquet(f"@{target_stage_name}/test1.parquet")
    res = res.orderBy(res.columns[0]).collect()
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
    database = session.get_current_database().replace('"', "")
    schema = f"schema_{Utils.random_alphanumeric_str(10)}"
    double_quoted_schema = f'"{schema}.{schema}"'

    def create_and_append_check_answer(table_name_input):
        parsed_table_name_array = (
            parse_table_name(table_name_input)
            if isinstance(table_name_input, str)
            else table_name_input
        )
        full_table_name_str = (
            ".".join(table_name_input)
            if not isinstance(table_name_input, str)
            else table_name_input
        )
        try:
            assert session._table_exists(parsed_table_name_array) is False
            Utils.create_table(session, full_table_name_str, "a int, b int")
            assert session._table_exists(parsed_table_name_array) is True
            assert session.table(full_table_name_str).count() == 0

            df = session.create_dataframe([[1, 2]], schema=["a", "b"])
            df.write.save_as_table(table_name_input, mode="append", table_type="temp")
            Utils.check_answer(session.table(table_name_input), [Row(1, 2)])
        finally:
            session._run_query(f"drop table if exists {full_table_name_str}")

    try:
        Utils.create_schema(session, schema)
        Utils.create_schema(session, double_quoted_schema)
        # basic scenario
        table_name = f"{Utils.random_table_name()}"
        create_and_append_check_answer(table_name)

        # schema.table
        create_and_append_check_answer(f"{schema}.{Utils.random_table_name()}")

        # database.schema.table
        create_and_append_check_answer(
            f"{database}.{schema}.{Utils.random_table_name()}"
        )

        # database..table
        create_and_append_check_answer(f"{database}..{Utils.random_table_name()}")

        # table name containing dot (.)
        table_name = f'"{Utils.random_table_name()}.{Utils.random_table_name()}"'
        create_and_append_check_answer(table_name)

        # table name containing quotes
        table_name = f'"""{Utils.random_table_name()}"""'
        create_and_append_check_answer(table_name)

        # table name containing quotes and dot
        table_name = f'"""{Utils.random_table_name()}...{Utils.random_table_name()}"""'
        create_and_append_check_answer(table_name)

        # quoted schema and quoted table

        # "schema"."table"
        table_name = f'"{Utils.random_table_name()}.{Utils.random_table_name()}"'
        full_table_name = f"{double_quoted_schema}.{table_name}"
        create_and_append_check_answer(full_table_name)

        # db."schema"."table"
        table_name = f'"{Utils.random_table_name()}.{Utils.random_table_name()}"'
        full_table_name = f"{database}.{double_quoted_schema}.{table_name}"
        create_and_append_check_answer(full_table_name)

        # db.."table"
        table_name = f'"{Utils.random_table_name()}.{Utils.random_table_name()}"'
        full_table_name = f"{database}..{table_name}"
        create_and_append_check_answer(full_table_name)

        # schema + table name containing dots and quotes
        table_name = f'"""{Utils.random_table_name()}...{Utils.random_table_name()}"""'
        full_table_name = f"{schema}.{table_name}"
        create_and_append_check_answer(full_table_name)

        # test list of input table name
        # table
        create_and_append_check_answer([f"{Utils.random_table_name()}"])

        # schema table
        create_and_append_check_answer([schema, f"{Utils.random_table_name()}"])

        # database schema table
        create_and_append_check_answer(
            [database, schema, f"{Utils.random_table_name()}"]
        )

        # database schema table
        create_and_append_check_answer([database, "", f"{Utils.random_table_name()}"])

        # quoted table
        create_and_append_check_answer(
            [f'"{Utils.random_table_name()}.{Utils.random_table_name()}"']
        )

        # quoted schema and quoted table
        create_and_append_check_answer(
            [
                f"{double_quoted_schema}",
                f'"{Utils.random_table_name()}.{Utils.random_table_name()}"',
            ]
        )

        # db, quoted schema and quoted table
        create_and_append_check_answer(
            [
                database,
                f"{double_quoted_schema}",
                f'"{Utils.random_table_name()}.{Utils.random_table_name()}"',
            ]
        )

        # db, missing schema, quoted table
        create_and_append_check_answer(
            [database, "", f'"{Utils.random_table_name()}.{Utils.random_table_name()}"']
        )

        # db, missing schema, quoted table with escaping quotes
        create_and_append_check_answer(
            [
                database,
                "",
                f'"""{Utils.random_table_name()}.{Utils.random_table_name()}"""',
            ]
        )
    finally:
        # drop schema
        Utils.drop_schema(session, schema)
        Utils.drop_schema(session, double_quoted_schema)
