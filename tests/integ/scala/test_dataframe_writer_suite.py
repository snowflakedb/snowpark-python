#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import copy

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark._internal.utils import TempObjectType, parse_table_name
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from tests.utils import TestFiles, Utils


def test_write_with_target_column_name_order(session):
    table_name = Utils.random_table_name()
    session.create_dataframe(
        [],
        schema=StructType(
            [
                StructField("a", IntegerType()),
                StructField("b", IntegerType()),
            ]
        ),
    ).write.save_as_table(table_name, table_type="temporary")
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
        session.table(table_name).drop_table()

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
    session.create_dataframe(
        [],
        schema=StructType(
            [StructField("a", IntegerType()), StructField("b", IntegerType())]
        ),
    ).write.save_as_table(table_name, table_type="temporary")
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
        session.table(table_name).drop_table()


def test_write_with_target_column_name_order_all_kinds_of_dataframes(
    session, resources_path
):
    table_name = Utils.random_table_name()
    session.create_dataframe(
        [],
        schema=StructType(
            [StructField("a", IntegerType()), StructField("b", IntegerType())]
        ),
    ).write.save_as_table(table_name, table_type="temporary")
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
        session.table(table_name).drop_table()

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


def test_writer_csv(session, tmpdir_factory):

    """Tests for df.write.csv()."""
    df = session.create_dataframe([[1, 2], [3, 4], [5, 6], [3, 7]], schema=["a", "b"])
    ROWS_COUNT = 4
    schema = StructType(
        [StructField("a", IntegerType()), StructField("b", IntegerType())]
    )

    temp_stage = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    Utils.create_stage(session, temp_stage, is_temporary=True)

    try:
        # test default case
        path1 = f"{temp_stage}/test_csv_example1"
        result1 = df.write.csv(path1)
        assert result1[0].rows_unloaded == ROWS_COUNT
        data1 = session.read.schema(schema).csv(f"@{path1}_0_0_0.csv.gz")
        Utils.assert_rows_count(data1, ROWS_COUNT)

        # test overwrite case
        result2 = df.write.csv(path1, overwrite=True)
        assert result2[0].rows_unloaded == ROWS_COUNT
        data2 = session.read.schema(schema).csv(f"@{path1}_0_0_0.csv.gz")
        Utils.assert_rows_count(data2, ROWS_COUNT)

        # partition by testing cases
        path3 = f"{temp_stage}/test_csv_example3/"
        result3 = df.write.csv(path3, partition_by=col("a"))
        assert result3[0].rows_unloaded == ROWS_COUNT
        data3 = session.read.schema(schema).csv(f"@{path3}")
        Utils.assert_rows_count(data3, ROWS_COUNT)

        path4 = f"{temp_stage}/test_csv_example4/"
        result4 = df.write.csv(path4, partition_by="a")
        assert result4[0].rows_unloaded == ROWS_COUNT
        data4 = session.read.schema(schema).csv(f"@{path4}")
        Utils.assert_rows_count(data4, ROWS_COUNT)

        # test single case
        path5 = f"{temp_stage}/test_csv_example5/my_file.csv"
        result5 = df.write.csv(path5, single=True)
        assert result5[0].rows_unloaded == ROWS_COUNT
        data5 = session.read.schema(schema).csv(f"@{path5}")
        Utils.assert_rows_count(data5, ROWS_COUNT)

        # test compression case
        path6 = f"{temp_stage}/test_csv_example6/my_file.csv.gz"
        result6 = df.write.csv(
            path6, format_type_options=dict(compression="gzip"), single=True
        )

        assert result6[0].rows_unloaded == ROWS_COUNT
        data6 = session.read.schema(schema).csv(f"@{path6}")
        Utils.assert_rows_count(data6, ROWS_COUNT)
    finally:
        Utils.drop_stage(session, temp_stage)


def test_writer_json(session, tmpdir_factory):

    """Tests for df.write.json()."""
    df = session.sql(
        """
        select parse_json('[{a: 1, b: 2}, {a: 3, b: 0}]') raw_data
            union all select parse_json('[{a: -1, b: 4}, {a: 17, b: -6}]')
    """
    )

    ROWS_COUNT = 2

    temp_stage = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    Utils.create_stage(session, temp_stage, is_temporary=True)

    try:
        # test default case
        path1 = f"{temp_stage}/test_json_example1"
        result1 = df.write.json(path1)
        assert result1[0].rows_unloaded == ROWS_COUNT
        data1 = session.read.json(f"@{path1}_0_0_0.json")
        Utils.assert_rows_count(data1, ROWS_COUNT)

        # test overwrite case
        result2 = df.write.json(path1, overwrite=True)
        assert result2[0].rows_unloaded == ROWS_COUNT
        data2 = session.read.json(f"@{path1}_0_0_0.json")
        Utils.assert_rows_count(data2, ROWS_COUNT)

        # test single case
        path3 = f"{temp_stage}/test_json_example3/my_file.json"
        result3 = df.write.json(path3, single=True)
        assert result3[0].rows_unloaded == ROWS_COUNT
        data3 = session.read.json(f"@{path3}")
        Utils.assert_rows_count(data3, ROWS_COUNT)

        # test compression case
        path4 = f"{temp_stage}/test_json_example3/my_file.json.gz"
        result4 = df.write.json(
            path4, format_type_options=dict(compression="gzip"), single=True
        )

        assert result4[0].rows_unloaded == ROWS_COUNT
        data4 = session.read.json(f"@{path4}")
        Utils.assert_rows_count(data4, ROWS_COUNT)
    finally:
        Utils.drop_stage(session, temp_stage)


def test_writer_parquet(session, tmpdir_factory):
    """Tests for df.write.parquet()."""
    df = session.create_dataframe([[1, 2], [3, 4], [5, 6]], schema=["a", "b"])
    ROWS_COUNT = 3

    temp_stage = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    Utils.create_stage(session, temp_stage, is_temporary=True)

    try:
        # test default case
        path1 = f"{temp_stage}/test_parquet_example1/"
        result1 = df.write.parquet(path1)
        assert result1[0].rows_unloaded == ROWS_COUNT
        data1 = session.read.parquet(f"@{path1}data_0_0_0.snappy.parquet")
        Utils.assert_rows_count(data1, ROWS_COUNT)

        # test overwrite case
        result2 = df.write.parquet(path1, overwrite=True)
        assert result2[0].rows_unloaded == ROWS_COUNT
        data2 = session.read.parquet(f"@{path1}data_0_0_0.snappy.parquet")
        Utils.assert_rows_count(data2, ROWS_COUNT)

        # test single case
        path3 = f"{temp_stage}/test_parquet_example3/my_file.parquet"
        result3 = df.write.parquet(path3, single=True)
        assert result3[0].rows_unloaded == ROWS_COUNT
        data3 = session.read.parquet(f"@{path3}")
        Utils.assert_rows_count(data3, ROWS_COUNT)

        # test compression case
        path4 = f"{temp_stage}/test_parquet_example4/"
        result4 = df.write.parquet(
            path4, format_type_options=dict(compression="snappy")
        )
        assert result4[0].rows_unloaded == ROWS_COUNT
        data4 = session.read.parquet(f"@{path4}data_0_0_0.snappy.parquet")
        Utils.assert_rows_count(data4, ROWS_COUNT)
    finally:
        Utils.drop_stage(session, temp_stage)
