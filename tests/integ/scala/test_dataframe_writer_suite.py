#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import copy
import logging

import pytest

import snowflake.connector.errors

from snowflake.snowpark.exceptions import SnowparkClientException
from snowflake.snowpark import Row
from snowflake.snowpark._internal.utils import TempObjectType, parse_table_name
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import col, lit, object_construct, parse_json
from snowflake.snowpark.mock.exceptions import SnowparkLocalTestingException
from snowflake.snowpark.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from tests.utils import TestFiles, Utils, iceberg_supported, is_in_stored_procedure


@pytest.fixture(scope="function")
def temp_stage(session):
    temp_stage = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    Utils.create_stage(session, temp_stage, is_temporary=True)
    yield temp_stage
    Utils.drop_stage(session, temp_stage)


def test_write_with_target_column_name_order(session, local_testing_mode):
    table_name = Utils.random_table_name()
    empty_df = session.create_dataframe(
        [],
        schema=StructType(
            [
                StructField("a", IntegerType()),
                StructField("b", IntegerType()),
            ]
        ),
    )
    empty_df.write.save_as_table(table_name, table_type="temporary")
    try:
        df1 = session.create_dataframe([[1, 2]], schema=["b", "a"])

        # By default, it is by index
        df1.write.save_as_table(table_name, mode="append", table_type="temp")
        Utils.check_answer(session.table(table_name), [Row(**{"A": 1, "B": 2})])

        # Explicitly use "index"
        empty_df.write.save_as_table(
            table_name, mode="truncate", table_type="temporary"
        )
        df1.write.save_as_table(
            table_name, mode="append", column_order="index", table_type="temp"
        )
        Utils.check_answer(session.table(table_name), [Row(**{"A": 1, "B": 2})])

        # use order by "name"
        empty_df.write.save_as_table(
            table_name, mode="truncate", table_type="temporary"
        )
        df1.write.save_as_table(
            table_name, mode="append", column_order="name", table_type="temp"
        )
        Utils.check_answer(session.table(table_name), [Row(**{"A": 2, "B": 1})])

        # If target table doesn't exist, "order by name" is not actually used.
        Utils.drop_table(session, table_name)
        df1.write.save_as_table(table_name, mode="append", column_order="name")
        # NOTE: Order is different in the below check
        # because the table returns columns in the order of the order of the schema `df1`
        Utils.check_answer(session.table(table_name), [Row(**{"B": 1, "A": 2})])
    finally:
        session.table(table_name).drop_table()

    if not local_testing_mode:
        # column name and table name with special characters
        special_table_name = '"test table name"'
        Utils.create_table(
            session, special_table_name, '"a a" int, "b b" int', is_temporary=True
        )
        try:
            df2 = session.create_dataframe([(1, 2)]).to_df("b b", "a a")
            df2.write.save_as_table(
                special_table_name,
                mode="append",
                column_order="name",
                table_type="temp",
            )
            Utils.check_answer(session.table(special_table_name), [Row(2, 1)])
        finally:
            Utils.drop_table(session, special_table_name)


def test_snow_1668862_repro_save_null_data(session):
    table_name = Utils.random_table_name()
    test_data = session.create_dataframe([(1,), (2,)], ["A"])
    df = test_data.with_column("b", lit(None))
    try:
        df.write.save_as_table(table_name=table_name, mode="truncate")
        assert session.table(table_name).collect() == [Row(1, None), Row(2, None)]
    finally:
        Utils.drop_table(session, table_name)


def test_write_truncate_with_less_columns(session):
    # test truncate mode saving dataframe with fewer columns than the target table but column name in the same order
    schema1 = StructType(
        [
            StructField("A", LongType(), False),
            StructField("B", LongType(), True),
        ]
    )
    schema2 = StructType([StructField("A", LongType(), False)])
    df1 = session.create_dataframe([(1, 2), (3, 4)], schema=schema1)
    df2 = session.create_dataframe([1, 2], schema=schema2)
    table_name1 = Utils.random_table_name()

    try:
        df1.write.save_as_table(table_name1, mode="truncate")
        Utils.check_answer(session.table(table_name1), [Row(1, 2), Row(3, 4)])
        df2.write.save_as_table(table_name1, mode="truncate")
        Utils.check_answer(session.table(table_name1), [Row(1, None), Row(2, None)])
    finally:
        Utils.drop_table(session, table_name1)

    # test truncate mode saving dataframe with fewer columns than the target table but column name not in order
    schema3 = StructType(
        [
            StructField("A", LongType(), True),
            StructField("B", LongType(), True),
        ]
    )
    schema4 = StructType([StructField("B", LongType(), False)])
    df3 = session.create_dataframe([(1, 2), (3, 4)], schema=schema3)
    df4 = session.create_dataframe([1, 2], schema=schema4)
    table_name2 = Utils.random_table_name()

    try:
        df3.write.save_as_table(table_name2, mode="truncate")
        Utils.check_answer(session.table(table_name2), [Row(1, 2), Row(3, 4)])
        df4.write.save_as_table(table_name2, mode="truncate")
        Utils.check_answer(session.table(table_name2), [Row(None, 1), Row(None, 2)])
    finally:
        Utils.drop_table(session, table_name2)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL query feature AUTOINCREMENT not supported",
    run=False,
)
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


def test_iceberg(session, local_testing_mode):
    if not iceberg_supported(session, local_testing_mode) or is_in_stored_procedure():
        pytest.skip("Test requires iceberg support.")

    session.sql(
        "alter session set FEATURE_INCREASED_MAX_LOB_SIZE_PERSISTED=DISABLED"
    ).collect()
    session.sql(
        "alter session set FEATURE_INCREASED_MAX_LOB_SIZE_IN_MEMORY=DISABLED"
    ).collect()

    table_name = Utils.random_table_name()
    df = session.create_dataframe(
        [],
        schema=StructType(
            [
                StructField("a", StringType()),
                StructField("b", IntegerType()),
            ]
        ),
    )
    df.write.save_as_table(
        table_name,
        iceberg_config={
            "external_volume": "PYTHON_CONNECTOR_ICEBERG_EXVOL",
            "catalog": "SNOWFLAKE",
            "base_location": "snowpark_python_tests",
        },
    )
    try:
        ddl = session._run_query(f"select get_ddl('table', '{table_name}')")
        assert (
            ddl[0][0]
            == f"create or replace ICEBERG TABLE {table_name} (\n\tA STRING,\n\tB LONG\n)\n EXTERNAL_VOLUME = 'PYTHON_CONNECTOR_ICEBERG_EXVOL'\n CATALOG = 'SNOWFLAKE'\n BASE_LOCATION = 'snowpark_python_tests/';"
        )
    finally:
        session.table(table_name).drop_table()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="BUG: SNOW-1235716 should raise not implemented error not AttributeError: 'MockExecutionPlan' object has no attribute 'schema_query'",
)
def test_writer_options(session, temp_stage):
    df = session.create_dataframe([[1, 2], [3, 4], [5, 6], [7, 8]], schema=["a", "b"])

    # default case
    result = df.write.csv(f"@{temp_stage}/test_options")
    assert result[0].rows_unloaded == 4

    # overwrite case with option
    result = df.write.option("overwrite", True).csv(f"@{temp_stage}/test_options")
    assert result[0].rows_unloaded == 4

    # mixed case with format type option and copy option
    result = df.write.options({"single": True, "compression": "None"}).csv(
        f"@{temp_stage}/test_mixed_options"
    )
    assert result[0].rows_unloaded == 4
    files = session.sql(f"list @{temp_stage}/test_mixed_options").collect()
    assert len(files) == 1
    assert (files[0].name).lower() == f"{temp_stage.lower()}/test_mixed_options"

    # mixed case with options passed as kwargs
    result = df.write.options(single=True, sep=";").csv(
        f"@{temp_stage}/test_mixed_options_kwargs"
    )
    assert result[0].rows_unloaded == 4
    files = session.sql(f"list @{temp_stage}/test_mixed_options_kwargs").collect()
    assert len(files) == 1
    assert (files[0].name).lower() == f"{temp_stage.lower()}/test_mixed_options_kwargs"


def test_writer_options_negative(session):
    with pytest.raises(
        ValueError,
        match="Cannot set options with both a dictionary and keyword arguments",
    ):
        session.create_dataframe([[1, 2]], schema=["a", "b"]).write.options(
            {"overwrite": True}, overwrite=True
        ).csv("test_path")

    with pytest.raises(ValueError, match="No options were provided"):
        session.create_dataframe([[1, 2]], schema=["a", "b"]).write.options().csv(
            "test_path"
        )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="BUG: SNOW-1235716 should raise not implemented error not AttributeError: 'MockExecutionPlan' object has no attribute 'schema_query'",
)
def test_writer_partition_by(session, temp_stage):
    df = session.create_dataframe(
        [[1, "a"], [1, "b"], [2, "c"], [2, "d"]], schema=["a", "b"]
    )
    df.write.partition_by(col("a")).csv(f"@{temp_stage}/test_partition_by_a")
    cols = session.sql(f"list @{temp_stage}/test_partition_by_a").collect()
    num_files = len(cols)
    assert num_files == 2, cols

    # test kwarg supersedes .partition_by
    df.write.partition_by(col("a")).csv(
        f"@{temp_stage}/test_partition_by_b", partition_by=col("b")
    )
    cols = session.sql(f"list @{temp_stage}/test_partition_by_b").collect()
    num_files = len(cols)
    assert num_files == 4, cols


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


def test_write_with_target_column_name_order_all_kinds_of_dataframes_without_truncates(
    session,
):
    table_name = Utils.random_table_name()

    session.create_dataframe(
        [],
        schema=StructType(
            [StructField("a", IntegerType()), StructField("b", IntegerType())]
        ),
    ).write.save_as_table(table_name, table_type="temporary")

    try:
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


def test_write_with_target_column_name_order_with_nullable_column(
    session, local_testing_mode
):
    table_name, non_nullable_table_name = (
        Utils.random_table_name(),
        Utils.random_table_name(),
    )

    session.create_dataframe(
        [],
        schema=StructType(
            [
                StructField("a", IntegerType()),
                StructField("b", IntegerType()),
                StructField("c", StringType(), nullable=True),
                StructField("d", StringType(), nullable=True),
            ]
        ),
    ).write.save_as_table(table_name, table_type="temporary")

    session.create_dataframe(
        [],
        schema=StructType(
            [
                StructField("a", IntegerType()),
                StructField("b", StringType(), nullable=False),
            ]
        ),
    ).write.save_as_table(non_nullable_table_name, table_type="temporary")
    try:
        df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["b", "a"])

        df1.write.save_as_table(
            table_name, mode="append", table_type="temp", column_order="name"
        )
        Utils.check_answer(
            session.table(table_name),
            [
                Row(
                    **{
                        "A": 2,
                        "B": 1,
                        "C": None,
                        "D": None,
                    }
                ),
                Row(
                    **{
                        "A": 4,
                        "B": 3,
                        "C": None,
                        "D": None,
                    }
                ),
            ],
        )

        df2 = session.create_dataframe([[1], [2]], schema=["a"])
        with pytest.raises(
            SnowparkLocalTestingException
            if local_testing_mode
            else snowflake.connector.errors.IntegrityError
        ):
            df2.write.save_as_table(
                non_nullable_table_name,
                mode="append",
                table_type="temp",
                column_order="name",
            )
    finally:
        session.table(table_name).drop_table()
        session.table(non_nullable_table_name).drop_table()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: Inserting data into table by matching columns is not supported",
)
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: session._table_exists not supported",
)
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="BUG: SNOW-1235716 should raise not implemented error not AttributeError: 'MockExecutionPlan' object has no attribute 'schema_query'",
)
@pytest.mark.parametrize("format_type", ["csv", "json", "parquet"])
def test_format_save(session, temp_stage, format_type):
    df = session.create_dataframe([[1, 2], [1, 3], [2, 4], [2, 5]], schema=["a", "b"])

    path = f"{temp_stage}/test_format_save"
    result = (
        df.select(object_construct("*"))
        .write.option("compression", "None")
        .format(format_type)
        .save(path)
    )
    assert result[0].rows_unloaded == 4
    files = session.sql(f"list @{path}").collect()
    assert len(files) == 1
    assert files[0].name.endswith(format_type)


def test_format_save_negative(session):
    df = session.create_dataframe(
        [
            [1, 2],
        ],
        schema=["a", "b"],
    )
    with pytest.raises(
        ValueError,
        match="Unsupported file format. Expected.*, got 'unsupported_format'",
    ):
        df.write.format("unsupported_format").save("test_path")

    with pytest.raises(
        ValueError,
        match="File format type is not specified. Call `format` before calling `save`",
    ):
        df.write.save("test_path")


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="BUG: SNOW-1235716 should raise not implemented error not AttributeError: 'MockExecutionPlan' object has no attribute 'replace_repeated_subquery_with_cte'",
)
def test_writer_csv(session, temp_stage, caplog):

    """Tests for df.write.csv()."""
    df = session.create_dataframe([[1, 2], [3, 4], [5, 6], [3, 7]], schema=["a", "b"])
    ROWS_COUNT = 4
    schema = StructType(
        [StructField("a", IntegerType()), StructField("b", IntegerType())]
    )

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

    # test option alias case
    path7 = f"{temp_stage}/test_csv_example7/my_file.csv.gz"
    with caplog.at_level(logging.WARNING):
        result7 = df.write.csv(
            path7,
            format_type_options={"SEP": ":", "quote": '"'},
            single=True,
            header=True,
        )
    assert "Option 'SEP' is aliased to 'FIELD_DELIMITER'." in caplog.text
    assert "Option 'quote' is aliased to 'FIELD_OPTIONALLY_ENCLOSED_BY'." in caplog.text

    assert result7[0].rows_unloaded == ROWS_COUNT
    data7 = (
        session.read.schema(schema)
        .option("header", True)
        .option("inferSchema", True)
        .option("SEP", ":")
        .option("quote", '"')
        .csv(f"@{path7}")
    )
    Utils.check_answer(data7, df)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="BUG: SNOW-1235716 should raise not implemented error not AttributeError: 'MockExecutionPlan' object has no attribute 'replace_repeated_subquery_with_cte', FEAT: parquet support",
)
def test_writer_json(session, tmpdir_factory):

    """Tests for df.write.json()."""
    df1 = session.create_dataframe(
        ["[{a: 1, b: 2}, {a: 3, b: 0}]"], schema=["raw_data"]
    )
    df2 = session.create_dataframe(
        ["[{a: -1, b: 4}, {a: 17, b: -6}]"], schema=["raw_data"]
    )
    df = df1.select(parse_json(col("raw_data"))).union_all(
        df2.select(parse_json("raw_data"))
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="BUG: SNOW-1235716 should raise not implemented error not AttributeError: 'MockExecutionPlan' object has no attribute 'replace_repeated_subquery_with_cte', FEAT: parquet support",
)
def test_writer_parquet(session, tmpdir_factory, local_testing_mode):
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


def test_insert_into(session, local_testing_mode):
    """
    Test the insert_into API with positive and negative test cases.
    """
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)

    try:
        # Create a DataFrame with initial data
        df = session.create_dataframe(
            [["Alice", "Smith"], ["Bob", "Brown"]],
            schema=["FIRST_NAME", "LAST_NAME"],
        )
        df.write.save_as_table(table_name)

        # Positive Test: Append data to the table
        df_append = session.create_dataframe(
            [["Charlie", "White"]],
            schema=["FIRST_NAME", "LAST_NAME"],
        )
        df_append.write.insert_into(table_name)
        Utils.check_answer(
            session.table(table_name),
            [
                Row(FIRST_NAME="Alice", LAST_NAME="Smith"),
                Row(FIRST_NAME="Bob", LAST_NAME="Brown"),
                Row(FIRST_NAME="Charlie", LAST_NAME="White"),
            ],
        )

        # Positive Test: Overwrite data in the table
        df_overwrite = session.create_dataframe(
            [["David", "Green"]],
            schema=["FIRST_NAME", "LAST_NAME"],
        )
        df_overwrite.write.insert_into(table_name, overwrite=True)
        Utils.check_answer(
            session.table(table_name), [Row(FIRST_NAME="David", LAST_NAME="Green")]
        )

        # Negative Test: Schema mismatch, more columns
        df_more_columns = session.create_dataframe(
            [["Extra", "Column", 123]],
            schema=["FIRST_NAME", "LAST_NAME", "AGE"],
        )
        with pytest.raises(
            SnowparkSQLException,
            match="Insert value list does not match column list expecting 2 but got 3"
            if not local_testing_mode
            else "Cannot append because incoming data has different schema",
        ):
            df_more_columns.write.insert_into(table_name)

        # Negative Test: Schema mismatch, less columns
        df_less_column = session.create_dataframe(
            [["Column"]],
            schema=["FIRST_NAME"],
        )
        with pytest.raises(
            SnowparkSQLException,
            match="Insert value list does not match column list expecting 2 but got 1"
            if not local_testing_mode
            else "Cannot append because incoming data has different schema",
        ):
            df_less_column.write.insert_into(table_name)

        # Negative Test: Schema mismatch, type
        df_not_same_type = session.create_dataframe(
            [[[1, 2, 3, 4], False]],
            schema=["FIRST_NAME", "LAST_NAME"],
        )

        if not local_testing_mode:
            # SNOW-1890315: Local Testing missing type coercion check
            with pytest.raises(
                SnowparkSQLException,
                match="Expression type does not match column data type",
            ):
                df_not_same_type.write.insert_into(table_name)

        # Negative Test: Table does not exist
        with pytest.raises(
            SnowparkClientException,
            match="Table non_existent_table does not exist or not authorized.",
        ):
            df.write.insert_into("non_existent_table")

    finally:
        Utils.drop_table(session, table_name)
