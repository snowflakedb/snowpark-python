#!/usr/bin/env python3

#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import numpy as np
import pytest

from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.column import Column
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import TestFiles, Utils, multithreaded_run


@pytest.fixture(scope="module")
def tmp_table_basic(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(
        session, table_name, "id integer, foot_size float, shoe_model varchar"
    )
    session.sql(f"insert into {table_name} values (1, 32.0, 'medium')").collect()
    session.sql(f"insert into {table_name} values (2, 27.0, 'small')").collect()
    session.sql(f"insert into {table_name} values (3, 40.0, 'large')").collect()

    try:
        yield table_name
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.parametrize("index_col", ["ID", ["FOOT_SIZE"], ["ID", "ID", "FOOT_SIZE"]])
@pytest.mark.parametrize(
    "columns",
    [
        ["SHOE_MODEL"],
        ["ID", "FOOT_SIZE", "SHOE_MODEL"],
        ["FOOT_SIZE", "SHOE_MODEL", "SHOE_MODEL"],
    ],
)
@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_to_snowpark_pandas_basic(
    session, tmp_table_basic, index_col, columns, enforce_ordering
) -> None:
    expected_query_count = 6 if enforce_ordering else 3
    # One less query when we don't have a multi-index
    with SqlCounter(
        query_count=expected_query_count
        if isinstance(index_col, list) and len(index_col) > 1
        else expected_query_count - 1
    ):
        snowpark_df = session.table(tmp_table_basic)

        snowpark_pandas_df = snowpark_df.to_snowpark_pandas(
            index_col, columns, enforce_ordering=enforce_ordering
        )

        # verify index columns
        snowpandas_index = snowpark_pandas_df.index
        if index_col:
            expected_index = index_col if isinstance(index_col, list) else [index_col]
            assert snowpandas_index.names == expected_index
        else:
            assert snowpandas_index.dtype == np.dtype("int64")
            assert sorted(snowpandas_index.values.tolist()) == [0, 1, 2]

        # verify data columns
        if columns:
            assert snowpark_pandas_df.columns.tolist() == columns
        else:
            expected_data_cols = ["ID", "FOOT_SIZE", "SHOE_MODEL"]
            if index_col:
                # only keep the columns that is not in index_col
                index_col_list = (
                    index_col if isinstance(index_col, list) else [index_col]
                )
                expected_data_cols = [
                    col for col in expected_data_cols if col not in index_col_list
                ]
                assert snowpark_pandas_df.columns.tolist() == expected_data_cols


@multithreaded_run()
@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_to_snowpark_pandas_from_views(
    session, tmp_table_basic, enforce_ordering
) -> None:
    with SqlCounter(query_count=6 if enforce_ordering else 3):
        snowpark_df = session.sql(
            f"SELECT ID, SHOE_MODEL FROM {tmp_table_basic} WHERE ID > 1"
        )
        snowpark_pandas_df = snowpark_df.to_snowpark_pandas(
            enforce_ordering=enforce_ordering
        )

        # verify all columns are data columns
        assert snowpark_pandas_df.columns.tolist() == ["ID", "SHOE_MODEL"]
        # verify a default row_position column is created
        snowpandas_index = snowpark_pandas_df.index
        assert snowpandas_index.dtype == np.dtype("int64")
        assert sorted(snowpandas_index.values.tolist()) == [0, 1]


@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_to_snowpark_pandas_with_operations(
    session, tmp_table_basic, enforce_ordering
) -> None:
    with SqlCounter(query_count=6 if enforce_ordering else 3):
        snowpark_df = session.table(tmp_table_basic)
        snowpark_df = (
            snowpark_df.select(
                Column("ID"),
                Column("FOOT_SIZE").as_('"size"'),
                Column("SHOE_MODEL").as_('"model"'),
            )
            .where(Column("ID") > 2)
            .select(Column('"size"'), Column('"model"'))
        )

        snowpark_pandas_df = snowpark_df.to_snowpark_pandas(
            enforce_ordering=enforce_ordering
        )
        # verify all columns are data columns
        assert snowpark_pandas_df.columns.tolist() == ["size", "model"]
        # verify a default row_position column is created
        snowpandas_index = snowpark_pandas_df.index
        assert snowpandas_index.dtype == np.dtype("int64")
        assert sorted(snowpandas_index.values.tolist()) == [0]


@pytest.mark.parametrize("enforce_ordering", [True, False])
@sql_count_checker(query_count=0)
def test_to_snowpark_pandas_duplicated_columns_raises(
    session, tmp_table_basic, enforce_ordering
) -> None:
    sql_simplifier_enabled_original = session.sql_simplifier_enabled
    # Error is raised only when SQL simplifier is enabled.
    session.sql_simplifier_enabled = True

    snowpark_df = session.table(tmp_table_basic)
    snowpark_df = snowpark_df.select(
        Column("ID"),
        Column("FOOT_SIZE").as_('"shoe"'),
        Column("SHOE_MODEL").as_('"shoe"'),
    )

    pattern = (
        "duplicate column name 'shoe'"
        if enforce_ordering
        else "ambiguous column name 'shoe'"
    )

    with pytest.raises(SnowparkSQLException, match=pattern):
        snowpark_df.to_snowpark_pandas(enforce_ordering=enforce_ordering).head()
    session.sql_simplifier_enabled = sql_simplifier_enabled_original


@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_to_snowpark_pandas_columns_not_list_raises(
    session, tmp_table_basic, enforce_ordering
) -> None:
    with SqlCounter(query_count=1 if enforce_ordering else 0):
        snowpark_df = session.table(tmp_table_basic)

        with pytest.raises(ValueError, match="columns must be provided as list"):
            snowpark_df.to_snowpark_pandas(
                columns="FOOT_SIZE", enforce_ordering=enforce_ordering
            )


def test_to_snowpark_pandas_with_multiple_queries_forces_ordering(
    session,
    resources_path,
):
    tmp_stage_name = Utils.random_stage_name()
    test_files = TestFiles(resources_path)
    test_file_on_stage = f"@{tmp_stage_name}/testCSV.csv"

    Utils.create_stage(session, tmp_stage_name, is_temporary=True)
    Utils.upload_to_stage(
        session, "@" + tmp_stage_name, test_files.test_file_csv, compress=False
    )
    user_schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", DoubleType()),
        ]
    )
    snowpark_df = (
        session.read.option("purge", False).schema(user_schema).csv(test_file_on_stage)
    )
    with SqlCounter(query_count=8):
        df = snowpark_df.to_snowpark_pandas(enforce_ordering=False)
        assert len(df.columns) == 3
