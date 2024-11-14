#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import numpy as np
import pytest

from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.column import Column
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import Utils, multithreaded_run


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
def test_to_snowpark_pandas_basic(session, tmp_table_basic, index_col, columns) -> None:
    # One less query when we don't have a multi-index
    with SqlCounter(
        query_count=3 if isinstance(index_col, list) and len(index_col) > 1 else 2
    ):
        snowpark_df = session.table(tmp_table_basic)

        snowpark_pandas_df = snowpark_df.to_snowpark_pandas(index_col, columns)

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
@sql_count_checker(query_count=3)
def test_to_snowpark_pandas_from_views(session, tmp_table_basic) -> None:
    snowpark_df = session.sql(
        f"SELECT ID, SHOE_MODEL FROM {tmp_table_basic} WHERE ID > 1"
    )
    snowpark_pandas_df = snowpark_df.to_snowpark_pandas()

    # verify all columns are data columns
    assert snowpark_pandas_df.columns.tolist() == ["ID", "SHOE_MODEL"]
    # verify a default row_position column is created
    snowpandas_index = snowpark_pandas_df.index
    assert snowpandas_index.dtype == np.dtype("int64")
    assert sorted(snowpandas_index.values.tolist()) == [0, 1]


@sql_count_checker(query_count=3)
def test_to_snowpark_pandas_with_operations(session, tmp_table_basic) -> None:
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

    snowpark_pandas_df = snowpark_df.to_snowpark_pandas()
    # verify all columns are data columns
    assert snowpark_pandas_df.columns.tolist() == ["size", "model"]
    # verify a default row_position column is created
    snowpandas_index = snowpark_pandas_df.index
    assert snowpandas_index.dtype == np.dtype("int64")
    assert sorted(snowpandas_index.values.tolist()) == [0]


@sql_count_checker(query_count=0)
def test_to_snowpark_pandas_duplicated_columns_raises(session, tmp_table_basic) -> None:
    snowpark_df = session.table(tmp_table_basic)
    snowpark_df = snowpark_df.select(
        Column("ID"),
        Column("FOOT_SIZE").as_('"shoe"'),
        Column("SHOE_MODEL").as_('"shoe"'),
    )

    with pytest.raises(SnowparkSQLException, match="duplicate column name 'shoe'"):
        snowpark_df.to_snowpark_pandas()


@sql_count_checker(query_count=1)
def test_to_snowpark_pandas_columns_not_list_raises(session, tmp_table_basic) -> None:
    snowpark_df = session.table(tmp_table_basic)

    with pytest.raises(ValueError, match="columns must be provided as list"):
        snowpark_df.to_snowpark_pandas(columns="FOOT_SIZE")
