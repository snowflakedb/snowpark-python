#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd

from snowflake.snowpark._internal.utils import TempObjectType
import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_frame_equal, assert_index_equal
from tests.integ.utils.sql_counter import sql_count_checker
from tests.utils import Utils


@sql_count_checker(query_count=5, join_count=1)
def test_read_filter_join(session):
    # test a chain of operations that are fully supported in faster pandas

    # create tables
    table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[1, 11], [2, 12], [3, 13]], columns=["A", "B"])
    ).write.save_as_table(table_name1, table_type="temp")
    table_name2 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[1, 21], [2, 22], [3, 23]], columns=["C", "D"])
    ).write.save_as_table(table_name2, table_type="temp")

    # create snow dataframes
    df1 = pd.read_snowflake(table_name1)
    df2 = pd.read_snowflake(table_name2)
    snow_result = df1[df1["B"] > 11].merge(
        df2[df2["D"] == 22], left_on="A", right_on="C"
    )

    # verify that the input dataframes have a populated relaxed query compiler
    assert df1._query_compiler._relaxed_query_compiler is not None
    assert df1._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    assert df2._query_compiler._relaxed_query_compiler is not None
    assert df2._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df1 = df1.to_pandas()
    native_df2 = df2.to_pandas()
    native_result = native_df1[native_df1["B"] > 11].merge(
        native_df2[native_df2["D"] == 22], left_on="A", right_on="C"
    )

    # compare results
    assert_frame_equal(snow_result, native_result)


@sql_count_checker(query_count=6, join_count=2)
def test_read_filter_join_on_index(session):
    # test a chain of operations that are fully supported in faster pandas

    # create tables
    table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[1, 11], [2, 12], [3, 13]], columns=["A", "B"])
    ).write.save_as_table(table_name1, table_type="temp")
    table_name2 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[1, 21], [2, 22], [3, 23]], columns=["C", "D"])
    ).write.save_as_table(table_name2, table_type="temp")

    # create snow dataframes
    df1 = pd.read_snowflake(table_name1)
    df2 = pd.read_snowflake(table_name2)
    snow_result = df1.merge(df2, left_index=True, right_index=True)

    # verify that the input dataframes have a populated relaxed query compiler
    assert df1._query_compiler._relaxed_query_compiler is not None
    assert df1._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    assert df2._query_compiler._relaxed_query_compiler is not None
    assert df2._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df1 = df1.to_pandas()
    native_df2 = df2.to_pandas()
    native_result = native_df1.merge(native_df2, left_index=True, right_index=True)

    # compare results
    # first ensire that indexes are the same
    assert_index_equal(snow_result.index, native_result.index)
    # then compare the data columns exclduing the index column
    # (because row position assignement is not necessarily idential)
    assert_frame_equal(
        snow_result.to_pandas().sort_values(by="A").reset_index(drop=True),
        native_result.sort_values(by="A").reset_index(drop=True),
    )


@sql_count_checker(query_count=3)
def test_read_filter_groupby_agg(session):
    # test a chain of operations that are not fully supported in faster pandas

    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[1, 11], [2, 12], [2, 13]], columns=["A", "B"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result = df[df["B"] > 11].groupby("A").min()

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe has an empty relaxed query compiler
    # because groupby() and min() are not supported in faster pandas yet
    assert snow_result._query_compiler._relaxed_query_compiler is None
    assert snow_result._query_compiler._dummy_row_pos_mode is False

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result = native_df[native_df["B"] > 11].groupby("A").min()

    # compare results
    assert_frame_equal(snow_result, native_result)
