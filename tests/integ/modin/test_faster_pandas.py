#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import copy
from contextlib import contextmanager
import modin.pandas as pd
import pandas as native_pd
import pytest
from pandas._testing import assert_almost_equal

from snowflake.snowpark._internal.utils import TempObjectType
import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.session import (
    _SNOWPARK_PANDAS_DUMMY_ROW_POS_OPTIMIZATION_ENABLED,
    Session,
)
from tests.integ.modin.utils import (
    assert_frame_equal,
    assert_index_equal,
    assert_series_equal,
)
from tests.integ.utils.sql_counter import sql_count_checker
from tests.utils import Utils


@contextmanager
def session_parameter_override(session, parameter_name, value):
    """Context manager to temporarily override a session parameter and restore it afterwards"""
    original_value = getattr(session, parameter_name)
    setattr(session, parameter_name, value)
    try:
        yield
    finally:
        setattr(session, parameter_name, original_value)


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
    # first ensure that indexes are the same
    assert_index_equal(snow_result.index, native_result.index)
    # then compare the data columns exclduing the index column
    # (because row position assignement is not necessarily idential)
    assert_frame_equal(
        snow_result.to_pandas().sort_values(by="A").reset_index(drop=True),
        native_result.sort_values(by="A").reset_index(drop=True),
    )


@sql_count_checker(query_count=3, join_count=2)
def test_read_filter_iloc_index(session):
    # test a chain of operations that are not yet fully supported in faster pandas

    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[1, 11], [2, 12], [2, 13]], columns=["A", "B"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result = df.iloc[[1], :]

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe has an empty relaxed query compiler
    # because iloc for index is not supported in faster pandas yet
    assert snow_result._query_compiler._relaxed_query_compiler is None
    assert snow_result._query_compiler._dummy_row_pos_mode is False

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result = native_df.iloc[[1], :]

    # compare results
    assert_frame_equal(snow_result, native_result)


@sql_count_checker(query_count=5, join_count=1)
def test_read_filter_join_flag_disabled(session):
    # test a chain of operations that are fully supported in faster pandas
    # but with the dummy_row_pos_optimization_enabled flag turned off
    with session_parameter_override(
        session, "dummy_row_pos_optimization_enabled", False
    ):
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

        # verify that the input dataframes have an empty relaxed query compiler
        assert df1._query_compiler._relaxed_query_compiler is None
        assert df2._query_compiler._relaxed_query_compiler is None
        # verify that the output dataframe also has an empty relaxed query compiler
        assert snow_result._query_compiler._relaxed_query_compiler is None

        # create pandas dataframes
        native_df1 = df1.to_pandas()
        native_df2 = df2.to_pandas()
        native_result = native_df1[native_df1["B"] > 11].merge(
            native_df2[native_df2["D"] == 22], left_on="A", right_on="C"
        )

        # compare results
        assert_frame_equal(snow_result, native_result)


@pytest.mark.parametrize(
    "func",
    [
        "min",
        "max",
        "count",
        "sum",
        "mean",
        "median",
        "std",
        "var",
    ],
)
@sql_count_checker(query_count=6)
def test_agg(session, func):
    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[2, 12], [1, 11], [3, 13]], columns=["A", "B"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result1 = getattr(df, func)()
    snow_result2 = df.agg([func])
    snow_result3 = getattr(df["B"], func)()
    snow_result4 = df["B"].agg([func])

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result1._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result1._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )
    assert snow_result2._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result2._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result1 = getattr(native_df, func)()
    native_result2 = native_df.agg([func])
    native_result3 = getattr(native_df["B"], func)()
    native_result4 = native_df["B"].agg([func])

    # compare results
    assert_series_equal(snow_result1, native_result1, check_dtype=False)
    assert_frame_equal(snow_result2, native_result2, check_dtype=False)
    assert_almost_equal(snow_result3, native_result3)
    assert_series_equal(snow_result4, native_result4, check_dtype=False)


@sql_count_checker(query_count=3)
def test_drop(session):
    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[2, True], [1, False], [3, False]], columns=["A", "B"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result = df.drop(columns=["B"])

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result = native_df.drop(columns=["B"])

    # compare results
    assert_frame_equal(snow_result, native_result)


@sql_count_checker(query_count=3, join_count=2)
def test_drop_duplicates(session):
    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[2, 12], [2, 12], [3, 13]], columns=["A", "B"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result = df.drop_duplicates()

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result = native_df.drop_duplicates()

    # compare results
    assert_frame_equal(snow_result, native_result)


@sql_count_checker(query_count=3, join_count=1)
def test_duplicated(session):
    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[2, 12], [2, 12], [3, 13]], columns=["A", "B"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result = df.duplicated()

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result = native_df.duplicated()

    # compare results
    assert_series_equal(snow_result, native_result)


@pytest.mark.parametrize(
    "func",
    [
        "min",
        "max",
        "count",
        "sum",
        "mean",
        "median",
        "std",
        "var",
    ],
)
@sql_count_checker(query_count=6)
def test_groupby_agg(session, func):
    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[2, 12], [2, 11], [3, 13]], columns=["A", "B"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result1 = getattr(df.groupby("A"), func)()
    snow_result2 = df.groupby("A").agg([func])
    snow_result3 = getattr(df.groupby("A")["B"], func)()
    snow_result4 = df.groupby("A")["B"].agg([func])

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result1._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result1._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )
    assert snow_result2._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result2._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result1 = getattr(native_df.groupby("A"), func)()
    native_result2 = native_df.groupby("A").agg([func])
    native_result3 = getattr(native_df.groupby("A")["B"], func)()
    native_result4 = native_df.groupby("A")["B"].agg([func])

    # compare results
    assert_frame_equal(snow_result1, native_result1, check_dtype=False)
    assert_frame_equal(snow_result2, native_result2, check_dtype=False)
    assert_series_equal(snow_result3, native_result3, check_dtype=False)
    assert_frame_equal(snow_result4, native_result4, check_dtype=False)


@sql_count_checker(query_count=9, join_count=1, udtf_count=1)
def test_groupby_apply(session):
    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[2, 12], [2, 11], [3, 13]], columns=["A", "B"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name).sort_values("B", ignore_index=True)
    snow_result = df.groupby("A").apply(lambda x: x + 1)

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result = native_df.groupby("A").apply(lambda x: x + 1)

    # compare results
    assert_frame_equal(
        snow_result, native_result, check_dtype=False, check_index_type=False
    )


@sql_count_checker(query_count=5)
def test_iloc_head(session):
    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[1, 11], [2, 12], [3, 13]], columns=["A", "B"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result1 = df.iloc[:, [1]]
    snow_result2 = df.iloc[0:2:1, [1]]
    snow_result3 = df.head()

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result1._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result1._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )
    assert snow_result2._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result2._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )
    assert snow_result3._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result3._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result1 = native_df.iloc[:, [1]]
    native_result2 = native_df.iloc[0:2:1, [1]]
    native_result3 = native_df.head()

    # compare results
    assert_frame_equal(snow_result1, native_result1)
    assert_frame_equal(snow_result2, native_result2)
    assert_frame_equal(snow_result3, native_result3)


@sql_count_checker(query_count=3)
def test_invert(session):
    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[2, True], [1, False], [3, False]], columns=["A", "B"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result = ~df["B"]

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result = ~native_df["B"]

    # compare results
    assert_series_equal(snow_result, native_result)


@pytest.mark.parametrize("func", ["isna", "isnull", "notna", "notnull"])
@sql_count_checker(query_count=3)
def test_isna_notna(session, func):
    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[1, 11], [2, None], [3, 13]], columns=["A", "B"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result = df[getattr(df["B"], func)()]

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result = native_df[getattr(native_df["B"], func)()]

    # compare results
    assert_frame_equal(snow_result, native_result, check_dtype=False)


@sql_count_checker(query_count=3)
def test_isin_list(session):
    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[1, 11], [2, 12], [3, 13]], columns=["A", "B"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result = df[df["B"].isin([12, 13])]

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result = native_df[native_df["B"].isin([12, 13])]

    # compare results
    assert_frame_equal(snow_result, native_result, check_dtype=False)


@sql_count_checker(query_count=3)
def test_isin_series(session):
    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[1, 11], [2, 12], [3, 13]], columns=["A", "B"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result = df[df["B"].isin(df["A"])]

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result = native_df[native_df["B"].isin(native_df["A"])]

    # compare results
    assert_frame_equal(snow_result, native_result, check_dtype=False)


@sql_count_checker(query_count=3)
def test_rename(session):
    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[2, 12], [1, 11], [3, 13]], columns=["A", "B"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result = df.rename(columns={"A": "a", "B": "b"})

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result = native_df.rename(columns={"A": "a", "B": "b"})

    # compare results
    assert_frame_equal(snow_result, native_result)


@sql_count_checker(query_count=3)
def test_str_contains(session):
    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([["abc"], ["def"], ["ghi"]], columns=["A"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result = df["A"].str.contains("ab")

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result = native_df["A"].str.contains("ab")

    # compare results
    assert_series_equal(snow_result, native_result)


@pytest.mark.parametrize("func", ["startswith", "endswith"])
@sql_count_checker(query_count=3)
def test_str_startswith_endswith(session, func):
    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([["abc"], ["def"], ["cba"]], columns=["A"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result = getattr(df["A"].str, func)("c")

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result = getattr(native_df["A"].str, func)("c")

    # compare results
    assert_series_equal(snow_result, native_result)


@sql_count_checker(query_count=3)
def test_str_slice(session):
    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([["abc"], ["def"], ["ghi"]], columns=["A"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result = df["A"].str.slice(0, 2, 1)

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result = native_df["A"].str.slice(0, 2, 1)

    # compare results
    assert_series_equal(snow_result, native_result)


@pytest.mark.parametrize(
    "property_name",
    [
        "date",
        "time",
        "hour",
        "minute",
        "second",
        "microsecond",
        "nanosecond",
        "year",
        "month",
        "day",
        "quarter",
        "is_month_start",
        "is_month_end",
        "is_quarter_start",
        "is_quarter_end",
        "is_year_start",
        "is_year_end",
        "is_leap_year",
        "days_in_month",
        "daysinmonth",
    ],
)
@sql_count_checker(query_count=3)
def test_dt_properties(session, property_name):
    datetime_index = native_pd.DatetimeIndex(
        [
            "2014-04-04 23:56:01.000000001",
            "2014-07-18 21:24:02.000000002",
            "2015-11-22 22:14:03.000000003",
            "2015-11-23 20:12:04.1234567890",
            pd.NaT,
        ],
        tz="US/Eastern",
    )
    native_ser = native_pd.Series(datetime_index)

    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame(native_ser, columns=["A"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result = getattr(df["A"].dt, property_name)

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result = getattr(native_df["A"].dt, property_name)

    # compare results
    assert_series_equal(snow_result, native_result)


@sql_count_checker(query_count=3)
def test_sort_values(session):
    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[2, 12], [1, 11], [3, 13]], columns=["A", "B"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result = df.sort_values(by="A")

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result = native_df.sort_values(by="A")

    # compare results
    assert_frame_equal(snow_result, native_result)


@sql_count_checker(query_count=3)
def test_set_2d_labels_from_same_df(session):
    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[2, 12], [1, 11], [3, 13]], columns=["A", "B"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    df["C"] = df["B"] + 1
    snow_result = df

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_df["C"] = native_df["B"] + 1
    native_result = native_df

    # compare results
    assert_frame_equal(snow_result, native_result)


@pytest.mark.parametrize(
    "input_df2",
    [
        native_pd.DataFrame([[2, 112], [1, 111], [3, 113]], columns=["A", "B"]),
        native_pd.DataFrame([[2, 112]], columns=["A", "B"]),
    ],
)
@sql_count_checker(query_count=5, join_count=2)
def test_set_2d_labels_from_different_df(session, input_df2):
    # create tables
    table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[2, 12], [1, 11], [3, 13]], columns=["A", "B"])
    ).write.save_as_table(table_name1, table_type="temp")

    table_name2 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(input_df2).write.save_as_table(
        table_name2, table_type="temp"
    )

    # create snow dataframes
    df1 = pd.read_snowflake(table_name1)
    df2 = pd.read_snowflake(table_name2)
    # ensure both dataframes have same order
    df1 = df1.sort_values("A", ignore_index=True)
    df2 = df2.sort_values("A", ignore_index=True)
    df3 = df1
    df3["C"] = df2["B"] + 1
    snow_result = df3

    # verify that the input dataframe has a populated relaxed query compiler
    assert df1._query_compiler._relaxed_query_compiler is not None
    assert df1._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df1 = df1.to_pandas()
    native_df2 = df2.to_pandas()
    native_df3 = native_df1
    native_df3["C"] = native_df2["B"] + 1
    native_result = native_df3

    # compare results
    assert_frame_equal(snow_result, native_result)


@sql_count_checker(query_count=3)
def test_set_columns(session):
    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame([[2, 12], [1, 11], [3, 13]], columns=["A", "B"])
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result = df
    snow_result.columns = ["X", "Y"]

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result = native_df
    native_result.columns = ["X", "Y"]

    # compare results
    assert_frame_equal(snow_result, native_result)


@sql_count_checker(query_count=3)
def test_dataframe_to_datetime(session):
    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame(
            [[2021, 9, 30], [2021, 10, 30], [2021, 11, 30]],
            columns=["year", "month", "day"],
        )
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result = pd.to_datetime(df)

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result = native_pd.to_datetime(native_df)

    # compare results
    assert_series_equal(snow_result, native_result)


@sql_count_checker(query_count=3)
def test_series_to_datetime(session):
    # create tables
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        native_pd.DataFrame(
            [
                [1, "2022-09-30 12:00:00"],
                [2, "2022-10-30 12:00:00"],
                [3, "2022-11-30 12:00:00"],
            ],
            columns=["A", "B"],
        )
    ).write.save_as_table(table_name, table_type="temp")

    # create snow dataframes
    df = pd.read_snowflake(table_name)
    snow_result = pd.to_datetime(df["B"])

    # verify that the input dataframe has a populated relaxed query compiler
    assert df._query_compiler._relaxed_query_compiler is not None
    assert df._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    # verify that the output dataframe also has a populated relaxed query compiler
    assert snow_result._query_compiler._relaxed_query_compiler is not None
    assert (
        snow_result._query_compiler._relaxed_query_compiler._dummy_row_pos_mode is True
    )

    # create pandas dataframes
    native_df = df.to_pandas()
    native_result = native_pd.to_datetime(native_df["B"])

    # compare results
    assert_series_equal(snow_result, native_result)


@sql_count_checker(query_count=0)
def test_dummy_row_pos_optimization_enabled_on_session(db_parameters):
    with Session.builder.configs(db_parameters).create() as new_session:
        default_value = new_session.dummy_row_pos_optimization_enabled
        new_session.dummy_row_pos_optimization_enabled = not default_value
        assert new_session.dummy_row_pos_optimization_enabled is not default_value
        new_session.dummy_row_pos_optimization_enabled = default_value
        assert new_session.dummy_row_pos_optimization_enabled is default_value

        parameters = copy.deepcopy(db_parameters)
        parameters["session_parameters"] = {
            _SNOWPARK_PANDAS_DUMMY_ROW_POS_OPTIMIZATION_ENABLED: not default_value
        }
        with Session.builder.configs(parameters).create() as new_session2:
            assert new_session2.dummy_row_pos_optimization_enabled is not default_value
