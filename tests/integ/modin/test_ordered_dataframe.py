#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Optional, Union
from unittest.mock import patch

import numpy as np
import pandas as native_pd
import pandas as pd
import pytest

import snowflake.snowpark
from snowflake.snowpark import Session
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.column import Column
from snowflake.snowpark.dataframe import DataFrame as SnowparkDataFrame
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import (
    col,
    count,
    flatten,
    max as max_,
    min as min_,
    to_array,
)
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import (
    DataFrameReference,
    OrderedDataFrame,
    OrderingColumn,
)
from snowflake.snowpark.modin.plugin._internal.utils import (
    unquote_name_if_quoted,
    create_initial_ordered_dataframe,
)
from tests.integ.modin.utils import (
    assert_frame_equal,
    get_snowpark_dataframe_quoted_identifiers,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.fixture
def df1():
    return native_pd.DataFrame(
        {
            "A": [1, 1, 0, 4],
            "B": [2, 1, 5, 3],
            "C": [3.0, np.nan, 4.0, 5.0],
            "row_pos": [0, 1, 2, 3],
        }
    )


@pytest.fixture
def df2():
    return native_pd.DataFrame(
        {"A": [1, 2, np.nan], "B": [2, 6, 4], "C": [1, 4, 4], "row_pos": [0, 1, 2]}
    )


@pytest.fixture(scope="function")
def ordered_df(session):
    return OrderedDataFrame(
        DataFrameReference(
            session.create_dataframe(
                [[0, 4, 5, 6], [1, 1, 2, 3]],
                schema=['"row_position"', '"a"', '"b"', '"c"'],
            ),
            snowflake_quoted_identifiers=['"row_position"', '"a"', '"b"', '"c"'],
        ),
        projected_column_snowflake_quoted_identifiers=['"row_position"', '"a"', '"c"'],
        ordering_columns=[OrderingColumn('"b"'), OrderingColumn('"c"')],
        row_position_snowflake_quoted_identifier='"row_position"',
    )


def _verify_dataframe_reference(
    ordered_df1: OrderedDataFrame, ordered_df2: OrderedDataFrame, should_match: bool
):
    if should_match:
        assert ordered_df1._dataframe_ref == ordered_df2._dataframe_ref
        assert (
            ordered_df1._dataframe_ref.snowpark_dataframe
            == ordered_df2._dataframe_ref.snowpark_dataframe
        )
        assert ordered_df1._dataframe_ref._id == ordered_df2._dataframe_ref._id
    else:
        assert ordered_df1._dataframe_ref != ordered_df2._dataframe_ref
        assert (
            ordered_df1._dataframe_ref.snowpark_dataframe
            != ordered_df2._dataframe_ref.snowpark_dataframe
        )
        assert ordered_df1._dataframe_ref._id != ordered_df2._dataframe_ref._id


def _verify_dataframe_reference_quoted_identifiers(ordered_df: OrderedDataFrame):
    dataframe_ref = ordered_df._dataframe_ref
    snowpark_dataframe_quoted_identifiers = get_snowpark_dataframe_quoted_identifiers(
        dataframe_ref.snowpark_dataframe
    )
    assert (
        dataframe_ref.snowflake_quoted_identifiers
        == snowpark_dataframe_quoted_identifiers
    )


def _create_ordered_dataframe(
    session: Session,
    pandas_df: native_pd.DataFrame,
    projected_columns: Optional[list[str]] = None,
    ordering_columns: Optional[list[str]] = None,
    row_position_column: Optional[str] = None,
):
    snowpark_df = session.create_dataframe(pandas_df)
    column_quoted_identifiers = [
        f.column_identifier.quoted_name for f in snowpark_df.schema.fields
    ]
    if projected_columns is not None:
        projected_columns_snowflake_quoted_identifiers = projected_columns
    else:
        projected_columns_snowflake_quoted_identifiers = column_quoted_identifiers
    if ordering_columns is not None:
        orderings = [OrderingColumn(col) for col in ordering_columns]
    else:
        orderings = [
            OrderingColumn(col)
            for col in projected_columns_snowflake_quoted_identifiers
        ]
    ordered_df = OrderedDataFrame(
        dataframe_ref=DataFrameReference(
            snowpark_df, snowflake_quoted_identifiers=column_quoted_identifiers
        ),
        projected_column_snowflake_quoted_identifiers=projected_columns_snowflake_quoted_identifiers,
        ordering_columns=orderings,
        row_position_snowflake_quoted_identifier=row_position_column,
    )
    return ordered_df


def _unquote_and_validate_snowflake_quoted_identifiers(
    quoted_identifier: str,
    validation_str: str,
    prefix_match: bool = True,
) -> str:
    """
    Unquote the quoted_identifier and validate the unquoted identifier against the
    given validation. If prefix_match is set to true, check that the quoted identifier
    starts with the given validation_str, otherwise, check the unquoted identifier is
    exactly the same as the validation str.

    Returns:
        The unquoted identifier
    """
    unquoted_identifier = unquote_name_if_quoted(quoted_identifier)
    if prefix_match:
        assert unquoted_identifier != validation_str
        assert unquoted_identifier.startswith(validation_str)
    else:
        assert unquoted_identifier == validation_str

    return unquoted_identifier


def _join_or_align_result_validation_helper(
    result_ordered_df: OrderedDataFrame,
    left_ordered_df: OrderedDataFrame,
    right_ordered_df: OrderedDataFrame,
    left_pandas_df: pd.DataFrame,
    right_pandas_df: pd.DataFrame,
    left_on_cols: Optional[list[str]],
    right_on_cols: Optional[list[str]],
    how: str,
    sort: bool = False,
):
    # verify the result columns have columns in order of left + right de-conflicted
    left_unquoted_names = [
        unquote_name_if_quoted(quoted_identifier)
        for quoted_identifier in left_ordered_df.projected_column_snowflake_quoted_identifiers
    ]
    for quoted_identifier, validation_label in zip(
        result_ordered_df.projected_column_snowflake_quoted_identifiers[
            : len(left_pandas_df.columns)
        ],
        left_unquoted_names,
    ):
        _unquote_and_validate_snowflake_quoted_identifiers(
            quoted_identifier, validation_label, prefix_match=False
        )

    right_unquoted_names = [
        unquote_name_if_quoted(quoted_identifier)
        for quoted_identifier in right_ordered_df.projected_column_snowflake_quoted_identifiers
    ]
    right_labels = [
        _unquote_and_validate_snowflake_quoted_identifiers(
            quoted_identifier,
            validation_label,
            prefix_match=(validation_label in left_unquoted_names),
        )
        for quoted_identifier, validation_label in zip(
            result_ordered_df.projected_column_snowflake_quoted_identifiers[
                len(left_pandas_df.columns) : len(left_pandas_df.columns)
                + len(right_pandas_df.columns)
            ],
            right_unquoted_names,
        )
    ]

    right_name_map = dict(zip(right_unquoted_names, right_labels))
    left_on_labels = None
    right_on_labels = None
    if left_on_cols is not None:
        left_on_labels = [
            unquote_name_if_quoted(quoted_identifier)
            for quoted_identifier in left_on_cols
        ]
    if right_on_cols is not None:
        right_on_labels = [
            unquote_name_if_quoted(quoted_identifier)
            for quoted_identifier in right_on_cols
        ]
        right_on_labels = [right_name_map[label] for label in right_on_labels]

    # construct pandas result for comparing
    right_pandas_df.columns = right_labels
    expected_df = left_pandas_df.merge(
        right_pandas_df,
        left_on=left_on_labels,
        right_on=right_on_labels,
        how=how,
        sort=sort,
    )

    projected_ordered_df = result_ordered_df
    if len(result_ordered_df.projected_column_snowflake_quoted_identifiers) > len(
        left_pandas_df.columns
    ) + len(right_pandas_df.columns):
        projected_ordered_df = result_ordered_df.select(
            result_ordered_df.projected_column_snowflake_quoted_identifiers[
                : len(left_pandas_df.columns) + len(right_pandas_df.columns)
            ]
        )
    assert_frame_equal(projected_ordered_df, expected_df, check_dtype=False)


def _verify_order_by_query(
    dataframe: Union[OrderedDataFrame, SnowparkDataFrame], should_include_order_by: bool
) -> None:
    if should_include_order_by:
        assert "order by" in dataframe.queries["queries"][0].lower()
    else:
        assert "order by" not in dataframe.queries["queries"][0].lower()


@pytest.mark.parametrize(
    "how",
    ["left", "right", "inner", "outer", "cross"],
)
@sql_count_checker(query_count=1, join_count=1)
def test_join_no_column_conflict(session, df1, df2, how):
    if how == "outer":
        pytest.xfail("SNOW-1321662 - outer join issue")
    ordered_df1 = _create_ordered_dataframe(
        session, df1, ordering_columns=['"row_pos"'], row_position_column='"row_pos"'
    )

    # reset the name of df2 to avoid duplication with df1
    df2.columns = ["D", "E", "F", "row_pos2"]
    ordered_df2 = _create_ordered_dataframe(
        session, df2, ordering_columns=['"row_pos2"'], row_position_column='"row_pos2"'
    )

    # join df1 and df2
    if how == "cross":
        left_on_cols = None
        right_on_cols = None
        joined_ordered_df = ordered_df1.join(ordered_df2, how=how)
    else:
        left_on_cols = ['"B"']
        right_on_cols = ['"F"']
        joined_ordered_df = ordered_df1.join(
            ordered_df2, left_on_cols=left_on_cols, right_on_cols=right_on_cols, how=how
        )

    # verify joined ordered df have the final ordering column correct
    if how == "right":
        assert joined_ordered_df.ordering_column_snowflake_quoted_identifiers == [
            '"row_pos2"',
            '"row_pos"',
        ]
    else:
        assert joined_ordered_df.ordering_column_snowflake_quoted_identifiers == [
            '"row_pos"',
            '"row_pos2"',
        ]

    # verify the result columns have columns in order of left + right
    assert joined_ordered_df.projected_column_snowflake_quoted_identifiers == [
        '"A"',
        '"B"',
        '"C"',
        '"row_pos"',
        '"D"',
        '"E"',
        '"F"',
        '"row_pos2"',
    ]

    # verify the join result doesn't share the same dataframe reference as ordered_df1 or ordered_df2
    _verify_dataframe_reference(joined_ordered_df, ordered_df1, should_match=False)
    _verify_dataframe_reference(joined_ordered_df, ordered_df2, should_match=False)
    _verify_dataframe_reference_quoted_identifiers(joined_ordered_df)

    _join_or_align_result_validation_helper(
        joined_ordered_df,
        ordered_df1,
        ordered_df2,
        df1,
        df2,
        left_on_cols=left_on_cols,
        right_on_cols=right_on_cols,
        how=how,
    )


@pytest.mark.parametrize(
    "how",
    ["left", "right", "inner", "outer", "cross"],
)
@sql_count_checker(query_count=1, join_count=1)
def test_join_with_column_conflict(session, df1, df2, how):
    if how == "outer":
        pytest.xfail("SNOW-1321662 - outer join issue")
    ordered_df1 = _create_ordered_dataframe(
        session, df1, ordering_columns=['"row_pos"'], row_position_column='"row_pos"'
    )

    # reset the name of df2 to avoid duplication with df1
    ordered_df2 = _create_ordered_dataframe(
        session, df2, ordering_columns=['"row_pos"'], row_position_column='"row_pos"'
    )

    if how == "cross":
        left_on_cols = None
        right_on_cols = None
        joined_ordered_df = ordered_df1.join(ordered_df2, how=how)
    else:
        left_on_cols = ['"C"', '"row_pos"']
        right_on_cols = ['"C"', '"row_pos"']
        joined_ordered_df = ordered_df1.join(
            ordered_df2,
            left_on_cols=left_on_cols,
            right_on_cols=right_on_cols,
            how=how,
        )

    _verify_dataframe_reference(joined_ordered_df, ordered_df1, should_match=False)
    _verify_dataframe_reference(joined_ordered_df, ordered_df2, should_match=False)
    _verify_dataframe_reference_quoted_identifiers(joined_ordered_df)

    # verify joined ordered df have the final ordering column correct
    if how == "right":
        # the first ordering comes from right, which should be the one after de-duplication
        _unquote_and_validate_snowflake_quoted_identifiers(
            joined_ordered_df.ordering_column_snowflake_quoted_identifiers[0],
            "row_pos",
            prefix_match=True,
        )
        # the second ordering comes from left, which should be the original identifier
        _unquote_and_validate_snowflake_quoted_identifiers(
            joined_ordered_df.ordering_column_snowflake_quoted_identifiers[1],
            "row_pos",
            prefix_match=False,
        )
    else:
        _unquote_and_validate_snowflake_quoted_identifiers(
            joined_ordered_df.ordering_column_snowflake_quoted_identifiers[0],
            "row_pos",
            prefix_match=False,
        )
        _unquote_and_validate_snowflake_quoted_identifiers(
            joined_ordered_df.ordering_column_snowflake_quoted_identifiers[1],
            "row_pos",
            prefix_match=True,
        )

    _join_or_align_result_validation_helper(
        joined_ordered_df,
        ordered_df1,
        ordered_df2,
        df1,
        df2,
        left_on_cols=left_on_cols,
        right_on_cols=right_on_cols,
        how=how,
    )


@pytest.mark.parametrize(
    "how",
    ["left", "outer", "inner"],
)
@sql_count_checker(query_count=1, join_count=1)
def test_align_on_matching_columns(session, how):
    df1 = native_pd.DataFrame({"A": [3, 2, 3], "B": [2, 2, 1], "row_pos": [0, 1, 2]})
    df2 = native_pd.DataFrame(
        {"A_2": [2, 3, 3], "B_2": [2, 2, 1], "row_pos_2": [0, 1, 2]}
    )
    ordered_df1 = _create_ordered_dataframe(
        session, df1, ordering_columns=['"row_pos"'], row_position_column='"row_pos"'
    )
    ordered_df2 = _create_ordered_dataframe(
        session,
        df2,
        ordering_columns=['"row_pos_2"'],
        row_position_column='"row_pos_2"',
    )

    aligned_ordered_df = ordered_df1.align(
        ordered_df2,
        left_on_cols=['"B"', '"row_pos"'],
        right_on_cols=['"B_2"', '"row_pos_2"'],
        how=how,
    )

    # verify the aligned result have projected columns started with left and right
    expected_columns = [
        '"A"',
        '"B"',
        '"row_pos"',
        '"A_2"',
        '"B_2"',
        '"row_pos_2"',
    ]
    assert (
        aligned_ordered_df.projected_column_snowflake_quoted_identifiers[:6]
        == expected_columns
    )

    # verify the aligned result doesn't share the same dataframe reference with original dataframe
    _verify_dataframe_reference(aligned_ordered_df, ordered_df1, should_match=False)
    _verify_dataframe_reference(aligned_ordered_df, ordered_df2, should_match=False)
    _verify_dataframe_reference_quoted_identifiers(aligned_ordered_df)

    # drop off the ordering columns from the projected columns for result comparison
    projected_result = aligned_ordered_df.select(expected_columns)
    expected_df = native_pd.DataFrame(
        {
            "A": [3, 2, 3],
            "B": [2, 2, 1],
            "row_pos": [0, 1, 2],
            "A_2": [2, 3, 3],
            "B_2": [2, 2, 1],
            "row_pos_2": [0, 1, 2],
        }
    )
    assert_frame_equal(projected_result, expected_df, check_dtype=False)


@pytest.mark.parametrize(
    "how",
    ["outer"],
)
@sql_count_checker(query_count=1, join_count=1)
def test_align_on_mismatch_columns(session, df1, df2, how):
    ordered_df1 = _create_ordered_dataframe(
        session, df1, ordering_columns=['"row_pos"'], row_position_column='"row_pos"'
    )
    ordered_df2 = _create_ordered_dataframe(
        session, df2, ordering_columns=['"row_pos"'], row_position_column='"row_pos"'
    )

    left_on_cols = ['"C"']
    right_on_cols = ['"C"']
    aligned_ordered_df = ordered_df1.align(
        ordered_df2,
        left_on_cols=['"C"'],
        right_on_cols=['"C"'],
        how=how,
    )

    # verify the underneath dataframe reference is not shared
    _verify_dataframe_reference(aligned_ordered_df, ordered_df1, should_match=False)
    _verify_dataframe_reference(aligned_ordered_df, ordered_df2, should_match=False)
    _verify_dataframe_reference_quoted_identifiers(aligned_ordered_df)

    sort = False
    if how == "outer":
        sort = True
    _join_or_align_result_validation_helper(
        aligned_ordered_df,
        ordered_df1,
        ordered_df2,
        df1,
        df2,
        left_on_cols=left_on_cols,
        right_on_cols=right_on_cols,
        how=how,
        sort=sort,
    )


@sql_count_checker(query_count=0)
def test_align_on_matching_columns_right_negative(session):
    how = "right"
    df1 = native_pd.DataFrame({"A": [3, 2, 3], "B": [2, 2, 1], "row_pos": [0, 1, 2]})
    df2 = native_pd.DataFrame(
        {"A_2": [2, 3, 3], "B_2": [2, 2, 1], "row_pos_2": [0, 1, 2]}
    )
    ordered_df1 = _create_ordered_dataframe(
        session, df1, ordering_columns=['"row_pos"'], row_position_column='"row_pos"'
    )
    ordered_df2 = _create_ordered_dataframe(
        session,
        df2,
        ordering_columns=['"row_pos_2"'],
        row_position_column='"row_pos_2"',
    )

    with pytest.raises(
        ValueError,
        match=f"how={how} is not valid argument for ordered_dataframe.align.",
    ):
        ordered_df1.align(
            ordered_df2,
            left_on_cols=['"B"', '"row_pos"'],
            right_on_cols=['"B_2"', '"row_pos_2"'],
            how=how,
        )


@pytest.mark.parametrize("how", ["inner", "left", "right", "outer"])
def test_self_join_on_row_position_column(ordered_df, how):
    for right in [ordered_df, ordered_df.select('"row_position"', '"a"')]:
        joined_ordered_df = ordered_df.join(
            right,
            left_on_cols=[ordered_df.row_position_snowflake_quoted_identifier],
            right_on_cols=[right.row_position_snowflake_quoted_identifier],
            how=how,
        )
        with SqlCounter(query_count=1, join_count=0):
            joined_ordered_df.collect()
        _verify_dataframe_reference(joined_ordered_df, ordered_df, True)
        _verify_dataframe_reference_quoted_identifiers(joined_ordered_df)
        assert all(
            column
            in joined_ordered_df.projected_column_snowflake_quoted_identifiers
            + joined_ordered_df.ordering_column_snowflake_quoted_identifiers
            for column in ordered_df.projected_column_snowflake_quoted_identifiers
            + ordered_df.ordering_column_snowflake_quoted_identifiers
        )
        assert joined_ordered_df.ordering_columns == ordered_df.ordering_columns
        assert (
            joined_ordered_df.row_position_snowflake_quoted_identifier
            == ordered_df.row_position_snowflake_quoted_identifier
        )


def test_self_cross_join_on_row_position_column(ordered_df):
    joined_ordered_df = ordered_df.join(
        ordered_df,
        how="cross",
    )
    # we can't optimize cross join, so it still happens
    with SqlCounter(query_count=1, join_count=1):
        joined_ordered_df.collect()

    _verify_dataframe_reference(joined_ordered_df, ordered_df, False)
    _verify_dataframe_reference_quoted_identifiers(joined_ordered_df)
    assert joined_ordered_df.row_position_snowflake_quoted_identifier is None


@sql_count_checker(query_count=0)
def test_schema(ordered_df):
    def get_schema_field_quoted_identifiers(df: OrderedDataFrame):
        return [f.column_identifier.quoted_name for f in df.schema.fields]

    assert get_schema_field_quoted_identifiers(ordered_df) == [
        '"row_position"',
        '"a"',
        '"c"',
    ]
    assert get_schema_field_quoted_identifiers(ordered_df.select('"a"')) == ['"a"']
    assert get_schema_field_quoted_identifiers(
        ordered_df.select('"a"', '"c"', '"row_position"')
    ) == ['"a"', '"c"', '"row_position"']


@sql_count_checker(query_count=0)
def test_select_basic(ordered_df):
    # select column with identifiers (str) and a column object with an alias
    # dataframe reference is shared and ordering columns are kept
    new_ordered_df = ordered_df.select('"a"', '"c"', col('"c"').as_('"c1"'))
    _verify_dataframe_reference(new_ordered_df, ordered_df, True)
    _verify_dataframe_reference_quoted_identifiers(new_ordered_df)
    assert new_ordered_df.ordering_columns == ordered_df.ordering_columns
    assert new_ordered_df.projected_column_snowflake_quoted_identifiers == [
        '"a"',
        '"c"',
        '"c1"',
    ]
    assert (
        new_ordered_df.row_position_snowflake_quoted_identifier
        == ordered_df.row_position_snowflake_quoted_identifier
    )


@sql_count_checker(query_count=0)
def test_select_star(ordered_df):
    # select '*', i.e., select all projected columns
    new_ordered_df = ordered_df.select("*")
    _verify_dataframe_reference(new_ordered_df, ordered_df, True)
    _verify_dataframe_reference_quoted_identifiers(new_ordered_df)
    assert new_ordered_df.ordering_columns == ordered_df.ordering_columns
    assert (
        new_ordered_df.projected_column_snowflake_quoted_identifiers
        == ordered_df.projected_column_snowflake_quoted_identifiers
    )
    assert (
        new_ordered_df.row_position_snowflake_quoted_identifier
        == ordered_df.row_position_snowflake_quoted_identifier
    )


@sql_count_checker(query_count=0)
def test_table_function_call(ordered_df):
    # select a table function call. The dataframe reference is not shared
    new_ordered_df = ordered_df.select(flatten(to_array('"a"')), '"c"')
    _verify_dataframe_reference(new_ordered_df, ordered_df, False)
    _verify_dataframe_reference_quoted_identifiers(new_ordered_df)


@sql_count_checker(query_count=0)
def test_select_negative(ordered_df):
    # selecting a column with conflicting alias name is invalid
    with pytest.raises(SnowparkSQLException):
        ordered_df.select(col('"c1"').as_('"c"'))
        ordered_df.to_pandas()

    # selecting a column with no alias name is invalid
    with pytest.raises(AssertionError, match="only column with alias name is allowed"):
        ordered_df.select(col('"c1"'))

    # selecting a column not in project columns is invalid
    with pytest.raises(AssertionError, match="is not in active columns"):
        ordered_df.select('"e"')


@sql_count_checker(query_count=0)
def test_sort(ordered_df):
    new_ordered_df = ordered_df.sort([OrderingColumn('"a"'), OrderingColumn('"c"')])
    _verify_dataframe_reference(new_ordered_df, ordered_df, False)
    _verify_dataframe_reference_quoted_identifiers(new_ordered_df)
    assert new_ordered_df.ordering_columns == [
        OrderingColumn('"a"'),
        OrderingColumn('"c"'),
    ]
    assert (
        new_ordered_df.projected_column_snowflake_quoted_identifiers
        == ordered_df.projected_column_snowflake_quoted_identifiers
    )
    assert new_ordered_df.row_position_snowflake_quoted_identifier is None
    # verify sort doesn't actually generate order by clause
    _verify_order_by_query(new_ordered_df, should_include_order_by=False)

    # selecting the same ordering columns will return the original ordered dataframe
    assert (
        new_ordered_df.sort([OrderingColumn('"a"'), OrderingColumn('"c"')])
        is new_ordered_df
    )

    with pytest.raises(AssertionError, match="not found in"):
        new_ordered_df.sort(OrderingColumn('"b"'))


@sql_count_checker(query_count=0)
def test_ensure_row_position_column(ordered_df):
    assert ordered_df.ensure_row_position_column() is ordered_df

    ordered_df.row_position_snowflake_quoted_identifier = None
    assert ordered_df.row_position_snowflake_quoted_identifier is None

    new_ordered_df = ordered_df.ensure_row_position_column()
    _verify_dataframe_reference(new_ordered_df, ordered_df, True)
    _verify_dataframe_reference_quoted_identifiers(new_ordered_df)
    assert new_ordered_df.projected_column_snowflake_quoted_identifiers == [
        '"row_position"',
        '"a"',
        '"c"',
        '"__row_position__"',
    ]
    # verify ordering column does not change
    assert new_ordered_df.ordering_columns == ordered_df.ordering_columns
    assert (
        new_ordered_df.row_position_snowflake_quoted_identifier == '"__row_position__"'
    )


@sql_count_checker(query_count=0)
def test_ensure_row_position_column_with_row_position_unprojected(ordered_df):
    # do a select to un-project the row position column
    ordered_dataframe = ordered_df.select('"a"', '"b"', '"c"')

    # check the row_position_snowflake_quoted_identifier is still there
    assert ordered_dataframe.row_position_snowflake_quoted_identifier is not None

    ordered_dataframe = ordered_dataframe.ensure_row_position_column()

    # verify the row position column is projected back
    assert ordered_dataframe.projected_column_snowflake_quoted_identifiers == [
        '"a"',
        '"b"',
        '"c"',
        '"row_position"',
    ]


@sql_count_checker(query_count=0)
def test_ensure_row_count_column(ordered_df):
    ordered_df = ordered_df.ensure_row_count_column()
    assert ordered_df.ensure_row_count_column() is ordered_df
    assert ordered_df.row_count_snowflake_quoted_identifier == '"__row_count__"'

    ordered_df.row_count_snowflake_quoted_identifier = None
    assert ordered_df.row_count_snowflake_quoted_identifier is None

    new_ordered_df = ordered_df.ensure_row_count_column()
    _verify_dataframe_reference(new_ordered_df, ordered_df, True)
    _verify_dataframe_reference_quoted_identifiers(new_ordered_df)
    assert new_ordered_df.projected_column_snowflake_quoted_identifiers == [
        '"row_position"',
        '"a"',
        '"c"',
        '"__row_count__"',
        new_ordered_df.row_count_snowflake_quoted_identifier,
    ]
    assert new_ordered_df.row_count_snowflake_quoted_identifier != '"__row_count__"'
    # verify ordering column does not change
    assert new_ordered_df.ordering_columns == ordered_df.ordering_columns


@sql_count_checker(query_count=0)
def test_ensure_row_count_column_with_row_count_unprojected(ordered_df):
    ordered_df = ordered_df.ensure_row_count_column()

    # do a select to un-project the row position column
    ordered_dataframe = ordered_df.select('"a"', '"b"', '"c"')

    # check the row_position_snowflake_quoted_identifier is still there
    assert ordered_dataframe.row_count_snowflake_quoted_identifier is not None

    ordered_dataframe = ordered_dataframe.ensure_row_count_column()

    # verify the row position column is projected back
    assert ordered_dataframe.projected_column_snowflake_quoted_identifiers == [
        '"a"',
        '"b"',
        '"c"',
        '"__row_count__"',
    ]


@sql_count_checker(query_count=0)
def test_dropna(ordered_df):
    new_ordered_df = ordered_df.dropna()
    _verify_dataframe_reference(new_ordered_df, ordered_df, False)
    _verify_dataframe_reference_quoted_identifiers(new_ordered_df)
    # ordered dataframe is included
    assert new_ordered_df.projected_column_snowflake_quoted_identifiers == [
        '"row_position"',
        '"a"',
        '"c"',
        '"b"',
    ]
    assert new_ordered_df.ordering_columns == ordered_df.ordering_columns
    assert new_ordered_df.row_position_snowflake_quoted_identifier is None


@sql_count_checker(query_count=0)
def test_union_all(ordered_df):
    new_ordered_df = ordered_df.union_all(ordered_df)
    _verify_dataframe_reference(new_ordered_df, ordered_df, False)
    _verify_dataframe_reference_quoted_identifiers(new_ordered_df)
    # ordered columns are included
    assert (
        new_ordered_df.projected_column_snowflake_quoted_identifiers
        == ordered_df.projected_column_snowflake_quoted_identifiers
    )
    assert (
        new_ordered_df.ordering_column_snowflake_quoted_identifiers
        == ordered_df.projected_column_snowflake_quoted_identifiers
    )
    assert new_ordered_df.row_position_snowflake_quoted_identifier is None


@sql_count_checker(query_count=0)
def test_groupby(ordered_df):
    new_ordered_df = ordered_df.group_by(
        ['"a"', '"c"'], min_('"a"').as_('"min_a"'), max_('"c"').as_('"max_c"')
    )
    _verify_dataframe_reference(new_ordered_df, ordered_df, False)
    _verify_dataframe_reference_quoted_identifiers(new_ordered_df)
    assert new_ordered_df.ordering_column_snowflake_quoted_identifiers == ['"a"', '"c"']
    assert new_ordered_df.row_position_snowflake_quoted_identifier is None


@sql_count_checker(query_count=0)
def test_pivot(ordered_df):
    new_ordered_df = ordered_df.pivot('"a"', None, None, min_('"a"'))
    _verify_dataframe_reference(new_ordered_df, ordered_df, False)
    _verify_dataframe_reference_quoted_identifiers(new_ordered_df)
    assert (
        new_ordered_df.projected_column_snowflake_quoted_identifiers
        == new_ordered_df.ordering_column_snowflake_quoted_identifiers
    )
    assert new_ordered_df.row_position_snowflake_quoted_identifier is None


@sql_count_checker(query_count=0)
def test_unpivot(ordered_df):
    new_ordered_df = ordered_df.unpivot('"d"', '"e"', ['"a"', '"c"'])
    _verify_dataframe_reference(new_ordered_df, ordered_df, False)
    _verify_dataframe_reference_quoted_identifiers(new_ordered_df)
    assert (
        new_ordered_df.projected_column_snowflake_quoted_identifiers
        == new_ordered_df.ordering_column_snowflake_quoted_identifiers
    )
    assert new_ordered_df.row_position_snowflake_quoted_identifier is None


@sql_count_checker(query_count=1)
def test_unpivot_column_mapping(ordered_df):
    new_ordered_df = ordered_df.unpivot(
        '"d"', '"e"', ['"a"', '"c"'], {'"a"': '"Apple"'}
    )
    _verify_dataframe_reference(new_ordered_df, ordered_df, False)
    _verify_dataframe_reference_quoted_identifiers(new_ordered_df)
    assert new_ordered_df.to_pandas()["e"].to_list() == [
        "Apple",
        "c",
        "Apple",
        "c",
    ]
    assert (
        new_ordered_df.projected_column_snowflake_quoted_identifiers
        == new_ordered_df.ordering_column_snowflake_quoted_identifiers
    )
    assert new_ordered_df.row_position_snowflake_quoted_identifier is None


@sql_count_checker(query_count=0)
def test_agg(ordered_df):
    new_ordered_df = ordered_df.agg(
        min_('"a"').as_('"max_a"'), max_('"c"').as_('"max_c"')
    )
    _verify_dataframe_reference(new_ordered_df, ordered_df, False)
    _verify_dataframe_reference_quoted_identifiers(new_ordered_df)
    assert (
        new_ordered_df.projected_column_snowflake_quoted_identifiers
        == new_ordered_df.ordering_column_snowflake_quoted_identifiers
    )
    assert new_ordered_df.row_position_snowflake_quoted_identifier is None


@sql_count_checker(query_count=0)
def test_filter(ordered_df):
    new_ordered_df = ordered_df.filter(col('"a"') == 1)
    _verify_dataframe_reference(new_ordered_df, ordered_df, False)
    _verify_dataframe_reference_quoted_identifiers(new_ordered_df)
    # ordered dataframe is included
    assert new_ordered_df.projected_column_snowflake_quoted_identifiers == [
        '"row_position"',
        '"a"',
        '"c"',
        '"b"',
    ]
    assert new_ordered_df.ordering_columns == ordered_df.ordering_columns
    assert new_ordered_df.row_position_snowflake_quoted_identifier is None


@sql_count_checker(query_count=0)
def test_limit(ordered_df):
    new_ordered_df = ordered_df.limit(10)
    _verify_dataframe_reference(new_ordered_df, ordered_df, False)
    _verify_dataframe_reference_quoted_identifiers(new_ordered_df)
    # ordered dataframe is included
    assert new_ordered_df.projected_column_snowflake_quoted_identifiers == [
        '"row_position"',
        '"a"',
        '"c"',
        '"b"',
    ]
    assert new_ordered_df.ordering_columns == ordered_df.ordering_columns
    assert new_ordered_df.row_position_snowflake_quoted_identifier is not None


@sql_count_checker(query_count=1)
def test_write(session, ordered_df):
    table_name = random_name_for_temp_object(TempObjectType.TABLE)
    ordered_df.write.save_as_table(table_name, table_type="temp")
    snowpark_df = session.table(table_name)
    all_columns_set = set(snowpark_df.columns)
    # verify projected columns, ordering columns and row position column are all in the result columns
    assert set(ordered_df.projected_column_snowflake_quoted_identifiers).issubset(
        all_columns_set
    )
    assert set(ordered_df.ordering_column_snowflake_quoted_identifiers).issubset(
        all_columns_set
    )
    assert ordered_df.row_position_snowflake_quoted_identifier in all_columns_set


@sql_count_checker(query_count=1)
def test_to_pandas(ordered_df):
    assert_frame_equal(
        ordered_df.to_pandas(),
        native_pd.DataFrame([[1, 1, 3], [0, 4, 6]], columns=["row_position", "a", "c"]),
        check_dtype=False,
    )


@pytest.mark.parametrize(
    "how",
    ["left", "outer"],
)
@pytest.mark.parametrize(
    "align_on_cols",
    [['"A"'], ['"A"', '"C"'], ['"row_pos"', '"C"']],
)
@sql_count_checker(query_count=1, join_count=0)
def test_self_align_optimizable(session, df1, how, align_on_cols):
    ordered_df1 = _create_ordered_dataframe(
        session, df1, ordering_columns=['"row_pos"'], row_position_column='"row_pos"'
    )
    # project new columns from ordered_df1
    ordered_df2 = ordered_df1.select(
        '"A"', (Column('"B"') + 1).as_('"B_postfix"'), '"C"', '"row_pos"'
    )
    aligned_ordered_df = ordered_df1.align(
        ordered_df2,
        left_on_cols=align_on_cols,
        right_on_cols=align_on_cols,
        how=how,
    )

    # verify the result frame share the same frame as ordered_df1
    _verify_dataframe_reference(aligned_ordered_df, ordered_df1, should_match=True)
    _verify_dataframe_reference(aligned_ordered_df, ordered_df2, should_match=True)
    _verify_dataframe_reference_quoted_identifiers(aligned_ordered_df)

    # verify the result ordering column is the same as ordered_df1
    assert aligned_ordered_df.ordering_columns == ordered_df1.ordering_columns

    # verify the row position column is the same as ordered_df1
    assert (
        aligned_ordered_df.row_position_snowflake_quoted_identifier
        == ordered_df1.row_position_snowflake_quoted_identifier
    )

    # verify the result is the same as join on row pos column
    df2 = df1.copy()
    df2["B"] = df2["B"] + 1

    _join_or_align_result_validation_helper(
        aligned_ordered_df,
        ordered_df1,
        ordered_df2,
        df1,
        df2,
        left_on_cols=["row_pos"],
        right_on_cols=["row_pos"],
        how=how,
    )


@pytest.mark.parametrize(
    "how",
    ["left", "outer"],
)
@pytest.mark.parametrize(
    "left_on_cols, right_on_cols, right_ordering_columns",
    [
        (['"A"'], ['"A"'], [OrderingColumn('"C"')]),
        (['"A"', '"B"'], ['"A"', '"B_postfix"'], None),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_self_align_not_optimizable(
    session, df1, how, left_on_cols, right_on_cols, right_ordering_columns
):
    ordered_df1 = _create_ordered_dataframe(
        session, df1, ordering_columns=['"row_pos"'], row_position_column='"row_pos"'
    )
    # project new columns from ordered_df1
    ordered_df2 = ordered_df1.select(
        '"A"', (Column('"B"') + 1).as_('"B_postfix"'), '"C"', '"row_pos"'
    )
    if right_ordering_columns is not None:
        ordered_df2 = ordered_df2.sort(right_ordering_columns)

    aligned_ordered_df = ordered_df1.align(
        ordered_df2,
        left_on_cols=left_on_cols,
        right_on_cols=right_on_cols,
        how=how,
    )

    # verify the result frame do not share the same frame as ordered_df1
    _verify_dataframe_reference(aligned_ordered_df, ordered_df1, should_match=False)
    _verify_dataframe_reference(aligned_ordered_df, ordered_df2, should_match=False)
    _verify_dataframe_reference_quoted_identifiers(aligned_ordered_df)

    df2 = df1.copy()
    df2["B"] = df2["B"] + 1
    if right_ordering_columns is not None:
        ordering_column_identifiers = (
            ordered_df2.ordering_column_snowflake_quoted_identifiers
        )
        sort_columns = [
            unquote_name_if_quoted(quoted_identifier)
            for quoted_identifier in ordering_column_identifiers
        ]
        df2.sort_values(sort_columns)

    sort = False
    if how == "outer":
        sort = True
    _join_or_align_result_validation_helper(
        aligned_ordered_df,
        ordered_df1,
        ordered_df2,
        df1,
        df2,
        left_on_cols=left_on_cols,
        right_on_cols=right_on_cols,
        how=how,
        sort=sort,
    )


@pytest.mark.parametrize("include_ordering_columns", [True, False])
@pytest.mark.parametrize("include_row_position_column", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@sql_count_checker(query_count=0, join_count=0)
def test_to_projected_snowpark_dataframe(
    session, df1, include_ordering_columns, include_row_position_column, sort
):
    ordered_df = _create_ordered_dataframe(
        session,
        df1,
        projected_columns=['"A"', '"C"'],
        ordering_columns=['"B"', '"C"'],
        row_position_column='"row_pos"',
    )
    snowpark_dataframe = ordered_df.to_projected_snowpark_dataframe(
        include_ordering_columns=include_ordering_columns,
        include_row_position_column=include_row_position_column,
        sort=sort,
    )
    snowpark_dataframe_quoted_identifiers = [
        f.column_identifier.quoted_name for f in snowpark_dataframe.schema.fields
    ]
    result_columns_quoted_identifiers = ['"A"', '"C"']
    if include_ordering_columns:
        result_columns_quoted_identifiers += ['"B"']
    if include_row_position_column:
        result_columns_quoted_identifiers += ['"row_pos"']
    assert snowpark_dataframe_quoted_identifiers == result_columns_quoted_identifiers
    _verify_order_by_query(snowpark_dataframe, should_include_order_by=sort)


@sql_count_checker(query_count=1, join_count=0)
def test_to_projected_snowpark_dataframe_with_rename(ordered_df):
    # rename column '"a"' to '"b"', which is the same as the ordering column that will be dropped
    col_mapper = {'"a"': '"b"', '"c"': '"e"'}
    snowpark_dataframe = ordered_df.to_projected_snowpark_dataframe(
        col_mapper=col_mapper
    )

    # call collect to trigger evaluation with no failure
    snowpark_dataframe.collect()

    # verify the renamed column
    result_columns_quoted_identifiers = ['"row_position"', '"b"', '"e"']
    snowpark_dataframe_quoted_identifiers = get_snowpark_dataframe_quoted_identifiers(
        snowpark_dataframe
    )
    assert snowpark_dataframe_quoted_identifiers == result_columns_quoted_identifiers


@sql_count_checker(query_count=0)
def test_snowpark_pandas_statement_params(session, df1):
    ordered_df1 = _create_ordered_dataframe(
        session, df1, ordering_columns=['"row_pos"'], row_position_column='"row_pos"'
    )

    with patch.object(snowflake.snowpark.DataFrame, "collect") as mocked_collect:
        ordered_df1.collect()
        mocked_collect.assert_called_once()
        assert (
            "pandas"
            == mocked_collect.call_args.kwargs["statement_params"]["SNOWPARK_API"]
        )

    with patch.object(snowflake.snowpark.DataFrame, "collect") as mocked_collect:
        ordered_df1.collect(statement_params={"abc": "efg"})
        mocked_collect.assert_called_once()
        assert (
            "pandas"
            == mocked_collect.call_args.kwargs["statement_params"]["SNOWPARK_API"]
        )
        assert "efg" == mocked_collect.call_args.kwargs["statement_params"]["abc"]


@sql_count_checker(query_count=5)
@pytest.mark.parametrize("columns", [["A", "b", "C"]])
def test_ordered_dataframe_row_count(session, columns):
    num_rows = 10
    data = [[0] * len(columns) for _ in range(num_rows)]
    test_table_name = random_name_for_temp_object(TempObjectType.TABLE)
    snowpark_df = session.create_dataframe(data, schema=columns)
    snowpark_df.write.save_as_table(test_table_name, mode="overwrite")

    (
        ordered_df1,
        row_position_quoted_identifier,
    ) = create_initial_ordered_dataframe(test_table_name, enforce_ordering=True)
    (
        ordered_df2,
        row_position_quoted_identifier,
    ) = create_initial_ordered_dataframe(test_table_name, enforce_ordering=True)

    # Ensure that the row_count and row_count_upper_bound are being set correctly
    assert ordered_df1.row_count == 10
    assert ordered_df1.row_count_upper_bound == 10
    assert ordered_df2.row_count == 10
    assert ordered_df2.row_count_upper_bound == 10

    # Ensure row_count_upper_bound does not change with the following operations:
    # SORT, DROPNA, FILTER, GROUP_BY
    ordered_df2 = ordered_df2.sort([OrderingColumn('"A"')])
    ordered_df2 = ordered_df2.dropna()
    ordered_df2 = ordered_df2.filter(col('"A"') == 1)
    ordered_df2 = ordered_df2.group_by(['"A"'], count(col("*")).alias("count"))
    assert ordered_df2.row_count_upper_bound == 10

    # Ensure LIMIT sets row_count_upper_bound to n rows
    ordered_df2 = ordered_df2.filter(col('"A"') == 1).limit(n=8)
    assert ordered_df2.row_count_upper_bound == 8

    # Ensure SAMPLE sets row_count_upper_bound correctly
    ordered_df2 = ordered_df2.sample(
        n=5, frac=None
    )  # Set upper bound to n if frac == None
    assert ordered_df2.row_count_upper_bound == 5
    ordered_df2 = ordered_df2.sample(
        n=None, frac=0.5
    )  # Set upper bound to ceil(row_count_upper_bound * frac)
    assert ordered_df2.row_count_upper_bound == 3

    # Ensure ALIGN and JOIN set row_count_upper_bound to row_count_upper_bound * other.row_count_upper_bound
    ordered_df2 = ordered_df2.join(ordered_df1, ['"A"'], ['"A"'])
    assert ordered_df2.row_count_upper_bound == 30  # Set upper bound to 3 * 10 = 30
    ordered_df2 = ordered_df2.align(ordered_df1, ['"C"'], ['"C"'])
    assert ordered_df2.row_count_upper_bound == 40  # Set upper bound to 30 + 10 = 40

    # Ensure UNION_ALL sets row_count_upper_bound to row_count_upper_bound + other.row_count_upper_bound
    ordered_df2 = ordered_df2.union_all(ordered_df1)
    assert ordered_df2.row_count_upper_bound == 50  # Set upper bound to 40 + 10 = 50

    # Ensure AGG sets row_count_upper_bound to 1
    ordered_df1 = ordered_df1.agg(max_(col('"B"')).alias("max"))
    assert ordered_df1.row_count_upper_bound == 1
