#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

from snowflake.snowpark._internal.analyzer.analyzer_utils import quote_name
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.modin.plugin._internal.utils import (
    create_initial_ordered_dataframe,
    get_object_metadata_row_count,
)
from snowflake.snowpark.modin.plugin.extensions.utils import (
    ensure_index,
    try_convert_index_to_native,
)
from tests.integ.modin.utils import assert_index_equal
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import Utils


@pytest.mark.parametrize("columns", [["A", "b", "C"], ['"a"', '"B"', '"c"']])
@sql_count_checker(query_count=3)
def test_create_snowpark_dataframe_with_readonly_temp_table(session, columns):
    num_rows = 10
    data = [[0] * len(columns) for _ in range(num_rows)]
    test_table_name = random_name_for_temp_object(TempObjectType.TABLE)
    snowpark_df = session.create_dataframe(data, schema=columns)
    snowpark_df.write.save_as_table(test_table_name, mode="overwrite")

    (
        ordered_df,
        row_position_quoted_identifier,
    ) = create_initial_ordered_dataframe(test_table_name, enforce_ordering=True)

    # verify the ordered df columns are row_position_quoted_identifier + quoted_identifiers
    assert ordered_df.projected_column_snowflake_quoted_identifiers == [
        row_position_quoted_identifier
    ] + [quote_name(c) for c in columns]
    assert [
        row[0] for row in ordered_df.select(row_position_quoted_identifier).collect()
    ] == list(range(num_rows))


@pytest.mark.parametrize("columns", [["A", "b", "C"], ['"a"', '"B"', '"c"']])
@sql_count_checker(query_count=2)
def test_create_snowpark_dataframe_with_no_readonly_temp_table(session, columns):
    num_rows = 10
    data = [[0] * len(columns) for _ in range(num_rows)]
    test_table_name = random_name_for_temp_object(TempObjectType.TABLE)
    snowpark_df = session.create_dataframe(data, schema=columns)
    snowpark_df.write.save_as_table(test_table_name, mode="overwrite")

    (
        ordered_df,
        row_position_quoted_identifier,
    ) = create_initial_ordered_dataframe(test_table_name, enforce_ordering=False)

    # verify the ordered df columns are row_position_quoted_identifier + quoted_identifiers
    assert ordered_df.projected_column_snowflake_quoted_identifiers == [
        row_position_quoted_identifier
    ] + [quote_name(c) for c in columns]
    assert [
        row[0] for row in ordered_df.select(row_position_quoted_identifier).collect()
    ] == list(range(num_rows))


INDEX_DATA = [
    [1, 2, 3],
    ["a", "b", "c"],
    [1, "a", -5, "abc"],
    np.array([1, 2, 3]),
    [[1, 2, 3], ["a", "b", "c"]],
]


@pytest.mark.parametrize("data", INDEX_DATA)
def test_assert_index_equal(data):
    if isinstance(data[0], list):
        a = pd.MultiIndex.from_arrays(data)
        b = pd.MultiIndex.from_arrays(data)
        c = pd.MultiIndex.from_arrays([[-1, 2, 3], ["a", "b", "c"]])
    else:
        a = pd.Index(data)
        b = pd.Index(data)
        c = pd.Index([-1, 2, 3])
    with SqlCounter(query_count=0 if isinstance(data[0], list) else 6):
        assert_index_equal(a, b)
        with pytest.raises(AssertionError):
            assert_index_equal(a, c)
        with pytest.raises(AssertionError):
            assert_index_equal(b, c)


@pytest.mark.parametrize("data", INDEX_DATA)
def test_try_convert_to_native_index(data):

    # we only convert a snowpark pandas index to a native pandas index
    data = try_convert_index_to_native(data)
    assert isinstance(data, (list, np.ndarray))

    if isinstance(data[0], list):
        index = pd.MultiIndex.from_arrays(data)
        index2 = pd.MultiIndex.from_arrays(data)
    else:
        index = pd.Index(data)
        index2 = pd.Index(data)
    with SqlCounter(query_count=0 if isinstance(data[0], list) else 2):
        index = try_convert_index_to_native(index)
        assert isinstance(index, native_pd.Index)
        assert_index_equal(index, index2)


def ensure_index_test_helper(data, qc):
    with SqlCounter(query_count=qc):
        new_data = ensure_index(data)
        # if the given data is a list of lists, ensure_index should be converting to a multiindex
        if isinstance(data[0], list):
            assert isinstance(new_data, pd.MultiIndex)
            assert_index_equal(new_data, pd.MultiIndex.from_arrays(data))
        # if the given data is a list of tuples, ensure_index should be converting to a multiindex
        # this case would also apply if the given data was a multiindex
        elif isinstance(data[0], tuple):
            assert isinstance(new_data, pd.MultiIndex)
            assert_index_equal(new_data, pd.MultiIndex.from_tuples(data))
        # otherwise, ensure_index should convert to a pd.Index
        else:
            assert isinstance(new_data, pd.Index)
            assert_index_equal(new_data, native_pd.Index(data), exact=False)


@pytest.mark.parametrize(
    "data",
    [
        [1, 2, 3],
        ["a", "b", "c"],
        [1, "a", -5, "abc"],
        np.array([1, 2, 3]),
        native_pd.Index([1, 2, 3]),
        native_pd.Index(["a", "b", "c"]),
        native_pd.Series([1, 2, 3]),
        native_pd.MultiIndex.from_arrays([[1, 2, 3], ["a", "b", "c"]]),
    ],
)
def test_ensure_index(data):
    qc = 1
    if isinstance(data, native_pd.MultiIndex):
        qc = 0
    elif isinstance(data, native_pd.Index):
        # test on native_pd.Index
        ensure_index_test_helper(data, 1)
        # convert to pd.Index to test on pd.Index later
        data = pd.Index(data)
        qc = 4
    elif isinstance(data, native_pd.Series):
        # test on native_pd.Series
        ensure_index_test_helper(data, 1)
        # convert to pd.Series to test on pd.Series later
        data = pd.Series(data)
        qc = 7
    ensure_index_test_helper(data, qc)


@sql_count_checker(
    query_count=15,
    high_count_expected=True,
    high_count_reason="Tests multiple table types",
)
def test_get_object_metadata_row_count(session):
    # Test with a table that exists and has rows.
    table_name_with_rows = random_name_for_temp_object(TempObjectType.TABLE)
    num_rows = 5
    # Use temporary table to ensure it's cleaned up after session.
    session.create_dataframe(
        [[i] for i in range(num_rows)], schema=["a"]
    ).write.save_as_table(
        table_name_with_rows, mode="overwrite", table_type="temporary"
    )

    # Test with different qualifications
    current_db = session.get_current_database()
    current_schema = session.get_current_schema()

    assert get_object_metadata_row_count(table_name_with_rows) == num_rows
    assert (
        get_object_metadata_row_count(f"{current_schema}.{table_name_with_rows}")
        == num_rows
    )
    assert (
        get_object_metadata_row_count(
            f"{current_db}.{current_schema}.{table_name_with_rows}"
        )
        == num_rows
    )

    # Test with views; should return None
    try:
        df = pd.read_snowflake(f"SELECT * FROM {table_name_with_rows}")
        view_name = f"{table_name_with_rows}_V"
        df.to_view(view_name)
        assert get_object_metadata_row_count(view_name) is None
        assert get_object_metadata_row_count(f"{current_schema}.{view_name}") is None
        assert (
            get_object_metadata_row_count(f"{current_db}.{current_schema}.{view_name}")
            is None
        )
    finally:
        Utils.drop_view(session, view_name)

    # Test with materialized views; should return None ( unfortunately )
    try:
        materialized_view_name = f"{table_name_with_rows}_MV"
        session.sql(
            f"CREATE MATERIALIZED VIEW {materialized_view_name} AS SELECT * FROM {table_name_with_rows}"
        )
        assert get_object_metadata_row_count(materialized_view_name) is None
        assert (
            get_object_metadata_row_count(f"{current_schema}.{materialized_view_name}")
            is None
        )
        assert (
            get_object_metadata_row_count(
                f"{current_db}.{current_schema}.{materialized_view_name}"
            )
            is None
        )
    finally:
        Utils.drop_view(session, materialized_view_name)

    # Test with a non-existent table.
    non_existent_table = random_name_for_temp_object(TempObjectType.TABLE)
    assert get_object_metadata_row_count(non_existent_table) is None

    # Test with an invalid name.
    assert get_object_metadata_row_count("a.b.c.d.e") is None
