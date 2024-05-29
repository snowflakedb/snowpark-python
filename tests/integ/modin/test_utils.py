#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.pandas as pd
from snowflake.snowpark._internal.analyzer.analyzer_utils import quote_name
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.modin.pandas.utils import (
    ensure_index,
    try_convert_to_native_index,
)
from snowflake.snowpark.modin.plugin._internal.utils import (
    create_ordered_dataframe_with_readonly_temp_table,
)
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import assert_index_equal


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
    ) = create_ordered_dataframe_with_readonly_temp_table(test_table_name)

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
@sql_count_checker(query_count=0)
def test_assert_index_equal(data):
    if isinstance(data[0], list):
        a = pd.MultiIndex.from_arrays(data)
        b = pd.MultiIndex.from_arrays(data)
        c = pd.MultiIndex.from_arrays([[-1, 2, 3], ["a", "b", "c"]])
    else:
        a = pd.Index(data)
        b = pd.Index(data)
        c = pd.Index([-1, 2, 3])
    assert_index_equal(a, b)
    with pytest.raises(AssertionError):
        assert_index_equal(a, c)
    with pytest.raises(AssertionError):
        assert_index_equal(b, c)


@pytest.mark.parametrize("data", INDEX_DATA)
@sql_count_checker(query_count=0)
def test_try_convert_to_native_index(data):
    data = try_convert_to_native_index(data)
    assert isinstance(data, (list, np.ndarray))
    if isinstance(data[0], list):
        index = pd.MultiIndex.from_arrays(data)
        index2 = pd.MultiIndex.from_arrays(data)

    else:
        index = pd.Index(data)
        index2 = pd.Index(data)
    index = try_convert_to_native_index(index)
    assert isinstance(index, native_pd.Index)
    assert_index_equal(index, index2)


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
        [[1, 2, 3], ["a", "b", "c"]],
    ],
)
def test_ensure_index(data):
    qc = 0
    if isinstance(data, native_pd.Index):
        data = pd.Index(data)
    elif isinstance(data, native_pd.Series):
        data = pd.Series(data)
        qc = 2
    with SqlCounter(query_count=qc):
        new_data = ensure_index(data)
        if isinstance(new_data, pd.MultiIndex):
            assert_index_equal(new_data, pd.MultiIndex.from_arrays(data))
        else:
            assert isinstance(new_data, pd.Index)
            assert_index_equal(new_data, native_pd.Index(data))
