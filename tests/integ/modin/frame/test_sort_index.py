#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result


@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("na_position", ["first", "last"])
@pytest.mark.parametrize("ignore_index", [True, False])
@pytest.mark.parametrize("inplace", [True, False])
@sql_count_checker(query_count=1)
def test_sort_index_dataframe(ascending, na_position, ignore_index, inplace):
    native_df = native_pd.DataFrame(
        [1, 2, np.nan, 4, 5], index=[np.nan, 29, 234, 1, 150], columns=["A"]
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.sort_index(
            ascending=ascending,
            na_position=na_position,
            ignore_index=ignore_index,
            inplace=inplace,
        ),
        inplace=inplace,
    )


@sql_count_checker(query_count=0)
def test_sort_index_dataframe_axis_1_unsupported():
    snow_df = pd.DataFrame(
        [1, 2, np.nan, 4, 5], index=[np.nan, 29, 234, 1, 150], columns=["A"]
    )
    with pytest.raises(NotImplementedError):
        snow_df.sort_index(axis=1)


@sql_count_checker(query_count=0)
def test_sort_index_dataframe_multiindex_unsupported():
    arrays = [
        np.array(["qux", "qux", "foo", "foo", "baz", "baz", "bar", "bar"]),
        np.array(["two", "one", "two", "one", "two", "one", "two", "one"]),
    ]
    snow_df = pd.DataFrame([1, 2, 3, 4, 5, 6, 7, 8], index=arrays, columns=["A"])
    with pytest.raises(NotImplementedError):
        snow_df.sort_index(level=1)
    with pytest.raises(NotImplementedError):
        snow_df.sort_index(sort_remaining=True)
    with pytest.raises(NotImplementedError):
        snow_df.sort_index(ascending=[True, False])
