#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize(
    "data, index, columns",
    [
        ([[0, 1], [2, 3]], ["cat", "dog"], ["weight", "height"]),
        ([[0, np.nan], [np.nan, 3]], ["cat", "dog"], ["weight", "height"]),
    ],
)
@pytest.mark.parametrize("dropna", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@sql_count_checker(query_count=1)
def test_stack(data, index, columns, dropna, sort):
    eval_snowpark_pandas_result(
        *create_test_dfs(data=data, index=index, columns=columns),
        lambda df: df.stack(dropna=dropna, sort=sort),
    )


@pytest.mark.parametrize("dropna", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@sql_count_checker(query_count=1)
def test_stack_with_index_name(dropna, sort):
    index = native_pd.Index(data=["cat", "dog"], name="hello")
    native_df = native_pd.DataFrame(
        data=[[0, 1], [2, 3]], index=index, columns=["weight", "height"]
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.stack(dropna=dropna, sort=sort),
    )


@sql_count_checker(query_count=0)
def test_stack_level_unsupported():
    df_single_level_cols = pd.DataFrame(
        [[0, 1], [2, 3]], index=["cat", "dog"], columns=["weight", "height"]
    )

    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas doesn't yet support 'level != -1' in stack API",
    ):
        df_single_level_cols.stack(level=0)


@sql_count_checker(query_count=0)
def test_stack_multiindex_unsupported():
    multicol1 = pd.MultiIndex.from_tuples([("weight", "kg"), ("weight", "pounds")])
    df_multi_level_cols1 = pd.DataFrame(
        [[1, 2], [2, 4]], index=["cat", "dog"], columns=multicol1
    )

    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas doesn't support multiindex columns in stack API",
    ):
        df_multi_level_cols1.stack()


@sql_count_checker(query_count=0)
def test_stack_timedelta_unsupported():
    with pytest.raises(NotImplementedError):
        eval_snowpark_pandas_result(
            *create_test_dfs(
                [[0, 1], [2, 3]],
                index=["cat", "dog"],
                columns=["weight", "height"],
                dtype="timedelta64[ns]",
            ),
            lambda df: df.stack(),
        )
