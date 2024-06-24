#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pytest

from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result


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
        *create_test_dfs(data, index, columns),
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
