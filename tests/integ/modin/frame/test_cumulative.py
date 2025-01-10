#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

TEST_LABELS = np.array(["A", "B", "C", "D"])
TEST_DATA = [[0, 1, 2, 3], [0, 0, 0, 0], [None, 0, None, 0], [None, None, None, None]]

# which original dataframe (constructed from slicing) to test for
TEST_SLICES = [
    (slice(None), 0),
    (slice(None), slice(None)),
    (slice(None), -1),
]


@pytest.mark.parametrize("axes_slices", TEST_SLICES)
@pytest.mark.parametrize("func_name", ["cumsum", "cummin", "cummax"])
@pytest.mark.parametrize("skipna", [True, False])
def test_dataframe_cumfunc(axes_slices, func_name, skipna):
    df = pd.DataFrame(
        pd.DataFrame(TEST_DATA, columns=TEST_LABELS).iloc[
            axes_slices[0], axes_slices[1]
        ]
    )
    native_df = native_pd.DataFrame(
        native_pd.DataFrame(TEST_DATA, columns=TEST_LABELS).iloc[
            axes_slices[0], axes_slices[1]
        ]
    )
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            df,
            native_df,
            lambda df: getattr(df, func_name)(axis=0, skipna=skipna),
        )


@pytest.mark.parametrize("func_name", ["cumsum", "cummin", "cummax"])
@sql_count_checker(query_count=0)
def test_dataframe_cumfunc_axis_negative(func_name):
    df = pd.DataFrame(TEST_DATA, columns=TEST_LABELS)
    with pytest.raises(ValueError, match="No axis named 2 for object type DataFrame"):
        getattr(df, func_name)(axis=2)
    with pytest.raises(NotImplementedError):
        getattr(df, func_name)(axis=1)
