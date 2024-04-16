#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import pandas as native_pd
import pytest

import snowflake.snowpark.modin.pandas as pd
from tests.integ.modin.sql_counter import sql_count_checker


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "data, index",
    [
        ([1, None, 4, 3, 4], ["A", "B", "C", "D", "E"]),
        ([1, None, 4, 3, 4], [None, "B", "C", "D", "E"]),
        ([1, 10, 4, 3, 4], ["E", "D", "C", "A", "B"]),
    ],
)
@pytest.mark.parametrize("func", ["idxmax", "idxmin"])
@pytest.mark.parametrize(
    "skipna",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.xfail(
                reason="When the data column is None, Snowpark pandas returns None instead of nan"
            ),
        ),
    ],
)
def test_idxmax_idxmin_series(data, index, func, skipna):
    native_series = native_pd.Series(data=data, index=index)
    snow_series = pd.Series(native_series)

    native_output = native_series.__getattribute__(func)(axis=0, skipna=skipna)
    snow_output = snow_series.__getattribute__(func)(axis=0, skipna=skipna)
    assert native_output == snow_output


@pytest.mark.parametrize("func", ["idxmax", "idxmin"])
@pytest.mark.parametrize("skipna", [True, False])
@sql_count_checker(query_count=0)
def test_series_idxmax_idxmin_with_multiindex(
    multiindex_native_int_series, func, skipna
):
    """
    Test DataFrameGroupBy.idxmax and DataFrameGroupBy.idxmin with a MultiIndex DataFrame.
    Here, the MultiIndex DataFrames are grouped by `level` and not `by`.
    """
    # Create MultiIndex DataFrames.
    native_series = multiindex_native_int_series
    snow_series = pd.Series(native_series)
    with pytest.raises(
        NotImplementedError,
        match=f"{func} is not yet supported when the index is a MultiIndex.",
    ):
        snow_series.__getattribute__(func)(axis=0, skipna=skipna)
