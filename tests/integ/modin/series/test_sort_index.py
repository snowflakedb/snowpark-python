#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.pandas as pd
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result


@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("na_position", ["first", "last"])
@pytest.mark.parametrize("ignore_index", [True, False])
@sql_count_checker(query_count=1)
def test_sort_index_series(ascending, na_position, ignore_index):
    native_series = native_pd.Series(["a", "b", np.nan, "d"], index=[3, 2, 1, np.nan])
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda s: s.sort_index(
            ascending=ascending,
            na_position=na_position,
            ignore_index=ignore_index,
        ),
    )


@sql_count_checker(query_count=0)
def test_sort_index_series_inplace_unsupported():
    snow_series = pd.Series(["a", "b", np.nan, "d"], index=[3, 2, 1, np.nan])
    with pytest.raises(NotImplementedError):
        snow_series.sort_index(inplace=True)


@sql_count_checker(query_count=0)
def test_sort_index_series_multiindex_unsupported():
    arrays = [
        np.array(["qux", "qux", "foo", "foo", "baz", "baz", "bar", "bar"]),
        np.array(["two", "one", "two", "one", "two", "one", "two", "one"]),
    ]
    snow_series = pd.Series([1, 2, 3, 4, 5, 6, 7, 8], index=arrays)
    with pytest.raises(NotImplementedError):
        snow_series.sort_index(level=1)
    with pytest.raises(NotImplementedError):
        snow_series.sort_index(sort_remaining=True)
    with pytest.raises(NotImplementedError):
        snow_series.sort_index(ascending=[True, False])
