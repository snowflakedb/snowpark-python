#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pytest

from tests.integ.modin.sql_counter import sql_count_checker


@pytest.mark.parametrize(
    "agg_func, agg_func_kwargs",
    [
        ("count", None),
        ("sum", None),
        ("mean", None),
        ("median", None),
        ("var", 1),
        ("std", 1),
        ("min", None),
        ("max", None),
        ("corr", None),
        ("cov", None),
        ("skew", None),
        ("kurt", None),
        ("apply", "min"),
        ("aggregate", "min"),
        ("quantile", 0.5),
        ("sem", 1),
        ("rank", None),
    ],
)
@sql_count_checker(query_count=0)
def test_expanding_aggregation_dataframe_unsupported(agg_func, agg_func_kwargs):
    snow_df = pd.DataFrame({"B": [0, 1, 2, np.nan, 4]})
    with pytest.raises(NotImplementedError):
        getattr(snow_df.expanding(), agg_func)(agg_func_kwargs)


@pytest.mark.parametrize(
    "agg_func, agg_func_kwargs",
    [
        ("count", None),
        ("sum", None),
        ("mean", None),
        ("median", None),
        ("var", 1),
        ("std", 1),
        ("min", None),
        ("max", None),
        ("corr", None),
        ("cov", None),
        ("skew", None),
        ("kurt", None),
        ("apply", "min"),
        ("aggregate", "min"),
        ("quantile", 0.5),
        ("sem", 1),
        ("rank", None),
    ],
)
@sql_count_checker(query_count=0)
def test_expanding_aggregation_series_unsupported(agg_func, agg_func_kwargs):
    snow_df = pd.Series([2, 3, 4, 1], index=["a", "b", "c", "d"])
    with pytest.raises(NotImplementedError):
        getattr(snow_df.expanding(), agg_func)(agg_func_kwargs)
