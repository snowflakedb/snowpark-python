#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.pandas as pd
from snowflake.snowpark import Session
from tests.integ.modin.sql_counter import SqlCounter
from tests.parameters import CONNECTION_PARAMETERS

sesh = Session.builder.configs(CONNECTION_PARAMETERS).create()
data = {"col1": [1, 2, 3], "col2": [3, 4, 5], "col3": [5, 6, 7]}
vals_dtype = "values, dtype"
vals_dtype_params = [
    ([1, 2, 305], None),
    (list("abc"), None),
    ([1, 2, -3], "int64"),
    ([1.248, 1, 7.0], "float"),
    ([1.0, -15, 3.6], "float"),
    ([1.0, 0.2, 3.6], "float64"),
    ([7, 0, 1], "int64"),
    ([1.0, 0, -1], None),
    (list("abb"), "str"),
    (list("cba"), None),
    (["abc", "d", "-efgh"], None),
    (list("ccb"), None),
    ([[1, 2, 3], [4, 5, 6], [7, 8, 9]], None),
    ([[1, 2, 3], ["a", "b", "c"], [12, "b%", "2c"]], None),
    ([["123", "456", "789"], ["a", "b", "c"], ["#42q", "%28bc", "*b26"]], None),
]


@pytest.mark.parametrize(vals_dtype, vals_dtype_params)
@pytest.mark.parametrize(
    "func, qc",
    [
        ("values", 2),
        ("names", 0),
    ],
)
def test_index_arrays(values, dtype, func, qc):
    native_df = native_pd.DataFrame(data=data, index=values, dtype=dtype)
    snow_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=qc):
        arr1 = getattr(snow_df.index, func)
        arr2 = getattr(native_df.index, func)
        assert np.array_equal(arr1, arr2)

        ind1 = pd.Index(values, name="ind1", dtype=dtype)
        native_ind1 = native_pd.Index(values, name="ind1", dtype=dtype)
        arr1 = getattr(ind1, func)
        arr2 = getattr(native_ind1, func)
        assert np.array_equal(arr1, arr2)
