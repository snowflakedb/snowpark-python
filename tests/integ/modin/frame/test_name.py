#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest
from pandas._testing import assert_index_equal

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
)


@pytest.mark.parametrize(
    "sample",
    [
        native_pd.Series([1, 2, 3], name="abc"),
        native_pd.Index([1, 2, 3], name="abc"),
        native_pd.Index([], name="abc"),
        native_pd.Index([("a", "b"), ("a", "c")], name=("a", "b")),
        native_pd.Index(
            [("a", "b"), ("a", "c")], tupleize_cols=False, name="('a', 'b')"
        ),
        [
            native_pd.Series([1, 2, 3], name="a"),
            native_pd.Index([1, 2, 3], name="b"),
        ],
    ],
)
@sql_count_checker(query_count=1)
def test_create_dataframe_from_object_with_name(sample):
    # name in sample will be kept as column name
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        pd.DataFrame(sample),
        native_pd.DataFrame(sample),
    )


@sql_count_checker(query_count=3)
def test_create_dataframe_from_snowpark_pandas_series():
    df = pd.DataFrame([[2, 3, 4], [5, 6, 7]], columns=["X", "Y", "Z"])
    df = pd.DataFrame([df.X, df.iloc[:, 2]])
    assert_index_equal(df.index, pd.Index(["X", "Z"]))
