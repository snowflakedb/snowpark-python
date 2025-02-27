#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_snowpark_pandas_equal_to_pandas
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize("data", [list("abca"), list("mxyzptlk")])
@sql_count_checker(query_count=1)
def test_get_dummies_series(data):

    pandas_ser = native_pd.Series(data)

    snow_ser = pd.Series(pandas_ser)

    assert_snowpark_pandas_equal_to_pandas(
        pd.get_dummies(snow_ser), native_pd.get_dummies(pandas_ser), check_dtype=False
    )


@sql_count_checker(query_count=1)
def test_get_dummies_pandas_no_row_pos_col():
    pandas_ser = native_pd.Series(["a", "b", "a"]).sort_values()

    snow_ser = pd.Series(["a", "b", "a"]).sort_values()
    assert (
        snow_ser._query_compiler._modin_frame.row_position_snowflake_quoted_identifier
        is None
    )

    assert_snowpark_pandas_equal_to_pandas(
        pd.get_dummies(snow_ser), native_pd.get_dummies(pandas_ser), check_dtype=False
    )


@pytest.mark.parametrize("data", [[1, 2, 3, 4, 5, 6], [True, False]])
@sql_count_checker(query_count=0)
def test_get_dummies_series_negative(data):

    pandas_ser = native_pd.Series(data)

    snow_ser = pd.Series(pandas_ser)

    with pytest.raises(NotImplementedError):
        assert_snowpark_pandas_equal_to_pandas(
            pd.get_dummies(snow_ser),
            native_pd.get_dummies(pandas_ser),
            check_dtype=False,
        )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("kwargs", [{"dummy_na": False}, {}])
@pytest.mark.parametrize(
    "data", [["a", "a", None, "c"], ["a", "a", "c", "c"], ["a", "NULL"], [None, "NULL"]]
)
def test_get_dummies_null_values(kwargs, data):
    ser = native_pd.Series(data, name="col")
    expected = native_pd.get_dummies(ser, **kwargs)
    actual = pd.get_dummies(pd.Series(ser), **kwargs)
    assert_snowpark_pandas_equal_to_pandas(actual, expected, check_dtype=False)
