#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark._internal.utils import (
    PIVOT_DEFAULT_ON_NULL_WARNING,
    PIVOT_VALUES_NONE_OR_DATAFRAME_WARNING,
)
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import assert_snowpark_pandas_equal_to_pandas


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
def test_get_dummies_does_not_raise_pivot_warning_snow_1344848(caplog):
    # Test get_dummies, which uses the `default_on_null` parameter of
    # snowflake.snowpark.dataframe.pivot()
    pd.get_dummies(pd.Series(["a"])).to_pandas()
    assert PIVOT_DEFAULT_ON_NULL_WARNING not in caplog.text
    assert PIVOT_VALUES_NONE_OR_DATAFRAME_WARNING not in caplog.text
