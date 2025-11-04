#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re
import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equal_to_pandas,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker
import numpy as np


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


class TestDtypeParameter:
    @pytest.mark.parametrize(
        "dtype",
        [
            np.int64,
            int,
            "int",
            float,
            np.float64,
            "float64",
            str,
            "str",
            np.str_,
            bool,
            "bool",
            np.bool_,
            "datetime64[ns]",
            pytest.param(
                "timedelta64[ns]",
                marks=pytest.mark.xfail(strict=True, raises=NotImplementedError),
            ),
            None,
        ],
    )
    @sql_count_checker(query_count=1)
    def test_valid_dtype(self, dtype):
        ser = native_pd.Series(["a", "b", "a"])
        # note that we're using the default check_dtype=True to check that we
        # are producing the correct dtypes.
        assert_snowpark_pandas_equal_to_pandas(
            pd.get_dummies(pd.Series(ser), dtype=dtype),
            native_pd.get_dummies(ser, dtype=dtype),
        )

    @sql_count_checker(query_count=1)
    def test_valid_dtype_int32(self):
        """Test int32 separately because Snowpark pandas always produces int64 for integers."""
        ser = native_pd.Series(["a", "b", "a"])
        # note that we're using the default check_dtype=True to check that we
        # are producing the correct dtypes.
        assert_snowpark_pandas_equal_to_pandas(
            pd.get_dummies(pd.Series(ser), dtype=np.int32),
            native_pd.get_dummies(ser, dtype=np.int32).astype(np.int64),
        )

    @sql_count_checker(query_count=0)
    def test_unknown_dtype(self):
        eval_snowpark_pandas_result(
            pd,
            native_pd,
            lambda module: module.get_dummies(
                module.DataFrame({"A": ["a", "b", "a"]}), dtype="invalid_dtype"
            ),
            expect_exception=True,
            expect_exception_type=TypeError,
            expect_exception_match=re.escape(
                "data type 'invalid_dtype' not understood"
            ),
        )

    @sql_count_checker(query_count=0)
    @pytest.mark.parametrize("dtype", ["object", np.dtype("object")])
    def test_invalid_dtype_object(self, dtype):
        eval_snowpark_pandas_result(
            pd,
            native_pd,
            lambda module: module.get_dummies(
                module.DataFrame({"A": ["a", "b", "a"]}), dtype=dtype
            ),
            expect_exception=True,
            expect_exception_type=ValueError,
            expect_exception_match=re.escape(
                "dtype=object is not a valid dtype for get_dummies"
            ),
        )
