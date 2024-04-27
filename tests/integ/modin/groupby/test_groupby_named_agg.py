#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import pytest

from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result


@sql_count_checker(query_count=1)
def test_invalid_named_agg_errors(basic_snowpark_pandas_df):
    eval_snowpark_pandas_result(
        basic_snowpark_pandas_df,
        basic_snowpark_pandas_df.to_pandas(),
        lambda df: df.groupby("col1").agg(args=80, valid_agg=("col2", min)),
        expect_exception=True,
        expect_exception_match="Must provide 'func' or tuples of '\\(column, aggfunc\\).",
        assert_exception_equal=False,  # There is a typo in the pandas exception.
        expect_exception_type=TypeError,
    )


@sql_count_checker(query_count=6)
@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=AssertionError,
)
def test_invalid_func_with_named_agg_errors(basic_snowpark_pandas_df):
    # This test checks that a SnowparkSQLException is raised by this code, since the
    # code is invalid. This code relies on falling back to native pandas though,
    # so until SNOW-1336091 is fixed, a RuntimeError will instead by raised by the
    # Snowpark pandas code. This test then errors out with an AssertionError, since
    # the assertion that the raised exception is a SnowparkSQLException is False,
    # so we mark it as xfail with raises=AssertionError. When SNOW-1336091 is fixed,
    # this test should pass automatically.
    eval_snowpark_pandas_result(
        basic_snowpark_pandas_df,
        basic_snowpark_pandas_df.to_pandas(),
        lambda df: df.groupby("col1").agg(80, valid_agg=("col2", min)),
        expect_exception=True,
        assert_exception_equal=False,  # We fallback and then raise the correct error.
        expect_exception_type=SnowparkSQLException,
    )


@sql_count_checker(query_count=1)
def test_valid_func_with_named_agg_errors(basic_snowpark_pandas_df):
    eval_snowpark_pandas_result(
        basic_snowpark_pandas_df,
        basic_snowpark_pandas_df.to_pandas(),
        lambda df: df.groupby("col1").agg(max, new_col=("col2", min)),
        expect_exception=True,
        expect_exception_match="DataFrameGroupBy.max\\(\\) got an unexpected keyword argument 'new_col'",
        assert_exception_equal=False,  # There is a difference in our errors.
        expect_exception_type=TypeError,
    )
