#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)


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


@sql_count_checker(query_count=5)
def test_invalid_func_with_named_agg_errors(basic_snowpark_pandas_df):
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


@sql_count_checker(query_count=2)
def test_valid_func_valid_kwarg_should_work(basic_snowpark_pandas_df):
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        basic_snowpark_pandas_df.groupby("col1").agg(max, min_count=2),
        basic_snowpark_pandas_df.to_pandas().groupby("col1").max(min_count=2),
    )
