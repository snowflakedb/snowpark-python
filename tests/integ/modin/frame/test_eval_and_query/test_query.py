#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
from pytest import param
import pandas as native_pd
import modin.pandas as pd
import re

from tests.integ.utils.sql_counter import sql_count_checker
from snowflake.snowpark.modin.plugin._internal.utils import MODIN_IS_AT_LEAST_0_36_0
import logging
from tests.integ.modin.frame.test_eval_and_query.utils import (
    engine_parameters,
    ENGINE_IGNORED_MESSAGE,
)
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)

pytestmark = pytest.mark.skipif(
    not MODIN_IS_AT_LEAST_0_36_0,
    reason="Modin 0.36 had an important performant fix for query().",
)

global_int = 10


def python_query(df, expr, *, inplace=False, **kwargs):
    """
    Implement DataFrame.query(), but always use engine='python' for pandas dataframes.

    pandas query() by default will use numexpr engine if numexpr is available,
    but numexpr lacks some features that are available with the python engine.
    Snowpark pandas ignores the engine argument, so we test against
    engine='python' in pandas.
    """
    # have to add an extra stack level since we are wrapping query in another
    # function call.
    kwargs["level"] = kwargs.get("level", 0) + 1
    if isinstance(df, pd.DataFrame):
        return df.query(expr, inplace=inplace, **kwargs)
    assert isinstance(df, native_pd.DataFrame)
    return df.query(expr, inplace=inplace, **(kwargs | {"engine": "python"}))


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "expr",
    [
        "CUSTOMER_KEY < @global_int",
        "~(CUSTOMER_KEY < @global_int)",
        "MARKET_SEGMENT == 'MACHINERY' or ACCOUNT_BALANCE > @local_int",
        "MARKET_SEGMENT.isin(('AUTOMOBILE', 'MACHINERY'))",
        "`PURCHASE COUNT` != 0",
        "@df.isin([0])",
    ],
)
@engine_parameters
def test_default_parameters(
    test_dfs,
    expr,
    engine_kwargs,
):
    def do_query(df, expr):
        local_int = 0  # noqa: F841
        local_list = ["MACHINERY", "FURNITURE"]  # noqa: F841
        return python_query(df, expr, **engine_kwargs)

    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: do_query(df, expr),
    )


@sql_count_checker(query_count=1)
@engine_parameters
def test_local_dict(test_dfs, engine_kwargs):
    def do_query(df):
        local_int = 0  # noqa: F841
        local_list = ["MACHINERY", "FURNITURE"]  # noqa: F841
        return python_query(
            df,
            """
            ACCOUNT_BALANCE > @local_int or MARKET_SEGMENT.isin(@local_list) and not @local_str == MARKET_SEGMENT
            """,
            local_dict={
                "local_int": 100,
                "local_list": ["MACHINERY"],
                "local_str": "MACHINERY",
            },
            **engine_kwargs,
        )

    eval_snowpark_pandas_result(*test_dfs, do_query)


@sql_count_checker(query_count=1)
@engine_parameters
def test_global_dict(test_dfs, engine_kwargs):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: python_query(
            df,
            "ACCOUNT_BALANCE > @global_int or ACCOUNT_BALANCE < @dict_int",
            global_dict={"global_int": global_int + 1000, "dict_int": 3},
            **engine_kwargs,
        ),
    )


@engine_parameters
class TestParser:
    @sql_count_checker(query_count=1)
    def test_pandas_supports_list_comparison(
        self,
        test_dfs,
        engine_kwargs,
    ):
        """Test that pandas parser supports comparing series to list"""

        def do_query(df):
            return python_query(
                df,
                "MARKET_SEGMENT == ['AUTOMOBILE', 'MACHINERY']",
                parser="pandas",
                **engine_kwargs,
            )

        eval_snowpark_pandas_result(
            *test_dfs,
            do_query,
        )

    @sql_count_checker(query_count=0)
    def test_python_fails_list_comparison(self, test_dfs, engine_kwargs):
        """Test that python parser fails with comparing series to list"""

        def do_query(df):
            return python_query(
                df,
                "MARKET_SEGMENT == ['AUTOMOBILE', 'MACHINERY']",
                parser="python",
                **engine_kwargs,
            )

        eval_snowpark_pandas_result(
            *test_dfs,
            do_query,
            expect_exception=True,
            expect_exception_type=NotImplementedError,
            expect_exception_match=re.escape("'In' nodes are not implemented"),
        )

    @sql_count_checker(query_count=1)
    def test_pandas_supports_boolean_operators(self, test_dfs, engine_kwargs):
        """Test that pandas parser supports 'and'/'or' boolean operators"""

        def do_query(df):
            return python_query(
                df,
                "CUSTOMER_KEY > 0 and ACCOUNT_BALANCE > 50",
                parser="pandas",
                **engine_kwargs,
            )

        eval_snowpark_pandas_result(*test_dfs, do_query)

    @sql_count_checker(query_count=0)
    def test_python_fails_boolean_operators(self, test_dfs, engine_kwargs):
        """Test that python parser fails with 'and'/'or' boolean operators"""

        def do_query(df):
            return python_query(
                df,
                "CUSTOMER_KEY > 0 and ACCOUNT_BALANCE > 50",
                parser="python",
                **engine_kwargs,
            )

        eval_snowpark_pandas_result(
            *test_dfs,
            do_query,
            expect_exception=True,
            expect_exception_type=NotImplementedError,
            expect_exception_match=re.escape("'BoolOp' nodes are not implemented"),
        )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("parser", ["pandas", "python"])
    def test_works_with_bitwise_operators(
        self,
        test_dfs,
        parser,
        engine_kwargs,
    ):
        """Test that both parsers work with bitwise operators (&/|)"""

        def do_query(df):
            return python_query(
                df,
                "(CUSTOMER_KEY > 0) & (ACCOUNT_BALANCE > 50)",
                parser=parser,
                **engine_kwargs,
            )

        eval_snowpark_pandas_result(*test_dfs, do_query)


@sql_count_checker(query_count=1)
def test_warning_for_explicit_numexpr_engine(test_dfs, caplog):  # noqa: F811
    with caplog.at_level(logging.WARNING):
        eval_snowpark_pandas_result(
            *test_dfs,
            lambda df: python_query(df, "CUSTOMER_KEY > 0", engine="numexpr"),
        )

    assert any(record.message == ENGINE_IGNORED_MESSAGE for record in caplog.records)


@sql_count_checker(query_count=1)
def test_no_warning_for_default_numexpr_engine(test_dfs, caplog):  # noqa: F811
    with caplog.at_level(logging.WARNING):
        eval_snowpark_pandas_result(
            *test_dfs,
            lambda df: python_query(df, "CUSTOMER_KEY > 0"),
        )

    assert not any(
        record.message == ENGINE_IGNORED_MESSAGE for record in caplog.records
    )


@sql_count_checker(query_count=1)
@engine_parameters
def test_resolvers(test_dfs, engine_kwargs):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: python_query(
            df,
            "ACCOUNT_BALANCE > key1 or ACCOUNT_BALANCE == key2",
            resolvers=({"key1": 0}, {"key1": 1000, "key2": 53}),
            **engine_kwargs,
        ),
    )


@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.parametrize("level", [0, 1, 2])
@engine_parameters
def test_level(
    test_dfs,
    level,
    engine_kwargs,
):
    def level_0(df):
        local_var = "i0"  # noqa: F841
        return python_query(df, "index == @local_var", level=level, **engine_kwargs)

    def level_1(df):
        local_var = "i1"  # noqa: F841
        return level_0(df)

    def level_2(df):
        local_var = "i2"  # noqa: F841
        return level_1(df)

    eval_snowpark_pandas_result(
        *test_dfs,
        level_2,
    )


@engine_parameters
class TestInplace:
    @pytest.mark.parametrize(
        "inplace_kwargs",
        [
            param({}, id="inplace_default"),
            param({"inplace": False}, id="inplace_False"),
        ],
    )
    @sql_count_checker(query_count=2)
    def test_inplace_false_does_not_mutate_df(
        self, test_dfs, engine_kwargs, inplace_kwargs
    ):
        snowpark_input, pandas_input = test_dfs
        pandas_original = pandas_input.copy()
        eval_snowpark_pandas_result(
            snowpark_input,
            pandas_input,
            lambda df: python_query(
                df, "ACCOUNT_BALANCE < 0", **engine_kwargs, **inplace_kwargs
            ),
        )
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snowpark_input, pandas_original
        )

    @sql_count_checker(query_count=1)
    def test_inplace_true_mutates_df(
        self,
        test_dfs,
        engine_kwargs,
    ):
        eval_snowpark_pandas_result(
            *test_dfs,
            lambda df: python_query(
                df, "ACCOUNT_BALANCE < 0", **engine_kwargs, inplace=True
            ),
            inplace=True,
        )


@pytest.mark.parametrize(
    "do_query",
    (
        param(
            lambda df: python_query(df, "ACCOUNT_BALANCE == @undefined_variable"),
            id="undefined_variable",
        ),
        param(
            lambda df: python_query(df, "x = ACCOUNT_BALANCE * 2"),
            id="assignment",
        ),
        param(
            lambda df: python_query(df, "ACCOUNT_BALANCE < 0\nACCOUNT_BALANCE > 0"),
            id="multiple_expression_lines",
        ),
        param(
            lambda df: python_query(df, "ACCOUNT_BALANCE ==", inplace=True),
            id="invalid_syntax",
        ),
    ),
)
@sql_count_checker(query_count=0)
def test_user_error(test_dfs, do_query):
    eval_snowpark_pandas_result(
        *test_dfs,
        do_query,
        expect_exception=True,
    )


@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.parametrize("expr", ("ilevel_0 == 'i0'", "index == 'i0'"))
def test_refer_to_unnamed_single_level_index(test_dfs, expr):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: python_query(df, expr=expr),
    )


@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.parametrize("expr", ("index_name == 'i0'", "index == 'i0'"))
def test_refer_to_named_single_level_index(test_dfs_with_named_index, expr):
    eval_snowpark_pandas_result(
        *test_dfs_with_named_index,
        lambda df: python_query(df, expr=expr),
    )


@pytest.mark.xfail(
    strict=True, raises=NotImplementedError, reason="No support for multiindex"
)
def test_multiindex(test_dfs_multiindex):
    eval_snowpark_pandas_result(
        *test_dfs_multiindex,
        lambda df: python_query(df, expr="level_1_name == 'i01'"),
    )


@sql_count_checker(query_count=2)
def test_target(test_dfs):
    snow_df, pandas_df = test_dfs
    snow_df_copy = snow_df.copy()
    pandas_df_copy = pandas_df.copy()
    snow_df.query("ACCOUNT_BALANCE < 0", target=snow_df_copy, inplace=True)
    pandas_df.query("ACCOUNT_BALANCE < 0", target=pandas_df_copy, inplace=True)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, pandas_df)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_df_copy, pandas_df_copy
    )
