#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re

import pytest
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_dfs,
    eval_snowpark_pandas_result,
)
import pandas as native_pd
from tests.integ.utils.sql_counter import sql_count_checker
import logging
from snowflake.snowpark.modin.plugin._internal.utils import MODIN_IS_AT_LEAST_0_36_0
from pytest import param
import modin.pandas as pd

pytestmark = pytest.mark.skipif(
    not MODIN_IS_AT_LEAST_0_36_0,
    reason="Modin 0.36 had an important performant fix for eval().",
)


ENGINE_IGNORED_MESSAGE = (
    "The argument `engine` of `eval` has been ignored by Snowpark pandas "
    + "API:\nSnowpark pandas always uses the python engine in favor of "
    + "the numexpr engine, even if the numexpr engine is available."
)


engine_parameters = pytest.mark.parametrize(
    "engine_kwargs",
    [
        param({"engine": "python"}, id="engine_python"),
        param({"engine": "numexpr"}, id="engine_numexpr"),
        param({}, id="default_engine"),
    ],
)


def python_eval(df, expr, *, inplace=False, **kwargs):
    """
    Implement DataFrame.eval(), but always use engine='python' for pandas dataframes.

    pandas eval() by default will use numexpr engine if numexpr is available,
    but numexpr does not propagate attrs and it lacks some features that are
    available with the python engine. Snowpark pandas ignores the engine
    argument, so we test against engine='python' in pandas.
    """
    # have to add an extra stack level since we are wrapping eval in another
    # function call.
    kwargs["level"] = kwargs.get("level", 0) + 1
    if isinstance(df, pd.DataFrame):
        return df.eval(expr, inplace=inplace, **kwargs)
    assert isinstance(df, native_pd.DataFrame)
    return df.eval(expr, inplace=inplace, **(kwargs | {"engine": "python"}))


@pytest.fixture
def test_dfs():
    return create_test_dfs(
        native_pd.DataFrame(
            {
                "CUSTOMER_KEY": [-10, -10, -5, 30, 0, 1, 25],
                "ACCOUNT_BALANCE": [-101, -101, -51, 30, 0, 53, 105],
                "MARKET_SEGMENT": [
                    "AUTOMOBILE",
                    "AUTOMOBILE",
                    "FURNITURE",
                    "AUTOMOBILE",
                    "FURNITURE",
                    "MACHINERY",
                    "HOUSEHOLD",
                ],
            },
            index=["i0", "i1", "i2", "i3", "i4", "i5", "i6"],
        )
    )


@pytest.fixture
def test_dfs_with_named_index():
    return create_test_dfs(
        native_pd.DataFrame(
            {
                "CUSTOMER_KEY": [-10, -10, -5, 30, 0, 1, 25],
                "ACCOUNT_BALANCE": [-101, -101, -51, 30, 0, 53, 105],
                "MARKET_SEGMENT": [
                    "AUTOMOBILE",
                    "AUTOMOBILE",
                    "FURNITURE",
                    "AUTOMOBILE",
                    "FURNITURE",
                    "MACHINERY",
                    "HOUSEHOLD",
                ],
            },
            index=native_pd.Index(
                ["i0", "i1", "i2", "i3", "i4", "i5", "i6"], name="index_name"
            ),
        )
    )


@pytest.fixture
def test_dfs_multiindex():
    return create_test_dfs(
        native_pd.DataFrame(
            {
                "CUSTOMER_KEY": [-10, -10, -5, 30, 0, 1, 25],
                "ACCOUNT_BALANCE": [-101, -101, -51, 30, 0, 53, 105],
                "MARKET_SEGMENT": [
                    "AUTOMOBILE",
                    "AUTOMOBILE",
                    "FURNITURE",
                    "AUTOMOBILE",
                    "FURNITURE",
                    "MACHINERY",
                    "HOUSEHOLD",
                ],
            },
            index=native_pd.MultiIndex.from_tuples(
                [
                    ("i00", "i01"),
                    ("i10", "i11"),
                    ("i20", "i21"),
                    (
                        "i30",
                        "i31",
                    ),
                    ("i40", "i41"),
                    ("i50", "i51"),
                    ("i60", "i61"),
                ],
                names=["level_0_name", "level_1_name"],
            ),
        )
    )


global_int = 10


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "expr",
    [
        "CUSTOMER_KEY + 1",
        "CUSTOMER_KEY + @global_int",
        "CUSTOMER_KEY < @local_int",
        "~(CUSTOMER_KEY < @global_int)",
        "MARKET_SEGMENT == 'MACHINERY' or ACCOUNT_BALANCE > @global_int",
        "AUTOMOBILE_OR_MACHINERY = MARKET_SEGMENT.isin(('AUTOMOBILE', 'MACHINERY'))",
        """
            double_balance = ACCOUNT_BALANCE * 2
            absolute_customer_key = CUSTOMER_KEY.abs()
        """,
        """
            double_balance = ACCOUNT_BALANCE * 2
            quadruple_balance = double_balance * 2
        """,
    ],
)
@engine_parameters
def test_default_parameters_with_no_multiindex(
    test_dfs,
    expr,
    engine_kwargs,
):
    def do_eval(df, expr):
        local_int = 0  # noqa: F841
        local_list = ["MACHINERY", "FURNITURE"]  # noqa: F841
        return python_eval(df, expr, **engine_kwargs)

    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: do_eval(df, expr),
    )


@sql_count_checker(query_count=1)
@engine_parameters
def test_local_dict(test_dfs, engine_kwargs):
    def do_eval(df):
        local_int = 0  # noqa: F841
        local_list = ["MACHINERY", "FURNITURE"]  # noqa: F841
        return python_eval(
            df,
            """
            local_int = @local_int
            in_local_list = MARKET_SEGMENT.isin(@local_list)
            local_str = @local_str
            """,
            local_dict={
                "local_int": 1,
                "local_list": ["MACHINERY"],
                "local_str": "MACHINERY",
            },
            **engine_kwargs,
        )

    eval_snowpark_pandas_result(*test_dfs, do_eval)


@sql_count_checker(query_count=0)
@engine_parameters
def test_global_dict(test_dfs, engine_kwargs):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: python_eval(
            df,
            "[@dict_int, @global_int]",
            global_dict={"global_int": global_int - 1, "dict_int": 3},
            **engine_kwargs,
        ),
        comparator=list.__eq__,
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

        def do_eval(df):
            return python_eval(
                df,
                "MARKET_SEGMENT == ['AUTOMOBILE', 'MACHINERY']",
                parser="pandas",
                **engine_kwargs,
            )

        eval_snowpark_pandas_result(
            *test_dfs,
            do_eval,
        )

    @sql_count_checker(query_count=0)
    def test_python_fails_list_comparison(self, test_dfs, engine_kwargs):
        """Test that python parser fails with comparing series to list"""

        def do_eval(df):
            return python_eval(
                df,
                "MARKET_SEGMENT == ['AUTOMOBILE', 'MACHINERY']",
                parser="python",
                **engine_kwargs,
            )

        eval_snowpark_pandas_result(
            *test_dfs,
            do_eval,
            expect_exception=True,
            expect_exception_type=NotImplementedError,
            expect_exception_match=re.escape("'In' nodes are not implemented"),
        )

    @sql_count_checker(query_count=1)
    def test_pandas_supports_boolean_operators(self, test_dfs, engine_kwargs):
        """Test that pandas parser supports 'and'/'or' boolean operators"""

        def do_eval(df):
            return python_eval(
                df,
                "CUSTOMER_KEY > 0 and ACCOUNT_BALANCE > 50",
                parser="pandas",
                **engine_kwargs,
            )

        eval_snowpark_pandas_result(*test_dfs, do_eval)

    @sql_count_checker(query_count=0)
    def test_python_fails_boolean_operators(self, test_dfs, engine_kwargs):
        """Test that python parser fails with 'and'/'or' boolean operators"""

        def do_eval(df):
            return python_eval(
                df,
                "CUSTOMER_KEY > 0 and ACCOUNT_BALANCE > 50",
                parser="python",
                **engine_kwargs,
            )

        eval_snowpark_pandas_result(
            *test_dfs,
            do_eval,
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

        def do_eval(df):
            return python_eval(
                df,
                "(CUSTOMER_KEY > 0) & (ACCOUNT_BALANCE > 50)",
                parser=parser,
                **engine_kwargs,
            )

        eval_snowpark_pandas_result(*test_dfs, do_eval)


@sql_count_checker(query_count=1)
def test_warning_for_explicit_numexpr_engine(test_dfs, caplog):  # noqa: F811
    with caplog.at_level(logging.WARNING):
        eval_snowpark_pandas_result(
            *test_dfs,
            lambda df: python_eval(df, "CUSTOMER_KEY", engine="numexpr"),
            test_attrs=False,
        )

    assert any(record.message == ENGINE_IGNORED_MESSAGE for record in caplog.records)


@sql_count_checker(query_count=1)
def test_no_warning_for_default_numexpr_engine(test_dfs, caplog):  # noqa: F811
    with caplog.at_level(logging.WARNING):
        eval_snowpark_pandas_result(
            *test_dfs,
            lambda df: python_eval(df, "CUSTOMER_KEY"),
            test_attrs=False,
        )

    assert not any(
        record.message == ENGINE_IGNORED_MESSAGE for record in caplog.records
    )


@sql_count_checker(query_count=0)
@engine_parameters
def test_resolvers(test_dfs, engine_kwargs):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: python_eval(
            df,
            "[key1, key2]",
            resolvers=({"key1": "value1"}, {"key1": "value2", "key2": "value3"}),
            **engine_kwargs,
        ),
        comparator=list.__eq__,
    )


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("level", [0, 1, 2])
@engine_parameters
def test_level(
    test_dfs,
    level,
    engine_kwargs,
):
    def level_0(df):
        local_var = 0  # noqa: F841
        return python_eval(df, "@local_var", level=level, **engine_kwargs)

    def level_1(df):
        local_var = 1  # noqa: F841
        return level_0(df)

    def level_2(df):
        local_var = 2  # noqa: F841
        return level_1(df)

    eval_snowpark_pandas_result(
        *test_dfs,
        level_2,
        comparator=int.__eq__,
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
    @pytest.mark.xfail(
        strict=True,
        raises=AssertionError,
        reason="https://github.com/modin-project/modin/issues/7669",
    )
    def test_inplace_false_with_assignment_does_not_mutate_df(
        self, test_dfs, engine_kwargs, inplace_kwargs
    ):
        snowpark_input, pandas_input = test_dfs
        pandas_original = pandas_input.copy()
        eval_snowpark_pandas_result(
            snowpark_input,
            pandas_input,
            lambda df: python_eval(
                df,
                "double_balance = ACCOUNT_BALANCE * 2",
                **engine_kwargs,
                **inplace_kwargs,
            ),
        )
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snowpark_input, pandas_original
        )

    @sql_count_checker(query_count=1)
    def test_inplace_true_with_assignment_mutates_df(
        self,
        test_dfs,
        engine_kwargs,
    ):
        eval_snowpark_pandas_result(
            *test_dfs,
            lambda df: python_eval(
                df,
                "double_balance = ACCOUNT_BALANCE * 2",
                **engine_kwargs,
                inplace=True,
            ),
            inplace=True,
        )


@pytest.mark.parametrize(
    "do_eval",
    (
        param(
            lambda df: python_eval(df, "@undefined_variable"), id="undefined_variable"
        ),
        param(
            lambda df: python_eval(df, "x = ACCOUNT_BALANCE * 2\n1"),
            id="assignment_followed_by_expression",
        ),
        param(
            lambda df: python_eval(df, "0\n1"),
            id="multiple_expression_lines",
        ),
        param(
            lambda df: python_eval(df, "1", inplace=True),
            id="inplace_with_no_assignment",
        ),
        param(
            lambda df: python_eval(df, "1 +", inplace=True),
            id="invalid_syntax",
        ),
    ),
)
@sql_count_checker(query_count=0)
def test_user_error(test_dfs, do_eval):
    eval_snowpark_pandas_result(
        *test_dfs,
        do_eval,
        expect_exception=True,
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("expr", ("ilevel_0", "index"))
def test_refer_to_unnamed_single_level_index(test_dfs, expr):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: python_eval(df, expr=expr),
        # pandas bug where getting index via eval() does not propagate attrs.
        test_attrs=False,
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("expr", ("index_name", "index"))
def test_refer_to_named_single_level_index(test_dfs_with_named_index, expr):
    eval_snowpark_pandas_result(
        *test_dfs_with_named_index,
        lambda df: python_eval(df, expr=expr),
        # pandas bug where getting index via eval() does not propagate attrs.
        test_attrs=False,
    )


@pytest.mark.xfail(
    strict=True, raises=NotImplementedError, reason="No support for multiindex"
)
def test_multiindex(test_dfs_multiindex):
    eval_snowpark_pandas_result(
        *test_dfs_multiindex,
        lambda df: python_eval(df, expr="level_1_name"),
        # pandas bug where getting index via eval() does not propagate attrs.
        test_attrs=False,
    )


@sql_count_checker(query_count=2)
def test_target(test_dfs):
    snow_df, pandas_df = test_dfs
    snow_df_copy = snow_df.copy()
    pandas_df_copy = pandas_df.copy()
    snow_df.eval("new_column = 0", target=snow_df_copy, inplace=True)
    pandas_df.eval("new_column = 0", target=pandas_df_copy, inplace=True)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, pandas_df)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_df_copy, pandas_df_copy
    )
