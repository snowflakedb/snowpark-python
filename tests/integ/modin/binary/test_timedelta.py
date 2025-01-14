#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import (
    assert_frame_equal,
    assert_series_equal,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_dfs,
    create_test_series,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker

PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1 = (
    native_pd.Series(
        [
            None,
            pd.Timestamp(year=1994, month=7, day=29),
            None,
            pd.Timestamp(year=1996, month=1, day=23),
            pd.Timestamp(year=2000, month=1, day=23),
            pd.Timestamp(
                year=1700, month=1, day=1, second=22, microsecond=12345, nanosecond=56
            ),
        ]
    ),
    native_pd.Series(
        [
            None,
            None,
            pd.Timestamp(year=1995, month=7, day=29),
            pd.Timestamp(year=1996, month=1, day=24),
            pd.Timestamp(year=2024, month=7, day=8),
            pd.Timestamp(
                year=1700, month=1, day=2, second=49, microsecond=7, nanosecond=98
            ),
        ]
    ),
)


@pytest.fixture
def timestamp_no_timezone_dataframes_1() -> tuple[pd.DataFrame, native_pd.DataFrame]:
    return create_test_dfs(
        [
            [
                pd.Timestamp(5, unit="ns"),
                pd.Timestamp(700, unit="ns"),
                pd.Timestamp(1399, unit="ns"),
            ],
            [
                pd.Timestamp(6, unit="ms"),
                pd.Timestamp(800, unit="ms"),
                pd.Timestamp(1499, unit="ms"),
            ],
        ]
    )


@pytest.fixture
def timestamp_no_timezone_series_2() -> tuple[pd.Series, native_pd.Series]:
    return create_test_series(
        [
            pd.Timestamp(5, unit="ns"),
            pd.Timestamp(700, unit="ns"),
            pd.Timestamp(1399, unit="ns"),
        ]
    )


@pytest.fixture
def timestamp_no_timezone_dataframes_2() -> tuple[pd.DataFrame, native_pd.DataFrame]:
    return create_test_dfs(
        [
            [pd.Timestamp(5, unit="ns"), pd.Timestamp(700, unit="ns")],
            [pd.Timestamp(6, unit="ns"), pd.Timestamp(800, unit="ns")],
            [pd.Timestamp(7, unit="ns"), pd.Timestamp(900, unit="ns")],
        ]
    )


@pytest.fixture
def timestamp_no_timezone_dataframes_3() -> tuple[pd.DataFrame, native_pd.DataFrame]:
    return create_test_dfs(
        [
            [pd.Timestamp(1, unit="ms"), pd.Timestamp(2, unit="ms")],
            [pd.Timestamp(3, unit="ms"), pd.Timestamp(4, unit="ms")],
        ]
    )


@pytest.fixture(
    params=[
        pd.NaT,
        datetime.datetime(year=2024, month=8, day=14, hour=2, minute=32, second=42),
        datetime.datetime(year=2023, month=3, day=14),
        pd.Timestamp(year=2020, month=3, day=25),
    ],
)
def timestamp_scalar(request):
    return request.param


@pytest.fixture(
    params=[
        pd.Timedelta("10 days 23:59:59.123456789"),
        pd.Timedelta(microseconds=1),
        pd.Timedelta(microseconds=2),
        pd.Timedelta(nanoseconds=1),
        pd.Timedelta(nanoseconds=2),
        pd.Timedelta(nanoseconds=3),
        pd.Timedelta(days=1),
        pd.Timedelta(days=1, hours=1),
        pd.Timedelta(days=10),
    ]
)
def timedelta_scalar_positive(request):
    return request.param


@pytest.fixture(
    params=[
        pd.Timedelta("10 days 23:59:59.123456789"),
        pd.Timedelta("-10 days 23:59:59.123456789"),
        datetime.timedelta(days=-10, hours=23),
        datetime.timedelta(microseconds=1),
        datetime.timedelta(microseconds=2),
        pd.Timedelta(nanoseconds=1),
        pd.Timedelta(nanoseconds=2),
        pd.Timedelta(nanoseconds=3),
        pd.Timedelta(days=1),
        pd.Timedelta(days=1, hours=1),
        pd.Timedelta(days=10),
        pd.Timedelta(days=-1),
    ]
)
def timedelta_scalar(request):
    return request.param


@pytest.fixture
def timedelta_dataframes_1() -> tuple[pd.DataFrame, native_pd.DataFrame]:
    return create_test_dfs(
        [
            [pd.Timedelta(days=1), pd.NaT],
            [
                pd.Timedelta(days=2),
                pd.Timedelta(days=3),
            ],
        ]
    )


@pytest.fixture
def timedelta_dataframes_postive_no_nulls_1_2x2() -> tuple[
    pd.DataFrame, native_pd.DataFrame
]:
    return create_test_dfs(
        [
            [pd.Timedelta(days=1), pd.Timedelta(days=4)],
            [
                pd.Timedelta(days=2),
                pd.Timedelta(days=3),
            ],
        ]
    )


@pytest.fixture
def timedelta_dataframes_with_negatives_no_nulls_1_2x2() -> tuple[
    pd.DataFrame, native_pd.DataFrame
]:
    return create_test_dfs(
        [
            [pd.Timedelta(days=1), pd.Timedelta(days=4)],
            [
                pd.Timedelta(days=2),
                pd.Timedelta(days=-3),
            ],
        ]
    )


@pytest.fixture
def timedelta_series_1() -> tuple[pd.Series, native_pd.Series]:
    return create_test_series(
        [
            None,
            pd.Timedelta(days=1),
            pd.Timedelta(days=2),
            pd.Timedelta(days=3),
            pd.Timedelta(days=4),
            pd.Timedelta(days=5),
        ]
    )


@pytest.fixture
def timedelta_series_positive_no_nulls_1_length_6() -> tuple[
    pd.Series, native_pd.Series
]:
    return create_test_series(
        [
            pd.Timedelta(days=1),
            pd.Timedelta(days=2),
            pd.Timedelta(days=3),
            pd.Timedelta(days=4),
            pd.Timedelta(days=5),
            pd.Timedelta(days=6),
        ]
    )


@pytest.fixture
def timedelta_series_no_nulls_2_length_6() -> tuple[pd.Series, native_pd.Series]:
    return create_test_series(
        [
            pd.Timedelta(microseconds=7),
            pd.Timedelta(hours=6, minutes=5),
            pd.Timedelta(hours=4, minutes=3),
            pd.Timedelta(hours=2, minutes=1),
            pd.Timedelta(hours=8, minutes=9),
            pd.Timedelta(hours=9, minutes=8),
        ]
    )


@pytest.fixture
def timedelta_series_no_nulls_3_length_2() -> tuple[pd.Series, native_pd.Series]:
    return create_test_series(
        [
            pd.Timedelta(microseconds=7),
            pd.Timedelta(hours=6, minutes=5),
        ]
    )


@pytest.fixture(
    params=[
        "sub",
        "rsub",
        "add",
        "radd",
        "div",
        "rdiv",
        "truediv",
        "rtruediv",
        "floordiv",
        "rfloordiv",
        "mod",
        "rmod",
        "eq",
        "ne",
        "gt",
        "lt",
        "ge",
        "le",
    ]
)
def op_between_timedeltas(request) -> list[str]:
    """Valid operations between timedeltas."""
    return request.param


@pytest.fixture(
    params=[
        "mul",
        "rmul",
        "div",
        "truediv",
        "floordiv",
        "mod",
        "eq",
        "ne",
    ]
)
def op_between_timedelta_and_numeric(request) -> list[str]:
    """Valid operations between a timedelta LHS and numeric RHS."""
    return request.param


@pytest.fixture(
    params=[
        "mul",
        "rmul",
        "rdiv",
        "rtruediv",
        "rfloordiv",
        "rmod",
        "eq",
        "ne",
    ]
)
def op_between_numeric_and_timedelta(request) -> list[str]:
    """Valid operations between a numeric RHS and timedelta LHS."""
    return request.param


@pytest.fixture(
    params=[
        -2.5,
        -2,
        -1,
        1,
        0.5,
        1.5,
        2,
        1001,
    ]
)
def numeric_scalar_non_null(request) -> list:
    return request.param


@pytest.fixture
def numeric_dataframes_postive_no_nulls_1_2x2() -> tuple[
    pd.DataFrame, native_pd.DataFrame
]:
    return create_test_dfs(
        [
            [1, 1.5],
            [
                2,
                1001,
            ],
        ]
    )


@pytest.fixture
def numeric_list_like_postive_no_nulls_1_length_6() -> list:
    return [
        1,
        0.5,
        1.5,
        2,
        1001,
        1000,
    ]


@pytest.fixture
def numeric_series_positive_no_nulls_1_length_6(
    numeric_list_like_postive_no_nulls_1_length_6,
) -> tuple[pd.Series, native_pd.Series]:
    return create_test_series(numeric_list_like_postive_no_nulls_1_length_6)


@pytest.fixture
def numeric_series_positive_no_nulls_2_length_2() -> tuple[pd.Series, native_pd.Series]:
    return create_test_series([0.5, 2.5])


class TestInvalid:
    """
    Test invalid binary operations, e.g. subtracting a timestamp from a timedelta.

    For simplicity, check these cases for operations between dataframes and
    scalars only.
    """

    @sql_count_checker(query_count=0)
    def test_timedelta_scalar_minus_timestamp_dataframe(self):
        eval_snowpark_pandas_result(
            *create_test_dfs([datetime.datetime(year=2024, month=8, day=21)]),
            lambda df: pd.Timedelta(1) - df,
            expect_exception=True,
            expect_exception_type=TypeError,
            expect_exception_match=re.escape(
                "bad operand type for unary -: 'DatetimeArray"
            ),
        )

    @sql_count_checker(query_count=0)
    @pytest.mark.parametrize(
        "operation,error_symbol",
        [("__or__", "|"), ("__ror__", "|"), ("__and__", "&"), ("__rand__", "&")],
    )
    def test_timedelta_dataframe_bitwise_operation_with_timedelta_scalar(
        self, operation, timedelta_dataframes_1, error_symbol
    ):
        eval_snowpark_pandas_result(
            *timedelta_dataframes_1,
            lambda df: getattr(df, operation)(pd.Timedelta(2)),
            expect_exception=True,
            # pandas exception depends on the input types and is something like
            # "unsupported operand type(s) for &: 'Timedelta' and 'TimedeltaArray'",
            # but Snowpwark pandas always gives the same exception.
            assert_exception_equal=False,
            expect_exception_type=TypeError,
            expect_exception_match=re.escape(
                f"unsupported operand type for {error_symbol}: Timedelta"
            ),
        )

    @sql_count_checker(query_count=0)
    @pytest.mark.parametrize("operation", ["pow", "rpow"])
    def test_timedelta_dataframe_exponentiation_with_timedelta_scalar(
        self, operation, timedelta_dataframes_1
    ):
        eval_snowpark_pandas_result(
            *timedelta_dataframes_1,
            lambda df: getattr(df, operation)(pd.Timedelta(2)),
            expect_exception=True,
            # pandas exception depends on the input types and is something
            # like "cannot perform __rpow__ with this index type:
            # TimedeltaArray", but Snowpwark pandas always gives the same
            # exception.
            assert_exception_equal=False,
            expect_exception_type=TypeError,
            expect_exception_match=re.escape(
                "unsupported operand type for **: Timedelta"
            ),
        )

    @sql_count_checker(query_count=0)
    @pytest.mark.parametrize("operation", ["mul", "rmul"])
    def test_timedelta_dataframe_multiplied_by_timedelta_scalar_invalid(
        self, operation, timedelta_dataframes_1
    ):
        eval_snowpark_pandas_result(
            *timedelta_dataframes_1,
            lambda df: getattr(df, operation)(pd.Timedelta(2)),
            expect_exception=True,
            expect_exception_type=np.core._exceptions._UFuncBinaryResolutionError,
            expect_exception_match=re.escape(
                "ufunc 'multiply' cannot use operands with types dtype('<m8[ns]') and dtype('<m8[ns]')"
            ),
        )

    @pytest.mark.parametrize("op", ["add", "radd", "sub", "rsub"])
    @pytest.mark.parametrize("value", [1, 1.5])
    @sql_count_checker(query_count=0)
    def test_timedelta_addition_and_subtraction_with_numeric(
        self, timedelta_dataframes_1, op, value
    ):
        eval_snowpark_pandas_result(
            *timedelta_dataframes_1,
            lambda df: getattr(df, op)(value),
            expect_exception=True,
            expect_exception_match=re.escape(
                "Snowpark pandas does not support addition or subtraction between timedelta values and numeric values."
            ),
            expect_exception_type=TypeError,
            assert_exception_equal=False,
        )

    @pytest.mark.parametrize("op", ["div", "floordiv", "truediv", "mod"])
    @sql_count_checker(query_count=0)
    def test_timedelta_divide_number_dataframe_by_timedelta_scalar(self, op):
        eval_snowpark_pandas_result(
            *create_test_dfs([1]),
            lambda df: getattr(df, op)(pd.Timedelta(1)),
            expect_exception=True,
            expect_exception_match=re.escape(
                "Snowpark pandas does not support dividing numeric values by timedelta values with div (/), mod (%), or floordiv (//)."
            ),
            expect_exception_type=TypeError,
            assert_exception_equal=False,
        )

    @pytest.mark.parametrize("op", ["rdiv", "rfloordiv", "rtruediv", "rmod"])
    @sql_count_checker(query_count=0)
    def test_divide_number_scalar_by_timedelta_dataframe(self, op):
        eval_snowpark_pandas_result(
            *create_test_dfs([pd.Timedelta(1)]),
            lambda df: getattr(df, op)(2),
            expect_exception=True,
            expect_exception_match=re.escape(
                "Snowpark pandas does not support dividing numeric values by timedelta values with div (/), mod (%), or floordiv (//)."
            ),
            expect_exception_type=TypeError,
            assert_exception_equal=False,
        )

    @sql_count_checker(query_count=0)
    @pytest.mark.parametrize("op", ["gt", "ge", "lt", "le"])
    def test_timedelta_less_than_or_greater_than_numeric(
        self, timedelta_dataframes_1, op
    ):
        eval_snowpark_pandas_result(
            *timedelta_dataframes_1,
            lambda df: getattr(df, op)(1),
            expect_exception=True,
            expect_exception_type=TypeError,
            expect_exception_match=re.escape(
                f"Snowpark pandas does not support binary operation {op} between timedelta and a non-timedelta type"
            ),
            assert_exception_equal=False,
        )

    @sql_count_checker(query_count=0)
    @pytest.mark.parametrize("op", ["pow", "rpow"])
    def test_pow_timedelta_numeric(self, op, timedelta_dataframes_1):
        eval_snowpark_pandas_result(
            *timedelta_dataframes_1,
            lambda df: getattr(df, op)(1),
            expect_exception=True,
            expect_exception_type=TypeError,
            expect_exception_match=re.escape(
                "Snowpark pandas does not support binary operation pow between timedelta and a non-timedelta type"
            ),
            assert_exception_equal=False,
        )

    @sql_count_checker(query_count=0)
    @pytest.mark.parametrize(
        "op,error_message_type",
        [
            ("pow", "Numeric"),
            ("rpow", "Numeric"),
            ("__and__", "Boolean"),
            ("__rand__", "Boolean"),
            ("__or__", "Boolean"),
            ("__ror__", "Boolean"),
        ],
    )
    def test_invalid_ops_between_timedelta_dataframe_and_string_scalar(
        self, op, timedelta_dataframes_1, error_message_type
    ):
        eval_snowpark_pandas_result(
            *timedelta_dataframes_1,
            lambda df: getattr(df, op)("4 days"),
            expect_exception=True,
            expect_exception_type=SnowparkSQLException,
            expect_exception_match=f"{error_message_type} value '.*' is not recognized",
            # pandas raises errors like "TypeError: ufunc 'power' not supported
            # for the input types, and the inputs could not be safely coerced
            # to any supported types according to the casting rule ''safe''."
            # We follow the general pattern for binary operations of not trying
            # to match pandas exactly.
            assert_exception_equal=False,
        )

    @sql_count_checker(query_count=0)
    @pytest.mark.parametrize(
        "op,error_pattern",
        [
            ("__or__", "__or__"),
            ("__and__", "__and__"),
            ("__ror__", "__or__"),
            ("__rand__", "__and__"),
        ],
    )
    def test_bitwise_timedelta_numeric(self, op, error_pattern, timedelta_dataframes_1):
        eval_snowpark_pandas_result(
            *timedelta_dataframes_1,
            lambda df: getattr(df, op)(1),
            expect_exception=True,
            expect_exception_type=TypeError,
            expect_exception_match=re.escape(
                f"Snowpark pandas does not support binary operation {error_pattern} between timedelta and a non-timedelta type"
            ),
            assert_exception_equal=False,
        )


class TestNumericEdgeCases:
    """
    Test numeric edge cases, e.g. arithmetic with zero and mod by a negative number.
    """

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize(
        "lhs,rhs,expected_pandas,expected_snow",
        [(3, -2, -1, 1), (-3, -2, -1, -1), (-3, 2, 1, -1)],
    )
    def test_timedelta_mod_with_negative_timedelta(
        self, lhs, rhs, expected_pandas, expected_snow
    ):
        """Snowflake sometimes has different behavior for mod with negative numbers."""
        snow_series, pandas_series = create_test_series(pd.Timedelta(lhs))
        assert_series_equal(
            pandas_series % pd.Timedelta(rhs),
            native_pd.Series(pd.Timedelta(expected_pandas)),
        )
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snow_series % pd.Timedelta(rhs),
            native_pd.Series(pd.Timedelta(expected_snow)),
        )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize(
        "lhs,rhs,expected_pandas,expected_snow",
        [(3, -2, 1, 1), (-3, -2, -1, -1), (-3, 2, -1, -1)],
    )
    def test_timedelta_mod_with_negative_numeric(
        self, lhs, rhs, expected_pandas, expected_snow
    ):
        """Snowflake sometimes has different behavior for mod with negative numbers."""
        snow_series, pandas_series = create_test_series(pd.Timedelta(lhs))
        assert_series_equal(
            pandas_series % rhs,
            native_pd.Series(pd.Timedelta(expected_pandas)),
        )
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snow_series % rhs,
            native_pd.Series(pd.Timedelta(expected_snow)),
        )

    @sql_count_checker(query_count=0)
    def test_divide_timedelta_by_zero_timedelta(self):
        snow_series, pandas_series = create_test_series(pd.Timedelta(1))
        assert_series_equal(pandas_series / pd.Timedelta(0), native_pd.Series(np.inf))
        with pytest.raises(SnowparkSQLException, match=re.escape("Division by zero")):
            (snow_series / pd.Timedelta(0)).to_pandas()

    @sql_count_checker(query_count=0)
    def test_divide_timdelta_by_zero_integer(self):
        snow_series, pandas_series = create_test_series(pd.Timedelta(1))
        assert_series_equal(
            pandas_series / 0, native_pd.Series(pd.NaT, dtype="timedelta64[ns]")
        )
        with pytest.raises(SnowparkSQLException, match=re.escape("Division by zero")):
            (snow_series / 0).to_pandas()

    @sql_count_checker(query_count=0)
    def test_floordiv_timedelta_by_zero_timedelta(self):
        snow_series, pandas_series = create_test_series(pd.Timedelta(1))
        assert_series_equal(pandas_series // pd.Timedelta(0), native_pd.Series(0))
        with pytest.raises(SnowparkSQLException, match=re.escape("Division by zero")):
            (snow_series // pd.Timedelta(0)).to_pandas()

    @sql_count_checker(query_count=0)
    def test_floordiv_timedelta_by_zero_integer(self):
        snow_series, pandas_series = create_test_series(pd.Timedelta(1))
        assert_series_equal(
            pandas_series // 0, native_pd.Series(pd.NaT, dtype="timedelta64[ns]")
        )
        with pytest.raises(SnowparkSQLException, match=re.escape("Division by zero")):
            (snow_series // 0).to_pandas()

    @sql_count_checker(query_count=1)
    def test_mod_timedelta_by_zero_timedelta(self):
        snow_series, pandas_series = create_test_series(pd.Timedelta(1))
        assert_series_equal(
            pandas_series % pd.Timedelta(0),
            native_pd.Series(1, dtype="timedelta64[ns]"),
        )
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snow_series % pd.Timedelta(0),
            native_pd.Series(pd.NaT, dtype="timedelta64[ns]"),
        )

    @sql_count_checker(query_count=1)
    def test_mod_timedelta_by_zero_integer(self):
        eval_snowpark_pandas_result(
            *create_test_series(pd.Timedelta(1)),
            lambda series: series % 0,
        )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("op", ["add", "radd", "sub", "rsub"])
    def test_timedelta_plus_or_minus_zero_timedelta(self, timedelta_dataframes_1, op):
        eval_snowpark_pandas_result(
            *timedelta_dataframes_1, lambda df: getattr(df, op)(pd.Timedelta(0))
        )


class TestNullsInTimedeltaComparisons:
    """
    Test comparisons (e.g. <=) between timedeltas where some operands are null.

    Snowpark pandas returns null for comparsions where either operand is null,
    whereas pandas considers null values to be equal to each other, and not
    less than, greater than, or equal to any other values.

    We test these cases separately because we can't use pandas to validate
    Snowpark pandas results.
    """

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("op", ["eq", "ne", "lt", "le", "gt", "ge"])
    def test_comparison_with_null(self, timedelta_dataframes_1, op):
        snow_df = timedelta_dataframes_1[0]
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            getattr(snow_df, op)(pd.NaT),
            native_pd.DataFrame([[None, None], [None, None]]),
        )

    @sql_count_checker(query_count=1)
    def test_eq_non_null(self, timedelta_dataframes_1):
        snow_df = timedelta_dataframes_1[0]
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snow_df == pd.Timedelta(days=1),
            native_pd.DataFrame([[True, None], [False, False]]),
        )

    @sql_count_checker(query_count=1)
    def test_neq_non_null(self, timedelta_dataframes_1):
        snow_df = timedelta_dataframes_1[0]
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snow_df != pd.Timedelta(days=1),
            native_pd.DataFrame([[False, None], [True, True]]),
        )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "op",
    [
        "sub",
        "rsub",
        "add",
        "radd",
    ],
)
def test_timedelta_timedelta_null_arithmetic_valid_in_pandas(
    op, timedelta_dataframes_1
):
    """These timedelta arithmetic operations with null are valid in Snowpark pandas and pandas."""
    eval_snowpark_pandas_result(
        *timedelta_dataframes_1, lambda df: getattr(df, op)(pd.NaT)
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("op", ["div", "rdiv", "mod", "rmod", "floordiv", "rfloordiv"])
def test_timedelta_timedelta_null_arithmetic_invalid_in_pandas(
    op, timedelta_dataframes_1
):
    """It is not valid in pandas to divide (div, rdiv, mod, rmod, floordiv, rfloordiv) timedelta with null scalar."""
    snow_df, pandas_df = timedelta_dataframes_1

    def op_with_null(df):
        return getattr(df, op)(pd.NaT)

    with pytest.raises(TypeError):
        op_with_null(pandas_df)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        op_with_null(snow_df), native_pd.DataFrame([[pd.NaT, pd.NaT], [pd.NaT, pd.NaT]])
    )


@sql_count_checker(query_count=1)
@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "op",
    [
        "sub",
        "rsub",
        "add",
        "radd",
        "div",
        "rdiv",
        "truediv",
        "rtruediv",
        "floordiv",
        "rfloordiv",
    ],
)
def test_valid_arithmetic_between_possibly_negative_timedeltas(
    timedelta_dataframes_with_negatives_no_nulls_1_2x2, op, timedelta_scalar
):
    """
    Test that all valid arithmetic (except mod) between Timedeltas works with negative operands.

    Test just dataframe left operand and scalar right operand in this test case.
    """
    if op == "rfloordiv" and timedelta_scalar in (
        datetime.timedelta(microseconds=1),
        datetime.timedelta(microseconds=2),
        pd.Timedelta(nanoseconds=1),
        pd.Timedelta(nanoseconds=2),
        pd.Timedelta(nanoseconds=3),
    ):
        # snowflake returns 0 for dividing tiny timedeltas by large (~days)
        # timedelta dataframes, so floordiv gives 0 in Snowflake but -1 in
        # pandas. e.g.  pd.Timedelta('0 days 00:00:00.000000003') / pd.DataFrame([pd.Timedelta('-3 days')])
        # translates to  pd.session.sql('SELECT 3.0 / -259200000000000.0').to_pandas(),
        # which gives 0
        pytest.xfail("precision difference in snowflake")
    eval_snowpark_pandas_result(
        *timedelta_dataframes_with_negatives_no_nulls_1_2x2,
        lambda df: getattr(df, op)(timedelta_scalar),
    )


class TestDataFrameAndScalar:
    @pytest.mark.parametrize("operation", ["sub", "rsub"])
    @sql_count_checker(query_count=1)
    def test_timestamp_minus_timestamp(
        self,
        timestamp_scalar,
        operation,
    ):
        eval_snowpark_pandas_result(
            *create_test_dfs(
                [
                    [datetime.datetime(year=2024, month=1, day=1), pd.NaT],
                    [
                        datetime.datetime(year=2023, month=1, day=1),
                        datetime.datetime(year=2030, month=1, day=1),
                    ],
                ]
            ),
            lambda df: getattr(df, operation)(timestamp_scalar),
        )

    @pytest.mark.parametrize(
        "operation",
        [
            "add",
            "radd",
            "sub",
        ],
    )
    @sql_count_checker(query_count=1)
    def test_timestamp_dataframe_plus_or_minus_timedelta_scalar(
        self, timedelta_scalar, operation
    ):
        eval_snowpark_pandas_result(
            *create_test_dfs(
                [
                    [datetime.datetime(year=2024, month=1, day=1), pd.NaT],
                    [
                        datetime.datetime(year=2023, month=1, day=1),
                        datetime.datetime(year=2030, month=1, day=1),
                    ],
                ]
            ),
            lambda df: getattr(df, operation)(timedelta_scalar),
        )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("operation", ["add", "radd"])
    def test_timedelta_dataframe_plus_timestamp_scalar(
        self, timedelta_dataframes_1, timestamp_scalar, operation
    ):
        eval_snowpark_pandas_result(
            *timedelta_dataframes_1, lambda df: getattr(df, operation)(timestamp_scalar)
        )

    @sql_count_checker(query_count=1)
    def test_timedelta_dataframe_with_timedelta_scalar(
        self,
        timedelta_dataframes_postive_no_nulls_1_2x2,
        timedelta_scalar_positive,
        op_between_timedeltas,
    ):
        eval_snowpark_pandas_result(
            *timedelta_dataframes_postive_no_nulls_1_2x2,
            lambda df: getattr(df, op_between_timedeltas)(timedelta_scalar_positive),
        )

    @sql_count_checker(query_count=1)
    def test_timedelta_dataframe_with_numeric_scalar(
        self,
        timedelta_dataframes_postive_no_nulls_1_2x2,
        op_between_timedelta_and_numeric,
        numeric_scalar_non_null,
    ):
        eval_snowpark_pandas_result(
            *timedelta_dataframes_postive_no_nulls_1_2x2,
            lambda df: getattr(df, op_between_timedelta_and_numeric)(
                numeric_scalar_non_null
            ),
        )

    @sql_count_checker(query_count=1)
    def test_numeric_dataframe_with_timedelta_scalar(
        self,
        numeric_dataframes_postive_no_nulls_1_2x2,
        op_between_numeric_and_timedelta,
        timedelta_scalar_positive,
    ):
        eval_snowpark_pandas_result(
            *numeric_dataframes_postive_no_nulls_1_2x2,
            lambda df: getattr(df, op_between_numeric_and_timedelta)(
                timedelta_scalar_positive
            ),
        )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("op", ["mul", "rmul"])
    def test_datetime_timedelta_scalar_multiply_with_numeric_dataframe(
        self,
        numeric_dataframes_postive_no_nulls_1_2x2,
        op,
    ):
        """
        Test valid multiplication between numeric dataframe and datetime.timedelta scalar

        We test this case separately from other timedelta scalar cases due to
        https://github.com/pandas-dev/pandas/issues/59656
        """
        eval_snowpark_pandas_result(
            *numeric_dataframes_postive_no_nulls_1_2x2,
            lambda df: getattr(df, op)(datetime.timedelta(microseconds=2)),
        )

    @sql_count_checker(query_count=1)
    def test_datetime_timedelta_scalar_divide_by_numeric_dataframe(
        self,
    ):
        """
        Test valid division between numeric dataframe and datetime.timedelta scalar

        We test this case separately from other timedelta scalar cases due to
        https://github.com/pandas-dev/pandas/issues/59656
        """
        modin_df, pandas_df = create_test_dfs([[1.5, 1001]])
        scalar = datetime.timedelta(microseconds=2)
        # Due to https://github.com/pandas-dev/pandas/issues/59656, pandas
        # produces a result with microsecond precision, i.e. dtype
        # timedelt64[us], so it represents pd.Timedelta(microseconds=2) /
        # 1.5 as pd.Timedelta(microseconds=1) instead of as
        # pd.Timedelta(nanoseconds=1333). Likewise, it represents
        # pd.Timedelta(microseconds=2) / 1001 as
        # pd.Timedelta(microseconds=0) instead of as
        # pd.Timedelta(nanoseconds=1).
        assert_frame_equal(
            scalar / pandas_df,
            native_pd.DataFrame(
                [
                    [
                        pd.Timedelta(microseconds=1),
                        pd.Timedelta(microseconds=0),
                    ],
                ],
                dtype="timedelta64[us]",
            ),
        )
        assert_frame_equal(
            scalar / modin_df,
            native_pd.DataFrame(
                [
                    [
                        pd.Timedelta(nanoseconds=1333),
                        pd.Timedelta(nanoseconds=1),
                    ],
                ]
            ),
        )


class TestSeriesAndScalar:
    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("operation", ["sub", "rsub"])
    def test_timestamp_series_minus_timestamp_scalar(self, operation, timestamp_scalar):
        eval_snowpark_pandas_result(
            *create_test_series(
                [
                    datetime.datetime(year=2024, month=1, day=1),
                    pd.NaT,
                    datetime.datetime(year=2023, month=1, day=1),
                    datetime.datetime(year=2030, month=1, day=1),
                ]
            ),
            lambda series: getattr(series, operation)(timestamp_scalar),
        )

    @pytest.mark.parametrize(
        "operation",
        [
            "add",
            "radd",
            "sub",
        ],
    )
    @sql_count_checker(query_count=1)
    def test_timestamp_series_plus_or_minus_timedelta_scalar(
        self, timedelta_scalar, operation
    ):
        eval_snowpark_pandas_result(
            *create_test_series(PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1[0]),
            lambda series: getattr(series, operation)(timedelta_scalar),
        )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("operation", ["add", "radd"])
    def test_timedelta_series_plus_timestamp_scalar(
        self, timestamp_scalar, timedelta_series_1, operation
    ):
        eval_snowpark_pandas_result(
            *timedelta_series_1,
            lambda series: getattr(series, operation)(timestamp_scalar),
        )

    @sql_count_checker(query_count=1)
    def test_timedelta_series_with_timedelta_scalar(
        self,
        timedelta_scalar_positive,
        timedelta_series_positive_no_nulls_1_length_6,
        op_between_timedeltas,
    ):
        eval_snowpark_pandas_result(
            *timedelta_series_positive_no_nulls_1_length_6,
            lambda series: getattr(series, op_between_timedeltas)(
                timedelta_scalar_positive
            ),
        )

    @sql_count_checker(query_count=1)
    def test_timedelta_series_with_numeric_scalar(
        self,
        timedelta_series_positive_no_nulls_1_length_6,
        op_between_timedelta_and_numeric,
        numeric_scalar_non_null,
    ):
        eval_snowpark_pandas_result(
            *timedelta_series_positive_no_nulls_1_length_6,
            lambda series: getattr(series, op_between_timedelta_and_numeric)(
                numeric_scalar_non_null
            ),
        )

    @sql_count_checker(query_count=1)
    def test_numeric_series_with_timedelta_scalar(
        self,
        numeric_series_positive_no_nulls_1_length_6,
        op_between_numeric_and_timedelta,
        timedelta_scalar_positive,
    ):
        eval_snowpark_pandas_result(
            *numeric_series_positive_no_nulls_1_length_6,
            lambda series: getattr(series, op_between_numeric_and_timedelta)(
                timedelta_scalar_positive
            ),
        )

    @pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="SNOW-1646604")
    @pytest.mark.parametrize(
        "op",
        [
            "sub",
            "add",
            "div",
            "truediv",
            "eq",
            "ne",
        ],
    )
    @sql_count_checker(query_count=1)
    def test_string_series_with_timedelta_scalar(self, timedelta_scalar_positive, op):
        # TODO(SNOW-1646604): Test other operand types (e.g. DataFrame of strings + scalar timedelta),
        # and all operations in `op_between_timedeltas`.
        # However, note that not all of those cases work in pandas due to
        # https://github.com/pandas-dev/pandas/issues/59653
        eval_snowpark_pandas_result(
            *create_test_series(["1 days", "1 day 1 hour"]),
            lambda series: getattr(series, op)(timedelta_scalar_positive),
        )


class TestDataFrameAndListLikeAxis1:
    @pytest.mark.parametrize("op", ["sub", "rsub"])
    @sql_count_checker(query_count=1)
    def test_timestamp_dataframe_minus_timestamp_list_like(
        self, op, timestamp_no_timezone_dataframes_1
    ):
        eval_snowpark_pandas_result(
            *timestamp_no_timezone_dataframes_1,
            lambda df: getattr(df, op)(
                [
                    pd.Timestamp(1, unit="ns"),
                    pd.Timestamp(300, unit="ns"),
                    pd.Timestamp(57, unit="ms"),
                ]
            ),
        )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("op", ["add", "radd", "sub"])
    def test_timestamp_dataframe_plus_or_minus_timedelta_list_like(
        self, op, timestamp_no_timezone_dataframes_1
    ):
        eval_snowpark_pandas_result(
            *timestamp_no_timezone_dataframes_1,
            lambda df: getattr(df, op)(
                [
                    pd.Timedelta(1, unit="ns"),
                    pd.Timedelta(300, unit="ns"),
                    pd.Timedelta(57, unit="ms"),
                ]
            ),
        )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("op", ["add", "radd"])
    def test_timedelta_dataframe_plus_timestamp_list_like(
        self, op, timedelta_dataframes_1
    ):
        eval_snowpark_pandas_result(
            *timedelta_dataframes_1,
            lambda df: getattr(df, op)(
                [
                    pd.Timestamp(1, unit="D"),
                    pd.Timestamp(-13, unit="D"),
                ]
            ),
        )

    @sql_count_checker(query_count=1)
    def test_timedelta_dataframe_with_timedelta_list_like(
        self, op_between_timedeltas, timedelta_dataframes_postive_no_nulls_1_2x2
    ):
        eval_snowpark_pandas_result(
            *timedelta_dataframes_postive_no_nulls_1_2x2,
            lambda df: getattr(df, op_between_timedeltas)(
                [
                    pd.Timedelta(days=2, milliseconds=140),
                    pd.Timedelta(days=1, milliseconds=770),
                ]
            ),
        )

    @sql_count_checker(query_count=1)
    def test_timedelta_dataframe_with_numeric_list_like(
        self,
        op_between_timedelta_and_numeric,
        timedelta_dataframes_postive_no_nulls_1_2x2,
    ):
        eval_snowpark_pandas_result(
            *timedelta_dataframes_postive_no_nulls_1_2x2,
            lambda df: getattr(df, op_between_timedelta_and_numeric)(
                [
                    1.5,
                    2,
                ]
            ),
        )

    @sql_count_checker(query_count=1)
    def test_numeric_dataframe_with_timedelta_list_like(
        self,
        op_between_numeric_and_timedelta,
        numeric_dataframes_postive_no_nulls_1_2x2,
    ):
        eval_snowpark_pandas_result(
            *numeric_dataframes_postive_no_nulls_1_2x2,
            lambda df: getattr(df, op_between_numeric_and_timedelta)(
                [
                    pd.Timedelta(days=2, milliseconds=140),
                    pd.Timedelta(days=1, milliseconds=770),
                ]
            ),
        )


class TestSeriesAndListLike:
    @sql_count_checker(query_count=1, join_count=1)
    @pytest.mark.parametrize("op", ["sub", "rsub"])
    def test_timestamp_series_minus_timestamp_list_like(
        self, op, timestamp_no_timezone_series_2
    ):
        eval_snowpark_pandas_result(
            *timestamp_no_timezone_series_2,
            lambda series: getattr(series, op)(
                [
                    pd.Timestamp(1, unit="ns"),
                    pd.Timestamp(300, unit="ns"),
                    pd.Timestamp(999, unit="ns"),
                ]
            ),
        )

    @sql_count_checker(query_count=1, join_count=1)
    @pytest.mark.parametrize("op", ["add", "radd", "sub"])
    def test_timestamp_series_plus_or_minus_timedelta_list_like(
        self, op, timestamp_no_timezone_series_2
    ):
        eval_snowpark_pandas_result(
            *timestamp_no_timezone_series_2,
            lambda series: getattr(series, op)(
                [
                    pd.Timedelta(1, unit="ns"),
                    pd.Timedelta(300, unit="ns"),
                    pd.Timedelta(57, unit="ms"),
                ]
            ),
        )

    @sql_count_checker(query_count=1, join_count=1)
    @pytest.mark.parametrize("op", ["add", "radd"])
    def test_timedelta_series_plus_timestamp_list_like(self, op, timedelta_series_1):
        eval_snowpark_pandas_result(
            *timedelta_series_1,
            lambda series: getattr(series, op)(
                [
                    pd.Timestamp(1, unit="D"),
                    pd.Timestamp(2, unit="D"),
                    pd.NaT,
                    pd.Timestamp(3, unit="D"),
                    pd.Timestamp(4, unit="D"),
                    pd.Timestamp(5, unit="D"),
                ]
            ),
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_timedelta_series_and_timedelta_list_like(
        self, op_between_timedeltas, timedelta_series_positive_no_nulls_1_length_6
    ):
        eval_snowpark_pandas_result(
            *timedelta_series_positive_no_nulls_1_length_6,
            lambda series: getattr(series, op_between_timedeltas)(
                [
                    pd.Timedelta(days=6, minutes=6),
                    pd.Timedelta(days=5, minutes=5),
                    pd.Timedelta(days=4, minutes=4),
                    pd.Timedelta(days=3, minutes=3),
                    pd.Timedelta(days=2, minutes=2),
                    pd.Timedelta(nanoseconds=2),
                ]
            ),
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_timedelta_series_and_numeric_list_like(
        self,
        op_between_timedelta_and_numeric,
        timedelta_series_positive_no_nulls_1_length_6,
        numeric_list_like_postive_no_nulls_1_length_6,
    ):
        eval_snowpark_pandas_result(
            *timedelta_series_positive_no_nulls_1_length_6,
            lambda series: getattr(series, op_between_timedelta_and_numeric)(
                numeric_list_like_postive_no_nulls_1_length_6
            ),
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_numeric_series_and_timedelta_list_like(
        self,
        op_between_numeric_and_timedelta,
        numeric_series_positive_no_nulls_1_length_6,
    ):
        eval_snowpark_pandas_result(
            *numeric_series_positive_no_nulls_1_length_6,
            lambda series: getattr(series, op_between_numeric_and_timedelta)(
                [
                    pd.Timedelta(days=1),
                    pd.Timedelta(days=2),
                    pd.Timedelta(days=3),
                    pd.Timedelta(days=4),
                    pd.Timedelta(days=5),
                    pd.Timedelta(days=6),
                ]
            ),
        )


class TestDataFrameAndListLikeAxis0:
    @sql_count_checker(query_count=1, join_count=1)
    @pytest.mark.parametrize("op", ["sub", "rsub"])
    def test_timestamp_dataframe_minus_timestamp_list_like(
        self, op, timestamp_no_timezone_dataframes_2
    ):
        eval_snowpark_pandas_result(
            *timestamp_no_timezone_dataframes_2,
            lambda df: getattr(df, op)(
                [
                    pd.Timestamp(1, unit="ns"),
                    pd.Timestamp(300, unit="ns"),
                    pd.Timestamp(999, unit="ns"),
                ],
                axis=0,
            ),
        )

    @pytest.mark.parametrize("op", ["add", "radd", "sub"])
    @sql_count_checker(query_count=1, join_count=1)
    def test_timestamp_dataframe_plus_or_minus_timedelta_list_like(
        self, op, timestamp_no_timezone_dataframes_2
    ):
        eval_snowpark_pandas_result(
            *timestamp_no_timezone_dataframes_2,
            lambda df: getattr(df, op)(
                [
                    pd.Timedelta(1, unit="ns"),
                    pd.Timedelta(300, unit="ns"),
                    pd.Timedelta(999, unit="ns"),
                ],
                axis=0,
            ),
        )

    @sql_count_checker(query_count=1, join_count=1)
    @pytest.mark.parametrize("op", ["add", "radd"])
    def test_timedelta_dataframe_plus_timestamp_list_like(
        self, op, timedelta_dataframes_1
    ):
        eval_snowpark_pandas_result(
            *timedelta_dataframes_1,
            lambda df: getattr(df, op)(
                [
                    pd.Timestamp(1, unit="D"),
                    pd.Timestamp(999, unit="D"),
                ],
                axis=0,
            ),
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_timedelta_dataframe_and_timedelta_list_like(
        self, op_between_timedeltas, timedelta_dataframes_postive_no_nulls_1_2x2
    ):
        eval_snowpark_pandas_result(
            *timedelta_dataframes_postive_no_nulls_1_2x2,
            lambda df: getattr(df, op_between_timedeltas)(
                [
                    pd.Timedelta(nanoseconds=1),
                    pd.Timedelta(microseconds=2),
                ],
                axis=0,
            ),
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_timedelta_dataframe_and_numeric_list_like(
        self,
        timedelta_dataframes_postive_no_nulls_1_2x2,
        op_between_timedelta_and_numeric,
    ):
        eval_snowpark_pandas_result(
            *timedelta_dataframes_postive_no_nulls_1_2x2,
            lambda df: getattr(df, op_between_timedelta_and_numeric)([1.5, 2], axis=0),
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_numeric_dataframe_and_timedelta_list_like(
        self,
        numeric_dataframes_postive_no_nulls_1_2x2,
        op_between_numeric_and_timedelta,
    ):
        eval_snowpark_pandas_result(
            *numeric_dataframes_postive_no_nulls_1_2x2,
            lambda df: getattr(df, op_between_numeric_and_timedelta)(
                [
                    pd.Timedelta(nanoseconds=1),
                    pd.Timedelta(microseconds=2.5),
                ],
                axis=0,
            ),
        )


class TestSeriesAndSeries:
    @pytest.mark.parametrize(
        "pandas_lhs,pandas_rhs",
        [
            PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1,
            [
                x.dt.tz_localize("UTC")
                for x in PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1
            ],
            (
                PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1[0].dt.tz_localize(
                    "UTC"
                ),
                PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1[1].dt.tz_localize(
                    "Asia/Kolkata"
                ),
            ),
        ],
    )
    @pytest.mark.parametrize("op", ["sub", "rsub"])
    @sql_count_checker(query_count=1, join_count=1)
    def test_timestamp_minus_timestamp(self, pandas_lhs, pandas_rhs, op):
        """Subtract two series of timestamps to get a timedelta."""
        snow_lhs = pd.Series(pandas_lhs)
        snow_rhs = pd.Series(pandas_rhs)
        eval_snowpark_pandas_result(
            (snow_lhs, snow_rhs),
            (pandas_lhs, pandas_rhs),
            lambda inputs: getattr(inputs[0], op)(inputs[1]),
        )

    @pytest.mark.parametrize(
        "pandas_lhs,pandas_rhs",
        [
            (
                PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1[0].dt.tz_localize(
                    "UTC"
                ),
                PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1[1],
            ),
            (
                PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1[0],
                PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1[1].dt.tz_localize(
                    "UTC"
                ),
            ),
        ],
    )
    @pytest.mark.parametrize("op", ["sub", "rsub"])
    @sql_count_checker(query_count=0)
    def test_subtract_two_timestamps_timezones_disallowed(
        self, pandas_lhs, pandas_rhs, op
    ):
        snow_lhs = pd.Series(pandas_lhs)
        snow_rhs = pd.Series(pandas_rhs)
        # pandas is inconsistent about including a period at the end of the end
        # of the error message, but Snowpark pandas is not.
        eval_snowpark_pandas_result(
            (snow_lhs, snow_rhs),
            (pandas_lhs, pandas_rhs),
            lambda inputs: getattr(inputs[0], op)(inputs[1]),
            expect_exception=True,
            expect_exception_match=re.escape(
                "Cannot subtract tz-naive and tz-aware datetime-like objects."
            ),
            assert_exception_equal=False,
            except_exception_type=TypeError,
        )

    @pytest.mark.parametrize("op", ["add", "radd", "sub"])
    @sql_count_checker(query_count=1, join_count=1)
    def test_timestamp_plus_or_minus_timedelta(self, op, timedelta_series_1):
        pandas_lhs = PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1[0]
        snow_lhs = pd.Series(pandas_lhs)
        snow_rhs, pandas_rhs = timedelta_series_1
        eval_snowpark_pandas_result(
            (snow_lhs, snow_rhs),
            (pandas_lhs, pandas_rhs),
            lambda inputs: getattr(inputs[0], op)(inputs[1]),
        )

    @pytest.mark.parametrize("op", ["add", "radd"])
    @sql_count_checker(query_count=1, join_count=1)
    def test_timedelta_plus_timestamp(self, op, timedelta_series_1):
        snow_lhs, pandas_lhs = timedelta_series_1
        pandas_rhs = PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1[0]
        snow_rhs = pd.Series(pandas_rhs)
        eval_snowpark_pandas_result(
            (snow_lhs, snow_rhs),
            (pandas_lhs, pandas_rhs),
            lambda inputs: getattr(inputs[0], op)(inputs[1]),
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_timedelta_and_timedleta(
        self,
        op_between_timedeltas,
        timedelta_series_positive_no_nulls_1_length_6,
        timedelta_series_no_nulls_2_length_6,
    ):
        snow_lhs, pandas_lhs = timedelta_series_positive_no_nulls_1_length_6
        snow_rhs, pandas_rhs = timedelta_series_no_nulls_2_length_6
        eval_snowpark_pandas_result(
            (snow_lhs, snow_rhs),
            (pandas_lhs, pandas_rhs),
            lambda inputs: getattr(inputs[0], op_between_timedeltas)(inputs[1]),
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_timedelta_and_numeric(
        self,
        op_between_timedelta_and_numeric,
        timedelta_series_positive_no_nulls_1_length_6,
        numeric_series_positive_no_nulls_1_length_6,
    ):
        snow_lhs, pandas_lhs = timedelta_series_positive_no_nulls_1_length_6
        snow_rhs, pandas_rhs = numeric_series_positive_no_nulls_1_length_6
        eval_snowpark_pandas_result(
            (snow_lhs, snow_rhs),
            (pandas_lhs, pandas_rhs),
            lambda inputs: getattr(inputs[0], op_between_timedelta_and_numeric)(
                inputs[1]
            ),
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_numeric_and_timedelta(
        self,
        op_between_numeric_and_timedelta,
        numeric_series_positive_no_nulls_1_length_6,
        timedelta_series_positive_no_nulls_1_length_6,
    ):
        snow_lhs, pandas_lhs = numeric_series_positive_no_nulls_1_length_6
        snow_rhs, pandas_rhs = timedelta_series_positive_no_nulls_1_length_6
        eval_snowpark_pandas_result(
            (snow_lhs, snow_rhs),
            (pandas_lhs, pandas_rhs),
            lambda inputs: getattr(inputs[0], op_between_numeric_and_timedelta)(
                inputs[1]
            ),
        )


class TestDataFrameAndSeriesAxis0:
    @pytest.mark.parametrize("op", ["sub", "rsub"])
    @sql_count_checker(query_count=1, join_count=1)
    def test_timestamp_dataframe_minus_timestamp_series(
        self, op, timestamp_no_timezone_dataframes_3
    ):
        snow_df, pandas_df = timestamp_no_timezone_dataframes_3
        snow_series, pandas_series = create_test_series(
            [
                pd.Timestamp(5, unit="ms"),
                pd.Timestamp(6, unit="ms"),
                pd.Timestamp(7, unit="ms"),
            ]
        )
        eval_snowpark_pandas_result(
            (snow_df, snow_series),
            (pandas_df, pandas_series),
            lambda t: getattr(t[0], op)(t[1], axis=0),
        )

    @pytest.mark.parametrize("op", ["add", "radd", "sub"])
    @sql_count_checker(query_count=1, join_count=1)
    def test_timestamp_dataframe_plus_or_minus_timedelta_series(
        self, op, timestamp_no_timezone_dataframes_3
    ):
        snow_df, pandas_df = timestamp_no_timezone_dataframes_3
        snow_series, pandas_series = create_test_series(
            [
                pd.Timedelta(5, unit="ms"),
                pd.Timedelta(6, unit="ms"),
                pd.Timedelta(7, unit="ms"),
            ]
        )
        eval_snowpark_pandas_result(
            (snow_df, snow_series),
            (pandas_df, pandas_series),
            lambda t: getattr(t[0], op)(t[1], axis=0),
        )

    @pytest.mark.parametrize(
        "op",
        [
            "add",
            "radd",
        ],
    )
    @sql_count_checker(query_count=1, join_count=1)
    def test_timedelta_dataframe_plus_timestamp_series(
        self, op, timedelta_dataframes_1
    ):
        snow_df, pandas_df = timedelta_dataframes_1
        snow_series, pandas_series = create_test_series(
            [
                pd.Timestamp(5, unit="D"),
                pd.Timestamp(6, unit="D"),
                pd.Timestamp(7, unit="D"),
            ]
        )
        eval_snowpark_pandas_result(
            (snow_df, snow_series),
            (pandas_df, pandas_series),
            lambda t: getattr(t[0], op)(t[1], axis=0),
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_timedelta_dataframe_and_timedelta_series(
        self,
        op_between_timedeltas,
        timedelta_dataframes_postive_no_nulls_1_2x2,
        timedelta_series_no_nulls_3_length_2,
    ):
        snow_df, pandas_df = timedelta_dataframes_postive_no_nulls_1_2x2
        snow_series, pandas_series = timedelta_series_no_nulls_3_length_2
        eval_snowpark_pandas_result(
            (snow_df, snow_series),
            (pandas_df, pandas_series),
            lambda t: getattr(t[0], op_between_timedeltas)(t[1], axis=0),
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_timedelta_dataframe_and_numeric_series(
        self,
        op_between_timedelta_and_numeric,
        timedelta_dataframes_postive_no_nulls_1_2x2,
        numeric_series_positive_no_nulls_2_length_2,
    ):
        snow_df, pandas_df = timedelta_dataframes_postive_no_nulls_1_2x2
        snow_series, pandas_series = numeric_series_positive_no_nulls_2_length_2
        eval_snowpark_pandas_result(
            (snow_df, snow_series),
            (pandas_df, pandas_series),
            lambda t: getattr(t[0], op_between_timedelta_and_numeric)(t[1], axis=0),
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_numeric_dataframe_and_timedelta_series(
        self,
        op_between_numeric_and_timedelta,
        numeric_dataframes_postive_no_nulls_1_2x2,
        timedelta_series_no_nulls_3_length_2,
    ):
        snow_df, pandas_df = numeric_dataframes_postive_no_nulls_1_2x2
        snow_series, pandas_series = timedelta_series_no_nulls_3_length_2
        eval_snowpark_pandas_result(
            (snow_df, snow_series),
            (pandas_df, pandas_series),
            lambda t: getattr(t[0], op_between_numeric_and_timedelta)(t[1], axis=0),
        )


class TestDataFrameAndSeriesAxis1:
    # One query to materialize the series so we can align it along axis 1, and
    # another query to materialize the result of the binary operation.
    QUERY_COUNT = 2

    @sql_count_checker(query_count=QUERY_COUNT)
    def test_timestamp_dataframe_minus_timestamp_series(
        self, timestamp_no_timezone_dataframes_3
    ):
        """
        Test subtracting a series of timestamps from a dataframe of timestamps on axis 1.
        pandas behavior is incorrect: https://github.com/pandas-dev/pandas/issues/59529
        """
        snow_df, pandas_df = timestamp_no_timezone_dataframes_3
        pandas_series = native_pd.Series(
            [
                pd.Timestamp(5, unit="ms"),
                pd.Timestamp(6, unit="ms"),
                pd.Timestamp(7, unit="ms"),
            ]
        )
        with pytest.raises(
            TypeError, match="cannot subtract DatetimeArray from ndarray"
        ):
            pandas_df - pandas_series
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snow_df - pd.Series(pandas_series),
            native_pd.DataFrame(
                [
                    [
                        native_pd.Timedelta(milliseconds=-4),
                        native_pd.Timedelta(milliseconds=-4),
                        pd.NaT,
                    ],
                    [
                        native_pd.Timedelta(milliseconds=-2),
                        native_pd.Timedelta(milliseconds=-2),
                        pd.NaT,
                    ],
                ]
            ),
        )

    @sql_count_checker(query_count=QUERY_COUNT)
    def test_timestamp_series_minus_timestamp_dataframe(
        self, timestamp_no_timezone_dataframes_3
    ):
        """
        Test subtracting a dataframe of timestamps from a series of timestamps.
        pandas behavior is incorrect: https://github.com/pandas-dev/pandas/issues/59529
        """
        snow_df, pandas_df = timestamp_no_timezone_dataframes_3
        pandas_series = native_pd.Series(
            [
                pd.Timestamp(5, unit="ms"),
                pd.Timestamp(6, unit="ms"),
                pd.Timestamp(7, unit="ms"),
            ]
        )
        with pytest.raises(
            np.core._exceptions.UFuncTypeError,
            match=re.escape(
                "ufunc 'subtract' cannot use operands with types dtype('<M8[ns]') and dtype('float64')"
            ),
        ):
            pandas_series - pandas_df
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            pd.Series(pandas_series) - snow_df,
            native_pd.DataFrame(
                [
                    [
                        native_pd.Timedelta(milliseconds=4),
                        native_pd.Timedelta(milliseconds=4),
                        pd.NaT,
                    ],
                    [
                        native_pd.Timedelta(milliseconds=2),
                        native_pd.Timedelta(milliseconds=2),
                        pd.NaT,
                    ],
                ]
            ),
        )

    @sql_count_checker(query_count=QUERY_COUNT)
    @pytest.mark.parametrize(
        "op,error_message",
        [
            pytest.param(
                "add",
                "ufunc 'add' cannot use operands with types dtype('float64') and dtype('<m8[ns]')",
                id="add",
            ),
            pytest.param(
                "radd",
                "ufunc 'add' cannot use operands with types dtype('<m8[ns]') and dtype('float64')",
                id="radd",
            ),
        ],
    )
    def test_timestamp_dataframe_plus_timedelta_series(
        self, op, timestamp_no_timezone_dataframes_3, error_message
    ):
        snow_df, pandas_df = timestamp_no_timezone_dataframes_3
        snow_series, pandas_series = create_test_series(
            [
                pd.Timedelta(5, unit="ms"),
                pd.Timedelta(6, unit="ms"),
                pd.Timedelta(7, unit="ms"),
            ]
        )
        # pandas incorrectly raises error due to https://github.com/pandas-dev/pandas/issues/59529
        with pytest.raises(
            np.core._exceptions.UFuncTypeError,
            match=re.escape(error_message),
        ):
            getattr(pandas_df, op)(pandas_series)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            getattr(snow_df, op)(snow_series),
            native_pd.DataFrame(
                [
                    [
                        native_pd.Timestamp(6, unit="ms"),
                        native_pd.Timestamp(8, unit="ms"),
                        pd.NaT,
                    ],
                    [
                        native_pd.Timestamp(8, unit="ms"),
                        native_pd.Timestamp(10, unit="ms"),
                        pd.NaT,
                    ],
                ]
            ),
        )

    @sql_count_checker(query_count=QUERY_COUNT)
    def test_timestamp_dataframe_minus_timedelta_series(
        self, timestamp_no_timezone_dataframes_3
    ):
        snow_df, pandas_df = timestamp_no_timezone_dataframes_3
        snow_series, pandas_series = create_test_series(
            [
                pd.Timedelta(1, unit="ms"),
                pd.Timedelta(2, unit="ms"),
                pd.Timedelta(3, unit="ms"),
            ]
        )
        # pandas incorrectly raises error due to https://github.com/pandas-dev/pandas/issues/59529
        with pytest.raises(
            np.core._exceptions.UFuncTypeError,
            match=re.escape(
                "ufunc 'add' cannot use operands with types dtype('<m8[ns]') and dtype('float64')"
            ),
        ):
            pandas_df - pandas_series

        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snow_df - snow_series,
            native_pd.DataFrame(
                [
                    [
                        native_pd.Timestamp(0, unit="ms"),
                        native_pd.Timestamp(0, unit="ms"),
                        pd.NaT,
                    ],
                    [
                        native_pd.Timestamp(2, unit="ms"),
                        native_pd.Timestamp(2, unit="ms"),
                        pd.NaT,
                    ],
                ]
            ),
        )

    @sql_count_checker(query_count=QUERY_COUNT)
    @pytest.mark.parametrize(
        "op,error_message",
        [
            pytest.param(
                "add",
                "ufunc 'add' cannot use operands with types dtype('float64') and dtype('<M8[ns]')",
                id="add",
            ),
            pytest.param(
                "radd",
                "ufunc 'add' cannot use operands with types dtype('<M8[ns]') and dtype('float64')",
                id="radd",
            ),
        ],
    )
    def test_timedelta_dataframe_plus_timestamp_series(
        self, timedelta_dataframes_1, timestamp_no_timezone_series_2, op, error_message
    ):
        snow_df, pandas_df = timedelta_dataframes_1
        snow_series, pandas_series = timestamp_no_timezone_series_2
        # pandas incorrectly raises error due to https://github.com/pandas-dev/pandas/issues/59529
        with pytest.raises(
            np.core._exceptions.UFuncTypeError,
            match=re.escape(error_message),
        ):
            getattr(pandas_df, op)(pandas_series)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            getattr(snow_df, op)(snow_series),
            native_pd.DataFrame(
                [
                    [pd.Timestamp("1970-01-02 00:00:00.000000005"), pd.NaT, pd.NaT],
                    [
                        pd.Timestamp("1970-01-03 00:00:00.000000005"),
                        pd.Timestamp("1970-01-04 00:00:00.000000700"),
                        pd.NaT,
                    ],
                ]
            ),
        )

    @sql_count_checker(query_count=QUERY_COUNT)
    def test_timedelta_dataframe_with_timedelta_series(
        self,
        timedelta_dataframes_postive_no_nulls_1_2x2,
        timedelta_series_no_nulls_3_length_2,
        op_between_timedeltas,
    ):
        snow_df, pandas_df = timedelta_dataframes_postive_no_nulls_1_2x2
        snow_series, pandas_series = timedelta_series_no_nulls_3_length_2
        eval_snowpark_pandas_result(
            (snow_df, snow_series),
            (pandas_df, pandas_series),
            lambda t: getattr(t[0], op_between_timedeltas)(t[1]),
        )

    @sql_count_checker(query_count=QUERY_COUNT)
    def test_timedelta_dataframe_and_numeric_series(
        self,
        op_between_timedelta_and_numeric,
        timedelta_dataframes_postive_no_nulls_1_2x2,
        numeric_series_positive_no_nulls_2_length_2,
    ):
        snow_df, pandas_df = timedelta_dataframes_postive_no_nulls_1_2x2
        snow_series, pandas_series = numeric_series_positive_no_nulls_2_length_2
        eval_snowpark_pandas_result(
            (snow_df, snow_series),
            (pandas_df, pandas_series),
            lambda t: getattr(t[0], op_between_timedelta_and_numeric)(t[1]),
        )

    @sql_count_checker(query_count=QUERY_COUNT)
    def test_numeric_dataframe_and_timedelta_series(
        self,
        op_between_numeric_and_timedelta,
        numeric_dataframes_postive_no_nulls_1_2x2,
        timedelta_series_no_nulls_3_length_2,
    ):
        snow_df, pandas_df = numeric_dataframes_postive_no_nulls_1_2x2
        snow_series, pandas_series = timedelta_series_no_nulls_3_length_2
        eval_snowpark_pandas_result(
            (snow_df, snow_series),
            (pandas_df, pandas_series),
            lambda t: getattr(t[0], op_between_numeric_and_timedelta)(t[1]),
        )


class TestDataFrameAndDataFrameAxis1:
    @pytest.mark.parametrize(
        "fill_value", [None, pd.NaT, pd.Timestamp(year=1999, month=12, day=31)]
    )
    @pytest.mark.parametrize("op", ["sub", "rsub"])
    @sql_count_checker(query_count=1, join_count=1)
    def test_timestamp_sub_dataframe_and_dataframe(self, fill_value, op):
        # at position [0, 0], both values are non-null.
        # at position [0, 1], both values are null.
        # at position [1, 0], lhs is null and rhs is not null.
        # at position [1, 1], lhs is non-null and rhs is null.
        snow_lhs, pandas_lhs = create_test_dfs(
            [
                [
                    pd.Timestamp(year=2000, month=1, day=1),
                    pd.NaT,
                ],
                [pd.NaT, pd.Timestamp(year=2001, month=2, day=2)],
            ]
        )
        snow_rhs, pandas_rhs = create_test_dfs(
            [
                [pd.Timestamp(year=2002, month=3, day=3), pd.NaT],
                [pd.Timestamp(year=2003, month=4, day=4), pd.NaT],
            ]
        )
        eval_snowpark_pandas_result(
            (snow_lhs, snow_rhs),
            (pandas_lhs, pandas_rhs),
            lambda t: getattr(t[0], op)(t[1], fill_value=fill_value),
        )

    @pytest.mark.parametrize("op", ["add", "radd", "sub"])
    @sql_count_checker(query_count=1, join_count=1)
    def test_timestamp_dataframe_plus_or_minus_timedelta_dataframe(self, op):
        snow_lhs, pandas_lhs = create_test_dfs(
            [
                [
                    pd.Timestamp(year=2000, month=1, day=1),
                    pd.NaT,
                ],
                [pd.NaT, pd.Timestamp(year=2001, month=2, day=2)],
            ]
        )
        snow_rhs, pandas_rhs = create_test_dfs(
            [
                [pd.Timedelta(days=1), pd.Timedelta(days=-3)],
                [pd.NaT, pd.NaT],
            ],
        )
        eval_snowpark_pandas_result(
            (snow_lhs, snow_rhs),
            (pandas_lhs, pandas_rhs),
            lambda t: getattr(t[0], op)(t[1]),
        )

    @pytest.mark.parametrize("op", ["add", "radd"])
    @sql_count_checker(query_count=1, join_count=1)
    def test_timedelta_dataframe_plus_timestamp_dataframe(self, op):
        snow_lhs, pandas_lhs = create_test_dfs(
            [
                [pd.Timedelta(days=1), pd.Timedelta(days=-3)],
                [pd.NaT, pd.NaT],
            ],
        )
        snow_rhs, pandas_rhs = create_test_dfs(
            [
                [
                    pd.Timestamp(year=2000, month=1, day=1),
                    pd.NaT,
                ],
                [pd.NaT, pd.Timestamp(year=2001, month=2, day=2)],
            ]
        )
        eval_snowpark_pandas_result(
            (snow_lhs, snow_rhs),
            (pandas_lhs, pandas_rhs),
            lambda t: getattr(t[0], op)(t[1]),
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_two_timedelta_dataframes(
        self, timedelta_dataframes_postive_no_nulls_1_2x2, op_between_timedeltas
    ):
        snow_lhs, pandas_lhs = timedelta_dataframes_postive_no_nulls_1_2x2
        snow_rhs, pandas_rhs = create_test_dfs(
            [
                [
                    pd.Timedelta(nanoseconds=1),
                    pd.Timedelta(microseconds=1),
                ],
                [pd.Timedelta(days=2), pd.Timedelta(days=1)],
            ]
        )
        eval_snowpark_pandas_result(
            (snow_lhs, snow_rhs),
            (pandas_lhs, pandas_rhs),
            lambda t: getattr(t[0], op_between_timedeltas)(t[1]),
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_timedelta_and_numeric(
        self,
        timedelta_dataframes_postive_no_nulls_1_2x2,
        numeric_dataframes_postive_no_nulls_1_2x2,
        op_between_timedelta_and_numeric,
    ):
        snow_lhs, pandas_lhs = timedelta_dataframes_postive_no_nulls_1_2x2
        snow_rhs, pandas_rhs = numeric_dataframes_postive_no_nulls_1_2x2
        eval_snowpark_pandas_result(
            (snow_lhs, snow_rhs),
            (pandas_lhs, pandas_rhs),
            lambda t: getattr(t[0], op_between_timedelta_and_numeric)(t[1]),
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_numeric_and_timedelta(
        self,
        numeric_dataframes_postive_no_nulls_1_2x2,
        timedelta_dataframes_postive_no_nulls_1_2x2,
        op_between_numeric_and_timedelta,
    ):
        snow_lhs, pandas_lhs = numeric_dataframes_postive_no_nulls_1_2x2
        snow_rhs, pandas_rhs = timedelta_dataframes_postive_no_nulls_1_2x2
        eval_snowpark_pandas_result(
            (snow_lhs, snow_rhs),
            (pandas_lhs, pandas_rhs),
            lambda t: getattr(t[0], op_between_numeric_and_timedelta)(t[1]),
        )
