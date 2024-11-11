#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.series.test_apply_and_map import (
    BASIC_DATA_FUNC_RETURN_TYPE_MAP,
    DATE_TIME_TIMESTAMP_DATA_FUNC_RETURN_TYPE_MAP,
    TEST_NUMPY_FUNCS,
    create_func_with_return_type_hint,
)
from tests.integ.modin.utils import (
    PANDAS_VERSION_PREDICATE,
    assert_snowpark_pandas_equal_to_pandas,
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

pytestmark = pytest.mark.skipif(
    PANDAS_VERSION_PREDICATE,
    reason="SNOW-1739034: tests with UDFs/sprocs cannot run without pandas 2.2.3 in Snowflake anaconda",
)


@pytest.mark.parametrize("data,func,return_type", BASIC_DATA_FUNC_RETURN_TYPE_MAP)
@sql_count_checker(query_count=7, udf_count=1)
def test_applymap_basic_without_type_hints(data, func, return_type):
    frame_data = {0: data, 1: data}
    native_df = native_pd.DataFrame(frame_data)
    snow_df = pd.DataFrame(frame_data)
    eval_snowpark_pandas_result(snow_df, native_df, lambda x: x.applymap(func))


@pytest.mark.parametrize("data,func,return_type", BASIC_DATA_FUNC_RETURN_TYPE_MAP)
@sql_count_checker(query_count=7, udf_count=1)
def test_applymap_basic_with_type_hints(data, func, return_type):
    func_with_type_hint = create_func_with_return_type_hint(func, return_type)

    frame_data = {0: data, 1: data}
    native_df = native_pd.DataFrame(frame_data)
    snow_df = pd.DataFrame(frame_data)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda x: x.applymap(func_with_type_hint)
    )


@pytest.mark.parametrize(
    "data,func,return_type,expected_result",
    DATE_TIME_TIMESTAMP_DATA_FUNC_RETURN_TYPE_MAP,
)
@sql_count_checker(query_count=7, udf_count=1)
def test_applymap_date_time_timestamp(data, func, return_type, expected_result):
    func_with_type_hint = create_func_with_return_type_hint(func, return_type)

    # concat the expected result (which is series) to a dataframe
    frame_data = {0: data, 1: data}
    frame_expected_result = native_pd.concat([expected_result, expected_result], axis=1)

    snow_df = pd.DataFrame(frame_data)
    result = snow_df.applymap(func_with_type_hint)
    assert_snowpark_pandas_equal_to_pandas(result, frame_expected_result)


@pytest.mark.xfail(strict=True, raises=NotImplementedError)
@sql_count_checker(query_count=0)
def test_frame_with_timedelta_index():
    eval_snowpark_pandas_result(
        *create_test_dfs(
            native_pd.DataFrame([0], index=[native_pd.Timedelta(1)]),
        ),
        lambda df: df.applymap(lambda x: x),
    )


def test_applymap_kwargs():
    def f(x, y=1) -> int:
        return x + y

    data = [[1, 2], [3, 4]]
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(data)

    with SqlCounter(query_count=7, udf_count=1):
        eval_snowpark_pandas_result(snow_df, native_df, lambda x: x.applymap(f, y=2))

    with SqlCounter(query_count=6):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda x: x.applymap(f, y=2, z=3),
            expect_exception=True,
            expect_exception_type=SnowparkSQLException,
            expect_exception_match="got an unexpected keyword argument",
            assert_exception_equal=False,
        )


@pytest.mark.parametrize("func", TEST_NUMPY_FUNCS)
def test_applymap_numpy(func):
    data = [[1.0, 2.0], [3.0, 4.0]]
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(data)

    with SqlCounter(query_count=7, udf_count=1):
        eval_snowpark_pandas_result(snow_df, native_df, lambda x: x.applymap(func))


@sql_count_checker(query_count=0)
def test_applymap_na_action_ignore():
    snow_df = pd.DataFrame([1, 1.1, "NaN", None], dtype="Float64")
    msg = "Snowpark pandas applymap API doesn't yet support na_action == 'ignore'"
    with pytest.raises(NotImplementedError, match=msg):
        snow_df.applymap(lambda x: x is None, na_action="ignore")

    data = ["cat", "dog", np.nan, "rabbit"]
    snow_df = pd.DataFrame(data)
    with pytest.raises(NotImplementedError, match=msg):
        snow_df.applymap("I am a {}".format, na_action="ignore")


@pytest.mark.parametrize("invalid_input", ["min", [np.min], {"a": np.max}])
@sql_count_checker(query_count=0)
def test_applymap_invalid_input(invalid_input):
    snow_df = pd.DataFrame([1])
    native_df = native_pd.DataFrame([1])
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda x: x.applymap(invalid_input),
        expect_exception=True,
        expect_exception_match="is not callable",
        assert_exception_equal=False,
    )


def test_preserve_order():
    native_df = native_pd.DataFrame([[10, 2.12], [3.356, 4.567]])
    df = pd.DataFrame(native_df)

    with SqlCounter(query_count=7, udf_count=1):
        eval_snowpark_pandas_result(df, native_df, lambda x: x.applymap(lambda y: -y))

    native_df = native_df.sort_values(0)
    df = pd.DataFrame(native_df)

    """
    >>> df.sort_values(0)
            0      1
    1   3.356  4.567
    0  10.000  2.120

    >>> df.applymap(lambda y: -y)
            0      1
    1  -3.356 -4.567
    0 -10.000 -2.120
    """
    with SqlCounter(query_count=7, udf_count=1):
        eval_snowpark_pandas_result(df, native_df, lambda x: x.applymap(lambda y: -y))


def test_applymap_variant_json_null():
    def f(x):
        if native_pd.isna(x):
            return x
        elif x == 1:
            return None
        elif x == 2:
            return np.nan
        elif x == 3:
            return native_pd.NA
        else:
            return x

    # the last column is a variant column [None, pd.NA], where both None and pd.NA
    # are mapped to SQL null by Python UDF in the input
    df = pd.DataFrame([[1, 2, None], [3, 4, pd.NA]])
    with SqlCounter(query_count=9):
        df = df.applymap(f)

    with SqlCounter(query_count=1, udf_count=1):
        assert df.isna().to_numpy().tolist() == [
            [False, True, True],
            [True, False, True],
        ]
