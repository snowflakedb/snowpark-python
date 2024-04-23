#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import datetime
import logging
import math
from typing import Callable

import numpy as np
import pandas as native_pd
import pytest
from pandas.testing import assert_series_equal

import snowflake.snowpark.modin.pandas as pd
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import udf
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.types import DoubleType, VariantType
from tests.integ.conftest import running_on_public_ci
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import (
    ColumnSchema,
    assert_snowpark_pandas_equal_to_pandas,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_snow_df_with_table_and_data,
    eval_snowpark_pandas_result,
)

BASIC_DATA_FUNC_RETURN_TYPE_MAP = [
    ([1, 2, 3, None], lambda x: x + 1, "int"),
    (["s", "n", "o", "w"], lambda x: x * 2, "str"),
    ([1.0, 1.5, 2.0], math.exp, "float"),
    ([True, True, False], int, "int"),
    ([bytes("snow", "utf-8"), bytes("flake", "utf-8")], lambda x: x.decode(), "str"),
    ([1, 2, 3], lambda x: [x, x + 1], "list"),
    ([1, 2, 3], lambda x: {str(x): x * 8}, "dict"),
    ([None, 1], lambda x: bool(np.isnan(x)), "bool"),
    ([np.nan, None, 2.1], lambda x: x, "float"),
]

# TODO SNOW-876999: Test return date/time/timestamp type when
#  timezone-aware type annotation on UDF is supported
DATE_TIME_TIMESTAMP_DATA_FUNC_RETURN_TYPE_MAP = [
    # data in DATE column will be encoded as pd.Timestamp,
    # and NULL will be encoded as pd.NaT in vectorized udf
    (
        [datetime.date(2023, 1, 1), None],
        str,
        "str",
        native_pd.Series(["2023-01-01 00:00:00", "NaT"]),
    ),
    (
        [datetime.date(2023, 1, 1), None],
        type,
        "str",
        native_pd.Series(
            [
                "<class 'pandas._libs.tslibs.timestamps.Timestamp'>",
                "<class 'pandas._libs.tslibs.nattype.NaTType'>",
            ]
        ),
    ),
    # data in TIME column will be encoded as pd.Timedelta (instead of pd.Timestamp!),
    # and NULL will be encoded as pd.NaT in vectorized udf
    (
        [datetime.time(1, 2, 3), None],
        str,
        "str",
        native_pd.Series(["0 days 01:02:03", "NaT"]),
    ),
    (
        [datetime.time(1, 2, 3), None],
        type,
        "str",
        native_pd.Series(
            [
                "<class 'pandas._libs.tslibs.timedeltas.Timedelta'>",
                "<class 'pandas._libs.tslibs.nattype.NaTType'>",
            ]
        ),
    ),
    # data in TIMESTAMP_NTZ/LTZ column will be encoded as pd.Timestamp,
    # and NULL will be encoded as pd.NaT in vectorized udf
    (
        [datetime.datetime(2023, 1, 1, 1, 2, 3), None],
        str,
        "str",
        native_pd.Series(["2023-01-01 01:02:03", "NaT"]),
    ),
    (
        [datetime.datetime(2023, 1, 1, 1, 2, 3), None],
        type,
        "str",
        native_pd.Series(
            [
                "<class 'pandas._libs.tslibs.timestamps.Timestamp'>",
                "<class 'pandas._libs.tslibs.nattype.NaTType'>",
            ]
        ),
    ),
    # data in TIMESTAMP_TZ column will be encoded as pd.Timestamp,
    # and NULL will be encoded as None in vectorized udf
    (
        [
            datetime.datetime(2023, 1, 1, 1, 2, 3, tzinfo=datetime.timezone.utc),
            datetime.datetime(2023, 1, 1, 1, 2, 3),
            None,
        ],
        str,
        "str",
        native_pd.Series(
            ["2023-01-01 01:02:03+00:00", "2023-01-01 01:02:03-08:00", "None"]
        ),
    ),
    (
        [datetime.datetime(2023, 1, 1, 1, 2, 3, tzinfo=datetime.timezone.utc), None],
        type,
        "str",
        native_pd.Series(
            ["<class 'pandas._libs.tslibs.timestamps.Timestamp'>", "<class 'NoneType'>"]
        ),
    ),
]

FUNC_BODY_WITH_TYPE_HINTS_TEMPLATE = """
def f(x) -> {0}:
    return func(x)
"""


def create_func_with_return_type_hint(func: Callable, return_type: str) -> Callable:
    """
    Create a function with return type hint.
    We create a python function using exec on a string template,
    and add type annotation dynamically.
    variable `d` contains the created function with its name as the key
    """
    func_body = FUNC_BODY_WITH_TYPE_HINTS_TEMPLATE.format(return_type)
    d = {}
    exec(func_body, {**globals(), **locals()}, d)
    return d["f"]


TEST_NUMPY_FUNCS = [np.min, np.sqrt, np.tan, np.sum, np.median]


@pytest.mark.parametrize("data,func,return_type", BASIC_DATA_FUNC_RETURN_TYPE_MAP)
@sql_count_checker(query_count=4, udf_count=1)
def test_apply_basic_without_type_hints(data, func, return_type):
    native_series = native_pd.Series(data)
    snow_series = pd.Series(data)
    eval_snowpark_pandas_result(snow_series, native_series, lambda x: x.apply(func))


@pytest.mark.parametrize("data,func,return_type", BASIC_DATA_FUNC_RETURN_TYPE_MAP)
@sql_count_checker(query_count=8, udf_count=2)
def test_apply_and_map_basic_with_type_hints(data, func, return_type):
    func_with_type_hint = create_func_with_return_type_hint(func, return_type)

    native_series = native_pd.Series(data)
    snow_series = pd.Series(data)
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda x: x.apply(func_with_type_hint)
    )
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda x: x.map(func_with_type_hint)
    )


@pytest.mark.parametrize("data,func,return_type", BASIC_DATA_FUNC_RETURN_TYPE_MAP)
@sql_count_checker(query_count=4, udf_count=1)
def test_apply_and_map_type(data, func, return_type):
    native_series = native_pd.Series(data)
    snow_series = pd.Series(data)

    def type_func_with_type_hint(x) -> str:
        return type(x)

    result = snow_series.apply(type_func_with_type_hint).to_pandas()
    # We cannot use the assert_snowpark_pandas_equal_to_pandas helper function here
    # because it would materialize `result` twice
    assert_series_equal(
        result,
        # In the python udf, we can't return a type object (will raise an exception),
        # so the function has to return it as a string here.
        # However, in pandas, it's ok to keep type object in the series.
        # Therefore, I need to convert the type object to string first in native pandas series
        # so their values are equal.
        native_series.apply(type_func_with_type_hint).astype(str),
        check_dtype=False,
        check_index_type=False,
    )
    assert_series_equal(
        result,
        native_series.map(type_func_with_type_hint).astype(str),
        check_dtype=False,
        check_index_type=False,
    )


@pytest.mark.parametrize(
    "data,func,return_type,expected_result",
    DATE_TIME_TIMESTAMP_DATA_FUNC_RETURN_TYPE_MAP,
)
@sql_count_checker(query_count=4, udf_count=1)
def test_apply_date_time_timestamp(data, func, return_type, expected_result):
    func_with_type_hint = create_func_with_return_type_hint(func, return_type)

    snow_series = pd.Series(data)
    result = snow_series.apply(func_with_type_hint)
    assert_snowpark_pandas_equal_to_pandas(result, expected_result)


def test_variant_apply(session):
    data = [
        None,
        1,
        1.1,
        np.nan,
        "s",
        True,
        bytes("snow", "utf-8"),
        datetime.date(2023, 1, 1),
        datetime.time(1, 1, 1),
        datetime.datetime(2023, 1, 1, 1, 2, 3),
        datetime.datetime(2023, 1, 1, 1, 2, 3, tzinfo=datetime.timezone.utc),
        [1, 2, datetime.datetime(2023, 1, 1, 1, 2, 3)],
        {
            "s": 1.2,
            "d": datetime.datetime(2023, 1, 1, 1, 2, 3, tzinfo=datetime.timezone.utc),
        },
    ]
    with SqlCounter(query_count=2):
        snow_df = create_snow_df_with_table_and_data(
            session,
            random_name_for_temp_object(TempObjectType.TABLE),
            [ColumnSchema("col", VariantType())],
            [[e] for e in data],
        )

    expected_types = [
        "<class 'NoneType'>",
        "<class 'int'>",
        "<class 'float'>",
        "<class 'NoneType'>",
        "<class 'str'>",
        "<class 'bool'>",
        "<class 'str'>",
        "<class 'str'>",
        "<class 'str'>",
        "<class 'str'>",
        "<class 'str'>",
        "<class 'list'>",
        "<class 'dict'>",
    ]

    # convert first back and check types
    with SqlCounter(query_count=1):
        assert (
            snow_df["col"].to_pandas().apply(lambda x: str(type(x))).tolist()
            == expected_types
        )

    # then, apply UDF and check results
    with SqlCounter(query_count=4, udf_count=1):
        assert (
            snow_df["col"].apply(lambda x: str(type(x))).to_pandas().tolist()
            == expected_types
        )

    with SqlCounter(query_count=4, udf_count=1):
        assert snow_df["col"].apply(str).to_pandas().tolist() == [
            "None",
            "1",
            "1.1",
            "None",
            "s",
            "True",
            "736e6f77",
            "2023-01-01",
            "01:01:01",
            "2023-01-01T01:02:03",
            "2023-01-01T01:02:03+00:00",
            "[1, 2, '2023-01-01T01:02:03']",
            "{'d': '2023-01-01T01:02:03+00:00', 's': 1.2}",
        ]


def test_apply_null_nan():
    # data becomes 1, 1.1, NaN and NULL in a FLOAT column in Snowflake, which correspond to
    # 1, 1.1, np.nan, np.nan in vectorized UDF and returned as float64 type instead of Float64 type
    # So the last two elements both become missing values
    # So when checking the result, we compare with float64 type
    data = [1, 1.1, "NaN", None]
    snow_series = pd.Series(data, dtype="Float64")
    native_series = native_pd.Series(data, dtype="float64")

    with SqlCounter(query_count=4, udf_count=1):
        eval_snowpark_pandas_result(
            snow_series, native_series, lambda x: x.apply(lambda x: x)
        )

    with SqlCounter(query_count=4, udf_count=1):
        assert snow_series.apply(lambda x: str(type(x))).to_pandas().tolist() == [
            "<class 'float'>",
            "<class 'float'>",
            "<class 'float'>",
            "<class 'float'>",
        ]

    # It will become a number column in Snowflake, and in vectorized UDF, we use Int32/64Dtype
    # where NULL is encoded as pd.NA
    snow_series = pd.Series([None, None])
    with SqlCounter(query_count=4, udf_count=1):
        assert snow_series.apply(lambda x: str(type(x))).to_pandas().tolist() == [
            "<class 'pandas._libs.missing.NAType'>",
            "<class 'pandas._libs.missing.NAType'>",
        ]


@sql_count_checker(query_count=3)
def test_apply_json_serializable_negative():
    snow_series = pd.Series([1])

    # In Python UDF, if the return type is variant, the return value must be
    # json serializable so it can become a variant in Snowflake.
    # type() returns a type object which is not json serializable.
    with pytest.raises(SnowparkSQLException, match="is not JSON serializable"):
        snow_series.apply(type).to_pandas()


def test_apply_args_kwargs():
    def f(x, y, z=1) -> int:
        return x + y + z

    native_series = native_pd.Series([1, 2, 3])
    snow_series = pd.Series([1, 2, 3])

    with SqlCounter(query_count=3):
        eval_snowpark_pandas_result(
            snow_series,
            native_series,
            lambda x: x.apply(f),
            expect_exception=True,
            expect_exception_type=SnowparkSQLException,
            expect_exception_match="missing 1 required positional argument",
            assert_exception_equal=False,
        )

    with SqlCounter(query_count=4, udf_count=1):
        eval_snowpark_pandas_result(
            snow_series, native_series, lambda x: x.apply(f, args=(1,))
        )

    with SqlCounter(query_count=4, udf_count=1):
        eval_snowpark_pandas_result(
            snow_series, native_series, lambda x: x.apply(f, args=(1,), z=2)
        )

    with SqlCounter(query_count=3):
        eval_snowpark_pandas_result(
            snow_series,
            native_series,
            lambda x: x.apply(f, args=(1,), z=2, v=3),
            expect_exception=True,
            expect_exception_type=SnowparkSQLException,
            expect_exception_match="got an unexpected keyword argument",
            assert_exception_equal=False,
        )


@pytest.mark.skipif(running_on_public_ci(), reason="slow fallback test")
@sql_count_checker(
    query_count=20, fallback_count=2, sproc_count=2, expect_high_count=True
)
def test_apply_args_kwargs_with_snowpark_pandas_object_fallback():
    def f(x, y=None) -> int:
        return x + (y.sum() if y is not None else 0)

    native_series = native_pd.Series([1, 2, 3])
    snow_series = pd.Series([1, 2, 3])
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_series.apply(f, args=(pd.Series([1, 2]),)),
        native_series.apply(f, args=(native_pd.Series([1, 2]),)),
    )
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_series.apply(f, y=pd.Series([1, 2])),
        native_series.apply(f, y=native_pd.Series([1, 2])),
    )


@pytest.mark.parametrize("func", [str, int, float, bytes, list, dict])
@sql_count_checker(query_count=8, udf_count=2)
def test_apply_builtin(func):
    if func is list:
        data = [{"a": "b", "c": "d"}, {"b": "a", "d": "c"}]
    elif func is dict:
        data = [[("a", "b"), ("c", "d")], [("b", "a"), ("d", "c")]]
    else:
        data = [1, 2]
    native_series = native_pd.Series(data)
    snow_series = pd.Series(data)
    eval_snowpark_pandas_result(snow_series, native_series, lambda x: x.apply(func))
    eval_snowpark_pandas_result(snow_series, native_series, lambda x: x.map(func))


@pytest.mark.parametrize("func", TEST_NUMPY_FUNCS)
@sql_count_checker(query_count=8, udf_count=2)
def test_apply_and_map_numpy(func):
    data = [1.0, 2.0, 3.0]
    native_series = native_pd.Series(data)
    snow_series = pd.Series(data)
    eval_snowpark_pandas_result(snow_series, native_series, lambda x: x.apply(func))
    eval_snowpark_pandas_result(snow_series, native_series, lambda x: x.map(func))


@pytest.mark.parametrize(
    "native_series, expected_query_count, expected_udf_count",
    [
        (
            native_pd.Series(dtype=object, name="foo", index=pd.Index([], name="bar")),
            10,
            4,
        ),
        (native_pd.Series(index=[1, 2, 3], dtype=np.float64), 8, 2),
    ],
)
@sql_count_checker(query_count=8, udf_count=2)
def test_apply_and_map_empty(native_series, expected_query_count, expected_udf_count):
    def f(x) -> float:
        return x

    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(snow_series, native_series, lambda x: x.apply(f))
    eval_snowpark_pandas_result(snow_series, native_series, lambda x: x.map(f))


@sql_count_checker(query_count=3)
def test_apply_convert_dtype(caplog):
    snow_series = pd.Series([1])

    caplog.clear()
    WarningMessage.printed_warnings.clear()
    with caplog.at_level(logging.WARNING):
        snow_series.apply(lambda x: x, convert_dtype=True)
        assert "convert_dtype is ignored in Snowflake backend" in caplog.text


@pytest.mark.parametrize(
    "func",
    [[np.min], {2: np.min, 1: "max"}]
    # TODO SNOW-864025: enable following after str in df.apply is supported
    # ["min", "mode", "abs"]
)
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_apply_input_type_str_list_dict(func):
    data = [1.0, 2.0, 3.0]
    native_series = native_pd.Series(data)
    snow_series = pd.Series(data)
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda x: x.apply(func), check_index=False
    )


@sql_count_checker(
    query_count=16, fallback_count=2, sproc_count=2, expect_high_count=True
)
def test_map_na_action_ignore():
    snow_series = pd.Series([1, 1.1, "NaN", None], dtype="Float64")

    # In native pandas, the last two elements are NaN and pd.NA
    assert snow_series.map(
        lambda x: x is None, na_action="ignore"
    ).to_pandas().to_list() == [False, False, None, None]

    data = ["cat", "dog", np.nan, "rabbit"]
    snow_series = pd.Series(data)
    native_series = native_pd.Series(data)
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda x: x.map("I am a {}".format, na_action="ignore"),
    )


@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_map_dict():
    s = pd.Series(["cat", "dog", np.nan, "rabbit"])
    assert s.map({"cat": "kitten", "dog": "puppy"}).to_pandas().tolist() == [
        "kitten",
        "puppy",
        None,
        None,
    ]


@sql_count_checker(query_count=8, udf_count=2)
def test_apply_variant_json_null():
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

    # The last element in this numeric column becomes np.nan in Python UDF -> SQL null
    series = pd.Series([1, 2, 3, 4, None])
    assert series.apply(f).isna().tolist() == [False, True, True, False, True]

    # The last element in this string column becomes pd.NA in Python UDF -> SQL null
    series = pd.Series(["s", "t", "null", None])
    assert series.apply(f).isna().tolist() == [False, False, False, True]


# This import is related to the test below. Do not remove.
import scipy  # noqa: E402


@pytest.mark.parametrize(
    "package,expected_query_count",
    [("scipy", 7), ("scipy>=1.0", 7), ("scipy<1.12.0", 7), (scipy, 9)],
)
def test_3rd_party_package_with_udf_annotation(package, expected_query_count):

    with SqlCounter(
        query_count=expected_query_count,
        udf_count=1,
        high_count_expected=True,
        high_count_reason="Snowpark package management uses many queries.",
    ):
        # There are multiple ways on how to call UDFs depending on packages over a Series, test this here
        data = [1, 2, 3, 6, 7, 8]
        snow_series = pd.Series(data)
        native_series = native_pd.Series(data)

        try:
            # this snowpark setting is required for the last use case
            pd.session.custom_package_usage_config["enabled"] = True

            # Note: @udf decorator always requires specifying return_type
            @udf(packages=[package], return_type=DoubleType())
            def func_with_local_import(x):
                from scipy.stats import erlang

                # This API is regarded as stable for the test versions, i.e. no matter the package identifier - the result
                # should be identical.
                var = erlang.pdf(x, 4)
                return var

            snow_ans = snow_series.apply(func_with_local_import)

            # apply UDF without snowpark function decorator
            native_ans = native_series.apply(func_with_local_import.func)

            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                snow_ans, native_ans
            )
        finally:
            pd.session.clear_packages()
            pd.session.clear_imports()


# These imports are related to the test below. Do not remove.
import numpy as np  # noqa: E402
import statsmodels  # noqa: E402


@pytest.mark.parametrize(
    "packages,expected_query_count",
    [
        (["statsmodels", "numpy"], 4),
        (["statsmodels==0.14.0", "numpy>=1.0"], 4),
        ([statsmodels, np], 5),
    ],
)
def test_3rd_party_package_with_session(packages, expected_query_count):
    # Use in this test different package (statsmodels) to isolate test from scipy test above.
    with SqlCounter(query_count=expected_query_count, udf_count=1):
        # There are multiple ways on how to call UDFs depending on packages over a Series, test this here
        data = [1, 2, 3, 6, 7, 8]
        snow_series = pd.Series(data)
        native_series = native_pd.Series(data)

        # import outside of function
        import numpy as np
        import statsmodels.api as sm

        def func(nsample):
            x = np.linspace(0, 10, nsample)
            X = np.column_stack((x, x**2))
            beta = np.array([1, 0.1, 10])
            e = np.random.normal(size=nsample)
            X = sm.add_constant(X)
            y = np.dot(X, beta) + e
            model = sm.OLS(y, X)
            results = model.fit()
            return results.rsquared

        try:
            # this snowpark setting is required for the last use case
            pd.session.custom_package_usage_config["enabled"] = True
            pd.session.add_packages(packages)

            snow_ans = snow_series.apply(func)
        finally:
            pd.session.clear_packages()
            pd.session.clear_imports()

        native_ans = native_series.apply(func)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snow_ans, native_ans, atol=0.1
        )


@pytest.mark.parametrize("udf_packages,session_packages", [(["pandas", np], [scipy])])
@sql_count_checker(query_count=5, join_count=2, udf_count=1)
def test_3rd_party_package_mix_and_match(udf_packages, session_packages):

    snow_series = pd.Series([1])

    def func(x):
        import pandas as pd

        return (pd.__version__, np.__version__, scipy.__version__)

    try:
        # this snowpark setting is required for the last use case
        pd.session.custom_package_usage_config["enabled"] = True
        pd.session.add_packages(session_packages)

        snow_ans = snow_series.apply(func)
        ans = snow_ans.iloc[0]
    finally:
        pd.session.clear_packages()
        pd.session.clear_imports()

    assert len(ans) == 3
