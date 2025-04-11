#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import math
from typing import Callable

import modin.pandas as pd
import numpy as np
from collections import defaultdict
import pandas as native_pd
import pytest
import re
from pandas.testing import assert_series_equal
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import DoubleType, StringType, VariantType
from tests.integ.modin.utils import (
    ColumnSchema,
    assert_snowpark_pandas_equal_to_pandas,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_snow_df_with_table_and_data,
    create_test_series,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import RUNNING_ON_GH

BASIC_DATA_FUNC_RETURN_TYPE_MAP = [
    ([1, 2, 3, None], lambda x: x + 1, "int"),
    param(
        [1, 2, 3, None],
        lambda x: native_pd.Timedelta(x),
        "native_pd.Timedelta",
        id="return_timedelta_scalar",
        marks=pytest.mark.xfail(
            strict=True, raises=AssertionError, reason="SNOW-1619940"
        ),
    ),
    (["s", "n", "o", "w"], lambda x: x * 2, "str"),
    ([1.0, 1.5, 2.0], math.exp, "float"),
    ([True, True, False], int, "int"),
    ([bytes("snow", "utf-8"), bytes("flake", "utf-8")], lambda x: x.decode(), "str"),
    ([1, 2, 3], lambda x: [x, x + 1], "list"),
    ([1, 2, 3], lambda x: {str(x): x * 8}, "dict"),
    ([None, 1], lambda x: bool(np.isnan(x)), "bool"),
    ([np.nan, None, 2.1], lambda x: x, "float"),
    param(
        [native_pd.Timedelta(1), native_pd.Timedelta(2)],
        lambda x: x.value,
        "int",
        id="apply_on_frame_with_timedelta_data_columns_returns_int",
        marks=pytest.mark.xfail(strict=True, raises=NotImplementedError),
    ),
]

# TODO(SNOW-1798212): Test return date/time/timestamp type when
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


TEST_NUMPY_FUNCS = [np.min, np.sqrt, np.tan, np.sum, np.square, np.log1p, np.exp2]


@pytest.mark.parametrize("method", ["apply", "map"])
class TestApplyOrMapCallable:
    """
    Test Series.apply() and Series.map() with a callable mapper.

    Series.apply(f) and Series.map(f) with callable `f` are equivalent.
    Parametrize each method of this class by the method. Test each case with
    both apply and map.
    """

    @pytest.mark.parametrize("data,func,return_type", BASIC_DATA_FUNC_RETURN_TYPE_MAP)
    @sql_count_checker(query_count=4, udf_count=1)
    def test_basic_without_type_hints(self, method, data, func, return_type):
        native_series = native_pd.Series(data)
        snow_series = pd.Series(data)
        eval_snowpark_pandas_result(
            snow_series, native_series, lambda x: getattr(x, method)(func)
        )

    @pytest.mark.parametrize("data,func,return_type", BASIC_DATA_FUNC_RETURN_TYPE_MAP)
    @sql_count_checker(query_count=4, udf_count=1)
    def test_basic_with_type_hints(self, method, data, func, return_type):
        func_with_type_hint = create_func_with_return_type_hint(func, return_type)

        native_series = native_pd.Series(data)
        snow_series = pd.Series(data)
        eval_snowpark_pandas_result(
            snow_series,
            native_series,
            lambda x: getattr(x, method)(func_with_type_hint),
        )

    @pytest.mark.parametrize(
        "data",
        [
            [1, 2, 3, None],
            [1, 2, 3, None],
            ["s", "n", "o", "w"],
            [1.0, 1.5, 2.0],
            [True, True, False],
            [bytes("snow", "utf-8"), bytes("flake", "utf-8")],
            [1, 2, 3],
            [1, 2, 3],
            [None, 1],
            [np.nan, None, 2.1],
            param(
                [native_pd.Timedelta(1), native_pd.Timedelta(2)],
                id="input_timedleta_columns",
                marks=pytest.mark.xfail(strict=True, raises=NotImplementedError),
            ),
        ],
    )
    @sql_count_checker(query_count=4, udf_count=1)
    def test_input_has_correct_type(self, method, data):
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
            getattr(native_series, method)(type_func_with_type_hint).astype(str),
            check_dtype=False,
            check_index_type=False,
        )

    @pytest.mark.parametrize(
        "data,func,return_type,expected_result",
        DATE_TIME_TIMESTAMP_DATA_FUNC_RETURN_TYPE_MAP,
    )
    @sql_count_checker(query_count=4, udf_count=1)
    def test_date_time_timestamp(
        self, method, data, func, return_type, expected_result
    ):
        func_with_type_hint = create_func_with_return_type_hint(func, return_type)

        snow_series = pd.Series(data)
        result = getattr(snow_series, method)(func_with_type_hint)
        assert_snowpark_pandas_equal_to_pandas(result, expected_result)

    @pytest.mark.xfail(strict=True, raises=NotImplementedError)
    @sql_count_checker(query_count=0)
    def test_input_series_with_timedelta_index(self, method):
        eval_snowpark_pandas_result(
            *create_test_series(
                native_pd.Series([0], index=[native_pd.Timedelta(1)]),
            ),
            lambda series: getattr(series, method)(lambda x: x),
        )

    def test_variant_input(self, method, session):
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
                "d": datetime.datetime(
                    2023, 1, 1, 1, 2, 3, tzinfo=datetime.timezone.utc
                ),
            },
        ]
        with SqlCounter(query_count=3):
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
                getattr(snow_df["col"].to_pandas(), method)(
                    lambda x: str(type(x))
                ).tolist()
                == expected_types
            )

        # then, apply UDF and check results
        with SqlCounter(query_count=4, udf_count=1):
            assert (
                getattr(snow_df["col"], method)(lambda x: str(type(x)))
                .to_pandas()
                .tolist()
                == expected_types
            )

        with SqlCounter(query_count=4, udf_count=1):
            assert getattr(snow_df["col"], method)(str).to_pandas().tolist() == [
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

    def test_null_nan_input(self, method):
        # data becomes 1, 1.1, NaN and NULL in a FLOAT column in Snowflake, which correspond to
        # 1, 1.1, np.nan, np.nan in vectorized UDF and returned as float64 type instead of Float64 type
        # So the last two elements both become missing values
        # So when checking the result, we compare with float64 type
        data = [1, 1.1, "NaN", None]
        snow_series = pd.Series(data, dtype="Float64")
        native_series = native_pd.Series(data, dtype="float64")

        with SqlCounter(query_count=4, udf_count=1):
            eval_snowpark_pandas_result(
                snow_series, native_series, lambda x: getattr(x, method)(lambda x: x)
            )

        with SqlCounter(query_count=4, udf_count=1):
            assert getattr(snow_series, method)(
                lambda x: str(type(x))
            ).to_pandas().tolist() == [
                "<class 'float'>",
                "<class 'float'>",
                "<class 'float'>",
                "<class 'float'>",
            ]

        # It will become a number column in Snowflake, and in vectorized UDF, we use Int32/64Dtype
        # where NULL is encoded as pd.NA
        snow_series = pd.Series([None, None])
        with SqlCounter(query_count=4, udf_count=1):
            assert getattr(snow_series, method)(
                lambda x: str(type(x))
            ).to_pandas().tolist() == [
                "<class 'pandas._libs.missing.NAType'>",
                "<class 'pandas._libs.missing.NAType'>",
            ]

    @sql_count_checker(query_count=3)
    def test_json_serializable_output_negative(self, method):
        snow_series = pd.Series([1])

        # In Python UDF, if the return type is variant, the return value must be
        # json serializable so it can become a variant in Snowflake.
        # type() returns a type object which is not json serializable.
        with pytest.raises(SnowparkSQLException, match="is not serializable"):
            getattr(snow_series, method)(type).to_pandas()

    @pytest.mark.parametrize("func", [str, int, float, bytes, list, dict])
    @sql_count_checker(query_count=4, udf_count=1)
    def test_builtin_function(self, method, func):
        if func is list:
            data = [{"a": "b", "c": "d"}, {"b": "a", "d": "c"}]
        elif func is dict:
            data = [[("a", "b"), ("c", "d")], [("b", "a"), ("d", "c")]]
        else:
            data = [1, 2]
        native_series = native_pd.Series(data)
        snow_series = pd.Series(data)
        eval_snowpark_pandas_result(
            snow_series, native_series, lambda x: getattr(x, method)(func)
        )

    @pytest.mark.parametrize("func", TEST_NUMPY_FUNCS)
    @sql_count_checker(query_count=1)
    def test_apply_and_map_numpy(self, method, func):
        data = [1.0, 2.0, 3.0]
        native_series = native_pd.Series(data)
        snow_series = pd.Series(data)
        eval_snowpark_pandas_result(
            snow_series, native_series, lambda x: getattr(x, method)(func)
        )

    @pytest.mark.parametrize(
        "native_series",
        [
            native_pd.Series(
                dtype=object, name="foo", index=native_pd.Index([], name="bar")
            ),
            native_pd.Series(index=[1, 2, 3], dtype=np.float64),
        ],
    )
    @sql_count_checker(query_count=4, udf_count=1)
    def test_empty_input(self, method, native_series):
        def f(x) -> float:
            return x

        snow_series = pd.Series(native_series)
        eval_snowpark_pandas_result(
            snow_series, native_series, lambda x: getattr(x, method)(f)
        )

    @pytest.mark.parametrize(
        "input",
        [
            # The last element in this numeric column becomes np.nan in Python UDF -> SQL null
            [1, 2, 3, 4, None],
            # The last element in this string column becomes pd.NA in Python UDF -> SQL null
            ["s", "t", "null", None],
        ],
    )
    @sql_count_checker(query_count=4, udf_count=1)
    def test_variant_json_null(self, method, input):
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

        snow_series = pd.Series(input)
        native_series = native_pd.Series(input)
        eval_snowpark_pandas_result(
            snow_series, native_series, lambda x: getattr(x, method)(f).isna()
        )

    # This import is related to the test below. Do not remove.
    import scipy  # noqa: E402

    @pytest.mark.skipif(RUNNING_ON_GH, reason="Slow test")
    @pytest.mark.parametrize(
        "package,expected_query_count",
        [
            ("scipy", 7),
            ("scipy>=1.0", 7),
            ("scipy<1.12.0", 7),
            # TODO: SNOW-1478188 Re-enable quarantined tests for 8.23
            # (scipy, 9)
        ],
    )
    def test_3rd_party_package_with_udf_annotation(
        self, method, package, expected_query_count
    ):

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

                snow_ans = getattr(snow_series, method)(func_with_local_import)

                # apply UDF without snowpark function decorator
                native_ans = getattr(native_series, method)(func_with_local_import.func)

                assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                    snow_ans, native_ans
                )
            finally:
                pd.session.clear_packages()
                pd.session.clear_imports()

    # These imports are related to the test below. Do not remove.
    import numpy as np  # noqa: E402
    import statsmodels  # noqa: E402

    @pytest.mark.xfail(reason="SNOW-1478794 investigating the issue now")
    @pytest.mark.parametrize(
        "packages,expected_query_count",
        [
            (["statsmodels", "numpy"], 4),
            (["statsmodels==0.14.0", "numpy>=1.0"], 4),
            ([statsmodels, np], 5),
        ],
    )
    def test_3rd_party_package_with_session(
        self, method, packages, expected_query_count
    ):
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

                snow_ans = getattr(snow_series, method)(func)
            finally:
                pd.session.clear_packages()
                pd.session.clear_imports()

            native_ans = getattr(native_series, method)(func)
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                snow_ans, native_ans, atol=0.1
            )

    @pytest.mark.xfail(reason="TODO: SNOW-1478188 Re-enable quarantined tests for 8.23")
    @pytest.mark.parametrize(
        "udf_packages,session_packages", [(["pandas", np], [scipy])]
    )
    @sql_count_checker(query_count=5, join_count=2, udf_count=1)
    def test_3rd_party_package_mix_and_match(
        self, method, udf_packages, session_packages
    ):

        snow_series = pd.Series([1])

        def func(x):
            import pandas as pd

            return (pd.__version__, np.__version__, scipy.__version__)  # noqa: F821

        try:
            # this snowpark setting is required for the last use case
            pd.session.custom_package_usage_config["enabled"] = True
            pd.session.add_packages(session_packages)

            snow_ans = getattr(snow_series, method)(func)
            ans = snow_ans.iloc[0]
        finally:
            pd.session.clear_packages()
            pd.session.clear_imports()

        assert len(ans) == 3

    @sql_count_checker(query_count=7, udf_count=1)
    def test_SNOW_1344784_udf_decorator(self, method):
        # tests udf decorator with no packages specified

        df = pd.DataFrame(
            [[True, "test string"], [False, "another"], [True, "This is an emoji: 😀"]],
            columns=["is_valid_unitno", "column"],
        )

        # note that we don't specify any packages in the udf() decorator.
        @udf(input_types=[StringType()], return_type=StringType())
        def _remove_emoji(input_string):
            """This function takes a string and removes non-ascii characters like emojis"""
            return input_string.encode("ascii", "ignore").decode("ascii").strip()

        ans = getattr(df.loc[df["is_valid_unitno"], "column"], method)(_remove_emoji)
        expected = native_pd.Series(
            ["test string", "This is an emoji:"], index=[0, 2], name="column"
        )
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(ans, expected)


class TestApplyOnly:
    """
    This class is for cases that apply to Series.apply, but not to Series.map.

    For example, cases that test the behavior of the `convert_dtype` parameter
    belong here, because Series.apply takes that parameter, but Series.map does
    not.
    """

    @pytest.mark.skip(
        "SNOW-1896426 Test run into high failing rate, turn back on once fixed"
    )
    def test_args_and_kwargs(self):
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

    @sql_count_checker(query_count=0)
    def test_args_and_kwargs_with_snowpark_pandas_object_not_implemented(self):
        def f(x, y=None) -> int:
            return x + (y.sum() if y is not None else 0)

        snow_series = pd.Series([1, 2, 3])
        msg = "Snowpark pandas apply API doesn't yet support DataFrame or Series in 'args' or 'kwargs' of 'func'"
        with pytest.raises(NotImplementedError, match=msg):
            snow_series.apply(f, args=(pd.Series([1, 2]),))
        with pytest.raises(NotImplementedError, match=msg):
            snow_series.apply(f, y=pd.Series([1, 2]))

    @pytest.mark.parametrize(
        "func",
        [[np.min], {2: np.min, 1: "max"}]
        # TODO SNOW-864025: enable following after str in df.apply is supported
        # ["min", "mode", "abs"]
    )
    @sql_count_checker(query_count=0)
    def test_input_type_str_list_dict(self, func):
        snow_series = pd.Series([1.0, 2.0, 3.0])
        msg = "Snowpark pandas apply API only supports callables func"
        with pytest.raises(NotImplementedError, match=msg):
            snow_series.apply(func)


class TestMapOnly:
    """
    This class is for cases that apply to Series.map, but not to Series.apply.

    For example, cases that test the behavior of the `na_action` parameter
    belong here, because Series.map takes that parameter, but Series.apply does
    not.
    """

    @sql_count_checker(query_count=0)
    def test_na_action_ignore_not_implemented(self):
        snow_series = pd.Series([1, 1.1, "NaN", None], dtype="Float64")

        msg = "Snowpark pandas map API doesn't yet support na_action == 'ignore'"
        with pytest.raises(NotImplementedError, match=msg):
            snow_series.map(lambda x: x is None, na_action="ignore")

        snow_series = pd.Series(["cat", "dog", np.nan, "rabbit"])
        with pytest.raises(NotImplementedError, match=msg):
            snow_series.map("I am a {}".format, na_action="ignore")

    class CustomDefaultDict(dict):
        def __missing__(self, key):
            return key

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize(
        "arg",
        [
            param(
                {"cat": "kitten", "dog": "puppy"}, id="dict_mapping_string_to_string"
            ),
            param(
                {"cat": "kitten", "dog": None},
                id="dict_mapping_string_to_string_or_null",
            ),
            param(
                {"cat": "kitten", "dog": 0}, id="dict_mapping_string_to_string_and_int"
            ),
            param({"cat": 0, "dog": 1}, id="dict_mapping_some_strings_to_int"),
            param(
                {"cat": 0, "dog": 1, None: 2, "rabbit": 3},
                id="dict_mapping_every_value_to_int",
            ),
            param(
                {"cat": "kitten", pd.Timestamp(1): "timestamp_1"},
                id="dict_mapping_string_to_string_and_timestamp_to_string",
            ),
            param({None: "cub"}, id="dict_mapping_null_to_string"),
            param({}, id="empty_dict"),
            param(
                defaultdict((lambda: "kitten"), dog="puppy"),
                id="defaultdict_mapping_string_to_string",
            ),
            param(
                defaultdict((lambda: 0), dog="puppy"),
                id="defaultdict_mapping_string_to_string_or_int",
            ),
            param(defaultdict(lambda: "kitten"), id="defaultdict_empty"),
            param(
                native_pd.Series({"cat": "kitten"}),
                id="series_mapping_string_to_string",
            ),
            param(native_pd.Series(), id="empty_series"),
            param(
                CustomDefaultDict({"cat": "kitten"}),
                id="custom_defaultdict_mapping_string_to_string",
                marks=pytest.mark.xfail(
                    strict=True, raises=NotImplementedError, reason="SNOW-1804017"
                ),
            ),
            param(
                CustomDefaultDict(),
                id="custom_defaultdict_empty",
                marks=pytest.mark.xfail(
                    strict=True, raises=NotImplementedError, reason="SNOW-1804017"
                ),
            ),
        ],
    )
    def test_dict(self, arg):
        eval_snowpark_pandas_result(
            *create_test_series(["cat", "dog", None, "rabbit"]),
            lambda s: s.map(arg),
        )

    @sql_count_checker(query_count=4, udf_count=1)
    def test_defaultdict_has_all_values_but_no_default_factory(self):
        eval_snowpark_pandas_result(
            *create_test_series(1),
            lambda s: s.map(defaultdict(None, {1: 2})),
        )

    @sql_count_checker(query_count=3)
    def test_defaultdict_missing_values_but_no_default_factory(self):
        eval_snowpark_pandas_result(
            *create_test_series(1),
            lambda s: s.map(defaultdict()),
            expect_exception=True,
            expect_exception_match=re.escape("KeyError: 1"),
            assert_exception_equal=False,
            # Snowflake raises the KeyError from a UDF, so we get
            # SnowparkSQLException instead of a KeyError.
            expect_exception_type=SnowparkSQLException,
        )

    @sql_count_checker(query_count=0)
    def test_invalid_arg_type(self):
        eval_snowpark_pandas_result(
            *create_test_series(1),
            lambda s: s.map(3),
            expect_exception=True,
            expect_exception_match=re.escape(
                "`arg` should be a callable, a Mapping, or a pandas Series, "
                + "but instead it is of type int"
            ),
            assert_exception_equal=False,
            expect_exception_type=TypeError,
        )

    @sql_count_checker(query_count=3)
    def test_incorrect_inferred_type(self):
        s = pd.Series([1, 2, 17])
        # The return type of the lambda is inferred as int, but the return type is
        # mix of int and string.
        # Attempt to convert "abc" to int will raise an exception.
        with pytest.raises(SnowparkSQLException):
            s.map(lambda x: "abc" if x == 17 else x).to_pandas()


# NOTE: Please add test cases to one of TestApplyOrMapCallable, TestApplyOnly,
# or TestMapOnly, instead of adding separate test functions here.
