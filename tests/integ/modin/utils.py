#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import datetime
import json
from collections import namedtuple
from math import isnan
from typing import Any, Callable, Optional, Union

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pandas.testing as tm
import pytest
from modin.pandas import DataFrame, Index, Series
from pandas import isna
from pandas._typing import Scalar
from pandas.core.dtypes.common import is_list_like
from pandas.core.dtypes.inference import is_scalar

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.dataframe import DataFrame as SnowparkDataFrame
from snowflake.snowpark.modin.plugin.extensions.utils import try_convert_index_to_native
from snowflake.snowpark.modin.utils import SupportsPublicToPandas
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import StructField, StructType

ValuesEqualType = Optional[
    Union[
        Scalar,
        native_pd.DataFrame,
        native_pd.Series,
        native_pd.Index,
        list,
        set,
        np.array,
    ]
]

BASIC_TYPE_DATA1 = [
    1,
    "one",
    1.0,
    # TODO SNOW-667350: support datetime
    # datetime.datetime.strptime("2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f"),
    datetime.datetime.strptime("20:57:06", "%H:%M:%S").time(),
    datetime.datetime.strptime("2017-02-25", "%Y-%m-%d").date(),
    True,
    bytearray("a", "utf-8"),
    # TODO: SNOW-800907 fix a bug in Python connector's to_pandas() method which converts number with scale > 0 to
    # float64
    # Decimal(0.5),
]

BASIC_TYPE_DATA2 = [
    0,
    "",
    0.0,
    # datetime.datetime.now(),
    datetime.datetime.now().time(),
    datetime.date.today(),
    False,
    bytes("snowflake", "utf-8"),
    # Decimal(0),
]

SEMI_STRUCTURED_TYPE_DATA = [
    ["'", 2],
    [[1, 2], [2, 1]],
    {"'": 1},
    {"snow": {"fla": "ke"}},
]

BASIC_NUMPY_PANDAS_SCALAR_DATA = [
    np.int64(1),
    np.float64(-1.1),
    np.bool_(False),
    native_pd.Timestamp(datetime.datetime(2021, 1, 1)),
]

VALID_PANDAS_LABELS = [
    5,
    5.0,
    ("5", 5),
    " c o l ",
    '"col',
    '"c""ol',
    "'col",
    '"col"',
    '"COL"',
    "'co''l",
    "COL",
    "snowflake".encode("utf-16"),
    "チリヌル",
    "ป็นมนุ",
    "熊猫",
    json.dumps({"snow": {"fla": "ke"}}),
]

VALID_SNOWFLAKE_COLUMN_NAMES = [
    "col",
    "COL",
    "Col",
    '"C O L"',
    "__col__",
    '"co\'l"',
    '"col"',
    '"c""ol"',
]

VALID_SNOWFLAKE_COLUMN_NAMES_AND_ALIASES = [
    ("col", "col_alias"),
    ("COL", "COL_alias"),
    ("Col", "Col_alias"),
    ('"C O L"', '"C O L_alias"'),
    ("__col__", "__col___alias"),
    ('"co\'l"', '"co\'l_alias"'),
    ('"col"', '"col_alias"'),
    ('"c""ol"', '"c""ol_alias"'),
]

random_state = np.random.RandomState(seed=42)
# Size of test dataframes
NCOLS, NROWS = (2**3, 2**5)

# Range for values for test data
RAND_LOW = 0
RAND_HIGH = 100
TEST_DF_DATA = {
    "float_nan_data": {
        f"col{int((i - NCOLS / 2) % NCOLS + 1)}": [
            x if (j != i and j - 2 != i and j + 2 != i) else np.nan
            for j, x in enumerate(
                random_state.uniform(RAND_LOW, RAND_HIGH, size=(NROWS))
            )
        ]
        for i in range(NCOLS)
    },
}


TIMESTAMP_DATA_AND_TYPE = (
    [
        native_pd.Timestamp("2013-01-01"),
        native_pd.Timestamp("2013-02-01"),
        native_pd.Timestamp("2013-02-01"),
        native_pd.Timestamp("2013-01-01"),
    ],
    "Timestamp",
)
ARRAY_DATA_AND_TYPE = ([[1], [2], [10], [20]], "Array")
STRING_DATA_AND_TYPE = (["bb", "aa", "ca", "da"], "String")
MAP_DATA_AND_TYPE = (
    [
        {"a": 1, "b": 2},
        {"a": 1, "b": 2},
        {"a": 1, "b": 2},
        {"a": 1, "b": 2},
    ],
    "Object",
)
MIXED_NUMERIC_STR_DATA_AND_TYPE = ([1, "A", 2.5, None], "Variant")
MAX_DICTIONARY_FORMAT_STRING_SIZE = 2000


ColumnSchema = namedtuple("ColumnSchema", ["name", "snowpark_type"])


def create_test_dfs(*args, **kwargs) -> tuple[pd.DataFrame, native_pd.DataFrame]:
    """
    Create a snowpark pandas and native pandas dataframe with the given arguments.

    Args:
        args: Positional arguments for the dataframe constructor.
        kwargs: Keyword arguments for the dataframe constructor.

    Returns:
        A tuple where the first element is a snowpark pandas dataframe created
        by forwarding the arguments to the snowpark dataframe constructor, and
        the second element is a native pandas dataframe created by forwarding
        the arguments to the pandas dataframe constructor.
    """
    native_kw_args = kwargs.copy()
    if (
        "index" in native_kw_args
        and isinstance(native_kw_args["index"], native_pd.Index)
        and not isinstance(native_kw_args["index"], pd.MultiIndex)
    ):
        kwargs["index"] = pd.Index(native_kw_args["index"])
    if (
        "columns" in native_kw_args
        and isinstance(native_kw_args["columns"], native_pd.Index)
        and not isinstance(native_kw_args["columns"], pd.MultiIndex)
    ):
        kwargs["columns"] = native_pd.Index(native_kw_args["columns"])
    return (pd.DataFrame(*args, **kwargs), native_pd.DataFrame(*args, **native_kw_args))


def create_test_series(*args, **kwargs) -> tuple[pd.Series, native_pd.Series]:
    """
    Create a snowpark pandas and native pandas series with the given arguments.

    Args:
        args: Positional arguments for the series constructor.
        kwargs: Keyword arguments for the series constructor.

    Returns:
        A tuple where the first element is a snowpark pandas series created
        by forwarding the arguments to the snowpark series constructor, and
        the second element is a native pandas series created by forwarding
        the arguments to the pandas series constructor.
    """
    return (pd.Series(*args, **kwargs), native_pd.Series(*args, **kwargs))


def try_to_load_json_string(value: Any) -> Any:
    """
    Tries to deserialize a json string to a Python value.
    If not working, returns the original value.
    """
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


def assert_snowpark_pandas_equal_to_pandas(
    snow: DataFrame | Series,
    expected_pandas: native_pd.DataFrame | native_pd.Series,
    *,
    statement_params: dict[str, str] | None = None,
    expected_index_type: str = None,
    expected_dtypes: list[str] = None,
    **kwargs: Any,
) -> None:
    """
    Check a snowpark pandas dataframe/series is equal to a pandas dataframe/series by converting snowpark pandas dataframe/series
    to pandas dataframe/series first using to_pandas(), and then assert them are the same.
    Args:
        snow: snowpark pandas dataframe or series
        expected_pandas: native pandas dataframe or series
        statement_params: Dictionary of statement level parameters to be set while executing this action.
        expected_index_type: if not None then check snowpark pandas dataframe's index type is expected type
        expected_dtypes: if not None then check snowpark pandas dataframe's column data types
        **kwargs: other kwargs pass to assert_frame_equal

    Raises:
        AssertionError if the converted dataframe does not match with the original one
    """
    assert isinstance(snow, (DataFrame, Series, Index)), f"Got type: {type(snow)}"
    assert isinstance(
        expected_pandas, (native_pd.DataFrame, native_pd.Series, native_pd.Index)
    ), f"Got type: {type(expected_pandas)}"
    # Due to server-side compression, only check that index values are equivalent and ignore the
    # index types. Snowpark pandas will use the smallest possible dtype (typically int8), while
    # native pandas will default to int64.
    kwargs.update(check_index_type=False)
    if expected_dtypes is not None:
        kwargs.update(check_dtype=False)

    snow_to_native = snow.to_pandas(statement_params=statement_params)

    if isinstance(expected_pandas, native_pd.DataFrame):
        assert isinstance(snow, DataFrame)
        snow_to_native = snow_to_native.replace({None: pd.NA})
        tm.assert_frame_equal(snow_to_native, expected_pandas, **kwargs)
    elif isinstance(snow, Series):
        snow_to_native = snow_to_native.replace({None: pd.NA})
        tm.assert_series_equal(snow_to_native, expected_pandas, **kwargs)
    else:
        assert isinstance(snow, Index)
        if "check_dtype" in kwargs:
            kwargs.pop("check_dtype")
        if kwargs.pop("check_index_type"):
            kwargs.update(exact=False)
        tm.assert_index_equal(snow_to_native, expected_pandas, **kwargs)
    if expected_index_type is not None:
        assert (
            expected_index_type == snow_to_native.index.dtype.name
        ), f"Expected {expected_index_type} saw {snow_to_native.index.dtype.name}"
    if expected_dtypes is not None:
        if isinstance(snow_to_native, native_pd.Series):
            assert [str(snow_to_native.dtype)] == expected_dtypes
        else:
            assert [str(dt) for dt in snow_to_native.dtypes.tolist()] == expected_dtypes


def assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
    snow: DataFrame | Series | Index,
    native: native_pd.DataFrame | native_pd.Series | native_pd.Index,
    **kwargs,
) -> None:
    """
    Check a snowpark pandas dataframe/series is equal to a pandas dataframe/series without check dtype.
    """
    assert_snowpark_pandas_equal_to_pandas(snow, native, check_dtype=False, **kwargs)


def assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64(
    snow: DataFrame | Series | Index,
    native: native_pd.DataFrame | native_pd.Series | native_pd.Index,
    **kwargs,
) -> None:
    """
    Check a snowpark pandas dataframe/series is equal to a pandas dataframe/series assuming data is all float64.
    """
    # Due to server-side compression, only check that index values are equivalent and ignore the
    # index types. Snowpark pandas will use the smallest possible dtype (typically int8), while
    # native pandas will default to int64.
    kwargs.update(check_index_type=False)

    snow_to_native = snow.to_pandas()

    # Find all numeric (int, float) columns and create a mapping of col name to float64.  We also coerce int also
    # because sometimes there are failures due to int8 vs int64 etc differences, for this reason we also cast native
    # pandas the same way.
    #
    # Also, in some cases such as if a column has all None values, snowflake does not know the
    # intended column type and will default to 'object' even while pandas may infer a 'float64' data type.  Since we're
    # concerned with validating *values* here and not dtypes, we coerce to the snowpark pandas type in other cases.

    if isinstance(snow, DataFrame):
        coerce_col_types = {
            col_dt[0]: "float64"
            if col_dt[1] == "float64" or col_dt[1] == "int64"
            else col_dt[1]
            for col_dt in zip(snow.columns, snow.dtypes)
        }

        assert_frame_equal(
            snow_to_native.astype(coerce_col_types),
            native.astype(coerce_col_types),
            rtol=1.0e-5,
            **kwargs,
        )
    elif isinstance(snow, Series):
        assert_series_equal(
            snow_to_native.astype("float64"),
            native.astype("float64"),
            rtol=1.0e-5,
            **kwargs,
        )
    else:
        assert_index_equal(snow_to_native, native, **kwargs)


def assert_series_equal(*args, **kwargs) -> None:
    use_lhs = (
        args[0].to_pandas() if isinstance(args[0], SupportsPublicToPandas) else args[0]
    )
    use_rhs = (
        args[1].to_pandas() if isinstance(args[1], SupportsPublicToPandas) else args[1]
    )
    tm.assert_series_equal(use_lhs, use_rhs, *args[2:], **kwargs)


def assert_frame_equal(*args, **kwargs) -> None:
    use_lhs = (
        args[0].to_pandas() if isinstance(args[0], SupportsPublicToPandas) else args[0]
    )
    use_rhs = (
        args[1].to_pandas() if isinstance(args[1], SupportsPublicToPandas) else args[1]
    )
    tm.assert_frame_equal(use_lhs, use_rhs, *args[2:], **kwargs)


def eval_snowpark_pandas_result(
    snow_pandas: Any,
    native_pandas: Any,
    operation: Callable,
    *,
    # by default, we use the comparator without typecheck. Snowpark pandas does not guarantee
    # the exact type matching of pandas due to the following reason:
    # 1) Snowpark pandas backend is snowflake sql engine, and uses the snowflake type system.
    #    The actual type for a column stored or after computation is decided by snowflake, which
    #    can be different as pandas, for example, groupby().mean() on a column with int64, ends
    #    with a column with decimal(128), but pandas ends up with float64.
    # 2) Snowpark pandas to_pandas() maps the snowflake type to pandas type with a Snowpark defined type
    #    mapping, which further introduces potential consistency.
    # For general snowpark pandas api evaluation, we want to focus on the evaluation of the result
    # shape and values, the type mapping will be tested separately (SNOW-841273).
    comparator: Callable = assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    test_attrs: bool = True,
    inplace: bool = False,
    expect_exception: bool = False,
    expect_exception_type: type[Exception] | None = None,
    expect_exception_match: str | None = None,
    assert_exception_equal: bool = True,
    **kwargs: Any,
) -> None:
    """
    evaluates the error or result of the Snowpark pandas object and pandas object after apply the given operation.

    Args:
        snow_pandas: a Snowpark pandas object to apply the operation on
        native_pandas: a native pandas object to apply the operation on
        operation: Callable. The operation to be applied on the Snowpark pandas and pandas object
        comparator: Callable. Function used to perform the comparison, which must be in format of
                                comparator(snowpark_pandas_res, pandas_res, **key_words)
        test_attrs: bool. If True and the operation returns a DF/Series, sets `attrs` on the input
            to a sentinel value and ensures the output DF/Series has the same `attrs`.
        inplace: bool. Whether the operation is an inplace operation or not
        expect_exception: tuple of an Exception type. do we expect an exception during the operation
        expect_exception_type: if not None, assert the exception type is expected
        expect_exception_match: if not None, assert the exception match the expected regex
        assert_exception_equal: bool. Whether to assert the exception from Snowpark pandas eqauls to pandas

    Raises:
        Exception if 1) if exception is raised during operation but no exception is expected
                     2) if exception is expected, and during check exception, the exception type or message does't match
                     3) if exception is expected, but no exception is raised
                     4) if no exception is expected, and results are series or dataframe, but doesn't match
        NotImplementedError if the result of the operation is neither a series or dataframe
    """
    if expect_exception:
        with pytest.raises(Exception) as pd_e:
            operation(native_pandas)
        with pytest.raises(Exception) as snow_e:
            result = operation(snow_pandas)
            # some execution time errors need to be triggered by to_pandas()
            if inplace:
                # If the operation affected the snow_pandas object in place,
                # we have to call to_pandas() on snow_pandas.
                snow_pandas.to_pandas()
            elif isinstance(result, (DataFrame, Series, Index)):
                # otherwise, we have to call to_pandas() on the result.
                result.to_pandas()
        if expect_exception_type:
            assert (
                snow_e.type == expect_exception_type
            ), f"exception type {snow_e.type} does not match with expected type {expect_exception_type}"
        if expect_exception_match:
            assert snow_e.match(expect_exception_match)
        if assert_exception_equal:
            # check type
            assert isinstance(
                snow_e.value, type(pd_e.value)
            ), f"Got Snowpark pandas Exception type {type(snow_e.value)}, but pandas Exception type {type(pd_e.value)} was expected.\n Snowpark pandas exception: {str(snow_e)}\npandas exception: {str(pd_e)}"
            # check string message
            pandas_err_msg = str(pd_e.value)
            snow_err_msg = str(snow_e.value)
            # in pandas 2, snowpark to_pandas can create a different dtype than pandas does by default
            # in errors that print values that include the dtype, we should avoid failing
            # on cases where e.g. int8 != int64, despite the remaining values being correct.
            # We check up to the "dtype" argument in the message to solve this.
            assert pandas_err_msg == snow_err_msg or (
                "dtype" in pandas_err_msg
                and "dtype" in snow_err_msg
                and pandas_err_msg[: pandas_err_msg.index("dtype")]
                == snow_err_msg[: snow_err_msg.index("dtype")]
            ), f"Snowpark pandas Exception {snow_e.value} doesn't match pandas Exception {pd_e.value}"
    else:
        test_attrs_dict = {"key": "attrs propagation test"}
        if test_attrs and isinstance(snow_pandas, (Series, DataFrame)):
            native_pandas.attrs = test_attrs_dict
            snow_pandas.attrs = test_attrs_dict
        pd_result = operation(native_pandas)
        snow_result = operation(snow_pandas)
        if inplace:
            pd_result = native_pandas
            snow_result = snow_pandas
        if (
            test_attrs
            and isinstance(snow_pandas, (Series, DataFrame))
            and isinstance(snow_result, (Series, DataFrame))
        ):
            # Check that attrs was properly propagated.
            # Note that attrs may be empty--all that matters is that snow_result and pd_result agree.
            assert (
                snow_result.attrs == pd_result.attrs
            ), f"Snowpark pandas attrs {snow_result.attrs} doesn't match pandas attrs {pd_result.attrs}"
        comparator(snow_result, pd_result, **(kwargs or {}))


def _is_python_nan(v: Any) -> bool:
    """
    Tell whether the value is equal to the builtin python float nan.

    NOTE that this is true for both np.nan and float('nan'). That behavior
    matches the behavior of np.isnan()

    Arguments:
        v: The value to check

    Returns:
        bool
    """
    # calling isnan() on some types raises TypeError
    try:
        return isnan(v)
    except TypeError:
        return False


def assert_values_equal(
    actual: Any, expected: ValuesEqualType, *, check_index_type: bool = True
) -> None:
    """
    Tell whether two values are equal.

    Arguments:
        actual: The actual value
        expected: The expected value
        check_index_type: If the values are instances of native_pd.Index, whether to compare their class and dtypes

    Returns:
        bool telling whether the values are equal.
    """
    expected = try_convert_index_to_native(expected)
    actual = try_convert_index_to_native(actual)

    if isinstance(expected, native_pd.DataFrame):
        assert isinstance(
            actual, native_pd.DataFrame
        ), f"Expected object of type {native_pd.DataFrame} but instead got object {actual} of type {type(actual)}"
        tm.assert_frame_equal(actual, expected)
    elif isinstance(expected, native_pd.Series):
        assert isinstance(
            actual, native_pd.Series
        ), f"Expected object of type {native_pd.Series} but instead got object {actual} of type {type(actual)}"
        tm.assert_series_equal(actual, expected)
    elif isinstance(expected, native_pd.Index):
        assert_index_equal(actual, expected, exact=check_index_type and "equiv")
    elif is_list_like(expected):
        assert np.shape(expected) == np.shape(actual)
        # check that we have the same type on each side, so that e.g.
        # (1, 2) and [1, 2] are not equal. Also, if one of the arguments to
        # assert_array_equal() is a scalar, then assert_array_equal() checks
        # whether all the arguments in the other are equal to that scalar, so
        # e.g. assert_array_equal(1, [1]) is valid. This type check takes care
        # of that case without using assert_array_equal(strict=True), which would
        # also check dtypes, which we don't want to check.
        assert type(expected) == type(actual)
        np.testing.assert_array_equal(actual, expected, strict=False)
    elif expected is None:
        assert actual is None
    elif _is_python_nan(expected):
        assert _is_python_nan(actual)
    elif expected is pd.NaT:
        assert actual is pd.NaT
    elif expected is pd.NA:
        assert actual is pd.NA
    else:
        # finally, fall back to comparison with ==.
        # bool(np.array([1]) == 1) is True, so check that the shapes of
        # `actual` and `expected` are the same.
        assert np.shape(actual) == np.shape(expected)
        assert (
            actual == expected
        ), f"actual != expected.\nActual: {actual}\nExpected: {expected}"


def _short_dict_format_string(dictionary: dict) -> str:
    """
    Get a format string for the dictionary that limits the representation to MAX_DICTIONARY_FORMAT_STRING_SIZE.

    Arguments:
        dictionary: The dictionary

    Returns:
        The format string for the dictionary, possibly truncated to
        MAX_DICTIONARY_FORMAT_STRING_SIZE. If truncated, the
        message includes a string say that the dictionary's
        representation was truncated.
    """
    formatted = format(dictionary)
    return (
        (formatted[:MAX_DICTIONARY_FORMAT_STRING_SIZE] + "... (dict truncated)")
        if len(formatted) > MAX_DICTIONARY_FORMAT_STRING_SIZE
        else formatted
    )


def _repr_both_dicts(
    actual: dict[Any, Any], expected: dict[ValuesEqualType, ValuesEqualType]
) -> str:
    """
    Get a message containing the actual and expected dictionaries.

    The message contains the repr() of each dictionary with a label showing
    which is expected which is the actual result. The two representations are
    separated by a newline.

    Arguments:
        actual: The actual dictionary
        expected: The expected dctionary

    Returns:
        str: The representation of both dicts.
    """
    return f"Actual dict: {_short_dict_format_string(actual)}\nExpected dict: {_short_dict_format_string(expected)}"


def assert_dicts_equal(
    actual: dict[Any, Any], expected: dict[ValuesEqualType, ValuesEqualType], **kwargs
) -> None:
    """
    Raise ``AssertionError`` if two dicts are not equal.

    Arguments:
        actual: The actual dictionary
        expected: The expected dictionary
    """
    assert isinstance(expected, dict), (
        "Expected value is not a Dict, and instead is of type "
        + f"{type(expected)}. Expected value is {expected} "
    )
    # Check that the types match exactly.
    # Sometimes we check objects of types like PrettyDict and want to check
    # that we have a PrettyDict and not an instance of a subclass of Dict, so
    # we compare the types directly instead of using isinstance()
    assert type(expected) is type(actual), (
        "Actual type is not the same as expected type "
        + f"{type(expected)}, and instead is type {type(actual)}. Actual "
        + f"value is {actual}"
    )
    actual_len = len(actual)
    expected_len = len(expected)
    assert actual_len == expected_len, (
        f"Actual and expected dicts have different lengths {actual_len} and "
        + f"{expected_len} respectively.\n{_repr_both_dicts(actual, expected)}"
    )
    # Due to server-side compression, only check that index values are equivalent and ignore the
    # index types. Snowpark pandas will use the smallest possible dtype (typically int8), while
    # native pandas will default to int64.
    kwargs.update(check_index_type=False)
    for i, ((actual_key, actual_value), (expected_key, expected_value)) in enumerate(
        zip(actual.items(), expected.items())
    ):
        try:
            assert_values_equal(actual_key, expected_key, **kwargs)
        except AssertionError as e:
            raise AssertionError(
                f"Actual dict has the wrong key at position {i}. Actual "
                + f"key {actual_key} is not equal to expected key "
                + f"{expected_key}.\n{_repr_both_dicts(actual, expected)}"
            ) from e
        try:
            assert_values_equal(actual_value, expected_value, **kwargs)
        except AssertionError as e:
            raise AssertionError(
                f"Actual dict has the wrong value at position {i} with key "
                + f"{actual_key}. Actual value {actual_value} is not equal "
                + f"to expected value {expected_value}.\n"
                + _repr_both_dicts(actual, expected)
            ) from e


def create_snow_df_with_table_and_data(
    session: Session,
    table_name: str,
    column_schema: list[ColumnSchema],
    data: list[list[Any]],
    enforce_ordering: bool = False,
) -> pd.DataFrame:
    """
    Create a snowpark pandas dataframe out of a snowflake table. This function creates a snowflake
    table using the given table schema and data, then call read_snowflake to create a snowpark pandas
    dataframe out of it.

    Args:
        session: Session used to create the snowflake table.
        table_name: name for the snowflake table to create.
        column_schema: List[namedtuple(col_name, snowpark_type)]. List of pairs of column name and
                    snowpark column data type to create for the table. Please check snowflake.snowpark.types
                    for all types can be used.
                    the column name is treated as case-sensitive and quoted during table creation.
        data: data to insert into the created snowflake table, data is two-dimensional and each
              row corresponds to each row in the table.

    Returns:
        snowpark pandas dataframe: a snowpark pandas dataframe created out of the given snowflake table.

        For example: with column_schema: [('COL_0', IntegerType()), ('COL_1', FloatType())],
                          data: [[0, 1.1], [1, 1.2], [2, 1.3]]
                    you will get snowflake table as follows:
                        "COL_0"     "COL_1"
                          0           1.1
                          1           1.2
                          2           1.3
    """

    # create a table with the given table name
    table_column_schema = StructType(
        [
            StructField(f'"{col_name}"', snowpark_type)
            for col_name, snowpark_type in column_schema
        ]
    )

    # convert nan in data to None, because else string 'NaN' will be produced from np.nan e.g.
    data = [
        [None if is_scalar(x) and isna(x) else x for x in sublist] for sublist in data
    ]

    session.create_dataframe(data, schema=table_column_schema).write.save_as_table(
        table_name, table_type="temporary"
    )

    snow_df = pd.read_snowflake(table_name, enforce_ordering=enforce_ordering)
    return snow_df


def create_table_with_type(
    session: Session, name: str, schema: str, table_type: str = "temporary"
):
    session._run_query(f"create or replace {table_type} table {name} ({schema})")


def update_none_in_df_data_test_cases(
    test_cases_raw_data: list[tuple[dict[Any, Any], str]],
    empty_value: Any,
    test_case_label: str,
):
    return [
        (
            {
                key: [empty_value if value is None else value for value in values]
                for (key, values) in test_data.items()
            },
            f"{test_case}_{test_case_label}",
        )
        for test_data, test_case in test_cases_raw_data
    ]


def update_none_in_series_data_test_cases(
    test_cases_raw_data: list[tuple[list[Any], str]],
    empty_value: Any,
    test_case_label: str,
):
    return [
        (
            [empty_value if value is None else value for value in test_data],
            f"{test_case}_{test_case_label}",
        )
        for test_data, test_case in test_cases_raw_data
    ]


def try_cast_to_snowpark_pandas_series(value: Any) -> Any:
    """
    util function to convert an object to a Snowpark pandas Series. Helpful because pytest does not support
    Snowpark pandas Series in pytest.mark.parametrize yet.
    Args:
        value: a value to be converted

    Returns:
        Snowpark pandas Series object
    """
    if isinstance(value, native_pd.Series):
        return pd.Series(
            data=value.values, index=value.index, dtype=value.dtype, name=value.name
        )

    return pd.Series(value)


def try_cast_to_snowpark_pandas_dataframe(value: Any) -> Any:
    """
    util function to convert an object to a Snowpark pandas Dataframe. Helpful because pytest does not support
    Snowpark pandas Dataframe in pytest.mark.parametrize yet.
    Args:
        value: a value to be converted

    Returns:
        Snowpark pandas Dataframe object
    """
    if isinstance(value, native_pd.DataFrame):
        return pd.DataFrame(data=value.values, index=value.index, columns=value.columns)

    return pd.DataFrame(value)


def generate_a_random_permuted_list_exclude_self(value: list[Any]) -> list[Any]:
    """
    Given a value list, generate a new list that is a permutation of the given list, and the
    generated list has to be different compare with the given value list.
    """
    permuted_index_value = list(np.random.permutation(value))
    count = 0
    while permuted_index_value == value and count < 5:
        permuted_index_value = list(np.random.permutation(value))
        count += 1

    assert (
        permuted_index_value != value
    ), "Failed to generate permuted index value that is different than original index value"

    return permuted_index_value


def get_snowpark_dataframe_quoted_identifiers(
    snowpark_dataframe: SnowparkDataFrame,
) -> list[str]:
    return [f.column_identifier.quoted_name for f in snowpark_dataframe.schema.fields]


def assert_index_equal(
    left: pd.Index,
    right: pd.Index,
    exact: bool | str = "equiv",
    check_names: bool = True,
    check_exact: bool = True,
    check_categorical: bool = True,
    check_order: bool = True,
    rtol: float = 1.0e-5,
    atol: float = 1.0e-8,
    obj: str = "Index",
):
    """
    Check that left and right Index are equal.

    Parameters
    ----------
    left : Index
    right : Index
    exact : bool or {'equiv'}, default 'equiv'
        Whether to check the Index class, dtype and inferred_type
        are identical. If 'equiv', then RangeIndex can be substituted for
        Index with an int64 dtype as well.
    check_names : bool, default True
        Whether to check the names attribute.
    check_exact : bool, default True
        Whether to compare number exactly.
    check_categorical : bool, default True
        Whether to compare internal Categorical exactly.
    check_order : bool, default True
        Whether to compare the order of index entries as well as their values.
        If True, both indexes must contain the same elements, in the same order.
        If False, both indexes must contain the same elements, but in any order.
    rtol : float, default 1e-5
        Relative tolerance. Only used when check_exact is False.
    atol : float, default 1e-8
        Absolute tolerance. Only used when check_exact is False.
    obj : str, default 'Index'
        Specify object name being compared, internally used to show appropriate
        assertion message.
    """
    left = try_convert_index_to_native(left)
    right = try_convert_index_to_native(right)
    return native_pd._testing.assert_index_equal(
        left,
        right,
        exact=exact,
        check_names=check_names,
        check_exact=check_exact,
        check_categorical=check_categorical,
        check_order=check_order,
        rtol=rtol,
        atol=atol,
        obj=obj,
    )
