#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas.core.dtypes.common import is_datetime64_any_dtype, is_integer_dtype

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.types import (
    ArrayType,
    DataType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    VariantType,
)
from tests.integ.modin.utils import (
    assert_frame_equal,
    assert_series_equal,
    assert_snowpark_pandas_equal_to_pandas,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_series,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.hybrid_pytest_support import hybrid_xskip
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


def validate_series_snowpark_dtype(series: pd.Series, snowpark_type: DataType) -> None:
    internal_frame = series._query_compiler._modin_frame
    snowpark_dtypes = internal_frame.get_snowflake_type(
        internal_frame.data_column_snowflake_quoted_identifiers
    )
    assert len(snowpark_dtypes) == 1
    assert snowpark_dtypes[0] == snowpark_type


@pytest.mark.parametrize(
    "dataframe_input, input_dtype, logical_dtype",
    [
        ([1, 2, 3], np.dtype("int8"), np.dtype("int64")),
        ([1, 2, 3], np.dtype("int16"), np.dtype("int64")),
        ([1, 2, 3], np.dtype("int32"), np.dtype("int64")),
        ([1, 2, 3], np.dtype("int64"), np.dtype("int64")),
        (
            [1 << 10, 2 << 10, 3 << 10],
            np.dtype("int16"),
            np.dtype("int64"),
        ),
        (
            [1 << 10, 2 << 10, 3 << 10],
            np.dtype("int32"),
            np.dtype("int64"),
        ),
        (
            [1 << 10, 2 << 10, 3 << 10],
            np.dtype("int64"),
            np.dtype("int64"),
        ),
        (
            [1 << 20, 2 << 20, 3 << 20],
            np.dtype("int32"),
            np.dtype("int64"),
        ),
        (
            [1 << 20, 2 << 20, 3 << 20],
            np.dtype("int64"),
            np.dtype("int64"),
        ),
        (
            [1 << 40, 2 << 40, 3 << 40],
            np.dtype("int64"),
            np.dtype("int64"),
        ),
    ],
)
@sql_count_checker(query_count=2)
#@hybrid_xskip(throws=AssertionError, reason="assert dtype('int8') == dtype('int64')")
#@pytest.mark.hybrid
@pytest.mark.skipif(
        "pytestconfig.getoption('enable_modin_hybrid_mode')",
        reason="hybrid not supported for this test",
        run=False
    )
def test_integer(dataframe_input, input_dtype, logical_dtype):
    expected = native_pd.Series(dataframe_input, dtype=input_dtype)
    created = pd.Series(dataframe_input, dtype=input_dtype)
    assert created.dtype == logical_dtype
    roundtripped = created.to_pandas()
    assert_series_equal(
        roundtripped, expected, check_dtype=False, check_index_type=False
    )
    assert is_integer_dtype(roundtripped.dtype)

    expected = native_pd.DataFrame(
        dataframe_input,
        columns=["col"],
        index=native_pd.Index(data=[101, 102, 103], dtype=np.int64),
    )
    created = pd.DataFrame(
        dataframe_input,
        columns=["col"],
        index=native_pd.Index(data=[101, 102, 103], dtype=np.int64),
    )
    assert_series_equal(
        created.dtypes, native_pd.Series([logical_dtype], index=["col"])
    )
    roundtripped = created.to_pandas()
    assert_frame_equal(
        roundtripped, expected, check_dtype=False, check_index_type=False
    )
    assert all([is_integer_dtype(dt) for dt in roundtripped.dtypes])


@pytest.mark.parametrize(
    "values",
    [
        [pd.Timedelta("1 day"), None],
        [pd.Timedelta("1 day")],
    ],
)
@sql_count_checker(query_count=0)
def test_timedelta(values):
    eval_snowpark_pandas_result(
        *create_test_series(values, dtype="timedelta64[ns]"),
        lambda s: s.dtype,
        comparator=lambda snow_dtype, pandas_dtype: snow_dtype == pandas_dtype,
    )


@pytest.mark.parametrize(
    "dataframe_input, input_dtype, snowpark_dtype, logical_dtype",
    [
        ([1, 2, None], None, DoubleType(), np.dtype("float64")),
        ([1, 2, None], "Int64", LongType(), np.dtype("int64")),
        ([1, 2, None], "UInt64", LongType(), np.dtype("int64")),
        ([1, 2, None], "Int32", LongType(), np.dtype("int64")),
        ([1, 2, None], "UInt32", LongType(), np.dtype("int64")),
        ([1, 2, None], "Int16", LongType(), np.dtype("int64")),
        ([1, 2, None], "UInt16", LongType(), np.dtype("int64")),
        ([1, 2, None], "Int8", LongType(), np.dtype("int64")),
        ([1, 2, None], "UInt8", LongType(), np.dtype("int64")),
        ([1.0, 2.0, None], "Float32", DoubleType(), np.dtype("float64")),
        ([1.0, 2.0, None], "Float64", DoubleType(), np.dtype("float64")),
        # test cases for different pandas missing value representations, includes
        # np.nan, pd.NA, None. The missing values will be all mapped to snowflake NULL
        ([1, 2, pd.NA, None, pd.NA], "Int64", LongType(), np.dtype("int64")),
        (
            [1.0, 2.0, None, np.nan, "NaN", pd.NA],
            "Float64",
            DoubleType(),
            np.dtype("float64"),
        ),
    ],
)
@sql_count_checker(query_count=1)
def test_nullable_extension(
    dataframe_input, input_dtype, snowpark_dtype, logical_dtype
):
    expected = native_pd.Series(dataframe_input, dtype=input_dtype)
    assert expected.dtype == input_dtype
    created = pd.Series(dataframe_input, dtype=input_dtype)
    assert created.dtype == logical_dtype
    validate_series_snowpark_dtype(created, snowpark_dtype)
    roundtripped = created.to_pandas()
    # Note that this behavior is very unfortunate.
    assert roundtripped.dtype == np.dtype("float64")
    assert_series_equal(
        roundtripped, expected.astype("float64"), check_index_type=False
    )


@sql_count_checker(query_count=3)
def test_extended_float64_with_nan():
    # When creating native pandas series with the following data using
    # native_pd.Series([1.0, pd.NA, None, 2.0, np.nan, 3.0, 'NaN'], dtype="Float64")
    # the result will become [1.0, <NA>, <NA>, 2.0, <NA>, 3.9, NaN], where pd.NA, None,
    # and np.nan all becomes missing value NA, but 'NaN' will be mapped to Not A Number.
    # Such information will be retained when we write back to Snowflake.
    snow_se = pd.Series([1.0, pd.NA, None, 2.0, np.nan, 3.0, "NaN"], dtype="Float64")
    snowpark_res = snow_se._query_compiler._modin_frame.ordered_dataframe.collect()
    # verify that pd.NA, None, and np.nan are all mapped to NULL in snowflake, which is
    # represented as None in snowpark
    assert snowpark_res[0][1] == 1.0
    assert snowpark_res[1][1] is None
    assert snowpark_res[2][1] is None
    assert snowpark_res[3][1] == 2.0
    assert snowpark_res[4][1] is None
    assert snowpark_res[5][1] == 3.0
    # verify that 'NaN' in pandas extend float64 dtype is mapped to 'NaN' in snowflake,
    # which is mapped to np.nan in Snowpark
    assert snowpark_res[6][1] is not None
    assert np.isnan(snowpark_res[6][1])

    # verify that to_pandas mapped all pd.NA, None, np.nan, 'NaN' to np.nan
    expected = pd.Series(
        [1.0, np.nan, np.nan, 2.0, np.nan, 3.0, np.nan], dtype=np.dtype("float64")
    )
    assert_series_equal(snow_se.to_pandas(), expected)


@pytest.mark.parametrize(
    "dataframe_input, input_dtype, expected_dtype, logical_dtype",
    [
        (
            [1.1, 2.2, 3.3],
            np.dtype("float16"),
            np.dtype("float64"),
            np.dtype("float64"),
        ),
        (
            [1.1, 2.2, 3.3],
            np.dtype("float32"),
            np.dtype("float64"),
            np.dtype("float64"),
        ),
        (
            [1.1, 2.2, 3.3],
            np.dtype("float64"),
            np.dtype("float64"),
            np.dtype("float64"),
        ),
    ],
)
@sql_count_checker(query_count=2)
def test_float(dataframe_input, input_dtype, expected_dtype, logical_dtype):
    expected = native_pd.Series(dataframe_input, dtype=input_dtype)
    created = pd.Series(dataframe_input, dtype=input_dtype)
    assert created.dtype == logical_dtype
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        created, expected, expected_dtypes=[expected_dtype]
    )

    expected = native_pd.DataFrame(
        dataframe_input,
        columns=["col"],
        index=native_pd.Index(data=[101, 102, 103], dtype=np.int64),
    )
    created = pd.DataFrame(
        dataframe_input,
        columns=["col"],
        index=native_pd.Index(data=[101, 102, 103], dtype=np.int64),
    )
    assert_series_equal(
        created.dtypes, native_pd.Series([logical_dtype], index=["col"])
    )
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        created, expected, expected_dtypes=[expected_dtype]
    )


@pytest.mark.parametrize(
    "dataframe_input, input_dtype, index",
    [
        (["a", "b", "c"], np.object_, native_pd.Index(data=[101, 102, 103])),
        (
            ["a", "b", "c"],
            np.object_,
            native_pd.Index(data=["a101", "aa102", "aaa103"]),
        ),
    ],
)
@sql_count_checker(query_count=2)
def test_string(dataframe_input, input_dtype, index):
    expected = native_pd.Series(dataframe_input, dtype=input_dtype)
    created = pd.Series(dataframe_input)
    assert created.dtype == np.object_
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        created,
        expected,
        # Note that we always present strings back as object types.
        expected_dtypes=["object"],
    )

    expected = native_pd.DataFrame(dataframe_input, columns=["col"], index=index)
    created = pd.DataFrame(dataframe_input, columns=["col"], index=index)
    assert_series_equal(created.dtypes, native_pd.Series([np.object_], index=["col"]))
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        created, expected, expected_dtypes=["object"]
    )


@pytest.mark.parametrize(
    "dataframe_input, input_dtype, index",
    [
        (["a", "b", "c"], "string", native_pd.Index(data=[101, 102, 103])),
    ],
)
@sql_count_checker(query_count=1)
def test_string_explicit(dataframe_input, input_dtype, index):
    expected = native_pd.Series(dataframe_input, dtype=input_dtype)
    created = pd.Series(dataframe_input, dtype=input_dtype)
    assert created.dtype == np.object_
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        created,
        expected,
        # Note that we always present strings back as object types.
        expected_dtypes=["object"],
    )


@pytest.mark.parametrize(
    "label1, label2",
    [
        (["level0"], ["col1", "col2", "col3"]),
        # escaped
        (["leve'l0"], ["co'l1", 'co"l2', "col3"]),
        # duplicate
        (["level0"], ["col1", "col2", "col1"]),
    ],
)
@sql_count_checker(query_count=1)
def test_insert_multiindex_multi_label(label1, label2):
    arrays = [["apple", "apple", "banana", "banana"], [1, 2, 1, 2]]
    index = pd.MultiIndex.from_arrays(arrays, names=["first", "second"])
    columns = pd.MultiIndex.from_product((label1, label2), names=["a1", "a2"])
    data = [["p", 1, 1.0], ["q", 2, 2.0], ["r", 3, 3.0], ["s", 4, 4.0]]
    expected = native_pd.DataFrame(data=data, columns=columns, index=index)
    created = pd.DataFrame(data=data, columns=columns, index=index)

    # The dtypes of the frame before to_pandas is called matches the frame of expected
    # but this may not be the case after to_pandas
    assert_series_equal(created.dtypes, expected.dtypes)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        created, expected, check_index_type=False
    )


@pytest.mark.parametrize(
    "dataframe_input, input_dtype, expected_dtype, logical_dtype",
    [
        (
            [
                native_pd.Timestamp(1513393355, unit="s"),
                native_pd.Timestamp(1513393355, unit="s"),
            ],
            "datetime64[ns]",
            "datetime64[ns]",
            "datetime64[ns]",
        ),
        (
            [
                native_pd.Timestamp(1513393355, unit="s"),
                native_pd.Timestamp(1513393355, unit="s"),
                None,
                pd.NaT,  # pd.NaT is used as missing value for datetime type, and will be mapped to NULL in snowflake
            ],
            "datetime64[ns]",
            "datetime64[ns]",
            "datetime64[ns]",
        ),
        (
            [
                native_pd.Timestamp(1513393355, unit="s", tz="US/Pacific"),
                native_pd.Timestamp(1513393355, unit="s", tz="US/Pacific"),
            ],
            "datetime64[ns, America/Los_Angeles]",
            "datetime64[ns, UTC-08:00]",
            "datetime64[ns, UTC-08:00]",
        ),
        (
            [
                native_pd.Timestamp(1513393355, unit="s"),
                native_pd.Timestamp(1513393355, unit="s"),
                None,
            ],
            "object",
            "datetime64[ns]",
            "datetime64[ns]",
        ),
        (
            [
                native_pd.Timestamp(1513393355, unit="s", tz="US/Pacific"),
                None,
                pd.NaT,  # pd.NaT is used as missing value for datetime type, and will be mapped to NULL in snowflake
                native_pd.Timestamp(1513393355, unit="s", tz="US/Pacific"),
            ],
            "object",
            "datetime64[ns, UTC-08:00]",
            "datetime64[ns, UTC-08:00]",
        ),
    ],
)
def test_time(dataframe_input, input_dtype, expected_dtype, logical_dtype):
    expected = native_pd.Series(dataframe_input, dtype=expected_dtype)
    qc = (
        2
        if is_datetime64_any_dtype(expected.dtype)
        and getattr(expected.dtype, "tz", None) is not None
        else 1
    )
    with SqlCounter(query_count=qc):
        created = pd.Series(dataframe_input, dtype=input_dtype)
        # For snowpark pandas type mapping
        assert created.dtype == logical_dtype
        roundtripped = created.to_pandas()
        assert_series_equal(roundtripped, expected, check_index_type=False)


@pytest.mark.parametrize(
    "dataframe_input, input_dtype",
    [
        ([1, False, "a"], np.object_),
        ([1, None, 2.0, np.nan, "a"], np.object_),
    ],
)
@sql_count_checker(query_count=1)
def test_mixed(dataframe_input, input_dtype):
    expected = native_pd.Series(dataframe_input)
    assert expected.dtype == input_dtype
    created = pd.Series(dataframe_input)
    assert created.dtype == input_dtype
    assert_snowpark_pandas_equal_to_pandas(
        created,
        expected,
    )


@pytest.mark.parametrize(
    "dataframe_input, snowpark_dtype",
    [
        ([1.1, "a", None], VariantType()),
        (
            [["a", "b", "c"], ["1", "2", "3"]],
            ArrayType(),
        ),
        (
            [{"a": 1, "b": 2}, {"c": 3, "d": 4}],
            MapType(),
        ),
    ],
)
@sql_count_checker(query_count=1)
def test_object(dataframe_input, snowpark_dtype):
    expected = native_pd.Series(dataframe_input)
    assert expected.dtype == np.object_
    created = pd.Series(dataframe_input)
    assert created.dtype == np.object_
    validate_series_snowpark_dtype(created, snowpark_dtype)
    assert_snowpark_pandas_equal_to_pandas(created, expected)


@pytest.mark.parametrize(
    "input_dtype, expected_dtype, snowpark_dtype, to_pandas_dtype",
    [
        (np.dtype("int64"), np.dtype("int64"), LongType(), np.dtype("int64")),
        (np.dtype("O"), np.dtype("O"), VariantType(), np.dtype("O")),
        (np.dtype("float64"), np.dtype("float64"), DoubleType(), np.dtype("float64")),
        (None, np.dtype("object"), VariantType(), np.dtype("object")),
    ],
)
@sql_count_checker(query_count=1)
def test_empty(input_dtype, expected_dtype, snowpark_dtype, to_pandas_dtype):
    created = pd.Series(data=[], dtype=input_dtype)
    assert created.dtype == expected_dtype
    validate_series_snowpark_dtype(created, snowpark_dtype)
    roundtripped = created.to_pandas()
    assert roundtripped.dtype == to_pandas_dtype


@pytest.mark.parametrize(
    "index, expected_index_dtype",
    [
        (None, np.dtype("int64")),
        (native_pd.Index([]), np.dtype("object")),
        (native_pd.Index([], dtype="float64"), np.dtype("float64")),
    ],
)
@sql_count_checker(query_count=1)
def test_empty_index(index, expected_index_dtype):
    expected = native_pd.Series(data=[], index=index)
    assert expected.dtype == np.dtype("object")
    assert expected.index.dtype == expected_index_dtype
    created = pd.Series(data=[], index=index)
    assert created.dtype == np.dtype("object")
    assert created.index.dtype == expected_index_dtype
    roundtripped = created.to_pandas()
    assert roundtripped.dtype == np.dtype("object")
    assert roundtripped.index.dtype == expected_index_dtype


@pytest.mark.parametrize(
    "input_data, dtype, type_msg",
    [
        (native_pd.Categorical([1, 2, 3, 1, 2, 3]), "category", "category"),
        (native_pd.Categorical(["a", "b", "c", "a", "b", "c"]), "category", "category"),
        (
            native_pd.period_range("2015-02-03 11:22:33.4567", periods=5, freq="s"),
            None,
            r"period\[s\]",
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_unsupported_dtype_raises(input_data, dtype, type_msg) -> None:
    with pytest.raises(
        NotImplementedError, match=f"pandas type {type_msg} is not implemented"
    ):
        pd.Series(input_data, dtype=dtype)


@pytest.mark.parametrize(
    "input_data, input_dtype, expected_dtype, snowpark_dtype, to_pandas_dtype",
    [
        (
            [1, 1, np.nan, None],
            None,
            np.dtype("float64"),
            DoubleType(),
            np.dtype("float64"),
        ),
        (
            ["a", np.nan, "b", None],
            np.dtype("O"),
            np.dtype("O"),
            StringType(),
            np.dtype("O"),
        ),
        (
            [2.0, 1.0, np.nan, 3, np.nan, None],
            np.dtype("float64"),
            np.dtype("float64"),
            DoubleType(),
            np.dtype("float64"),
        ),
    ],
)
@sql_count_checker(query_count=1)
def test_str_float_type_with_nan(
    input_data, input_dtype, expected_dtype, snowpark_dtype, to_pandas_dtype
):
    snow_se = pd.Series(input_data, dtype=input_dtype)
    assert snow_se.dtype == expected_dtype
    validate_series_snowpark_dtype(snow_se, snowpark_dtype)
    native_se = snow_se.to_pandas()
    assert native_se.dtype == to_pandas_dtype
    expected = native_pd.Series(input_data, dtype=to_pandas_dtype)
    assert_series_equal(native_se, expected, check_index_type=False)


@pytest.mark.parametrize(
    "ts_data",
    [
        native_pd.date_range("2020-01-01", periods=10),
        native_pd.date_range("2020-01-01", periods=10, tz="US/Pacific"),
        native_pd.date_range("2020-01-01", periods=10, tz="UTC"),
        native_pd.date_range("2020-01-01", periods=10, tz="Asia/Tokyo"),
        native_pd.date_range("2020-01-01", periods=10, tz="UTC+1000"),
        native_pd.date_range("2020-01-01", periods=10, tz="UTC+1000").append(
            native_pd.date_range("2020-01-01", periods=10, tz="UTC")
        ),
    ],
)
def test_tz_dtype(ts_data):
    with SqlCounter(
        query_count=1
        if is_datetime64_any_dtype(ts_data.dtype) and ts_data.tz is None
        else 2
    ):
        s = pd.Series(ts_data)
        assert s.dtype == s.to_pandas().dtype


@sql_count_checker(query_count=1)
def test_tz_dtype_cache():
    s = pd.Series(native_pd.date_range("2020-10-01", periods=5, tz="UTC"))
    for _ in range(50):
        assert s.dtype == "datetime64[ns, UTC]"
