#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
from typing import Any, Union
from unittest.mock import patch

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas import DatetimeTZDtype
from pandas.core.dtypes.common import is_datetime64_any_dtype

import snowflake.snowpark
import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    generate_random_alphanumeric,
)
from snowflake.snowpark.types import ArrayType, MapType, StructType
from tests.integ.modin.utils import (
    BASIC_TYPE_DATA1,
    BASIC_TYPE_DATA2,
    VALID_PANDAS_LABELS,
    assert_frame_equal,
    assert_index_equal,
    assert_series_equal,
    assert_snowpark_pandas_equal_to_pandas,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import Utils

# Those index types are used to verify the type mapping in a round trip, i.e., from_pandas to to_pandas.
# TODO: SNOW-841273 verity the type mapping from Snowflake table to Snowpark pandas
FROM_TO_PANDAS_VALUE_TYPE_MATCH_INDICES = (
    "string",
    "int",
    "range",
    "float",
    "repeats",
    "bool-dtype",
    "tuples",
    "multi",
    # NumericIndex is a pandas 2.x feature
    "uint-small",
    "num_int8",
    "num_int16",
    "num_int64",
    "num_float64",
)

# These indices have matched data values, but index type is not recovered
FROM_TO_PANDAS_TYPE_MISMATCH_INDICES = {
    # key: name, actual dtype of the index to_pandas()
    "bool-object": "bool",
    "string-python": "object",
    # https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-pandas#snowflake-to-pandas-data-mapping
    "nullable_int": "float64",
    "nullable_uint": "float64",
    "nullable_float": "float64",
    "nullable_bool": "bool",
    # There is no unsigned integer type in snowflake, and uint64 data will be converted to
    # FIXED type in snowflake. When the data is converted back to pandas, it will become
    # either int16 or float64 (if values are larger than max int16)
    "uint": "int16",
    # NumericIndex is a pandas 2.x feature
    "num_int32": "int16",
    "num_uint64": "int16",
    "num_uint32": "int16",
    "num_uint16": "int16",
    "num_uint8": "int16",
    "num_float32": "float64",
    "multi-with-dt64tz-level": "object",
}

# Both the data values and the type of these indices are mismatched with native pandas
FROM_TO_PANDAS_VALUE_TYPE_MISMATCH_INDICES = (
    "categorical",
    "interval",
    "complex64",
    "complex128",
    "period",
    # failed due to AssertionError: (None, <BusinessDay>)
    "datetime",
    "datetime-tz",
    "timedelta",
)


def check_result_from_and_to_pandas(
    data: Any,
    *,
    index: Union[native_pd.Index, str] = None,
    columns: Union[str, list[str], native_pd.Index] = None,
    expected_index_type: str = None,
    expected_dtypes: list[str] = None,
    **kwargs,
) -> None:
    """
    Check a pandas dataframe is consistent with the result after converting to a snowpark pandas dataframe and converting
    back.
    Args:
        data: data field to create pandas dataframe
        index: index field to create pandas dataframe
        columns: used to populate columns filed to create pandas dataframe
        expected_index_type: if not None then check snowpark pandas dataframe's index type is expected type
        expected_dtypes: if not None then check snowpark pandas dataframe's column data types
        **kwargs: other kwargs pass to assert_frame_equal
    Raises:
        AssertionError if the converted dataframe does not match with the original one
    """
    if columns is not None and not isinstance(columns, (list, native_pd.Index)):
        columns = [columns]
    native_df = native_pd.DataFrame(data=data, index=index, columns=columns)
    snow_df = pd.DataFrame(native_df)
    kwargs.update(expected_index_type=expected_index_type)
    kwargs.update(expected_dtypes=expected_dtypes)
    assert_snowpark_pandas_equal_to_pandas(snow_df, native_df, **kwargs)


@pytest.mark.parametrize(
    "name",
    FROM_TO_PANDAS_VALUE_TYPE_MATCH_INDICES,
)
def test_value_type_match_index_type(name, indices_dict):
    expected_query_count = 1 if name in ("uint-small", "tuples", "multi") else 4
    with SqlCounter(query_count=expected_query_count):
        index = indices_dict[name]
        size = len(index)
        data = np.random.randn(size)
        check_result_from_and_to_pandas(
            data,
            index=index,
            columns=name,
        )


@pytest.mark.parametrize("name", FROM_TO_PANDAS_TYPE_MISMATCH_INDICES)
def test_type_mismatch_index_type(name, indices_dict):
    expected_query_count = (
        1 if name == "uint-small" or "multi-with-dt64tz" in name else 4
    )
    with SqlCounter(query_count=expected_query_count):
        index = indices_dict[name]
        expected_index_type = FROM_TO_PANDAS_TYPE_MISMATCH_INDICES[name]
        size = len(index)
        data = np.random.randn(size)
        check_result_from_and_to_pandas(
            data,
            index=index,
            columns=name,
            expected_index_type=expected_index_type,
            check_index_type=False,
        )


@pytest.mark.parametrize("name", FROM_TO_PANDAS_VALUE_TYPE_MISMATCH_INDICES)
def test_value_type_mismatch_index_type(name, indices_dict):
    # We can create a pandas dataframe out of the datetime and timedelta
    # values, so we execute some queries for those cases.
    expected_query_count = 4 if ("datetime" in name or "timedelta" in name) else 0
    with SqlCounter(query_count=expected_query_count):
        index = indices_dict[name]
        size = len(index)
        data = np.random.randn(size)
        # 1. raises AssertionError from assert_frame_equal()
        # 2. raises ArrowNotImplementedError when converting the pandas dataframe to parquet
        # files in connector's write_pandas, because arrow doesn't support complex128 type
        # see https://issues.apache.org/jira/browse/ARROW-14268
        with pytest.raises((AssertionError, NotImplementedError)):
            check_result_from_and_to_pandas(
                data, index=index, columns=name, check_index_type=False
            )


@sql_count_checker(query_count=1)
def test_basic_type_data():
    check_result_from_and_to_pandas(
        # Excluded the first int value since Snowflake may map integers to different size, e.g., int8 or int64
        [BASIC_TYPE_DATA1[1:], BASIC_TYPE_DATA2[1:]],
        expected_dtypes=[
            "object",
            "float64",
            "object",
            "object",
            "bool",
            "object",
        ],
    )


@sql_count_checker(query_count=1)
def test_base_index():
    # base class index (appears sometimes as well!)
    check_result_from_and_to_pandas(
        [1, 2, 3],
        index=native_pd.Index([8, 9, 9]),
        columns="base-index-homogeneous-type",
        check_dtype=False,
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "data,index,columns",
    [
        ([1, 2, 3], ["a", 10, 3.131], "base-index-non-homogeneous-type"),
        (None, ["A", "B", -2], ["X"]),
    ],
)
@sql_count_checker(query_count=1)
def test_base_index_with_variant_data(data, index, columns):
    check_result_from_and_to_pandas(
        data=data,
        index=native_pd.Index(["a", 10, 3.141]),
        columns=columns,
        check_dtype=False,
    )


@pytest.mark.parametrize("col_name", VALID_PANDAS_LABELS)
@sql_count_checker(query_count=1)
def test_column_name(col_name):
    check_result_from_and_to_pandas(
        [[1, 2], [2, 3]],
        columns=["5", col_name],
        check_dtype=False,
    )


@pytest.mark.parametrize("index_name", VALID_PANDAS_LABELS)
@sql_count_checker(query_count=1)
def test_index_name(index_name):
    check_result_from_and_to_pandas(
        [1, 2], index=pd.RangeIndex(2, name=index_name), check_dtype=False
    )


@pytest.mark.parametrize("pandas_label", [None, *VALID_PANDAS_LABELS])
@sql_count_checker(query_count=1)
def test_column_index_names(pandas_label):
    snow_df = pd.DataFrame({pandas_label: [1, 2]})
    expected_columns_index = native_pd.Index([pandas_label])
    # verify columns is same as original dataframe.
    assert_index_equal(snow_df.columns, expected_columns_index)
    # convert back to native pandas and verify columns is same as the original dataframe
    native_df = snow_df.to_pandas()
    assert_index_equal(native_df.columns, expected_columns_index)


@pytest.mark.parametrize("name", [None, *VALID_PANDAS_LABELS])
@sql_count_checker(query_count=1)
def test_to_pandas_column_index_names(name):
    df = pd.DataFrame(
        data=[[1] * 2, [2] * 2],
        columns=native_pd.Index([1, 2], name=name),
    )
    assert df.columns.names == [name]
    pdf = df.to_pandas()
    assert pdf.columns.names == [name]


@sql_count_checker(query_count=1)
def test_from_to_pandas_datetime64_support():
    # This test verifies the datetime64 columns and index conversions, including from and to pandas.
    # The test data columns include both datetime64 and other types to make sure all types are correctly converted.

    # DatetimeIndex will be converted to Snowflake timestamp type (similar to datetime64[ns])
    test_datetime_index = native_pd.DatetimeIndex(
        ["2017-12-31 16:00:00", "2017-12-31 17:00:00", "2017-12-31 18:00:00"],
        dtype="datetime64[ns]",
    )

    test_data_columns = {
        "int": [1, 2, 3],
        "timestamp": [
            native_pd.Timestamp("20010101"),
            native_pd.Timestamp("20010102"),
            native_pd.Timestamp("20010103"),
        ],
        "str": ["1", "2", "3"],
        "days": np.arange("2005-02-01", "2005-02-04", dtype="datetime64[D]"),
        "years": np.arange("2005", "2008", dtype="datetime64[Y]"),
        "nanoseconds": [
            native_pd.Timestamp(1513393355.123456789),
            native_pd.Timestamp(1513393355.123456790),
            native_pd.Timestamp(1513393355.123456791),
        ],
        "datetimeIndex": test_datetime_index,
    }
    # pandas only use datetime64[ns] for all datetime64 columns so snowpark pandas follows it too
    expected_dtypes = [
        "int64",
        "datetime64[ns]",
        "object",
        "datetime64[ns]",
        "datetime64[ns]",
        "datetime64[ns]",
        "datetime64[ns]",
    ]

    check_result_from_and_to_pandas(
        test_data_columns,
        index=test_datetime_index,
        expected_dtypes=expected_dtypes,
        expected_index_type="datetime64[ns]",
    )


@sql_count_checker(query_count=4)
def test_rw_datetimeindex():
    test_datetime_index = native_pd.DatetimeIndex(
        ["2017-12-31 16:00:00", "2017-12-31 17:00:00", "2017-12-31 18:00:00"],
        dtype="datetime64[ns]",
        freq="H",
    )
    test_datetime_index_tz = native_pd.DatetimeIndex(
        [
            "2017-12-31 16:00:00-08:00",
            "2017-12-31 17:00:00-08:00",
            "2017-12-31 18:00:00-08:00",
        ],
        dtype="datetime64[ns, US/Pacific]",
        freq="H",
    )
    df = pd.DataFrame({"ntz": test_datetime_index, "tz": test_datetime_index_tz})
    assert_series_equal(
        df.dtypes,
        native_pd.Series(
            ["datetime64[ns]", "datetime64[ns, UTC-08:00]"], index=["ntz", "tz"]
        ),
    )
    assert_series_equal(
        df.to_pandas().dtypes,
        native_pd.Series(
            ["datetime64[ns]", "datetime64[ns, UTC-08:00]"],
            index=["ntz", "tz"],
        ),
    )

    # When pulling from a datetime index from Snowpark pandas, `freq` is not supported and only timezone offset can be
    # preserved
    ntz_index = df.set_index("ntz").index
    assert_index_equal(
        ntz_index,
        native_pd.DatetimeIndex(
            ["2017-12-31 16:00:00", "2017-12-31 17:00:00", "2017-12-31 18:00:00"],
            dtype="datetime64[ns]",
            name="ntz",
            freq=None,  # freq has been lost
        ),
    )

    tz_index = df.set_index("tz").index
    assert_index_equal(
        tz_index,
        native_pd.DatetimeIndex(
            [
                "2017-12-31 16:00:00-08:00",
                "2017-12-31 17:00:00-08:00",
                "2017-12-31 18:00:00-08:00",
            ],
            dtype=pd.DatetimeTZDtype(unit="ns", tz="UTC-08:00"),
            # has preserved
            name="tz",
            freq=None,  # freq has been lost
        ),
    )


@sql_count_checker(query_count=1)
def test_from_to_pandas_datetime64_timezone_support():
    def get_series_with_tz(tz):
        return native_pd.Series([1] * 3).astype("int64").astype(f"datetime64[ns, {tz}]")

    # same timestamps representing in different time zone
    test_data_columns = {
        "utc": get_series_with_tz("UTC"),
        "pacific": get_series_with_tz("US/Pacific"),
        "tokyo": get_series_with_tz("Asia/Tokyo"),
    }

    expected_data_columns = {
        "utc": get_series_with_tz("UTC"),
        "pacific": get_series_with_tz("UTC-08:00"),
        "tokyo": get_series_with_tz("UTC+09:00"),
    }
    # expected to_pandas dataframe's timezone is controlled by session/statement parameter TIMEZONE
    expected_to_pandas = native_pd.DataFrame(expected_data_columns)
    assert_snowpark_pandas_equal_to_pandas(
        pd.DataFrame(test_data_columns),
        expected_to_pandas,
    )


@sql_count_checker(query_count=1)
def test_from_to_pandas_datetime64_multi_timezone_current_behavior():
    # This test also verifies the current behaviors of to_pandas() for datetime with no tz, same tz, or multi tz:
    # no tz    => TIMESTAMP_NTZ
    # same tz  => TIMESTAMP_TZ
    # multi tz => TIMESTAMP_TZ
    multi_tz_data = ["2019-05-21 12:00:00-06:00", "2019-05-21 12:15:00-07:00"]
    test_data_columns = {
        "no tz": native_pd.to_datetime(
            native_pd.Series(["2019-05-21 12:00:00", "2019-05-21 12:15:00"])
        ),  # dtype = datetime64[ns]
        "same tz": native_pd.to_datetime(
            native_pd.Series(["2019-05-21 12:00:00-06:00", "2019-05-21 12:15:00-06:00"])
        ),  # dtype = datetime64[ns, tz]
        "multi tz": native_pd.to_datetime(
            native_pd.Series(multi_tz_data)
        ),  # dtype = object and value type is Python datetime
    }

    expected_to_pandas = native_pd.DataFrame(test_data_columns)

    test_df = native_pd.DataFrame(test_data_columns)
    # dtype checks for each series
    no_tz_dtype = test_df.dtypes["no tz"]
    assert is_datetime64_any_dtype(no_tz_dtype) and not isinstance(
        no_tz_dtype, DatetimeTZDtype
    )
    same_tz_dtype = test_df.dtypes["same tz"]
    assert is_datetime64_any_dtype(same_tz_dtype) and isinstance(
        same_tz_dtype, DatetimeTZDtype
    )
    multi_tz_dtype = test_df.dtypes["multi tz"]
    assert (
        not is_datetime64_any_dtype(multi_tz_dtype)
        and not isinstance(multi_tz_dtype, DatetimeTZDtype)
        and str(multi_tz_dtype) == "object"
    )
    # sample value
    assert isinstance(test_df["multi tz"][0], datetime.datetime)
    assert test_df["multi tz"][0].tzinfo is not None
    assert_snowpark_pandas_equal_to_pandas(
        pd.DataFrame(test_df),
        expected_to_pandas,
    )


@sql_count_checker(query_count=1)
def test_from_pandas_duplicate_labels():
    # Duplicate data labels
    native_df = native_pd.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
    native_df.columns = ["a", "a", "A"]
    snow_df = pd.DataFrame(native_df)
    assert snow_df.columns.tolist() == ["a", "a", "A"]

    # Duplicate between index and data label
    native_df = native_pd.DataFrame(
        {"a": [1, 2]}, index=pd.RangeIndex(start=4, stop=6, step=1, name="a")
    )
    snow_df = pd.DataFrame(native_df)
    assert snow_df.columns.tolist() == ["a"]
    assert snow_df.index.name == "a"

    # Duplicate index labels
    native_df = native_pd.DataFrame(
        {"z": [1, 2]},
        index=native_pd.MultiIndex.from_arrays(
            [["u", "v"], ["x", "y"]], names=("a", "a")
        ),
    )
    snow_df = pd.DataFrame(native_df)
    assert snow_df.index.names == native_df.index.names


@pytest.mark.parametrize(
    "table_type, n_rows",
    [
        ("temporary", 20000),
        ("transient", 20000),
        ("temporary", 100),
        ("transient", 100),
    ],
)
@sql_count_checker(query_count=8)
def test_determinism_with_repeated_to_pandas(session, table_type, n_rows) -> None:
    ref_df = native_pd.DataFrame(
        {
            "A": [i for i in range(n_rows)],
            "B": [i for i in range(n_rows, 2 * n_rows)],
            "C": [i for i in range(2 * n_rows, 3 * n_rows)],
        }
    )

    test_table_name = f"test_table_{generate_random_alphanumeric().upper()}"
    snowpark_df = session.create_dataframe(ref_df)
    snowpark_df.write.save_as_table(
        test_table_name, mode="overwrite", table_type=table_type
    )

    # create snowpark pandas dataframe
    df = pd.read_snowflake(test_table_name)
    # Follow read_snowflake with a sort operation to ensure that ordering is stable and tests are not flaky.
    df = df.sort_values(df.columns.to_list())

    pandas_df = df.to_pandas()
    # verify to_pandas gives the same result for the same snowpark pandas
    # dataframe over different calls
    for _ in range(5):
        pandas_df_t = df.to_pandas()
        assert_frame_equal(pandas_df, pandas_df_t)


@pytest.mark.parametrize("pandas_label", VALID_PANDAS_LABELS)
@sql_count_checker(query_count=1)
def test_from_pandas_multiindex_on_column(pandas_label):
    index = pd.MultiIndex.from_tuples(
        [("baz", "A", 5), ("baz", pandas_label, "5"), ("zoo", "A", ""), ("zoo", "B", 0)]
    )
    check_result_from_and_to_pandas([[1, 2, 3, 4]], columns=index, check_dtype=False)


@sql_count_checker(query_count=2)
def test_from_pandas_series_with_tuple_name():
    # Constructing a DataFrame from a Series with tuple name should produce MultiIndex columns
    native_ser = native_pd.Series(name=("A", "B"))
    snow_ser = pd.Series(name=("A", "B"))
    assert_snowpark_pandas_equal_to_pandas(
        pd.DataFrame(native_ser),
        native_pd.DataFrame(native_ser),
    )
    assert_snowpark_pandas_equal_to_pandas(
        pd.DataFrame(snow_ser),
        native_pd.DataFrame(native_ser),
    )


@sql_count_checker(query_count=1)
def test_series_to_pandas():
    array = ["a", "b", "c"]
    pandas_series = native_pd.Series(data=array, index=array)
    snow_series = pd.Series(data=array, index=array)
    assert_series_equal(snow_series.to_pandas(), pandas_series)


@sql_count_checker(query_count=1, union_count=1)
def test_single_row_frame_to_series_to_pandas():
    # create a Snowpark pandas with single row
    native_df = native_pd.DataFrame(
        {"A": [0], "B": [1], "C": [2]}, index=native_pd.Index(["value"], name="index")
    )
    snow_df = pd.DataFrame(native_df)
    snow_series = pd.Series(query_compiler=snow_df._query_compiler)
    expected_series = native_df.squeeze()
    assert_series_equal(snow_series, expected_series, check_dtype=False)


@sql_count_checker(query_count=3)
def test_empty_variant_type_frame_to_pandas(session):
    # Tests type conversion of empty dataframes that have columns with the ARRAY and MAP types.
    # These dataframes must be constructed from pd.read_snowflake to have the correct type, since
    # using the pandas constructor would give them dtype=object
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.create_dataframe(
        [],
        schema=StructType().add("EMPTY_ARRAY", ArrayType()).add("EMPTY_MAP", MapType()),
    ).write.save_as_table(table_name, table_type="temp")
    df = pd.read_snowflake(table_name)
    native_df = native_pd.DataFrame(columns=["EMPTY_ARRAY", "EMPTY_MAP"], dtype=object)
    # Follow read_snowflake with a sort operation to ensure that ordering is stable and tests are not flaky.
    df = df.sort_values(df.columns.to_list())
    native_df = native_df.sort_values(native_df.columns.to_list())
    assert_frame_equal(
        df.to_pandas(),
        native_df,
        # Because the table has a __row_position__ column which is used as the index,
        # Snowpark pandas returns an empty Index(dtype="int64") instead of the expected RangeIndex
        check_index_type=False,
    )


@sql_count_checker(query_count=0)
def test_snowpark_pandas_statement_params():
    with patch.object(snowflake.snowpark.DataFrame, "to_pandas") as mock_to_pandas:
        pd.DataFrame({"a": [1, 2, 3]}).to_pandas()
        mock_to_pandas.assert_called_once()
        assert (
            "pandas"
            == mock_to_pandas.call_args.kwargs["statement_params"]["SNOWPARK_API"]
        )

    with patch.object(snowflake.snowpark.DataFrame, "to_pandas") as mock_to_pandas:
        pd.DataFrame({"a": [1, 2, 3]}).to_pandas(statement_params={"abc": "efg"})
        mock_to_pandas.assert_called_once()
        assert (
            "pandas"
            == mock_to_pandas.call_args.kwargs["statement_params"]["SNOWPARK_API"]
        )
        assert "efg" == mock_to_pandas.call_args.kwargs["statement_params"]["abc"]


@sql_count_checker(query_count=1, join_count=2)
def test_create_df_from_series():
    native_data = {
        "one": native_pd.Series([1, 2, 3], index=["a", "b", "c"]),
        "two": native_pd.Series([2, 3, 4, 5], index=["a", "b", "c", "d"]),
        "three": native_pd.Series([3, 4, 5], index=["b", "c", "d"]),
    }

    data = {
        "one": pd.Series([1, 2, 3], index=["a", "b", "c"]),
        "two": pd.Series([2, 3, 4, 5], index=["a", "b", "c", "d"]),
        "three": pd.Series([3, 4, 5], index=["b", "c", "d"]),
    }
    native_df = native_pd.DataFrame(native_data)
    snow_df = pd.DataFrame(data)

    assert_frame_equal(snow_df, native_df, check_dtype=False)
