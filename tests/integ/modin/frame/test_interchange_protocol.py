#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    eval_snowpark_pandas_result,
    create_test_dfs,
    assert_dicts_equal,
)
import re
from tests.integ.utils.sql_counter import sql_count_checker
import pandas.testing as tm
from pandas.core.interchange.from_dataframe import (
    categorical_column_to_series,
    string_column_to_ndarray,
    primitive_column_to_ndarray,
    datetime_column_to_ndarray,
)
from pytest import param
from enum import Enum, auto, unique
from pandas.api.interchange import from_dataframe
from pandas.core.interchange.dataframe_protocol import (
    DtypeKind,
    Column as InterchangeColumn,
)  # Import abstract definitions of the DataFrame and Column abstractions in the interchange protocol.
import numpy as np
from pandas.core.interchange.dataframe_protocol import DlpackDeviceType
import pandas._testing as pandas_internal_testing
from tests.integ.utils.sql_counter import SqlCounter

"""
To understand the tests in this file, it helps to reference the interchange
protocol, available in code [here](https://github.com/data-apis/dataframe-api/blob/7e76c386b0d9fda417eac13fbf08a4f73ef3437f/protocol/dataframe_protocol.py)
and in HTML [here](https://data-apis.org/dataframe-protocol/latest/API.html).

Paraphrasing the overview of concepts from the specifications there:

- A Buffer is a contiguous block of memory. It maps to a 1-D array and can be
  converted to NumPy, CuPy, etc.
- A Column has a single dtype. It can consist of multiple chunks. A single
  chunk of a Column (which may be the whole column if column.num_chunks() == 1)
  is modeled as again a Column instance. It contains 1 data buffer and
  (optionally) one mask for missing data.
- A DataFrame is an ordered collection of columns, which are identified with
  names that are unique strings. All the DataFrame's rows are the same length.
  It can consist of multiple chunks. A single chunk of a data frame is modeled
  as again a DataFrame instance.
"""


def single_chunk_column_to_pandas(column: InterchangeColumn) -> native_pd.Series:
    """
    Convert a single-chunk interchange protocol column to a native pandas Series.

    This method is nearly identical to
    native_pd.core.interchange.from_dataframe.protocol_df_chunk_to_pandas. The
    difference is that it takes as input a Column instead of a DataFrame object.
    We need to implement a method like this ourselves, because pandas does not
    provide a method to convert an interchange column to pandas.

    This method uses pandas methods like `primitive_column_to_ndarray`, which
    use the `data`, `validity`, and `offsets` attributes of the `Buffer`
    objects to fetch data and convert it to pandas.

    Parameters
    ----------
    column : InterchangeColumn
        The input column.

    Returns
    -------
    native_pd.Series
        The column as a pandas Series.
    """
    # The implementation here mostly follows
    # native_pd.core.interchange.from_dataframe.protocol_df_chunk_to_pandas
    dtype = column.dtype[0]
    if dtype in (
        DtypeKind.INT,
        DtypeKind.UINT,
        DtypeKind.FLOAT,
        DtypeKind.BOOL,
    ):
        data, buf = primitive_column_to_ndarray(column)
    elif dtype is DtypeKind.CATEGORICAL:
        data, buf = categorical_column_to_series(column)
    elif dtype is DtypeKind.STRING:
        data, buf = string_column_to_ndarray(column)
    elif dtype is DtypeKind.DATETIME:
        data, buf = datetime_column_to_ndarray(column)
    else:
        raise NotImplementedError(f"Data type {dtype} not handled yet")
    result = native_pd.Series(data)
    # We need to keep a pointer to `buf` to keep the memory that `result`
    # points to alive. We copy pandas's solution of storing the pointer to
    # `buf` in `attrs`.
    result.attrs["_INTERCHANGE_PROTOCOL_BUFFERS"] = buf
    return result


def column_to_pandas_series(column: InterchangeColumn) -> native_pd.Series:
    """
    Convert a column with one or more chunks to a native pandas Series.

    Parameters
    ----------
    column : InterchangeColumn
        The input column.

    Returns
    -------
    native_pd.Series
        The column as a pandas Series.
    """
    return native_pd.concat(
        (single_chunk_column_to_pandas(chunk) for chunk in column.get_chunks()),
        axis=0,
        ignore_index=True,
        copy=False,
    )


@unique
class TestingDf(Enum):
    """
    TestingDf represents an input dataframe for a test case.

    Dataframes are mutable, so we should return them from text fixtures instead
    of defining a list of dataframes that test cases can mutate. We give the
    dataframes labels in this enum, but create a new dataframe as required for
    each test case in the `pandas_df` fixture.
    """

    MIXED_TYPE = auto()
    EMPTY_NO_INDEX_OR_COLUMNS = auto()
    EMPTY_WITH_INDEX_BUT_NO_COLUMNS = auto()
    EMPTY_WITH_COLUMNS_BUT_NO_INDEX = auto()


@pytest.fixture
def pandas_df(request) -> native_pd.DataFrame:
    """
    Create a test dataframe matching the input value of TestingDf.
    """
    assert isinstance(request.param, TestingDf)
    if request.param is TestingDf.MIXED_TYPE:
        result = native_pd.DataFrame(
            {
                "int_col": [0, None, 2],
                0: [3, 4, 5],
                "string_col": ["string_0", "string_1", None],
                "datetime_col": [pd.Timestamp(0), pd.Timestamp(1), None],
                pd.Timestamp(0): [pd.Timestamp(2), pd.Timestamp(3), pd.Timestamp(4)],
            }
        )
        assert len(result.columns) == MIXED_TYPE_DF_WIDTH
        return result
    if request.param is TestingDf.EMPTY_NO_INDEX_OR_COLUMNS:
        return native_pd.DataFrame()
    if request.param is TestingDf.EMPTY_WITH_INDEX_BUT_NO_COLUMNS:
        return native_pd.DataFrame(index=["a", "b"])
    if request.param is TestingDf.EMPTY_WITH_COLUMNS_BUT_NO_INDEX:
        return native_pd.DataFrame(columns=["a", "b"])

    raise KeyError(f"Missing pandas test dataframe for {request.param}")


MIXED_TYPE_DF_WIDTH = 5


class TestDataFrame:
    """
    Test the API of the DataFrame class that we return from modin.pandas.DataFrame.__dataframe__()

    See the class specification here:
    https://github.com/data-apis/dataframe-api/blob/7e76c386b0d9fda417eac13fbf08a4f73ef3437f/protocol/dataframe_protocol.py#L362
    """

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("pandas_df", TestingDf, indirect=True)
    def test_metadata(self, pandas_df):
        # `metadata` holds the pandas index, i.e. row labels, which are not
        # part of the interchange protocol. pandas and Snowpark pandas need to
        # store the row labels as a separate field in the interchange object
        # so that from_dataframe() can recover the row labels from that field.
        eval_snowpark_pandas_result(
            *create_test_dfs(pandas_df),
            lambda df: df.__dataframe__().metadata,
            comparator=assert_dicts_equal,
        )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("pandas_df", TestingDf, indirect=True)
    def test_num_columns(self, pandas_df):
        eval_snowpark_pandas_result(
            *create_test_dfs(pandas_df),
            lambda df: df.__dataframe__().num_columns(),
            comparator=int.__eq__,
        )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("pandas_df", TestingDf, indirect=True)
    def test_num_rows(self, pandas_df):
        eval_snowpark_pandas_result(
            *create_test_dfs(pandas_df),
            lambda df: df.__dataframe__().num_rows(),
            comparator=int.__eq__,
        )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("pandas_df", TestingDf, indirect=True)
    def test_num_chunks(self, pandas_df):
        """
        Test that num_chunks() is a non-negative int equal to the length of get_chunks().

        The interchange protocol doesn't require any particular number of
        chunks, but num_chunks() should equal the number of chunks that
        the iterator returned by get_chunks() returns.
        """
        interchange = pd.DataFrame(pandas_df).__dataframe__()
        num_chunks = interchange.num_chunks()
        assert isinstance(num_chunks, int)
        assert num_chunks >= 0
        assert num_chunks == len(list(interchange.get_chunks()))

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("pandas_df", TestingDf, indirect=True)
    def test_column_names(self, pandas_df):
        eval_snowpark_pandas_result(
            *create_test_dfs(pandas_df),
            lambda df: df.__dataframe__().column_names(),
            comparator=tm.assert_index_equal,
        )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize(
        "get_column_by_name", [True, False], ids=["by_name", "by_id"]
    )
    @pytest.mark.parametrize("pandas_df", [TestingDf.MIXED_TYPE], indirect=True)
    @pytest.mark.parametrize("i", range(MIXED_TYPE_DF_WIDTH))
    def test_get_column(self, pandas_df, get_column_by_name, i):
        def column_getter(interchange_df):
            return (
                interchange_df.get_column_by_name(str(pandas_df.columns[i]))
                if get_column_by_name
                else interchange_df.get_column(i)
            )

        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                *create_test_dfs(pandas_df),
                lambda df: single_chunk_column_to_pandas(
                    column_getter(df.__dataframe__())
                ),
                comparator=tm.assert_series_equal,
            )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("pandas_df", TestingDf, indirect=True)
    def test_get_columns(self, pandas_df):
        def eval_columns_iterable(snow_result, pandas_result):
            snow_list = list(snow_result)
            pandas_list = list(pandas_result)
            assert len(snow_list) == len(
                pandas_list
            ), "Snowflake columns and pandas columns have different lengths"
            for i, (snow_column, pandas_column) in enumerate(
                zip(snow_list, pandas_list)
            ):
                try:
                    tm.assert_series_equal(
                        single_chunk_column_to_pandas(snow_column),
                        single_chunk_column_to_pandas(pandas_column),
                    )
                except AssertionError as error:
                    raise AssertionError(
                        f"Columns at position {i} were not equal"
                    ) from error

        eval_snowpark_pandas_result(
            *create_test_dfs(pandas_df),
            lambda df: df.__dataframe__().get_columns(),
            comparator=eval_columns_iterable,
        )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize(
        "indices",
        [
            (0,),
            [],
            (0, 1, 2, 3, 4),
            [4, 0],
        ],
    )
    @pytest.mark.parametrize("pandas_df", [TestingDf.MIXED_TYPE], indirect=True)
    @pytest.mark.parametrize(
        "select_columns_by_name", [True, False], ids=["by_name", "by_id"]
    )
    @sql_count_checker(query_count=1)
    def test_select_columns(self, pandas_df, indices, select_columns_by_name):
        def selector(interchange_df):
            return (
                interchange_df.select_columns_by_name(
                    list(pandas_df.columns[list(indices)].map(str))
                )
                if select_columns_by_name
                else interchange_df.select_columns(indices)
            )

        eval_snowpark_pandas_result(
            *create_test_dfs(pandas_df),
            # select_columns() and select_columns_by_name() return another
            # interchange DataFrame object. Check that converting df to
            # native pandas via the interchange protocol gives the same result
            # whether `df` is a native pandas dataframe or a Snowpark pandas
            # dataframe.
            lambda df: from_dataframe(selector(df.__dataframe__())),
            comparator=tm.assert_frame_equal,
        )

    @pytest.mark.parametrize("pandas_df", TestingDf, indirect=True)
    @sql_count_checker(query_count=1)
    def test_get_chunks(self, pandas_df):
        # Build a pandas dataframe by converting each chunk of the dataframe
        # to pandas and concatenating the results.
        pandas_dfs = []
        for chunk in pd.DataFrame(pandas_df).__dataframe__().get_chunks():
            pandas_dfs.append(from_dataframe(chunk))
        if len(pandas_dfs) == 1:
            chunks_result = pandas_dfs[0]
        else:
            chunks_result = native_pd.concat(
                pandas_dfs, axis=0, ignore_index=True, copy=False
            )
        # Check that the resulting pandas dataframe is equal to what we would
        # get by converting the pandas dataframe to an interchange dataframe
        # and then converting it back to pandas.
        tm.assert_frame_equal(chunks_result, from_dataframe(pandas_df.__dataframe__()))

    @pytest.mark.parametrize("pandas_df", TestingDf, indirect=True)
    @pytest.mark.parametrize("nan_as_null", [True, False])
    @sql_count_checker(query_count=1)
    def test_nan_as_null(self, pandas_df, nan_as_null):
        """
        Test the nan_as_null parameter to the __dataframe__ method.

        The interchange protocol has deprecated this parameter, which is no
        longer supposed to have any effect. Just check that we match pandas
        in a trip to the interchange protocol and then to pandas.
        """
        eval_snowpark_pandas_result(
            *create_test_dfs(pandas_df),
            lambda df: from_dataframe(df.__dataframe__(nan_as_null=nan_as_null)),
            comparator=tm.assert_frame_equal,
        )

    @pytest.mark.parametrize("pandas_df", [TestingDf.MIXED_TYPE], indirect=True)
    def test_allow_copy_false(self, pandas_df):
        """
        Test the allow_copy parameter to the __dataframe__ method.

        Snowpark pandas should never copy the data that each interchange object
        stores because it creates a new pandas dataframe object for each
        __dataframe__() call. It's difficult to test that we would never make a
        copy if we have `allow_copy=False`, so we check that if we call
        `__dataframe__()` twice on the same dataframe, we get pointers to 2
        different locations in memory.
        """
        modin_df = pd.DataFrame(pandas_df)
        with SqlCounter(query_count=1):
            interchange1 = modin_df.__dataframe__(allow_copy=False)
        with SqlCounter(query_count=1):
            interchange2 = modin_df.__dataframe__(allow_copy=False)

        def get_buffer_memory_pointer(interchange_dataframe):
            column = interchange_dataframe.get_column(0)
            chunks = list(column.get_chunks())
            assert len(chunks) == 1
            buffer, _ = column.get_buffers()["data"]
            return buffer.ptr

        assert get_buffer_memory_pointer(interchange1) != get_buffer_memory_pointer(
            interchange2
        )


@pytest.mark.parametrize("pandas_df", [TestingDf.MIXED_TYPE], indirect=True)
class TestColumn:
    """
    Test the API of the interchange column class.

    See the class specification here: https://github.com/data-apis/dataframe-api/blob/7e76c386b0d9fda417eac13fbf08a4f73ef3437f/protocol/dataframe_protocol.py#L172
    """

    @sql_count_checker(query_count=1)
    def test_size(self, pandas_df):
        eval_snowpark_pandas_result(
            *create_test_dfs(pandas_df),
            lambda df: df.__dataframe__().get_column(0).size(),
            comparator=int.__eq__,
        )

    @sql_count_checker(query_count=1)
    def test_offset(self, pandas_df):
        """
        Test that the column offset is a positive integer.

        We don't need the offset to be any value in particular, as long as we
        can read the data we expect at that offset using
        column_to_pandas_series(). We test our ability to read the column in
        TestDataFrame.test_get_column, among other test cases.
        """
        offset = pd.DataFrame(pandas_df).__dataframe__().get_column(0).offset
        assert isinstance(offset, int)
        assert offset >= 0

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("i", range(MIXED_TYPE_DF_WIDTH))
    def test_dtype(self, pandas_df, i):
        eval_snowpark_pandas_result(
            *create_test_dfs(pandas_df),
            lambda df: df.__dataframe__().get_column(i).dtype,
            comparator=tuple.__eq__,
        )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("i", range(MIXED_TYPE_DF_WIDTH))
    def test_describe_null(self, pandas_df, i):
        eval_snowpark_pandas_result(
            *create_test_dfs(pandas_df),
            lambda df: df.__dataframe__().get_column(i).describe_null,
            comparator=tuple.__eq__,
        )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("i", range(MIXED_TYPE_DF_WIDTH))
    def test_null_count(self, pandas_df, i):
        eval_snowpark_pandas_result(
            *create_test_dfs(pandas_df),
            lambda df: df.__dataframe__().get_column(i).null_count,
            comparator=int.__eq__,
        )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("i", range(MIXED_TYPE_DF_WIDTH))
    def test_metadata(self, pandas_df, i):
        eval_snowpark_pandas_result(
            *create_test_dfs(pandas_df),
            lambda df: df.__dataframe__().get_column(i).metadata,
            comparator=assert_dicts_equal,
        )

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("i", range(MIXED_TYPE_DF_WIDTH))
    def test_num_chunks(self, pandas_df, i):
        """
        Test that num_chunks() is a positive int that is equal to the length of get_chunks().

        We don't need the column to have any particular number of chunks, as
        long as converting the column to pandas with column_to_pandas_series()
        gives the data we expect. We test our ability to read the column in
        TestDataFrame.test_get_column, among other test cases.
        """
        column = pd.DataFrame(pandas_df).__dataframe__().get_column(i)
        num_chunks = column.num_chunks()
        assert isinstance(num_chunks, int)
        assert num_chunks >= 0
        assert num_chunks == len(list(column.get_chunks()))


@pytest.mark.parametrize("pandas_df", [TestingDf.MIXED_TYPE], indirect=True)
class TestBuffer:
    """
    Test the API of the interchange Buffer class.

    See the class specification here:
    https://github.com/data-apis/dataframe-api/blob/7e76c386b0d9fda417eac13fbf08a4f73ef3437f/protocol/dataframe_protocol.py#L116
    """

    @sql_count_checker(query_count=1)
    def test_bufsize(self, pandas_df):
        """
        Check that bufsize is a positive integer.

        In other test cases, we use `single_chunk_column_to_pandas` to test
        that that we can use `bufsize`, along with other attributes of each
        buffer, to read the data that we expect.
        """
        bufsize = (
            pd.DataFrame(pandas_df)
            .__dataframe__()
            .get_column(0)
            .get_buffers()["data"][0]
            .bufsize
        )
        assert isinstance(bufsize, int)
        assert bufsize >= 0

    @sql_count_checker(query_count=1)
    def test_ptr(self, pandas_df):
        """
        Check that ptr is a positive integer.

        In other test cases, we use `single_chunk_column_to_pandas` to test
        that that we can use `ptr`, along with other attributes of each buffer,
        to read the data that we expect.
        """
        ptr = (
            pd.DataFrame(pandas_df)
            .__dataframe__()
            .get_column(0)
            .get_buffers()["data"][0]
            .ptr
        )
        assert isinstance(ptr, int)
        assert ptr >= 0

    @sql_count_checker(query_count=1)
    def test___dlpack__(self, pandas_df):
        """
        Test that the buffer implements the Python array API standard's __dlpack__ method.

        We test that we can convert a chunk of interchange dataframe data to
        numpy with np.from_dlpack and get a numpy array that matches pandas.

        See the __dlpack__ specification here:
        https://github.com/data-apis/array-api/blob/6d205d72dde3db8fc8668ad6aef5d003cc8ef80f/src/array_api_stubs/_draft/array_object.py#L296
        """
        eval_snowpark_pandas_result(
            *create_test_dfs(pandas_df),
            lambda df: np.from_dlpack(
                df.__dataframe__().get_column(0).get_buffers()["data"][0]
            ),
            comparator=pandas_internal_testing.assert_numpy_array_equal,
        )

    @sql_count_checker(query_count=1)
    def test__dlpack_device__(self, pandas_df):
        """
        Test that the buffer implements the Python array API standard's __dlpack_device__().

        Check that the results of __dlpack_device__ match those of pandas.

        See the __dlpack_device__() specification here:
        https://github.com/data-apis/array-api/blob/6d205d72dde3db8fc8668ad6aef5d003cc8ef80f/src/array_api_stubs/_draft/array_object.py#L469
        """

        def get_device_type_and_id(df):
            return (
                df.__dataframe__()
                .get_column(0)
                .get_buffers()["data"][0]
                .__dlpack_device__()
            )

        snow_device_type, snow_device_id = get_device_type_and_id(
            pd.DataFrame(pandas_df)
        )
        pandas_device_type, pandas_device_id = get_device_type_and_id(pandas_df)
        assert snow_device_id is None
        assert pandas_device_id is None
        assert snow_device_type is DlpackDeviceType.CPU
        assert pandas_device_type is DlpackDeviceType.CPU


@pytest.mark.parametrize(
    "columns",
    [
        param([0, "0"], id="int_0_and_string_0"),
        param([0, 0], id="duplicate_int_column"),
        param(["0", "0"], id="duplicate_string_column"),
    ],
)
@sql_count_checker(query_count=1)
def test_conflicting_string_names(columns):
    """
    If a dataframe has two columns whose labels are equal when converted to
    string, converting the dataframe to pandas with from_dataframe() raises
    an exception in both native pandas and Snowpark pandas. The interchange
    protocol only allows string names and cannot handle duplicate column names.
    """
    eval_snowpark_pandas_result(
        *create_test_dfs(native_pd.DataFrame([[0, 1]], columns=columns)),
        lambda df: from_dataframe(df.__dataframe__()),
        expect_exception=True,
        expect_exception_type=TypeError,
        expect_exception_match=re.escape(
            "Expected a Series, got a DataFrame. This likely "
            + "happened because you called __dataframe__ on a DataFrame "
            + "which, after converting column names to string, resulted in "
            + f"duplicated names: {repr(native_pd.Index(columns).map(str))}. "
            + "Please rename these columns before using the interchange "
            + "protocol."
        ),
    )


@sql_count_checker(query_count=1)
def test_list_column_dtype():
    eval_snowpark_pandas_result(
        *create_test_dfs([[list("list_item")]]),
        lambda df: from_dataframe(df.__dataframe__()),
        expect_exception=True,
        expect_exception_type=NotImplementedError,
        expect_exception_match=re.escape(
            "Non-string object dtypes are not supported yet"
        ),
    )


@sql_count_checker(query_count=1)
def test_timedelta_dtype():
    eval_snowpark_pandas_result(
        *create_test_dfs([[pd.Timedelta(1)]]),
        lambda df: from_dataframe(df.__dataframe__()),
        expect_exception=True,
        expect_exception_type=NotImplementedError,
        expect_exception_match=re.escape(
            "Conversion of timedelta64[ns] to Arrow C format string is not "
            + "implemented."
        ),
    )
