#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import re
from datetime import datetime

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pandas._testing as tm
import pytest
from modin.pandas import Index, MultiIndex, Series

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_index_equal, assert_series_equal
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import running_on_public_ci


class TestRename:
    @pytest.fixture(scope="function")
    def snow_datetime_series(self, datetime_series):
        return pd.Series(datetime_series)

    def test_rename(self, snow_datetime_series):
        ts = snow_datetime_series

        def renamer(x):
            return x.strftime("%Y%m%d")

        with SqlCounter(query_count=0):
            msg = "Snowpark pandas rename API doesn't yet support callable mapper"
            with pytest.raises(NotImplementedError, match=msg):
                ts.rename(renamer)

        # dict
        with SqlCounter(query_count=3, join_count=1):
            renamed = ts.to_pandas().rename(renamer)
            rename_dict = dict(zip(ts.index, renamed.index))
            renamed2 = ts.rename(rename_dict)
            # Note: renaming index with dict on Snowflake will use variant as the new data type if rename includes type
            # change, e.g., here the dict turns datetime values into strings. When pulling the variant index out, the string
            # values in the variant column will be quoted
            assert_index_equal(renamed.index, renamed2.index.str.replace('"', ""))

    @sql_count_checker(query_count=1, join_count=1)
    def test_rename_partial_dict(self):
        # partial dict
        ser = Series(np.arange(4), index=["a", "b", "c", "d"], dtype="int64")
        renamed = ser.rename({"b": "foo", "d": "bar"})
        assert_index_equal(renamed.index, native_pd.Index(["a", "foo", "c", "bar"]))

    @sql_count_checker(query_count=0)
    def test_rename_retain_index_name(self):
        # index with name
        renamer = Series(
            np.arange(4),
            index=pd.Index(["a", "b", "c", "d"], name="name"),
            dtype="int64",
        )
        renamed = renamer.rename({})
        assert renamed.index.name == renamer.index.name

    @sql_count_checker(query_count=2, join_count=1)
    def test_rename_by_series(self):
        ser = Series(range(5), name="foo")
        renamer = Series({1: 10, 2: 20})
        result = ser.rename(renamer)
        expected = Series(range(5), index=[0, 10, 20, 3, 4], name="foo")
        assert_series_equal(result, expected)

    def test_rename_set_name(self):
        ser = Series(range(4), index=list("abcd"))
        for name in ["foo", 123, 123.0, datetime(2001, 11, 11), ("foo",)]:
            with SqlCounter(query_count=2):
                result = ser.rename(name)
                assert result.name == name
                tm.assert_numpy_array_equal(result.index.values, ser.index.values)
                assert ser.name is None

    @sql_count_checker(query_count=5)
    def test_rename_set_name_inplace(self):
        ser = Series(range(3), index=list("abc"))
        for name in ["foo", 123, 123.0, datetime(2001, 11, 11), ("foo",)]:
            ser.rename(name, inplace=True)
            assert ser.name == name

            exp = np.array(["a", "b", "c"], dtype=np.object_)
            tm.assert_numpy_array_equal(ser.index.values, exp)

    @sql_count_checker(query_count=0)
    def test_rename_axis_supported(self):
        # Supporting axis for compatibility, detailed in GH-18589
        ser = Series(range(5))
        ser.rename({}, axis=0)
        ser.rename({}, axis="index")

        with pytest.raises(ValueError, match="No axis named 5"):
            ser.rename({}, axis=5)

    @sql_count_checker(query_count=0)
    def test_rename_inplace(self, snow_datetime_series):
        def renamer(x):
            return x.strftime("%Y%m%d")

        msg = "Snowpark pandas rename API doesn't yet support callable mapper"
        with pytest.raises(NotImplementedError, match=msg):
            snow_datetime_series.rename(renamer, inplace=True)

    @sql_count_checker(query_count=0)
    def test_rename_with_custom_indexer(self):
        # GH 27814
        class MyIndexer:
            pass

        ix = MyIndexer()
        ser = Series([1, 2, 3]).rename(ix)
        assert ser.name is ix

    @sql_count_checker(query_count=0)
    def test_rename_with_custom_indexer_inplace(self):
        # GH 27814
        class MyIndexer:
            pass

        ix = MyIndexer()
        ser = Series([1, 2, 3])
        ser.rename(ix, inplace=True)
        assert ser.name is ix

    @sql_count_checker(query_count=0)
    def test_rename_callable(self):
        # GH 17407
        ser = Series(range(1, 6), index=Index(range(2, 7), name="IntIndex"))
        msg = "Snowpark pandas rename API doesn't yet support callable mapper"
        with pytest.raises(NotImplementedError, match=msg):
            ser.rename(str)

    @sql_count_checker(query_count=2)
    def test_rename_none(self):
        # GH 40977
        ser = Series([1, 2], name="foo")
        result = ser.rename(None)
        expected = Series([1, 2])
        assert_series_equal(result, expected)

    # TODO: will reenable this test once MI support is ready.
    @pytest.mark.skip(reason="TODO: SNOW-841607 support multiindex in join_utils.join")
    def test_rename_series_with_multiindex(self):
        # issue #43659
        arrays = [
            ["bar", "baz", "baz", "foo", "qux"],
            ["one", "one", "two", "two", "one"],
        ]

        index = MultiIndex.from_arrays(arrays, names=["first", "second"])
        ser = Series(np.ones(5), index=index)
        # Note: it seems a bug that if the index is a series, pands return KeyError: "['yes'] not found in axis"
        result = ser.rename(index={"one": "yes"}, level="second", errors="raise")

        arrays_expected = [
            ["bar", "baz", "baz", "foo", "qux"],
            ["yes", "yes", "two", "two", "yes"],
        ]

        index_expected = MultiIndex.from_arrays(
            arrays_expected, names=["first", "second"]
        )
        series_expected = Series(np.ones(5), index=index_expected)

        assert_series_equal(result, series_expected)

    @sql_count_checker(query_count=2, join_count=1)
    def test_rename_error_arg(self):
        # GH 46889
        ser = Series(["foo", "bar"])
        match = re.escape("[2] not found in axis")
        with pytest.raises(KeyError, match=match):
            ser.rename({2: 9}, errors="raise")

    @pytest.mark.skipif(running_on_public_ci(), reason="slow test")
    @sql_count_checker(query_count=8, join_count=12)
    def test_rename_copy_false(self):
        # GH 46889
        ser = Series(["foo", "bar"])
        shallow_copy = ser.rename({1: 9}, copy=False)
        # copy=False is ignored by Snowpark pandas; in pandas, ser[0] will be "foobar"
        # TODO: SNOW-917761 implement ser[0]
        assert ser[0] == shallow_copy[0]
        assert ser[1] == shallow_copy[9]

    @sql_count_checker(query_count=0)
    def test_rename_copy_warning(self, caplog):
        caplog.at_level(logging.WARNING)
        ser = Series(["foo", "bar"])
        msg = "The argument `copy` of `series.rename` has been ignored by Snowpark pandas API"
        caplog.clear()
        ser.rename("test")
        assert msg not in caplog.text

        ser.rename("test", copy=False)
        assert msg in caplog.text
