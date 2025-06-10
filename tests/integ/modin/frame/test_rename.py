#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import inspect
import logging
from collections import ChainMap

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from modin.pandas import DataFrame, Index, MultiIndex, Series

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_frame_equal,
    assert_index_equal,
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


class TestRename:
    @sql_count_checker(query_count=0)
    def test_rename_signature(self):
        sig = inspect.signature(DataFrame.rename)
        parameters = set(sig.parameters)
        assert parameters == {
            "self",
            "mapper",
            "index",
            "columns",
            "axis",
            "inplace",
            "copy",
            "level",
            "errors",
        }

    @pytest.mark.parametrize("klass", [Series, DataFrame])
    def test_rename_mi(self, klass):
        obj = klass(
            [11, 21, 31],
            index=MultiIndex.from_tuples([("A", x) for x in ["a", "B", "c"]]),
        )
        msg = "Snowpark pandas rename API is not yet supported for multi-index objects"
        if klass == DataFrame:
            with SqlCounter(query_count=0):
                with pytest.raises(NotImplementedError, match=msg):
                    obj.rename(["A"])
        else:
            with SqlCounter(query_count=2):
                native_obj = obj.to_pandas()
                eval_snowpark_pandas_result(obj, native_obj, lambda x: x.rename("A"))

    @pytest.fixture(scope="function")
    def snow_float_frame(self, float_frame):
        return pd.DataFrame(float_frame)

    def test_rename(self, snow_float_frame):
        mapping = {"A": "a", "B": "b", "C": "c", "D": "d"}

        renamed = snow_float_frame.rename(columns=mapping)
        renamed2 = snow_float_frame.rename(columns=str.lower)

        with SqlCounter(query_count=4):
            assert_frame_equal(renamed, renamed2)
            assert_frame_equal(
                renamed2.rename(columns=str.upper), snow_float_frame, check_names=False
            )

        # index
        data = {"A": {"foo": 0, "bar": 1}}

        # gets sorted alphabetical
        # pandas 2.2.1 behavior, this is no longer the that sort index
        # is called automatically as with pandas 2.1.4
        # Pandas Change:
        # https://github.com/pandas-dev/pandas/pull/55696
        with SqlCounter(query_count=2):
            df = DataFrame(data)
            assert_index_equal(df.index, DataFrame(data).index)

        with SqlCounter(query_count=1, join_count=1):
            renamed = df.rename(index={"foo": "foo2", "bar": "bar2"})
            assert_index_equal(renamed.index, native_pd.Index(["foo2", "bar2"]))

        # have to pass something
        with SqlCounter(query_count=0):
            with pytest.raises(TypeError, match="must pass an index to rename"):
                snow_float_frame.rename()

        # partial columns
        with SqlCounter(query_count=0):
            renamed = snow_float_frame.rename(columns={"C": "foo", "D": "bar"})
            assert_index_equal(
                renamed.columns, native_pd.Index(["A", "B", "foo", "bar"])
            )

        # other axis
        with SqlCounter(query_count=1, join_count=1):
            renamed = snow_float_frame.T.rename(index={"C": "foo", "D": "bar"})
            assert_index_equal(renamed.index, native_pd.Index(["A", "B", "foo", "bar"]))

        # index with name
        # Two extra queries, one for converting to native pandas in renamer Dataframe constructor, one to get the name
        with SqlCounter(query_count=2, join_count=1):
            index = Index(["foo", "bar"], name="name")
            renamer = DataFrame(data, index=index)
            renamed = renamer.rename(index={"foo": "bar", "bar": "foo"})
            assert_index_equal(
                renamed.index, native_pd.Index(["bar", "foo"], name="name")
            )
            assert renamed.index.name == renamer.index.name

    @sql_count_checker(query_count=0)
    def test_rename_str_upper_not_implemented(self):
        data = {"A": {"foo": 0, "bar": 1}}
        df = DataFrame(data)
        msg = "Snowpark pandas rename API doesn't yet support callable mapper"
        with pytest.raises(NotImplementedError, match=msg):
            df.rename(index=str.upper)

    @pytest.mark.parametrize(
        "args,kwargs",
        [
            ((ChainMap({"A": "a"}, {"B": "b"}),), {"axis": "columns"}),
            ((), {"columns": ChainMap({"A": "a"}, {"B": "b"})}),
        ],
    )
    @sql_count_checker(query_count=1)
    def test_rename_chainmap(self, args, kwargs):
        # see gh-23859
        colAData = range(1, 11)
        colBdata = np.random.randn(10)

        df = DataFrame({"A": colAData, "B": colBdata})
        result = df.rename(*args, **kwargs)

        expected = native_pd.DataFrame({"a": colAData, "b": colBdata})
        assert_frame_equal(result, expected, check_dtype=False, check_index_type=False)

    @sql_count_checker(query_count=0)
    def test_rename_multiindex_with_level(self):
        tuples_index = [("foo1", "bar1"), ("foo2", "bar2")]
        tuples_columns = [("fizz1", "buzz1"), ("fizz2", "buzz2")]
        index = MultiIndex.from_tuples(tuples_index, names=["foo", "bar"])
        columns = MultiIndex.from_tuples(tuples_columns, names=["fizz", "buzz"])
        df = DataFrame([(0, 0), (1, 1)], index=index, columns=columns)

        new_columns = MultiIndex.from_tuples(
            [("fizz3", "buzz1"), ("fizz2", "buzz2")], names=["fizz", "buzz"]
        )
        renamed = df.rename(columns={"fizz1": "fizz3", "buzz2": "buzz3"}, level=0)
        assert_index_equal(renamed.columns, new_columns)
        renamed = df.rename(columns={"fizz1": "fizz3", "buzz2": "buzz3"}, level="fizz")
        assert_index_equal(renamed.columns, new_columns)

        new_columns = MultiIndex.from_tuples(
            [("fizz1", "buzz1"), ("fizz2", "buzz3")], names=["fizz", "buzz"]
        )
        renamed = df.rename(columns={"fizz1": "fizz3", "buzz2": "buzz3"}, level=1)
        assert_index_equal(renamed.columns, new_columns)
        renamed = df.rename(columns={"fizz1": "fizz3", "buzz2": "buzz3"}, level="buzz")
        assert_index_equal(renamed.columns, new_columns)

        # function
        func = str.upper
        new_columns = MultiIndex.from_tuples(
            [("FIZZ1", "buzz1"), ("FIZZ2", "buzz2")], names=["fizz", "buzz"]
        )
        renamed = df.rename(columns=func, level=0)
        assert_index_equal(renamed.columns, new_columns)
        renamed = df.rename(columns=func, level="fizz")
        assert_index_equal(renamed.columns, new_columns)

        new_columns = MultiIndex.from_tuples(
            [("fizz1", "BUZZ1"), ("fizz2", "BUZZ2")], names=["fizz", "buzz"]
        )
        renamed = df.rename(columns=func, level=1)
        assert_index_equal(renamed.columns, new_columns)
        renamed = df.rename(columns=func, level="buzz")
        assert_index_equal(renamed.columns, new_columns)

    @sql_count_checker(query_count=0)
    def test_rename_multiindex_not_implemented(self):
        tuples_index = [("foo1", "bar1"), ("foo2", "bar2")]
        tuples_columns = [("fizz1", "buzz1"), ("fizz2", "buzz2")]
        index = MultiIndex.from_tuples(tuples_index, names=["foo", "bar"])
        columns = MultiIndex.from_tuples(tuples_columns, names=["fizz", "buzz"])
        df = DataFrame([(0, 0), (1, 1)], index=index, columns=columns)
        # without specifying level -> across all levels

        with pytest.raises(NotImplementedError):
            df.rename(
                index={"foo1": "foo3", "bar2": "bar3"},
                columns={"fizz1": "fizz3", "buzz2": "buzz3"},
            )

        # specifying level
        with pytest.raises(NotImplementedError):
            df.rename(index={"foo1": "foo3", "bar2": "bar3"}, level=0)

    @sql_count_checker(query_count=2)
    def test_rename_nocopy(self, snow_float_frame):
        renamed = snow_float_frame.rename(columns={"C": "foo"}, copy=False)
        # copy=False is ignored in Snowpark pandas
        assert not np.shares_memory(
            renamed["foo"].to_pandas()._values,
            snow_float_frame["C"].to_pandas()._values,
        )

    @sql_count_checker(query_count=0)
    def test_rename_inplace(self, snow_float_frame):
        snow_float_frame.rename(columns={"C": "foo"})
        assert "C" in snow_float_frame
        assert "foo" not in snow_float_frame

        c_values = snow_float_frame["C"]
        snow_float_frame = snow_float_frame.copy()
        return_value = snow_float_frame.rename(columns={"C": "foo"}, inplace=True)
        assert return_value is None

        assert "C" not in snow_float_frame
        assert "foo" in snow_float_frame
        # GH 44153
        # Used to be id(float_frame["foo"]) != c_id, but flaky in the CI
        assert snow_float_frame["foo"] is not c_values

    @sql_count_checker(query_count=1)
    def test_rename_bug(self):
        # GH 5344
        # rename set ref_locs, and set_index was not resetting
        df = DataFrame({0: ["foo", "bar"], 1: ["bah", "bas"], 2: [1, 2]})
        df = df.rename(columns={0: "a"})
        df = df.rename(columns={1: "b"})
        df = df.set_index(["a", "b"])
        df.columns = ["2001-01-01"]
        expected = native_pd.DataFrame(
            [[1], [2]],
            index=MultiIndex.from_tuples(
                [("foo", "bah"), ("bar", "bas")], names=["a", "b"]
            ),
            columns=["2001-01-01"],
        )
        assert_frame_equal(df, expected, check_dtype=False, check_index_type=False)

    @pytest.mark.skip(reason="TODO: SNOW-841607 support rename multiindex dataframe")
    def test_rename_bug2(self):
        # GH 19497
        # rename was changing Index to MultiIndex if Index contained tuples

        df = DataFrame(data=np.arange(3), index=[(0, 0), (1, 1), (2, 2)], columns=["a"])
        # note that this won't work in sp fallback because in fallback, we need to call to_pandas() which will turn
        # variant into string, then the rename below won't find the right original key
        df = df.rename({(1, 1): (5, 4)}, axis="index")
        expected = native_pd.DataFrame(
            data=np.arange(3), index=[(0, 0), (5, 4), (2, 2)], columns=["a"]
        )
        assert_index_equal(df.index, expected.index)
        assert_frame_equal(df, expected, check_dtype=False, check_index_type=False)

    @sql_count_checker(query_count=0)
    def test_rename_errors_raises(self):
        df = DataFrame(columns=["A", "B", "C", "D"])
        with pytest.raises(KeyError, match="'E'] not found in axis"):
            df.rename(columns={"A": "a", "E": "e"}, errors="raise")

    @pytest.mark.parametrize(
        "mapper, errors, expected_columns",
        [
            ({"A": "a", "E": "e"}, "ignore", ["a", "B", "C", "D"]),
            ({"A": "a"}, "raise", ["a", "B", "C", "D"]),
            (str.lower, "raise", ["a", "b", "c", "d"]),
        ],
    )
    @sql_count_checker(query_count=1)
    def test_rename_errors(self, mapper, errors, expected_columns):
        # GH 13473
        # rename now works with errors parameter
        df = DataFrame(columns=["A", "B", "C", "D"])
        result = df.rename(columns=mapper, errors=errors)
        expected = native_pd.DataFrame(columns=expected_columns)
        assert_frame_equal(result, expected, check_dtype=False, check_index_type=False)

    @pytest.fixture(scope="function")
    def snow_float_string_frame(self, float_string_frame):
        return pd.DataFrame(float_string_frame)

    @sql_count_checker(query_count=0)
    def test_rename_objects(self, snow_float_string_frame):
        renamed = snow_float_string_frame.rename(columns=str.upper)

        assert "FOO" in renamed
        assert "foo" not in renamed

    @sql_count_checker(query_count=6, join_count=2)
    def test_rename_axis_style(self):
        # https://github.com/pandas-dev/pandas/issues/12392
        df = DataFrame({"A": [1, 2], "B": [1, 2]}, index=["X", "Y"])
        expected = native_pd.DataFrame({"a": [1, 2], "b": [1, 2]}, index=["X", "Y"])

        result = df.rename(str.lower, axis=1)
        assert_frame_equal(result, expected, check_dtype=False, check_index_type=False)

        result = df.rename(str.lower, axis="columns")
        assert_frame_equal(result, expected, check_dtype=False, check_index_type=False)

        result = df.rename({"A": "a", "B": "b"}, axis=1)
        assert_frame_equal(result, expected, check_dtype=False, check_index_type=False)

        result = df.rename({"A": "a", "B": "b"}, axis="columns")
        assert_frame_equal(result, expected, check_dtype=False, check_index_type=False)

        # Index
        expected = native_pd.DataFrame({"A": [1, 2], "B": [1, 2]}, index=["x", "y"])
        result = df.rename({"X": "x", "Y": "y"}, axis=0)
        assert_frame_equal(result, expected, check_dtype=False, check_index_type=False)

        result = df.rename({"X": "x", "Y": "y"}, axis="index")
        assert_frame_equal(result, expected, check_dtype=False, check_index_type=False)

    @sql_count_checker(query_count=0)
    def test_rename_axis_style_not_implemented(self):
        df = DataFrame({"A": [1, 2], "B": [1, 2]}, index=["X", "Y"])
        msg = "Snowpark pandas rename API doesn't yet support callable mapper"
        with pytest.raises(NotImplementedError, match=msg):
            df.rename(str.lower, axis=0)
        with pytest.raises(NotImplementedError, match=msg):
            df.rename(str.lower, axis="index")
        with pytest.raises(NotImplementedError, match=msg):
            df.rename(mapper=str.lower, axis="index")

    @sql_count_checker(query_count=0)
    def test_rename_mapper_multi(self):
        df = DataFrame({"A": ["a", "b"], "B": ["c", "d"], "C": [1, 2]}).set_index(
            ["A", "B"]
        )
        msg = "Snowpark pandas rename API is not yet supported for multi-index objects"
        with pytest.raises(NotImplementedError, match=msg):
            df.rename(["X"])

    @sql_count_checker(query_count=0)
    def test_rename_positional_named(self):
        # https://github.com/pandas-dev/pandas/issues/12392
        df = DataFrame({"a": [1, 2], "b": [1, 2]}, index=["X", "Y"])
        msg = "Snowpark pandas rename API doesn't yet support callable mapper"
        with pytest.raises(NotImplementedError, match=msg):
            df.rename(index=str.lower, columns=str.upper)

    @sql_count_checker(query_count=0)
    def test_rename_axis_style_raises(self):
        # see gh-12392
        df = DataFrame({"A": [1, 2], "B": [1, 2]}, index=["0", "1"])

        # Named target and axis
        over_spec_msg = "Cannot specify both 'axis' and any of 'index' or 'columns'"
        with pytest.raises(TypeError, match=over_spec_msg):
            df.rename(index=str.lower, axis=1)

        with pytest.raises(TypeError, match=over_spec_msg):
            df.rename(index=str.lower, axis="columns")

        with pytest.raises(TypeError, match=over_spec_msg):
            df.rename(columns=str.lower, axis="columns")

        with pytest.raises(TypeError, match=over_spec_msg):
            df.rename(index=str.lower, axis=0)

        # Multiple targets and axis
        with pytest.raises(TypeError, match=over_spec_msg):
            df.rename(str.lower, index=str.lower, axis="columns")

        # Too many targets
        over_spec_msg = "Cannot specify both 'mapper' and any of 'index' or 'columns'"
        with pytest.raises(TypeError, match=over_spec_msg):
            df.rename(str.lower, index=str.lower, columns=str.lower)

        # Duplicates
        with pytest.raises(TypeError, match="multiple values"):
            df.rename(id, mapper=id)

    @sql_count_checker(query_count=0)
    def test_rename_positional_raises(self):
        # GH 29136
        df = DataFrame(columns=["A", "B"])
        msg = r"positional arguments"

        with pytest.raises(TypeError, match=msg):
            df.rename(None, str.lower)

    @sql_count_checker(query_count=0)
    def test_rename_no_mappings_raises(self):
        # GH 29136
        df = DataFrame([[1]])
        msg = "must pass an index to rename"
        with pytest.raises(TypeError, match=msg):
            df.rename()

        with pytest.raises(TypeError, match=msg):
            df.rename(None, index=None)

        with pytest.raises(TypeError, match=msg):
            df.rename(None, columns=None)

        with pytest.raises(TypeError, match=msg):
            df.rename(None, columns=None, index=None)

    @sql_count_checker(query_count=0)
    def test_rename_mapper_and_positional_arguments_raises(self):
        # GH 29136
        df = DataFrame([[1]])
        msg = "Cannot specify both 'mapper' and any of 'index' or 'columns'"
        with pytest.raises(TypeError, match=msg):
            df.rename({}, index={})

        with pytest.raises(TypeError, match=msg):
            df.rename({}, columns={})

        with pytest.raises(TypeError, match=msg):
            df.rename({}, columns={}, index={})

    @sql_count_checker(query_count=1, join_count=1)
    def test_rename_with_duplicate_columns(self):
        # GH#4403
        df4 = DataFrame(
            {"RT": [0.0454], "TClose": [22.02], "TExg": [0.0422]},
            index=MultiIndex.from_tuples(
                [(600809, 20130331)], names=["STK_ID", "RPT_Date"]
            ),
        )

        df5 = DataFrame(
            {
                "RPT_Date": [20120930, 20121231, 20130331],
                "STK_ID": [600809] * 3,
                "STK_Name": ["饡驦", "饡驦", "饡驦"],
                "TClose": [38.05, 41.66, 30.01],
            },
            index=MultiIndex.from_tuples(
                [(600809, 20120930), (600809, 20121231), (600809, 20130331)],
                names=["STK_ID", "RPT_Date"],
            ),
        )
        k = df4.join(df5, how="inner", lsuffix="_x", rsuffix="_y")
        df_with_dup = pd.DataFrame(k)
        result = df_with_dup.rename(
            columns={"TClose_x": "TClose", "TClose_y": "QT_Close"}
        )

        expected = native_pd.DataFrame(
            [[0.0454, 22.02, 0.0422, 20130331, 600809, "饡驦", 30.01]],
            columns=[
                "RT",
                "TClose",
                "TExg",
                "RPT_Date",
                "STK_ID",
                "STK_Name",
                "QT_Close",
            ],
        ).set_index(["STK_ID", "RPT_Date"], drop=False)
        assert_frame_equal(result, expected, check_dtype=False, check_index_type=False)

    @sql_count_checker(query_count=1, join_count=1)
    def test_rename_boolean_index(self):
        df = DataFrame(np.arange(15).reshape(3, 5), columns=[False, True, 2, 3, 4])
        mapper = {0: "foo", 1: "bar", 2: "bah"}
        res = df.rename(index=mapper)
        exp = native_pd.DataFrame(
            np.arange(15).reshape(3, 5),
            columns=[False, True, 2, 3, 4],
            index=[
                "foo",
                "bar",
                "bah",
            ],
        )
        assert_frame_equal(res, exp, check_dtype=False, check_index_type=False)

    @sql_count_checker(query_count=0)
    def test_rename_copy_warning(self, float_frame, caplog):
        caplog.at_level(logging.WARNING)
        snow_float_frame = pd.DataFrame(float_frame)
        msg = "The argument `copy` of `dataframe.rename` has been ignored by Snowpark pandas API"
        caplog.clear()
        snow_float_frame.rename(columns={"C": "foo"})
        assert msg not in caplog.text

        snow_float_frame.rename(columns={"C": "foo"}, copy=True)
        assert msg in caplog.text

    @pytest.mark.parametrize("axis, join_count", [(0, 1), (1, 0)])
    def test_rename_timedelta_values(self, axis, join_count):
        with SqlCounter(query_count=1, join_count=join_count):
            eval_snowpark_pandas_result(
                *create_test_dfs([pd.Timedelta(1)]),
                lambda df: df.rename(mapper={0: "a"}, axis=axis)
            )
