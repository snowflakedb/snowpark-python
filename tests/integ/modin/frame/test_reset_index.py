#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import VALID_PANDAS_LABELS, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker

# TODO SNOW-824304: Add tests for datetime index


@pytest.fixture(scope="function")
def native_df_simple():
    return native_pd.DataFrame(
        {
            "a": ["one", "two", "three"],
            "b": ["abc", "pqr", "xyz"],
            "dt": [
                native_pd.Timedelta("1 days"),
                native_pd.Timedelta("2 days"),
                native_pd.Timedelta("3 days"),
            ],
        },
        index=native_pd.Index(["a", "b", "c"], name="c"),
    )


@pytest.fixture(scope="function")
def native_df_multiindex():
    index = native_pd.MultiIndex.from_arrays(
        [["aaa", "bbb"], ["ccc", "ddd"]], names=("a", "b")
    )
    native_df = native_pd.DataFrame(
        [["one", "one"], ["two", "two"]], index=index, columns=["c", "d"]
    )
    return native_df


@pytest.fixture(scope="function")
def native_df_multiindex_multilevel():
    # 2-level index columns
    index = native_pd.MultiIndex.from_tuples(
        [
            ("bird", "falcon"),
            ("bird", "parrot"),
            ("mammal", "lion"),
            ("mammal", "monkey"),
        ],
        names=["il1", "il2"],
    )
    # 3-level column index
    columns = pd.MultiIndex.from_tuples(
        [("speed", "max", "l3"), ("species", "type", "t3")], names=("cl1", "cl2", "cl3")
    )
    native_df = native_pd.DataFrame(
        [(389.0, "fly"), (24.0, "fly"), (80.5, "run"), (np.nan, "jump")],
        index=index,
        columns=columns,
    )
    return native_df


@sql_count_checker(query_count=1)
def test_reset_index_drop_true(native_df_simple):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df, native_df_simple, lambda df: df.reset_index(drop=True)
    )


@sql_count_checker(query_count=1)
def test_reset_index_drop_false(native_df_simple):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(snow_df, native_df_simple, lambda df: df.reset_index())

    snow_df = snow_df.reset_index()
    assert ["c", "a", "b", "dt"] == list(snow_df.columns)

    snow_df = snow_df.reset_index()
    assert ["index", "c", "a", "b", "dt"] == list(snow_df.columns)

    snow_df = snow_df.reset_index()
    assert ["level_0", "index", "c", "a", "b", "dt"] == list(snow_df.columns)


@sql_count_checker(query_count=1)
def test_reset_index_case_insensitive_conflict(native_df_simple):
    # Verify no conflict if name is same but different case
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df, native_df_simple, lambda df: df.reset_index(names="A")
    )


@sql_count_checker(query_count=0)
def test_reset_index_name_conflict_negative(native_df_simple):
    # Provided name conflicts with existing data column.
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.reset_index(drop=False, names=["a"]),
        expect_exception=True,
    )

    # Generated name conflicts with existing data column.
    native_df2 = native_pd.DataFrame(
        {"index": ["one", "two"], "level_0": ["abc", "xyz"]}
    )
    snow_df2 = pd.DataFrame(native_df2)
    eval_snowpark_pandas_result(
        snow_df2, native_df2, lambda df: df.reset_index(), expect_exception=True
    )


@sql_count_checker(query_count=0)
def test_reset_index_tuple_as_index_name_negative(native_df_simple):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.reset_index(drop=False, names=("p", "q")),
        expect_exception=True,
    )


@pytest.mark.parametrize(
    "reset_func",
    [
        lambda df: df.reset_index(
            names="c"
        ),  # Provided name same as existing column name.
        lambda df: df.reset_index(names="d"),  # Provided new name.
        lambda df: df.reset_index(
            names=["d"]
        ),  # Provided new name as single element array.
        # verify that additional value does not result in error. Instead, the additional values are ignored
        lambda df: df.reset_index(names=["d", "e"]),
        # empty list is same as None
        lambda df: df.reset_index(names=[]),
    ],
)
@sql_count_checker(query_count=1)
def test_reset_index_names(native_df_simple, reset_func):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(snow_df, native_df_simple, reset_func)


@pytest.mark.parametrize("label", VALID_PANDAS_LABELS)
@sql_count_checker(query_count=1)
def test_reset_index_names_valid_labels(native_df_simple, label):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df, native_df_simple, lambda df: df.reset_index(names=[label])
    )


@sql_count_checker(query_count=1)
def test_reset_index_data_column_pandas_index_names():
    native_df = native_pd.DataFrame({"col1": ["one", "two"]})
    native_df.columns.set_names(["abc"], inplace=True)
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.reset_index())


@sql_count_checker(query_count=0)
def test_reset_index_allow_duplicates(native_df_simple):
    # Allow duplicates when provided name conflicts with existing data label.
    snow_df = pd.DataFrame(native_df_simple)
    snow_df = snow_df.reset_index(drop=False, allow_duplicates=True, names=["a"])
    assert ["a", "a", "b", "dt"] == list(snow_df.columns)

    # Verify even if allow_duplicates is True, "index" is not duplicated.
    snow_df = pd.DataFrame({"index": ["one", "two", "three"]})
    snow_df = snow_df.reset_index(drop=False, allow_duplicates=True)
    assert ["level_0", "index"] == list(snow_df.columns)

    # Verify that "level_0" is duplicated.
    snow_df = pd.DataFrame(
        {"index": ["one", "two", "three"], "level_0": ["abc", "pqr", "xyz"]}
    )
    snow_df = snow_df.reset_index(drop=False, allow_duplicates=True)
    assert ["level_0", "index", "level_0"] == list(snow_df.columns)


@sql_count_checker(query_count=2)
def test_reset_index_duplicates_in_original_df():
    native_df = native_pd.DataFrame([[1, 2], [3, 4]], columns=["a", "a"])
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.reset_index(),
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.reset_index(names=["a"]),
        expect_exception=True,
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.reset_index(names=["a"], allow_duplicates=True),
    )


@pytest.mark.parametrize(
    "reset_func",
    [
        lambda df: df.reset_index(inplace=True, drop=True),
        lambda df: df.reset_index(inplace=True, drop=False),
        lambda df: df.reset_index(inplace=True, drop=False),
        lambda df: df.reset_index(inplace=True, drop=False, names="p"),
        lambda df: df.reset_index(inplace=True, drop=False, names=["q"]),
        lambda df: df.reset_index(
            inplace=True, drop=False, allow_duplicates=True, names=["r"]
        ),
    ],
)
@sql_count_checker(query_count=1)
def test_reset_index_inplace(native_df_simple, reset_func):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        reset_func,
        inplace=True,
    )


@sql_count_checker(query_count=5)
def test_reset_index_multiindex(native_df_multiindex):
    native_df = native_df_multiindex
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.reset_index())
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.reset_index(drop=True)
    )
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.reset_index(names=["x", "y"])
    )
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.reset_index(names=["x", "y", "z"])
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.reset_index(allow_duplicates=True, names=["c", "y"]),
    )


@sql_count_checker(query_count=0)
def test_reset_index_multiindex_negative(native_df_multiindex):
    native_df = native_df_multiindex
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.reset_index(names=["x"]),
        expect_exception=True,
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.reset_index(names=[["c", "y"]]),
        expect_exception=True,
    )


@pytest.mark.parametrize("drop", [True, False])
@sql_count_checker(query_count=7)
def test_reset_index_level_single_index(native_df_simple, drop):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df, native_df_simple, lambda df: df.reset_index(level=[], drop=drop)
    )
    eval_snowpark_pandas_result(
        snow_df, native_df_simple, lambda df: df.reset_index(level=0, drop=drop)
    )
    eval_snowpark_pandas_result(
        snow_df, native_df_simple, lambda df: df.reset_index(level=[0], drop=drop)
    )
    eval_snowpark_pandas_result(
        snow_df, native_df_simple, lambda df: df.reset_index(level=["c"], drop=drop)
    )
    eval_snowpark_pandas_result(
        snow_df, native_df_simple, lambda df: df.reset_index(level=[-1], drop=drop)
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.reset_index(level="c", names="x", drop=drop),
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.reset_index(level="c", names=["x", "y"], drop=drop),
    )


@sql_count_checker(query_count=0)
def test_reset_index_level_single_index_negative(native_df_simple):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.reset_index(level="x"),
        expect_exception=True,
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.reset_index(level=["a"]),
        expect_exception=True,
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.reset_index(level=[1]),
        expect_exception=True,
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.reset_index(level=[-2]),
        expect_exception=True,
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.reset_index(level=[0, 1]),
        expect_exception=True,
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.reset_index(level=["c"], names=["a"]),
        expect_exception=True,
    )


@pytest.mark.parametrize("drop", [True, False])
@pytest.mark.parametrize(
    "level, names",
    [
        ([], None),
        (1, None),
        ([1], None),
        (["b"], None),
        ([-2], None),
        ([0, "b"], None),
        (["b", "a"], None),
        (["a"], ["e", "g"]),
        # c is a duplicate but actually will not be used
        (["b"], ["c", "e"]),
        ([1, 0], ["e", "g"]),
        ([1, 0], ["e", "g", "h"]),
    ],
)
@sql_count_checker(query_count=1)
def test_reset_index_level_multiindex(native_df_multiindex, level, names, drop):
    native_df = native_df_multiindex
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.reset_index(level=level, names=names, drop=drop),
    )


@sql_count_checker(query_count=0)
def test_reset_index_level_duplicates(native_df_multiindex):
    # the following behaviors are different from pandas
    # when there are duplicates in level argument
    snow_df_multiindex = pd.DataFrame(native_df_multiindex)
    df = snow_df_multiindex.reset_index(level=[0, "a"])
    # pandas returns None ("b" index is dropped), which is not reasonable
    assert df.index.name == "b"
    assert df.columns.tolist() == ["a", "c", "d"]


@sql_count_checker(query_count=3)
def test_reset_index_level_allow_duplicates(native_df_multiindex):
    native_df = native_df_multiindex
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.reset_index(level=["b", "a", "a"], allow_duplicates=True),
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.reset_index(
            level=["b", "a"], names=["c", "d"], allow_duplicates=True
        ),
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.reset_index(
            level=["b", "a"], names=["e", "e"], allow_duplicates=True
        ),
    )


@sql_count_checker(query_count=0)
def test_reset_index_level_multiindex_negative(native_df_multiindex):
    native_df = native_df_multiindex
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.reset_index(level=["a", "x"], names=["c", "d"]),
        expect_exception=True,
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.reset_index(level=[2], names=["c", "d"]),
        expect_exception=True,
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.reset_index(level=[-3], names=["c", "d"]),
        expect_exception=True,
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.reset_index(level=["b"], names=["c"]),
        expect_exception=True,
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.reset_index(level=[0, 1], names=["c", "e"]),
        expect_exception=True,
    )


@pytest.mark.parametrize(
    "level, names",
    [
        ([1], ["a", "b"]),
        ([1], [("a1", "b1"), ("a2", "b2")]),
        ("il1", ["a1", ("a2", "b2")]),
        ([0, 1], ["a", "b"]),
        (["il1", "il2"], [("a1", "b1"), ("a2", "b2")]),
        ([0, 1], ["a1", ("a2", "b2")]),
        ([0, 1], None),
    ],
)
@pytest.mark.parametrize("col_level", [0, 1, "cl1"])
@pytest.mark.parametrize("fill_value", ["fill", ("f1", "f2")])
@sql_count_checker(query_count=1)
def test_reset_index_col_level_and_fill(
    native_df_multiindex_multilevel, level, names, col_level, fill_value
):
    snow_df = pd.DataFrame(native_df_multiindex_multilevel)
    eval_snowpark_pandas_result(
        snow_df,
        native_df_multiindex_multilevel,
        lambda df: df.reset_index(
            level=level, names=names, col_level=col_level, col_fill=fill_value
        ),
    )


@pytest.mark.parametrize(
    "names, expected_names",
    [
        (["a", "b"], [("a", "a", "a"), ("b", "b", "b")]),
        (
            [("a1", "b1", "c1"), ("a2", "b2", "c2")],
            [("a1", "b1", "c1"), ("a2", "b2", "c2")],
        ),
    ],
)
@sql_count_checker(query_count=1)
def test_reset_index_col_fill_none(
    native_df_multiindex_multilevel, names, expected_names
):
    snow_df = pd.DataFrame(native_df_multiindex_multilevel)
    # According to pandas doc, if fill value is None, it repeats the index name.
    # Note that Snowpark pandas behavior is different compare with current pandas,
    # current pandas set the filling value with the first index name it finds, and
    # since it handles the index in reverse order, it fills with the last index value.
    # For example, if the index names are ['a', 'b'], 'b' is always used as filling
    # value even when fill the index 'a'. This is because the implementation does an inplace
    # update of col_fill, which seems an implementation bug, and not consistent with
    # the doc.
    # With Snowpark pandas, we provide the behavior same as the document that repeats
    # the index name for the index to fill.
    eval_snowpark_pandas_result(
        snow_df,
        native_df_multiindex_multilevel,
        lambda df: df.reset_index(
            level=[0, 1],
            names=names if isinstance(df, pd.DataFrame) else expected_names,
            col_level=0,
            col_fill=None,
        ),
    )


@pytest.mark.parametrize(
    "names",
    [["a", "b"], [("a1", "b1"), ("a2", "b2")]],
)
@sql_count_checker(query_count=1)
def test_reset_index_filling_default(native_df_multiindex_multilevel, names):
    snow_df = pd.DataFrame(native_df_multiindex_multilevel)
    eval_snowpark_pandas_result(
        snow_df,
        native_df_multiindex_multilevel,
        lambda df: df.reset_index(level=[0, 1], names=names),
    )


@pytest.mark.parametrize(
    "level, names, col_level, col_fill, error_type, msg, same_as_pandas",
    [
        (
            [0, 1],
            ["a", "b"],
            3,
            "",
            IndexError,
            "Too many levels: Index has only 3 levels, not 4",
            True,
        ),
        (
            [0, 1],
            ["a", "b"],
            5,
            "",
            IndexError,
            "Too many levels: Index has only 3 levels, not 6",
            True,
        ),
        (
            [0, 1],
            [("a1", "b1"), ("a2", "b2")],
            2,
            "",
            ValueError,
            "Constructed Label has 4 levels, which is larger than target level 3",
            False,
        ),
        (
            [0, 1],
            [("a1", "b1", "c1", "d1"), ("a2", "b2", "c2", "d2")],
            0,
            "",
            ValueError,
            "Constructed Label has 4 levels, which is larger than target level 3",
            False,
        ),
        (
            [0, 1],
            [("a1", "b1"), ("a2", "b2")],
            0,
            None,
            ValueError,
            "col_fill=None is incompatible with incomplete column name",
            False,
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_reset_index_invalid_level_raises(
    native_df_multiindex_multilevel,
    level,
    names,
    col_level,
    col_fill,
    error_type,
    msg,
    same_as_pandas,
):
    snow_df = pd.DataFrame(native_df_multiindex_multilevel)
    eval_snowpark_pandas_result(
        snow_df,
        native_df_multiindex_multilevel,
        lambda df: df.reset_index(
            level=level, names=names, col_level=col_level, col_fill=col_fill
        ),
        expect_exception=True,
        expect_exception_type=error_type,
        expect_exception_match=msg,
        assert_exception_equal=same_as_pandas,
    )
