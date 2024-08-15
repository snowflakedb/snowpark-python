#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker


@sql_count_checker(query_count=0)
def test_index_parent_name():
    """
    Check whether the Index's parent's name is updated correctly.
    Changing the index's name should also change the parent's name.
    """
    native_idx1 = native_pd.Index(["A", "B"], name="xyz")
    native_idx2 = native_pd.Index(["A", "B", "D", "E", "G", "H"], name="CFI")

    # DataFrame case.
    df = pd.DataFrame([[1, 2], [3, 4]], index=native_idx1)
    snow_idx1 = df.index
    assert snow_idx1.name == df.index.name == "xyz"  # compare original name
    snow_idx1.name = "new_name 1"  # set new name
    assert snow_idx1.name == df.index.name == "new_name 1"  # compare new name

    # Series case.
    s = pd.Series([1, 2, 4, 5, 6, 7], index=native_idx2, name="zyx")
    snow_idx2 = s.index
    assert snow_idx2.name == s.index.name == "CFI"  # compare original name
    snow_idx2.name = "new_name 2"  # set new name
    assert snow_idx2.name == s.index.name == "new_name 2"  # compare new name


@sql_count_checker(query_count=0)
def test_index_parent_names():
    """
    Check whether the Index's parent's name is updated correctly.
    Changing the index's name should also change the parent's name.
    """
    native_idx1 = native_pd.Index(["A", "B"], name="xyz")
    native_idx2 = native_pd.Index(["A", "B", "D", "E", "G", "H"], name="CFI")

    # DataFrame case.
    df = pd.DataFrame([[1, 2], [3, 4]], index=native_idx1)
    snow_idx1 = df.index
    assert snow_idx1.names == df.index.names == ["xyz"]  # compare original names
    snow_idx1.names = ["new_name"]  # set new names
    assert snow_idx1.names[0] == df.index.names[0] == "new_name"  # compare new names
    assert len(snow_idx1.names) == len(df.index.names) == 1

    # Series case.
    s = pd.Series([1, 2, 4, 5, 6, 7], index=native_idx2, name="zyx")
    snow_idx2 = s.index
    assert snow_idx2.names == s.index.names == ["CFI"]  # compare original names
    snow_idx2.names = ["new_name 2"]  # set new names
    assert snow_idx2.names == ["new_name 2"]  # compare new names
    assert snow_idx2.names == s.index.names


@pytest.mark.parametrize("new_name", [None, "grade", ("grade",), ("A", "B")])
@sql_count_checker(query_count=0)
def test_index_rename_inplace(new_name):
    native_idx = native_pd.Index(["A", "C", "A", "B"], name="score")
    snow_idx = pd.Index(native_idx)

    # Rename the index in place.
    native_res = native_idx.rename(new_name, inplace=True)
    snow_res = snow_idx.rename(new_name, inplace=True)

    # Verify that the return value is None, and `name` and `names` match.
    assert native_res is None
    assert snow_res is None
    assert native_idx.name == snow_idx.name == new_name
    assert native_idx.names == snow_idx.names == [new_name]


@pytest.mark.parametrize("new_name", [None, "grade", ("grade",), ("A", "B")])
@sql_count_checker(query_count=1)
def test_index_rename_copy(new_name):
    # 1 query to create the new index.
    native_idx = native_pd.Index(["A", "C", "A", "B"], name="score")
    snow_idx = pd.Index(native_idx)

    # Rename the index and create a new index.
    new_native_idx = native_idx.rename(new_name, inplace=False)
    new_snow_idx = snow_idx.rename(new_name, inplace=False)

    # Verify that `name` and `names` match, and the original index's name is unchanged.
    assert new_native_idx.name == new_snow_idx.name == new_name
    assert new_native_idx.names == new_snow_idx.names == [new_name]
    assert native_idx.name == snow_idx.name == "score"


@pytest.mark.parametrize("new_name", [None, "grade", ("grade",), ("A", "B")])
@sql_count_checker(query_count=1)
def test_df_index_rename_inplace(new_name):
    # 1 query to create the DataFrame.
    # Create the DataFrame and the new index.
    native_idx = native_pd.Index(["A", "C"], name="score")
    snow_idx = pd.Index(native_idx)
    data = [[1, 2], [3, 4]]
    native_df = native_pd.DataFrame(data, index=native_idx)
    snow_df = pd.DataFrame(data, index=snow_idx)

    # Rename the index in place.
    native_res = native_df.index.rename(new_name, inplace=True)
    snow_res = snow_df.index.rename(new_name, inplace=True)

    # Verify that the return value is None, and `name` and `names` match.
    assert native_res is None
    assert snow_res is None
    assert native_df.index.name == snow_df.index.name == new_name
    assert native_df.index.names == snow_df.index.names == [new_name]


@pytest.mark.parametrize("new_name", [None, "grade", ("grade",), ("A", "B")])
@sql_count_checker(query_count=2)
def test_df_index_rename_copy(new_name):
    # 1 query to create the DataFrame, 1 query to create the new index.
    # Create the DataFrame and the new index.
    native_idx = native_pd.Index(["A", "C"], name="score")
    snow_idx = pd.Index(native_idx)
    data = [[1, 2], [3, 4]]
    native_df = native_pd.DataFrame(data, index=native_idx)
    snow_df = pd.DataFrame(data, index=snow_idx)

    # Rename the index and create a new index.
    new_native_idx = native_df.index.rename(new_name, inplace=False)
    new_snow_idx = snow_df.index.rename(new_name, inplace=False)

    # Verify that `name` and `names` match, and the original index's name is unchanged.
    assert new_native_idx.name == new_snow_idx.name == new_name
    assert new_native_idx.names == new_snow_idx.names == [new_name]
    assert native_df.index.name == snow_df.index.name == "score"


@pytest.mark.parametrize("new_name", [None, "grade", ["grade"], ("grade",)])
@pytest.mark.parametrize("level", [0, -1])
@sql_count_checker(query_count=0)
def test_index_set_names_inplace(new_name, level):
    native_idx = native_pd.Index(["A", "C", "A", "B"], name="score")
    snow_idx = pd.Index(native_idx)

    # Rename the index in place.
    native_res = native_idx.set_names(new_name, level=level, inplace=True)
    snow_res = snow_idx.set_names(new_name, level=level, inplace=True)

    # Verify that the return value is None, and `name` and `names` match.
    assert native_res is None
    assert snow_res is None
    assert native_idx.name == snow_idx.name == (None if new_name is None else "grade")
    assert (
        native_idx.names
        == snow_idx.names
        == ([None] if new_name is None else ["grade"])
    )


@pytest.mark.parametrize("new_name", [None, "grade", ["grade"], ("grade",)])
@pytest.mark.parametrize("level", [0, -1])
@sql_count_checker(query_count=1)
def test_index_set_names_copy(new_name, level):
    # 1 query to create the new index.
    native_idx = native_pd.Index(["A", "C", "A", "B"], name="score")
    snow_idx = pd.Index(native_idx)

    # Rename the index and create a new index.
    new_native_idx = native_idx.set_names(new_name, level=level, inplace=False)
    new_snow_idx = snow_idx.set_names(new_name, level=level, inplace=False)

    # Verify that `name` and `names` match, and the original index's name is unchanged.
    assert (
        new_native_idx.name
        == new_snow_idx.name
        == (None if new_name is None else "grade")
    )
    assert (
        new_native_idx.names
        == new_snow_idx.names
        == ([None] if new_name is None else ["grade"])
    )
    assert native_idx.name == snow_idx.name == "score"


@pytest.mark.parametrize("new_name", [None, "grade", ["grade"], ("grade",)])
@pytest.mark.parametrize("level", [0, -1])
@sql_count_checker(query_count=1)
def test_df_index_set_names_inplace(new_name, level):
    # 1 query to create the DataFrame.
    # Create the DataFrame and the new index.
    native_idx = native_pd.Index(["A", "C"], name="score")
    snow_idx = pd.Index(native_idx)
    data = [[1, 2], [3, 4]]
    native_df = native_pd.DataFrame(data, index=native_idx)
    snow_df = pd.DataFrame(data, index=snow_idx)

    # Rename the index in place.
    native_res = native_df.index.set_names(new_name, level=level, inplace=True)
    snow_res = snow_df.index.set_names(new_name, level=level, inplace=True)

    # Verify that the return value is None, and `name` and `names` match.
    assert native_res is None
    assert snow_res is None
    assert (
        native_df.index.name
        == snow_df.index.name
        == (None if new_name is None else "grade")
    )
    assert (
        native_df.index.names
        == snow_df.index.names
        == ([None] if new_name is None else ["grade"])
    )


@pytest.mark.parametrize("new_name", [None, "grade", ["grade"], ("grade",)])
@pytest.mark.parametrize("level", [0, -1])
@sql_count_checker(query_count=2)
def test_df_index_set_names_copy(new_name, level):
    # 1 query to create the DataFrame, 1 query to create the new index.
    # Create the DataFrame and the new index.
    native_idx = native_pd.Index(["A", "C"], name="score")
    snow_idx = pd.Index(native_idx)
    data = [[1, 2], [3, 4]]
    native_df = native_pd.DataFrame(data, index=native_idx)
    snow_df = pd.DataFrame(data, index=snow_idx)

    # Rename the index and create a new index.
    new_native_idx = native_df.index.set_names(new_name, level=level, inplace=False)
    new_snow_idx = snow_df.index.set_names(new_name, level=level, inplace=False)

    # Verify that `name` and `names` match, and the original index's name is unchanged.
    assert (
        new_native_idx.name
        == new_snow_idx.name
        == (None if new_name is None else "grade")
    )
    assert (
        new_native_idx.names
        == new_snow_idx.names
        == ([None] if new_name is None else ["grade"])
    )
    assert native_df.index.name == snow_df.index.name == "score"


@pytest.mark.parametrize("inplace", [True, False])
def test_index_rename_list(inplace):
    # In native pandas, `rename` only works with hashable datatypes, however `set_names` works with
    # non-hashable datatypes as well (these are usually list-like types).
    # In Snowpark pandas, both `rename` and `set_names` work with non-hashable datatypes.
    # Verify the behavior in native pandas and Snowpark pandas.
    native_idx = native_pd.Index(["A", "C", "A", "B"], name="score")
    snow_idx = pd.Index(native_idx)
    new_name = ["grade"]

    with pytest.raises(TypeError, match="Index.name must be a hashable type"):
        native_idx.rename(new_name, inplace=inplace)

    with SqlCounter(query_count=0 if inplace else 1):
        # 1 query to create the new index.
        res = snow_idx.rename(new_name, inplace=inplace)
        if inplace:
            assert res is None
            assert snow_idx.name == "grade"
            assert snow_idx.names == ["grade"]
        else:
            assert res.name == "grade"
            assert res.names == ["grade"]
            assert snow_idx.name == "score"


@pytest.mark.parametrize("level", [-10, 1, "abc"])
@sql_count_checker(query_count=0)
def test_index_set_names_invalid_level(level):
    idx = pd.Index(["A", "C", "A", "B"], name="score")
    err_msg = f"Level does not exist: Index has only 1 level, {level} is not a valid level number."
    with pytest.raises(IndexError, match=err_msg):
        idx.set_names("grade", level=level)
