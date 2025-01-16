#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_frame_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


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
@sql_count_checker(query_count=0)
def test_index_rename_copy(new_name):
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
@sql_count_checker(query_count=0)
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
@sql_count_checker(query_count=0)
def test_df_index_rename_copy(new_name):
    # 1 query to create the DataFrame.
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
@sql_count_checker(query_count=0)
def test_index_set_names_inplace(new_name):
    native_idx = native_pd.Index(["A", "C", "A", "B"], name="score")
    snow_idx = pd.Index(native_idx)

    # Rename the index in place.
    native_res = native_idx.set_names(new_name, inplace=True)
    snow_res = snow_idx.set_names(new_name, inplace=True)

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
@sql_count_checker(query_count=0)
def test_index_set_names_copy(new_name):
    native_idx = native_pd.Index(["A", "C", "A", "B"], name="score")
    snow_idx = pd.Index(native_idx)

    # Rename the index and create a new index.
    new_native_idx = native_idx.set_names(new_name, inplace=False)
    new_snow_idx = snow_idx.set_names(new_name, inplace=False)

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
@sql_count_checker(query_count=0)
def test_df_index_set_names_inplace(new_name):
    # 1 query to create the DataFrame.
    # Create the DataFrame and the new index.
    native_idx = native_pd.Index(["A", "C"], name="score")
    snow_idx = pd.Index(native_idx)
    data = [[1, 2], [3, 4]]
    native_df = native_pd.DataFrame(data, index=native_idx)
    snow_df = pd.DataFrame(data, index=snow_idx)

    # Rename the index in place.
    native_res = native_df.index.set_names(new_name, inplace=True)
    snow_res = snow_df.index.set_names(new_name, inplace=True)

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
@sql_count_checker(query_count=0)
def test_df_index_set_names_copy(new_name):
    # 1 query to create the DataFrame.
    # Create the DataFrame and the new index.
    native_idx = native_pd.Index(["A", "C"], name="score")
    snow_idx = pd.Index(native_idx)
    data = [[1, 2], [3, 4]]
    native_df = native_pd.DataFrame(data, index=native_idx)
    snow_df = pd.DataFrame(data, index=snow_idx)

    # Rename the index and create a new index.
    new_native_idx = native_df.index.set_names(new_name, inplace=False)
    new_snow_idx = snow_df.index.set_names(new_name, inplace=False)

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
@sql_count_checker(query_count=0)
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


@pytest.mark.parametrize("level", [0, -1])
@sql_count_checker(query_count=0)
def test_index_set_names_level(level):
    # The level parameter works for Snowpark pandas even in the case of a single Index.
    # However, native pandas does not allow you to specify the level unless the index is
    # a MultiIndex.
    native_idx = native_pd.Index(["A", "C", "A", "B"], name="score")
    snow_idx = pd.Index(native_idx)
    with pytest.raises(ValueError, match="Level must be None for non-MultiIndex"):
        native_idx.set_names("grade", level=level)

    # Verifying the results.
    native_res = native_idx.set_names("grade")
    snow_res = snow_idx.set_names("grade", level=level)
    assert native_res.name == snow_res.name == "grade"


@sql_count_checker(query_count=0)
def test_index_non_hashable_name():
    idx = pd.Index(["A", "C", "A", "B"], name="score")
    with pytest.raises(TypeError, match="Index.name must be a hashable type"):
        idx.name = ["grade"]


@sql_count_checker(query_count=1)
def test_index_SNOW_1021837():
    """
    Bug SNOW-1021837:
    Previously, updating index.name inplace did not affect index column name after reset_index().
    This test verifies that index column names are correctly updated.
    """
    native_df = native_pd.DataFrame([0])
    snow_df = pd.DataFrame(native_df)

    # Set the index names.
    native_df.index.name = "index_name"
    snow_df.index.name = "index_name"

    # Perform reset index and check if name is correctly updated.
    native_df_reset = native_df.reset_index()
    snow_df_reset = snow_df.reset_index()
    assert_frame_equal(snow_df_reset, native_df_reset)


@sql_count_checker(query_count=0)
def test_index_non_list_like_names_negative():
    """
    Bug SNOW-1650853:
    Test that the correct error is raised.
    """
    native_df = native_pd.DataFrame(list(range(10)))
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: setattr(df.index, "names", 10),
        expect_exception=True,
        check_exception_type=ValueError,
    )


@sql_count_checker(query_count=2)
def test_index_names_with_lazy_index():
    # 1 query to convert index to list, 1 query for comparison of DataFrames.
    native_df = native_pd.DataFrame(list(range(10)))
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: setattr(
            df.index,
            "names",
            pd.Index(["A"]) if isinstance(df, pd.DataFrame) else native_pd.Index(["A"]),
        ),
        inplace=True,
    )


@sql_count_checker(query_count=1)
def test_index_names_replace_behavior():
    """
    Check that the index name of a DataFrame cannot be updated after the DataFrame has been modified.
    """
    data = {
        "A": [0, 1, 2, 3, 4, 4],
        "B": ["a", "b", "c", "d", "e", "f"],
    }
    idx = [1, 2, 3, 4, 5, 6]
    native_df = native_pd.DataFrame(data, native_pd.Index(idx, name="test"))
    snow_df = pd.DataFrame(data, index=pd.Index(idx, name="test"))

    # Get a reference to the index of the DataFrames.
    snow_index = snow_df.index
    native_index = native_df.index

    # Change the names.
    snow_index.name = "test2"
    native_index.name = "test2"

    # Compare the names.
    assert snow_index.name == native_index.name == "test2"
    assert snow_df.index.name == native_df.index.name == "test2"

    # Change the query compiler the DataFrame is referring to, change the names.
    snow_df.dropna(inplace=True)
    native_df.dropna(inplace=True)
    snow_index.name = "test3"
    native_index.name = "test3"

    # Compare the names. Changing the index name should not change the DataFrame's index name.
    assert snow_index.name == native_index.name == "test3"
    assert snow_df.index.name == native_df.index.name == "test2"


@sql_count_checker(query_count=1)
def test_index_names_multiple_renames():
    """
    Check that the index name of a DataFrame can be renamed any number of times.
    """
    data = {
        "A": [0, 1, 2, 3, 4, 4],
        "B": ["a", "b", "c", "d", "e", "f"],
    }
    idx = [1, 2, 3, 4, 5, 6]
    native_df = native_pd.DataFrame(data, native_pd.Index(idx, name="test"))
    snow_df = pd.DataFrame(data, index=pd.Index(idx, name="test"))

    # Get a reference to the index of the DataFrames.
    snow_index = snow_df.index
    native_index = native_df.index

    # Change and compare the names.
    snow_index.name = "test2"
    native_index.name = "test2"
    assert snow_index.name == native_index.name == "test2"
    assert snow_df.index.name == native_df.index.name == "test2"

    # Change the names again and compare.
    snow_index.name = "test3"
    native_index.name = "test3"
    assert snow_index.name == native_index.name == "test3"
    assert snow_df.index.name == native_df.index.name == "test3"
