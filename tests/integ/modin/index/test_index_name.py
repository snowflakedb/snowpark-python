#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker

NATIVE_DF_TEST_DATA = [
    {"col1": [1, 2, 3], "col2": [3, 4, 5], "col3": [5, 6, 7]},
    {},
    {"col1": [1, np.nan, False], "col2": [3, None, 5.0], "col3": ["abc", np.nan, True]},
]

NATIVE_INDEX_TEST_DATA = [
    [],
    [[1, 2], [2, 3], [3, 4]],
    [1, False, 3],
    [3, np.nan, True],
    [5, None, 7],
    ["a", "b", 1, 2],
]

TEST_NAMES = ["a", None, 1, -2.5, True]
TEST_NAMES_2 = ["hello", 7.1, False, -3, "-"]


@pytest.mark.parametrize("name", TEST_NAMES)
@pytest.mark.parametrize("name_2", TEST_NAMES_2)
@pytest.mark.parametrize("data", NATIVE_INDEX_TEST_DATA)
@sql_count_checker(query_count=0)
def test_index_name_names(data, name, name_2):
    native_index = native_pd.Index(data)
    snow_index = pd.Index(native_index)
    assert snow_index.name == native_index.name

    snow_index.name = name
    native_index.name = name
    assert snow_index.name == native_index.name

    snow_index.names = [name_2]
    native_index.names = [name_2]
    assert snow_index.names == native_index.names


@pytest.mark.parametrize("name", TEST_NAMES)
@pytest.mark.parametrize("name_2", TEST_NAMES_2)
@pytest.mark.parametrize("data", NATIVE_INDEX_TEST_DATA)
@sql_count_checker(query_count=0)
def test_index_set_names(data, name, name_2):
    native_index = native_pd.Index(data)
    snow_index = pd.Index(native_index)

    snow_index.set_names(names=[name], inplace=True)
    native_index.set_names(names=[name], inplace=True)

    assert snow_index.names == native_index.names

    new_snow_index = snow_index.set_names(names=[name_2], inplace=False)
    new_native_index = native_index.set_names(names=[name_2], inplace=False)

    assert new_snow_index.names == new_native_index.names
    assert new_snow_index.names != snow_index.names


@pytest.mark.parametrize("name", TEST_NAMES)
@pytest.mark.parametrize("name_2", TEST_NAMES_2)
@pytest.mark.parametrize("data", NATIVE_INDEX_TEST_DATA)
@sql_count_checker(query_count=0)
def test_index_rename(data, name, name_2):
    native_index = native_pd.Index(data)
    snow_index = pd.Index(native_index)

    snow_index.rename(name=name, inplace=True)
    native_index.rename(name=name, inplace=True)

    assert snow_index.names == native_index.names

    new_snow_index = snow_index.rename(name=name_2, inplace=False)
    new_native_index = native_index.rename(name=name_2, inplace=False)

    assert new_snow_index.names == new_native_index.names
    assert new_snow_index.names != snow_index.names


@pytest.mark.parametrize("name", TEST_NAMES)
@pytest.mark.parametrize("name_2", TEST_NAMES_2)
@pytest.mark.parametrize("data", NATIVE_DF_TEST_DATA)
@sql_count_checker(query_count=0)
def test_df_index_name_names(data, name, name_2):
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    assert snow_df.index.name == native_df.index.name

    snow_df.index.name = name
    native_df.index.name = name

    assert snow_df.index.name == native_df.index.name

    snow_df.index.names = [name_2]
    native_df.index.names = [name_2]

    assert snow_df.index.names == native_df.index.names


@pytest.mark.parametrize("name", TEST_NAMES)
@pytest.mark.parametrize("name_2", TEST_NAMES_2)
@pytest.mark.parametrize("data", NATIVE_DF_TEST_DATA)
@sql_count_checker(query_count=0)
def test_df_index_set_names(data, name, name_2):
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    snow_df.index.set_names(names=[name], inplace=True)
    native_df.index.set_names(names=[name], inplace=True)

    assert snow_df.index.names == native_df.index.names

    new_snow_index = snow_df.index.set_names(names=[name_2], inplace=False)
    new_native_index = native_df.index.set_names(names=[name_2], inplace=False)

    assert new_snow_index.names == new_native_index.names
    assert new_snow_index.names != snow_df.index.names


@pytest.mark.parametrize("name", TEST_NAMES)
@pytest.mark.parametrize("name_2", TEST_NAMES_2)
@pytest.mark.parametrize("data", NATIVE_DF_TEST_DATA)
@sql_count_checker(query_count=0)
def test_df_index_rename(data, name, name_2):
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    snow_df.index.rename(name=name, inplace=True)
    native_df.index.rename(name=name, inplace=True)

    assert snow_df.index.names == native_df.index.names

    new_snow_index = snow_df.index.rename(name=name_2, inplace=False)
    new_native_index = native_df.index.rename(name=name_2, inplace=False)

    assert new_snow_index.names == new_native_index.names
    assert new_snow_index.names != snow_df.index.names


@pytest.mark.parametrize("name", TEST_NAMES)
@pytest.mark.parametrize("name_2", TEST_NAMES_2)
@pytest.mark.parametrize("data", NATIVE_DF_TEST_DATA)
@sql_count_checker(query_count=0)
def test_df_columns_name_names(data, name, name_2):
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)
    assert snow_df.columns.name == native_df.columns.name

    snow_df.columns.name = name
    native_df.columns.name = name

    assert snow_df.columns.name == native_df.columns.name

    snow_df.columns.names = [name_2]
    native_df.columns.names = [name_2]

    assert snow_df.columns.names == native_df.columns.names


@pytest.mark.parametrize("name", TEST_NAMES)
@pytest.mark.parametrize("name_2", TEST_NAMES_2)
@pytest.mark.parametrize("data", NATIVE_DF_TEST_DATA)
@sql_count_checker(query_count=0)
def test_df_columns_set_names(data, name, name_2):
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    snow_df.columns.set_names(names=[name], inplace=True)
    native_df.columns.set_names(names=[name], inplace=True)

    assert snow_df.columns.names == native_df.columns.names

    new_snow_columns = snow_df.columns.set_names(names=[name_2], inplace=False)
    new_native_columns = native_df.columns.set_names(names=[name_2], inplace=False)

    assert new_snow_columns.names == new_native_columns.names
    assert new_snow_columns.names != snow_df.columns.names


@pytest.mark.parametrize("name", TEST_NAMES)
@pytest.mark.parametrize("name_2", TEST_NAMES_2)
@pytest.mark.parametrize("data", NATIVE_DF_TEST_DATA)
@sql_count_checker(query_count=0)
def test_df_columns_rename(data, name, name_2):
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    snow_df.columns.rename(name=name, inplace=True)
    native_df.columns.rename(name=name, inplace=True)

    assert snow_df.columns.names == native_df.columns.names

    new_snow_columns = snow_df.columns.rename(name=name_2, inplace=False)
    new_native_columns = native_df.columns.rename(name=name_2, inplace=False)

    assert new_snow_columns.names == new_native_columns.names
    assert new_snow_columns.names != snow_df.columns.names
