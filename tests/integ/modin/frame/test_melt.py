#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as npd
import pytest
from modin.pandas import DataFrame
from pandas.core.dtypes.common import is_list_like

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin._internal.unpivot_utils import (
    _general_unpivot,
    _simple_unpivot,
)
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equal_to_pandas,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker

data = [
    {"frame": {"abc": ["A", np.nan, "C"], "123": ["1", "2", np.nan]}, "kargs": {}},
    {"frame": {"abc": ["A", "B", "C"], "123": ["1", "2", "3"]}, "kargs": {}},
    {"frame": {"abc": ["A", "B", "C"], "123": [1, 2, 3]}, "kargs": {}},
    {"frame": {"123": [1, 2, 3], "456": [4, 5, 6]}, "kargs": {}},
    {"frame": {"123": [1, 2, 3], "456": [4.5, 5.5, 6.5]}, "kargs": {}},
    {
        "frame": {"abc": ["A", "B", "C"], "123": ["1", "2", "3"]},
        "kargs": {"id_vars": ["abc"]},
    },
    {
        "frame": {"abc": ["A", "B", "C"], "123": [1, 2, 3]},
        "kargs": {"id_vars": ["abc"]},
    },
    {
        "frame": {"abc": ["A", "B", "C"], "123": ["1", "2", "3"]},
        "kargs": {"value_vars": ["abc"]},
    },
    {
        "frame": {"abc": ["A", "B", "C"], "123": [1, 2, 3]},
        "kargs": {"value_vars": ["abc"]},
    },
    {
        "frame": {"abc": ["A", "B", "C"], "123": ["1", "2", "3"]},
        "kargs": {"id_vars": ["abc"], "value_vars": ["123"]},
    },
    {
        "frame": {"abc": ["A", "B", "C"], "123": ["1", "2", "3"], "456": [4, 5, 6]},
        "kargs": {"id_vars": ["abc"], "value_vars": ["123", "456"]},
    },
    {
        "frame": {
            "A": {0: "a", 1: "b", 2: "c"},
            "B": {0: 1, 1: 3, 2: 5},
            "C": {0: 2, 1: 4, 2: 6},
        },
        "kargs": {"id_vars": ["A"], "value_vars": ["B", "C"]},
    },
    {
        "frame": {"abc": ["A", "B", "C"], "123": ["1", "2", "3"]},
        "kargs": {"var_name": "independent"},
    },
    {
        "frame": {"abc": ["A", "B", "C"], "123": ["1", "2", "3"]},
        "kargs": {"value_name": "dependent"},
    },
    {
        "frame": {"abc": ["A", "B", "C"], "123": ["1", "2", "3"]},
        "kargs": {"var_name": "independent", "value_name": "dependent"},
    },
    # The order of the resulting dataframe should match the order of the columns
    {
        "frame": {"abc": ["A", "B", "C"], "123": ["1", "2", "3"], "456": [4, 5, 6]},
        "kargs": {},
    },
    {
        "frame": {"456": [4, 5, 6], "abc": ["A", "B", "C"], "123": ["1", "2", "3"]},
        "kargs": {},
    },
    {
        "frame": {
            "123": ["1", "2", "3"],
            "456": [4, 5, 6],
            "abc": ["A", "B", "C"],
        },
        "kargs": {},
    },
    {"frame": {"abc": ["A", np.nan, np.nan], "123": [np.nan, "2", "3"]}, "kargs": {}},
    {"frame": {"abc": ["A", np.nan, np.nan], "123": [np.nan, 2, 3]}, "kargs": {}},
]


def run_internal_melt(
    df,
    id_vars=None,
    value_vars=None,
    var_name="variable",
    value_name="value",
    col_level=None,
    ignore_index=True,
    use_simple_unpivot=True,
) -> DataFrame:

    # Since we are skipping a few steps to force a particular
    # implementation we need to emulate parts of the mapping
    # that occurs in the compiler and the dataframe layer
    if id_vars is None:
        id_vars = []
    if not is_list_like(id_vars):
        id_vars = [id_vars]
    if value_vars is None:
        value_vars = df.columns.difference(id_vars)
    if var_name is None:
        var_name = "variable"

    df_qc = df._query_compiler
    df_frame = df_qc._modin_frame

    if use_simple_unpivot is True:
        df_frame = _simple_unpivot(df_frame, id_vars, value_vars, var_name, value_name)
    else:
        df_frame = _general_unpivot(
            df_frame,
            id_vars,
            value_vars,
            var_name,
            value_name,
            ignore_index=ignore_index,
        )
    return df.__constructor__(query_compiler=SnowflakeQueryCompiler(df_frame))


@pytest.mark.parametrize(
    "data",
    data,
)
@sql_count_checker(query_count=1, union_count=0, join_count=0)
def test_melt_general_path(data):
    native_df = npd.DataFrame(data["frame"])
    snow_df = pd.DataFrame(native_df)
    ndf = native_df.melt(**data["kargs"])
    sdf = run_internal_melt(snow_df, **data["kargs"], use_simple_unpivot=False)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(sdf, ndf)


@pytest.mark.parametrize(
    "data",
    data,
)
@sql_count_checker(query_count=1, union_count=0, join_count=0)
def test_melt_simple_path(data):
    native_df = npd.DataFrame(data["frame"])
    snow_df = pd.DataFrame(native_df)
    ndf = native_df.melt(**data["kargs"])
    sdf = run_internal_melt(snow_df, **data["kargs"], use_simple_unpivot=True)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(sdf, ndf)


@pytest.mark.parametrize(
    "empty_data",
    [{"frame": {"abc": [], "123": []}, "kargs": {}}],
)
@sql_count_checker(query_count=1, union_count=0, join_count=0)
def test_empty_col_melt(empty_data):
    native_df = npd.DataFrame(empty_data["frame"])
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.melt())


@pytest.mark.parametrize(
    "empty_data",
    [{"frame": {}, "kargs": {}}],
)
@sql_count_checker(query_count=1, union_count=0, join_count=0)
def test_empty_melt(empty_data):
    native_df = npd.DataFrame(empty_data["frame"])
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.melt())


@sql_count_checker(query_count=1, union_count=0, join_count=0)
def test_duplicate_index():
    native_df = npd.DataFrame(
        [[1, 2], [3, 4]], index=["dupe", "dupe"], columns=["A", "B"]
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.melt())


@sql_count_checker(query_count=1, union_count=0, join_count=0)
def test_duplicate_cols():
    native_df = npd.DataFrame([[1, 2], [3, 4]], columns=["dupe", "dupe"])
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.melt())


@sql_count_checker(query_count=1, union_count=0, join_count=0)
def test_integer_colnames():
    native_df = npd.DataFrame([[1, 2], [3, 4]], columns=[1, 2])
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.melt())


@sql_count_checker(query_count=1, union_count=0, join_count=0)
def test_structured_colnames():
    native_df = npd.DataFrame([[1, 2], [3, 4]], columns=[("What", "Is"), ("This")])
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.melt())


@sql_count_checker(query_count=1, join_count=0, union_count=0)
def test_col_index_melt():
    native_df = npd.DataFrame(
        {"abc": ["A", "B", "C"], "123": ["1", "2", "3"]},
        index=["there", "be", "dragons"],
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.melt(ignore_index=True)
    )


@sql_count_checker(query_count=1, join_count=0, union_count=0)
def test_col_index_melt_keep_index():
    native_df = npd.DataFrame(
        {"abc": ["A", "B", "C"], "123": ["1", "2", "3"]},
        index=["there", "be", "dragons"],
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.melt(ignore_index=False)
    )


@sql_count_checker(query_count=1, join_count=0, union_count=0)
def test_multi_index_melt():
    index = npd.MultiIndex.from_tuples(
        [("one", "there"), ("two", "be"), ("two", "dragons")], names=["L1", "L2"]
    )
    data = {"abc": ["A", "B", "C"], "123": ["1", "2", "3"]}
    native_df = npd.DataFrame(data, index=index)
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.melt())


@sql_count_checker(query_count=0, join_count=0, union_count=0)
def test_multi_column_melt():
    index = npd.MultiIndex.from_tuples(
        [("one", "there"), ("two", "be"), ("two", "dragons")], names=["L1", "L2"]
    )
    data = [["A", "B", "C"], ["1", "2", "3"], ["X", "Y", "Z"]]
    native_df = npd.DataFrame(data, columns=index)
    snow_df = pd.DataFrame(native_df)
    try:
        eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.melt())
    except NotImplementedError:
        pass


@sql_count_checker(query_count=2, join_count=0, union_count=0)
def test_multi_melt():
    native_df = npd.DataFrame({"abc": ["A", "B", "C"], "123": ["1", "2", "3"]})
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.melt())
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.melt())


@sql_count_checker(query_count=1, join_count=0, union_count=0)
def test_pd_melt():
    native_df = npd.DataFrame({"abc": ["A", "B", "C"], "123": ["1", "2", "3"]})
    snow_df = pd.DataFrame(native_df)
    ndf = npd.melt(native_df)
    sdf = pd.melt(snow_df)
    assert_snowpark_pandas_equal_to_pandas(sdf, ndf)


@sql_count_checker(query_count=1, join_count=0, union_count=0)
def test_everything():
    index = npd.MultiIndex.from_tuples(
        [("one", "there"), ("two", "be"), ("two", "dragons")], names=["L1", "L2"]
    )
    data = {
        "abc": ["A", "B", np.nan],
        "123": [1, np.nan, 3],
        "state": ["CA", "WA", "NY"],
    }
    native_df = npd.DataFrame(data, index=index)
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.melt(
            id_vars=["state"],
            value_vars=["abc", "123"],
            ignore_index=False,
            var_name="independent",
            value_name="dependent",
        ),
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("value_vars", [["B"], ["B", "C"]])
def test_melt_timedelta(value_vars):
    native_df = npd.DataFrame(
        {
            "A": {0: "a", 1: "b", 2: "c"},
            "B": {0: 1, 1: 3, 2: 5},
            "C": {0: 2, 1: 4, 2: 6},
        }
    ).astype({"B": "timedelta64[ns]", "C": "timedelta64[ns]"})
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.melt(id_vars=["A"], value_vars=value_vars)
    )
