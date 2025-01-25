#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.pivot.pivot_utils import (
    pivot_table_test_helper,
    pivot_table_test_helper_expects_exception,
)
from tests.integ.utils.sql_counter import sql_count_checker
import re


@pytest.mark.parametrize(
    "pivot_table_kwargs",
    [
        # Reference to None not allowed
        {"index": [None], "columns": "E", "values": "F"},
        # Duplicate columns 'A' exist in the dataframe
        {"index": "A", "columns": "C", "values": "E"},
        # Reference a column multiple times that only occurs once in dataframe
        {"index": ["C", "C"], "columns": "F", "values": "E"},
        # Reference a non-existant column in dataframe
        {"index": "Z", "columns": "F", "values": "E"},
    ],
)
@sql_count_checker(query_count=0)
def test_pivot_table_invalid_index_columns_not_supported(
    df_data_with_duplicates, pivot_table_kwargs
):
    pivot_table_test_helper_expects_exception(
        df_data_with_duplicates, pivot_table_kwargs
    )


@pytest.mark.parametrize(
    "pivot_table_kwargs",
    [
        # Reference to None not allowed
        {"index": "E", "columns": [None], "values": "F"},
        # Duplicate columns 'A' exist in the dataframe
        {"index": "C", "columns": "A", "values": "E"},
        # Reference a column multiple times that only occurs once in dataframe
        {"index": None, "columns": ["C", "C"], "values": "E"},
        # Reference a non-existent column in dataframe
        {"index": "C", "columns": "Z", "values": "E"},
    ],
)
@sql_count_checker(query_count=0)
def test_pivot_table_invalid_pivot_labels_not_supported(
    df_data_with_duplicates, pivot_table_kwargs
):
    pivot_table_test_helper_expects_exception(
        df_data_with_duplicates, pivot_table_kwargs
    )


@pytest.mark.parametrize(
    "pivot_table_kwargs",
    [
        # Reference to None not allowed
        {"index": "A", "columns": "B", "values": [None]},
        # Reference to None not allowed
        {"index": "A", "columns": "B", "values": ["E", None]},
        # Reference a non-existent column in dataframe
        {"index": "A", "columns": "B", "values": "Z"},
    ],
)
@sql_count_checker(query_count=0)
def test_pivot_table_invalid_values_columns_not_supported(df_data, pivot_table_kwargs):
    pivot_table_test_helper_expects_exception(
        df_data,
        pivot_table_kwargs,
    )


@pytest.mark.parametrize(
    "aggfunc",
    [
        None,
        [None],
        ["sum", None],
        {"C": None},
        {"C": [None]},
        {"C": ["max", None]},
    ],
)
@sql_count_checker(query_count=0)
def test_pivot_table_not_supported_aggfunc_with_null(df_data, aggfunc):
    pivot_table_test_helper_expects_exception(
        df_data, {"index": "A", "columns": "C", "values": "E", "aggfunc": aggfunc}
    )


@sql_count_checker(query_count=0)
def test_pivot_table_not_supported_aggfunc_with_empty_list(df_data):
    pivot_table_test_helper_expects_exception(
        df_data,
        {"index": "A", "columns": "C", "values": "E", "aggfunc": []},
        expect_exception_type=ValueError,
        expect_exception_match="Expected at least one aggregation function",
        assert_exception_equal=False,
    )


@sql_count_checker(query_count=0)
def test_pivot_table_not_implemented_or_supported(df_data):
    snow_df = pd.DataFrame(df_data)

    with pytest.raises(NotImplementedError, match="Not implemented observed"):
        snow_df.pivot_table(index="A", columns="C", values="E", observed=True)

    with pytest.raises(NotImplementedError, match="Not implemented not sorted"):
        snow_df.pivot_table(index="A", columns="C", values="E", sort=False)

    class Foo:
        pass

    class Baz:
        pass

    foo = Foo()
    baz = Baz()
    snow_df2 = pd.DataFrame(
        {
            foo: ["abc", "def"],
            "A": ["a", "b"],
            "B": ["x", "y"],
            "C": [2, 3],
            baz: [3, 4],
        },
    )
    foo = Foo()
    baz = Baz()

    with pytest.raises(NotImplementedError, match="Not implemented non-string"):
        snow_df2.pivot_table(index=[foo], columns="B", values="C")

    with pytest.raises(NotImplementedError, match="Not implemented non-string"):
        snow_df2.pivot_table(index="A", columns=[foo], values="E")

    with pytest.raises(NotImplementedError, match="Not implemented non-string"):
        snow_df2.pivot_table(index="A", columns="B", values=[baz])


@sql_count_checker(query_count=0)
@pytest.mark.parametrize(
    "aggfunc,name_in_error",
    [
        ("foo", "'foo'"),
        ("kurt", "'kurt'"),
        ("prod", "'prod'"),
        ("sem", "'sem'"),
        (np.argmax, "np.argmax"),
        (np.argmin, "np.argmin"),
        (
            "all",
            "'all'",
        ),
        (np.all, "np.all"),
        ("any", "'any'"),
        (np.any, "np.any"),
        ("size", "'size'"),
        (len, "<built-in function len>"),
        ("nunique", "'nunique'"),
        ("idxmax", "'idxmax'"),
        ("idxmin", "'idxmin'"),
    ],
)
def test_not_implemented_single_aggfunc(df_data, aggfunc, name_in_error):
    with pytest.raises(
        NotImplementedError,
        match=re.escape(
            "Snowpark pandas DataFrame.pivot_table does not yet support "
            + f"the aggregation {name_in_error} with the given arguments."
        ),
    ):
        pd.DataFrame(df_data).pivot_table(
            index="A", columns="C", values="E", aggfunc=aggfunc
        )


def sensitive_function_name(col: native_pd.Series) -> int:
    return col.sum()


@pytest.mark.parametrize(
    "func, error_pattern",
    [
        param(
            sensitive_function_name,
            "Snowpark pandas DataFrame.pivot_table does not yet support the aggregation Callable with the given arguments",
            id="user_defined_function",
        ),
        param(
            [sensitive_function_name, "size"],
            "Snowpark pandas DataFrame.pivot_table does not yet support the aggregation \\[Callable, 'size'\\] with the given arguments",
            id="list_with_user_defined_function_and_string",
        ),
        param(
            (sensitive_function_name, "size"),
            "Snowpark pandas DataFrame.pivot_table does not yet support the aggregation \\[Callable, 'size'\\] with the given arguments",
            id="tuple_with_user_defined_function_and_string",
        ),
        param(
            {sensitive_function_name, "size"},
            "Snowpark pandas DataFrame.pivot_table does not yet support the aggregation \\[Callable, 'size'\\]|\\['size', Callable\\] with the given arguments",
            id="set_with_user_defined_function_and_string",
        ),
        param(
            (all, any, len, list, min, max, set, str, tuple, native_pd.Series.sum),
            "Snowpark pandas DataFrame.pivot_table does not yet support the aggregation "
            + "\\[<built-in function all>, <built-in function any>, <built-in function len>, list, <built-in function min>, <built-in function max>, set, str, tuple, Callable]"
            + " with the given arguments",
            id="tuple_with_builtins_and_native_pandas_function",
        ),
        param(
            {"D": sensitive_function_name, "E": sum, "F": [np.mean, "size"]},
            "Snowpark pandas DataFrame.pivot_table does not yet support the aggregation "
            + "{label: Callable, label: <built-in function sum>, label: \\[np\\.mean, 'size'\\]}"
            + " with the given arguments",
            id="dict",
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_pivot_table_aggfunc_not_implemented_or_supported(df_data, func, error_pattern):
    with pytest.raises(NotImplementedError, match=error_pattern):
        pd.DataFrame(df_data).pivot_table(
            index="A", columns="C", values=["D", "E", "F"], aggfunc=func
        )


@pytest.mark.xfail(strict=True, raises=NotImplementedError)
@pytest.mark.parametrize(
    "df_data",
    [
        {
            "A": ["foo", "bar"],
            "B": ["one", "two"],
            "C": [pd.Timedelta(1), pd.Timedelta(2)],
        },
        {
            "A": [pd.Timedelta(1), pd.Timedelta(2)],
            "B": ["one", "two"],
            "C": ["foo", "bar"],
        },
        {
            "A": ["one", "two"],
            "B": [pd.Timedelta(1), pd.Timedelta(2)],
            "C": ["foo", "bar"],
        },
    ],
)
def test_timedelta_input_not_supported(df_data):
    pivot_table_test_helper(
        df_data,
        pivot_table_kwargs=dict(index="A", columns="B", values="C", aggfunc="max"),
    )
