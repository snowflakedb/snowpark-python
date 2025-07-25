#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import sys
import textwrap

import pytest
import pandas as native_pd
import modin.pandas as pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin._internal.apply_utils import (
    get_cached_lambda_names,
    drop_named_udf_and_udtfs,
)

from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

# Fixtures are not run on test failure, so each test must be wrapped in a try/finally.
# If any persistent UDF/UDTFs were created, drop them to prevent interference with future test runs.


def get_applier(f, persist_args):
    # Since native pandas can't take the snowflake_udf_params object, we need this helper to omit it.
    def wrapper(obj):
        if isinstance(obj, (pd.DataFrame, pd.Series)):
            return obj.apply(f, snowflake_udf_params=persist_args)
        # native pandas
        return obj.apply(f)

    return wrapper


def get_mapper(f, persist_args):
    # Since native pandas can't take the snowflake_udf_params object, we need this helper to omit it.
    def wrapper(obj):
        if isinstance(obj, (pd.DataFrame, pd.Series)):
            return obj.map(f, snowflake_udf_params=persist_args)
        # native pandas
        return obj.map(f)

    return wrapper


def check_udf_exists(session, name):
    assert len(session.sql(f"SHOW USER FUNCTIONS LIKE '{name}'").collect()) > 0


@pytest.fixture(scope="module")
def stage_name(session):
    # Create a temporary stage for testing and clean it up after tests complete.
    stage_name = "test_apply_persist"
    session.sql(f"CREATE OR REPLACE STAGE {stage_name}").collect()
    yield f"@{stage_name}"
    session.sql(f"DROP STAGE IF EXISTS {stage_name}").collect()


def test_apply_existing_udf(session, stage_name):
    udf_name = "snowpandas_apply_test_existing_udf"
    session.sql(
        textwrap.dedent(
            f"""
            CREATE OR REPLACE FUNCTION {udf_name}(arg1 BIGINT)
                RETURNS BIGINT
                LANGUAGE PYTHON
                RUNTIME_VERSION = '{sys.version_info.major}.{sys.version_info.minor}'
                HANDLER = 'existing_udf'
            AS $$
            def existing_udf(x):
                return x * 2
            $$;
            """
        )
    ).collect()

    def dummy(x: int) -> int:
        return -1

    df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    with SqlCounter(query_count=4):
        # This emits a CREATE FUNCTION IF NOT EXISTS query, but does not replace the existing UDF.
        # Note that the function's in-memory __name__ differs from that used for UDF registration.
        try:
            result = df.map(
                dummy,
                snowflake_udf_params={
                    "stage_location": stage_name,
                    "name": udf_name,
                    "if_not_exists": True,
                },
            )
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                result,
                native_pd.DataFrame({"A": [2, 4], "B": [6, 8]}),
            )
        finally:
            drop_named_udf_and_udtfs()


@pytest.mark.parametrize(
    "operation",
    [
        "apply",
        "map",
    ],
)
def test_apply_or_map_permanent_def(session, stage_name, operation):
    if operation == "apply":
        wrapper = get_applier
        count = 6
    else:
        wrapper = get_mapper
        count = 4

    f_name = "test_double_udf"

    def double(x):
        return x * 2

    snow_df, native_df = create_test_dfs({"A": [1, 2, 3]})
    try:
        with SqlCounter(query_count=count):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                wrapper(
                    double,
                    {
                        "stage_location": stage_name,
                        "name": f_name,
                        "replace": True,
                    },
                ),
            )
        # Calling the function again with if_not_exists=True will re-use the same UDTF without creating a new one.
        with SqlCounter(query_count=count - 3):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                wrapper(
                    double,
                    {
                        "stage_location": stage_name,
                        "name": f_name,
                        "if_not_exists": True,
                    },
                ),
            )
        # Calling the function again with replace=True will not re-create the UDTF, even if the contents are the same.
        with SqlCounter(query_count=count - 3):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                wrapper(
                    double,
                    {
                        "stage_location": stage_name,
                        "name": f_name,
                        "replace": True,
                    },
                ),
            )
        check_udf_exists(session, f_name)
    finally:
        drop_named_udf_and_udtfs()


def test_apply_permanent_lambda(session, stage_name):
    triple = lambda x: x * 3  # noqa: E731

    snow_df, native_df = create_test_dfs({"A": [1, 2, 3]})
    # Using a lambda randomly generates a name that will be reused on subsequent calls.
    try:
        with SqlCounter(query_count=6):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                get_applier(
                    triple,
                    {
                        "stage_location": stage_name,
                        "replace": True,
                    },
                ),
            )
        with SqlCounter(query_count=3):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                get_applier(
                    triple,
                    {
                        "stage_location": stage_name,
                        "if_not_exists": True,
                    },
                ),
            )
        # Constructing a new lambda will construct a new UDTF.
        plusone = lambda x: x + 1  # noqa: E731
        with SqlCounter(query_count=6):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                get_applier(
                    plusone,
                    {
                        "stage_location": stage_name,
                    },
                ),
            )
        lambda_names = get_cached_lambda_names(session)
        assert len(lambda_names) == 2
        for lambda_name in lambda_names:
            assert (
                len(session.sql(f"SHOW USER FUNCTIONS LIKE '{lambda_name}'").collect())
                == 1
            )
    finally:
        drop_named_udf_and_udtfs()


@sql_count_checker(query_count=0)
def test_apply_named_udf_negative(stage_name):
    # Test combinations of invalid parameters.
    df = pd.DataFrame({"A": [1, 2, 3]})

    def identity(x):
        return x

    # Missing stage_location param
    with pytest.raises(ValueError, match="`stage_location` must be set"):
        df.apply(identity, snowflake_udf_params={"name": "temp_udf"})
    # Cannot have stage_location and name when is_permanent is False
    with pytest.raises(ValueError, match="`bad_one` and `bad_two` are unsupported"):
        df.apply(
            identity,
            snowflake_udf_params={
                "bad_one": False,
                "bad_two": True,
                "stage_location": "@some_stage",
                "name": "temp_udf",
            },
        )


def test_grouby_apply_permanent(session, stage_name):
    data = {"group": ["A", "A", "B", "B"], "value": [1, 2, 3, 4]}
    udf_name = "test_groupby"

    def sum_plus_one(x):
        return x.sum() + 1

    def do_groupby_apply(persist_params):
        def wrapper(df):
            group = df.groupby("group")["value"]
            if isinstance(group, pd.groupby.DataFrameGroupBy):
                return group.apply(sum_plus_one, snowflake_udf_params=persist_params)
            return group.apply(sum_plus_one)

        return wrapper

    try:
        with SqlCounter(query_count=5):
            eval_snowpark_pandas_result(
                *create_test_dfs(data),
                do_groupby_apply(
                    {
                        "stage_location": stage_name,
                        "name": udf_name,
                        "replace": True,
                    }
                ),
                test_attrs=False,
            )
        with SqlCounter(query_count=2):
            eval_snowpark_pandas_result(
                *create_test_dfs(data),
                do_groupby_apply(
                    {
                        "stage_location": stage_name,
                        "name": udf_name,
                        "if_not_exists": True,
                    }
                ),
                test_attrs=False,
            )
        check_udf_exists(session, udf_name)
    finally:
        drop_named_udf_and_udtfs()
