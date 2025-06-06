#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
import modin.pandas as pd
import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin._internal.utils import MODIN_IS_AT_LEAST_0_33_0

from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.fixture(scope="module", autouse=True)
def skip(pytestconfig):
    if not MODIN_IS_AT_LEAST_0_33_0:
        pytest.skip(
            "backend switching tests only work on newer modin versions",
            allow_module_level=True,
        )


@sql_count_checker(query_count=0)
def test_to_pandas():
    # SNOW-2106995: to_pandas should be registered on the pandas backend
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    assert df.get_backend() == "Pandas"
    assert df.to_pandas().equals(df._to_pandas())
    assert df["a"].to_pandas().equals(df["a"]._to_pandas())


def test_to_snowflake_and_to_snowpark():
    # SNOW-2115929: to_snowflake/to_snowpark should be registered on the pandas backend
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    with SqlCounter(query_count=5):
        assert df.get_backend() == "Pandas"
        assert (
            df.to_snowpark()
            .to_pandas()  # query #1
            .equals(df.move_to("snowflake").to_snowpark().to_pandas())  # query #2
        )
        assert df.get_backend() == "Pandas"
        df.to_snowflake(
            "hybrid_temp_test", if_exists="replace", index=False
        )  # query #3
        assert (
            pd.read_snowflake("hybrid_temp_test")  # query #4
            .to_pandas()  # query #5
            # to_snowflake() and back round trip changes dtypes.
            .astype(df.to_pandas().dtypes)  # no queries (data is native)
            .equals(df.to_pandas())
        )

    with SqlCounter(query_count=5):
        column = df["a"]
        assert column.get_backend() == "Pandas"
        assert (
            column.to_snowpark()
            .to_pandas()  # query #1
            .equals(column.move_to("snowflake").to_snowpark().to_pandas())  # query #2
        )
        assert column.get_backend() == "Pandas"
        column.to_snowflake(
            "hybrid_temp_test", if_exists="replace", index=False
        )  # query #3
        assert (
            pd.read_snowflake("hybrid_temp_test")  # query #4
            .to_pandas()  # query #5
            # # to_snowflake() and back round trip changes dtypes.
            .astype(column.to_pandas().dtype)  # no queries (data is native)
            .equals(column.to_pandas().to_frame())
        )
