#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import snowflake.snowpark.modin.plugin  # noqa: F401

from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=0)
def test_to_pandas():
    # SNOW-2106995: to_pandas should be registered on the pandas backend
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    assert df.get_backend() == "Pandas"
    assert df.to_pandas().equals(df._to_pandas())
    assert df["a"].to_pandas().equals(df["a"]._to_pandas())


@sql_count_checker(query_count=4)
def test_to_snowflake_and_to_snowpark():
    # SNOW-2115929: to_snowflake/to_snowpark should be registered on the pandas backend
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    assert df.get_backend() == "Pandas"
    assert (
        df.to_snowpark()
        .to_pandas()
        .equals(df.move_to("snowflake").to_snowpark().to_pandas())
    )
    assert df.get_backend() == "Pandas"
    df.to_snowflake("hybrid_temp_test", if_exists="replace", index=False)
    assert (
        pd.read_snowflake("hybrid_temp_test")
        .to_pandas()
        # to_snowflake() and back round trip changes dtypes.
        .astype(df.to_pandas().dtypes)
        .equals(df.to_pandas())
    )

    column = df["a"]
    assert column.get_backend() == "Pandas"
    assert (
        column.to_snowpark()
        .to_pandas()
        .equals(column.move_to("snowflake").to_snowpark().to_pandas())
    )
    assert column.get_backend() == "Pandas"
    column.to_snowflake("hybrid_temp_test", if_exists="replace", index=False)
    assert (
        pd.read_snowflake("hybrid_temp_test")
        .to_pandas()
        # # to_snowflake() and back round trip changes dtypes.
        .astype(column.to_pandas().dtype)
        .equals(column.to_pandas().to_frame())
    )
