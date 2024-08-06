#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import pandas as pd
import pytest

from snowflake.snowpark import DataFrame


@pytest.mark.filterwarnings("error::FutureWarning")
def test_extract_schema_from_df_without_future_warning(session):
    """
    Make sure that while converting a Pandas dataframe to a Snowflake dataframe no
    FutureWarnings are thrown, which hint at upcoming incompatibilities.
    """
    pandas_df = pd.DataFrame({"A": [1.0]}, dtype=float)
    df = session.create_dataframe(pandas_df)
    assert isinstance(df, DataFrame)

    pandas_df = pd.DataFrame({"Timestamp": [pd.to_datetime(1490195805, unit="s")]})
    df = session.create_dataframe(pandas_df)
    assert isinstance(df, DataFrame)
