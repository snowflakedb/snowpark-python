#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import warnings

import pandas as pd
import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.functions import col, date_trunc
from snowflake.snowpark.types import DateType, StructField, StructType

Pandas4Warning = getattr(pd.errors, "Pandas4Warning", None)


@pytest.mark.skipif(
    Pandas4Warning is None, reason="Pandas4Warning exists from pandas 3"
)
def test_date_trunc_week_has_no_pandas_deprecation_warning(session):
    df = session.create_dataframe(
        [[datetime.date(2024, 1, 31)]],
        schema=StructType([StructField("d", DateType())]),
    )
    with warnings.catch_warnings():
        warnings.simplefilter("error", Pandas4Warning)
        result = df.select(date_trunc("week", col("d"))).collect()
    assert result == [Row(datetime.date(2024, 1, 29))]
