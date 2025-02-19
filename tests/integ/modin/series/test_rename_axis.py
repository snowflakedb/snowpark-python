#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize("mapper", [None, "A", ["A"]])
@pytest.mark.parametrize("copy", [True, False])
@pytest.mark.parametrize("inplace", [True, False])
@sql_count_checker(query_count=1)
def test_rename_axis(mapper, copy, inplace):
    data = [10, 1, 1.6]

    native_ser = native_pd.Series(data)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.rename_axis(
            mapper=mapper,
            copy=copy,
            inplace=inplace,
        ),
        inplace=inplace,
        test_attrs=False if inplace else True,
        check_names=False if inplace else True,
    )
