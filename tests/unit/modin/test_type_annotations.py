#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import get_type_hints

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401


@pytest.mark.parametrize(
    "method,type_hints",
    [
        (pd.Series.empty.fget, {"return": bool}),
        (pd.DataFrame.columns.fget, {"return": native_pd.Index}),
    ],
)
def test_properties_snow_1374293(method, type_hints):
    assert get_type_hints(method) == type_hints


def test_dataframe_method():
    # Use a method with a simple signature to avoid
    # https://github.com/python/cpython/issues/118919
    assert get_type_hints(pd.DataFrame.compare) == {
        "keep_shape": bool,
        "keep_equal": bool,
        "return": pd.DataFrame,
    }
