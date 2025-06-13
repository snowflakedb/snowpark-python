#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import tempfile
from typing import Any, Tuple
import modin.pandas as pd
import pandas as native_pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_series_equal
from tests.integ.utils.sql_counter import sql_count_checker

temp_dir = tempfile.TemporaryDirectory()
TEMP_DIR_NAME = temp_dir.name


def get_filepaths(ext: Any, test_name: str) -> Tuple[str, str]:
    if ext:
        native_path = os.path.join(TEMP_DIR_NAME, f"native_{test_name}.{ext}")
        snow_path = os.path.join(TEMP_DIR_NAME, f"snow_{test_name}.{ext}")
    else:
        native_path = os.path.join(TEMP_DIR_NAME, f"native_{test_name}")
        snow_path = os.path.join(TEMP_DIR_NAME, f"snow_{test_name}")
    # Remove files if exist.
    if os.path.exists(native_path):
        os.remove(native_path)
    if os.path.exists(snow_path):
        os.remove(snow_path)
    return native_path, snow_path


@sql_count_checker(query_count=2)
def test_to_excel_basic():
    native_series = native_pd.Series(["one", None, "", "two"], name="A")
    native_path, snow_path = get_filepaths("xlsx", "basic")

    # Write excel file with native pandas.
    native_series.to_excel(native_path)
    # Write excel file with snowpark pandas.
    pd.Series(native_series).to_excel(snow_path)

    # Read excel file written by native pandas into a native pandas series.
    native_series = native_pd.read_excel(native_path).iloc[:, 0]
    # Read excel file written by snowpark pandas into a snowpark pandas series.
    snow_series = pd.read_excel(snow_path).iloc[:, 0]

    # compare series
    assert_series_equal(native_series, snow_series)
