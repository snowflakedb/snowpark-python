#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import modin.pandas as pd
import pandas as native_pd

from snowflake.snowpark._internal.utils import generate_random_alphanumeric
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import assert_frame_equal
from tests.utils import IS_WINDOWS, TestFiles, Utils

def test_read_excel(resources_path):
    test_files = TestFiles(resources_path)

    filename = test_files.test_file_excel

    with SqlCounter(query_count=1):
        assert_frame_equal(
            pd.read_excel(filename),
            native_pd.read_excel(filename),
            check_dtype=False,
        )

