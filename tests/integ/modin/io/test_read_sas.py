#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd

from tests.integ.modin.utils import assert_frame_equal
from tests.integ.utils.sql_counter import SqlCounter
from tests.utils import TestFiles


def test_read_sas_sas7bdat(resources_path):
    test_files = TestFiles(resources_path)

    filename = test_files.test_file_sas_sas7bdat

    with SqlCounter(query_count=1):
        assert_frame_equal(
            pd.read_sas(filename),
            native_pd.read_sas(filename),
            check_dtype=False,
        )


def test_read_sas_xpt(resources_path):
    test_files = TestFiles(resources_path)

    filename = test_files.test_file_sas_xpt

    with SqlCounter(query_count=1):
        assert_frame_equal(
            pd.read_sas(filename),
            native_pd.read_sas(filename),
            check_dtype=False,
            check_index_type=False,
        )
