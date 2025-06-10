#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import os.path
import pandas as native_pd

from tests.integ.modin.utils import assert_frame_equal
from tests.integ.utils.sql_counter import SqlCounter
from tests.utils import TestFiles, Utils


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


def test_read_sas_sas7bdat_from_stage(session, resources_path):
    test_files = TestFiles(resources_path)

    filename = test_files.test_file_sas_sas7bdat

    stage_name = Utils.random_stage_name()
    Utils.create_stage(session, stage_name, is_temporary=True)
    Utils.upload_to_stage(session, "@" + stage_name, filename, compress=False)

    with SqlCounter(query_count=2):
        assert_frame_equal(
            pd.read_sas(f"@{stage_name}/{os.path.basename(filename)}"),
            native_pd.read_sas(filename),
            check_dtype=False,
        )


def test_read_sas_xpt_from_stage(session, resources_path):
    test_files = TestFiles(resources_path)

    filename = test_files.test_file_sas_xpt

    stage_name = Utils.random_stage_name()
    Utils.create_stage(session, stage_name, is_temporary=True)
    Utils.upload_to_stage(session, "@" + stage_name, filename, compress=False)

    with SqlCounter(query_count=2):
        assert_frame_equal(
            pd.read_sas(f"@{stage_name}/{os.path.basename(filename)}"),
            native_pd.read_sas(filename),
            check_dtype=False,
            check_index_type=False,
        )
