#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import modin.pandas as pd
import pandas as native_pd
import pytest

from tests.integ.modin.utils import assert_frame_equal
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import TestFiles, Utils


def test_read_excel(resources_path):
    test_files = TestFiles(resources_path)

    filename = test_files.test_file_excel

    with SqlCounter(query_count=1):
        assert_frame_equal(
            pd.read_excel(filename),
            native_pd.read_excel(filename),
            check_dtype=False,
        )


@sql_count_checker(query_count=0)
def test_read_excel_no_lib_negative(resources_path):
    try:
        # skip test if we actually have calamine installed
        # this is not a common library to use
        import calamine  # noqa
    except Exception:
        test_files = TestFiles(resources_path)
        filename = test_files.test_file_excel
        with pytest.raises(
            ImportError, match="Snowpark Pandas requires an additional package"
        ):
            pd.read_excel(filename, engine="calamine")


def test_read_excel_from_stage(session, resources_path):
    test_files = TestFiles(resources_path)

    filename = test_files.test_file_excel

    stage_name = Utils.random_stage_name()
    Utils.create_stage(session, stage_name, is_temporary=True)
    Utils.upload_to_stage(session, "@" + stage_name, filename, compress=False)

    with SqlCounter(query_count=2):
        assert_frame_equal(
            pd.read_excel(f"@{stage_name}/{os.path.basename(filename)}"),
            native_pd.read_excel(filename),
            check_dtype=False,
        )


def test_read_excel_from_stage_inner_directory(session, resources_path):
    test_files = TestFiles(resources_path)

    filename = test_files.test_file_excel

    stage_name = Utils.random_stage_name()
    Utils.create_stage(session, stage_name, is_temporary=True)

    # Upload file to inner directory in stage
    inner_dir = "data/excel_files"
    stage_path_with_dir = f"@{stage_name}/{inner_dir}"
    Utils.upload_to_stage(session, stage_path_with_dir, filename, compress=False)

    with SqlCounter(query_count=2):
        assert_frame_equal(
            pd.read_excel(f"@{stage_name}/{inner_dir}/{os.path.basename(filename)}"),
            native_pd.read_excel(filename),
            check_dtype=False,
        )
