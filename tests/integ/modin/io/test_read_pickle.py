#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import tempfile

import modin.pandas as pd
import pandas as native_pd

from tests.integ.modin.utils import assert_frame_equal
from tests.integ.utils.sql_counter import SqlCounter
from tests.utils import Utils


def test_read_pickle():
    df = native_pd.DataFrame({"foo": range(5), "bar": range(5, 10)})

    with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp_file:
        filename = tmp_file.name
        df.to_pickle(filename)
        tmp_file.close()

        native_pd.to_pickle(df, filename)
        with SqlCounter(query_count=1):
            assert_frame_equal(
                pd.read_pickle(filename),
                native_pd.read_pickle(filename),
                check_dtype=False,
            )


def test_read_pickle_from_stage(session, resources_path):
    df = native_pd.DataFrame({"foo": range(5), "bar": range(5, 10)})

    with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp_file:
        filename = tmp_file.name
        df.to_pickle(filename)
        tmp_file.close()

        stage_name = Utils.random_stage_name()
        Utils.create_stage(session, stage_name, is_temporary=True)
        Utils.upload_to_stage(session, "@" + stage_name, filename, compress=False)

        with SqlCounter(query_count=2):
            assert_frame_equal(
                pd.read_pickle(f"@{stage_name}/{os.path.basename(filename)}"),
                native_pd.read_pickle(filename),
                check_dtype=False,
            )
