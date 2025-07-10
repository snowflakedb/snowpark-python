#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import uuid

import modin.pandas as pd
import pandas as native_pd

from tests.integ.modin.utils import assert_frame_equal
from tests.integ.utils.sql_counter import SqlCounter
from tests.utils import Utils


def test_read_orc():
    df = native_pd.DataFrame({"foo": range(5), "bar": range(5, 10)})

    filename = f"test_read_orc_{str(uuid.uuid4())}"
    try:
        df.to_orc(filename)
        with SqlCounter(query_count=1):
            assert_frame_equal(
                pd.read_orc(filename),
                native_pd.read_orc(filename),
                check_dtype=False,
            )
    finally:
        if os.path.exists(filename):
            os.remove(filename)


def test_read_orc_from_stage(session):
    df = native_pd.DataFrame({"foo": range(5), "bar": range(5, 10)})

    filename = f"test_read_orc_{str(uuid.uuid4())}"
    try:
        df.to_orc(filename)

        stage_name = Utils.random_stage_name()
        Utils.create_stage(session, stage_name, is_temporary=True)
        Utils.upload_to_stage(session, "@" + stage_name, filename, compress=False)

        with SqlCounter(query_count=2):
            assert_frame_equal(
                pd.read_orc(f"@{stage_name}/{os.path.basename(filename)}"),
                native_pd.read_orc(filename),
                check_dtype=False,
            )
    finally:
        if os.path.exists(filename):
            os.remove(filename)
