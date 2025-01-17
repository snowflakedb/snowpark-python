#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import uuid

import modin.pandas as pd
import pandas as native_pd

from tests.integ.modin.utils import assert_frame_equal
from tests.integ.utils.sql_counter import SqlCounter


def test_read_pickle():
    df = native_pd.DataFrame({"foo": range(5), "bar": range(5, 10)})

    filename = f"test_read_pickle_{str(uuid.uuid4())}"
    try:
        native_pd.to_pickle(df, filename)
        with SqlCounter(query_count=1):
            assert_frame_equal(
                pd.read_pickle(filename),
                native_pd.read_pickle(filename),
                check_dtype=False,
            )
    finally:
        if os.path.exists(filename):
            os.remove(filename)
