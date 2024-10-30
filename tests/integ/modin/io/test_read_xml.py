#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from io import StringIO

import modin.pandas as pd
import pandas as native_pd

from tests.integ.modin.utils import assert_frame_equal
from tests.integ.utils.sql_counter import SqlCounter


def test_read_xml():
    xml = """<?xml version='1.0' encoding='utf-8'?>
            <data xmlns="http://example.com">
                <row>
                    <shape>square</shape>
                    <degrees>360</degrees>
                    <sides>4.0</sides>
                </row>
                <row>
                    <shape>circle</shape>
                    <degrees>360</degrees>
                    <sides/>
                </row>
                <row>
                    <shape>triangle</shape>
                    <degrees>180</degrees>
                    <sides>3.0</sides>
                </row>
            </data>
        """

    with SqlCounter(query_count=1):
        assert_frame_equal(
            pd.read_xml(StringIO(xml)),
            native_pd.read_xml(StringIO(xml)),
            check_dtype=False,
        )
