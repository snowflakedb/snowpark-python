#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import uuid

import modin.pandas as pd
import pandas as native_pd

from tests.integ.modin.utils import assert_frame_equal
from tests.integ.utils.sql_counter import SqlCounter


def test_read_html():
    html_str = """
            <table>
                <tr>
                    <th>A</th>
                    <th colspan="1">B</th>
                    <th rowspan="1">C</th>
                </tr>
                <tr>
                    <td>a</td>
                    <td>b</td>
                    <td>c</td>
                </tr>
            </table>
        """
    filename = f"test_read_html_{str(uuid.uuid4())}"

    with open(filename, "w") as f:
        f.write(html_str)

    try:
        with SqlCounter(query_count=1):
            assert_frame_equal(
                pd.read_html(filename)[0],
                native_pd.read_html(filename)[0],
                check_dtype=False,
            )
    finally:
        if os.path.exists(filename):
            os.remove(filename)
