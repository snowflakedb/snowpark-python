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


def test_read_html_from_stage(session, resources_path):
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
        stage_name = Utils.random_stage_name()
        Utils.create_stage(session, stage_name, is_temporary=True)
        Utils.upload_to_stage(session, "@" + stage_name, filename, compress=False)

        with SqlCounter(query_count=2):
            assert_frame_equal(
                pd.read_html(f"@{stage_name}/{os.path.basename(filename)}")[0],
                native_pd.read_html(filename)[0],
                check_dtype=False,
            )
    finally:
        if os.path.exists(filename):
            os.remove(filename)
