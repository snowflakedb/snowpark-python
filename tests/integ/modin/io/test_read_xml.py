#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from io import BytesIO, StringIO
import os
import uuid

import modin.pandas as pd
import pandas as native_pd

from tests.integ.modin.utils import assert_frame_equal
from tests.integ.utils.sql_counter import SqlCounter
from tests.utils import Utils

xml = """<?xml version='1.0' encoding='utf-8'?>
<doc:data xmlns:doc="https://example.com">
  <doc:row>
    <doc:shape>square</doc:shape>
    <doc:degrees>360</doc:degrees>
    <doc:sides>4.0</doc:sides>
  </doc:row>
  <doc:row>
    <doc:shape>circle</doc:shape>
    <doc:degrees>360</doc:degrees>
    <doc:sides/>
  </doc:row>
  <doc:row>
    <doc:shape>triangle</doc:shape>
    <doc:degrees>180</doc:degrees>
    <doc:sides>3.0</doc:sides>
  </doc:row>
</doc:data>"""


def test_read_xml_basic():
    with SqlCounter(query_count=1):
        assert_frame_equal(
            pd.read_xml(StringIO(xml)),
            native_pd.read_xml(StringIO(xml)),
            check_dtype=False,
        )


def test_read_xml_iterparse():
    with SqlCounter(query_count=1):
        assert_frame_equal(
            pd.read_xml(
                BytesIO(xml.encode()), iterparse={"row": ["shape", "degrees", "sides"]}
            ),
            native_pd.read_xml(
                BytesIO(xml.encode()), iterparse={"row": ["shape", "degrees", "sides"]}
            ),
            check_dtype=False,
        )


def test_read_xml_xpath():
    with SqlCounter(query_count=1):
        assert_frame_equal(
            pd.read_xml(
                StringIO(xml),
                xpath="//doc:row",
                namespaces={"doc": "https://example.com"},
            ),
            native_pd.read_xml(
                StringIO(xml),
                xpath="//doc:row",
                namespaces={"doc": "https://example.com"},
            ),
            check_dtype=False,
        )


def test_read_xml_from_stage(session, resources_path):
    df = native_pd.DataFrame({"foo": range(5), "bar": range(5, 10)})

    filename = f"test_read_xml_{str(uuid.uuid4())}"

    try:
        df.to_xml(filename)

        stage_name = Utils.random_stage_name()
        Utils.create_stage(session, stage_name, is_temporary=True)
        Utils.upload_to_stage(session, "@" + stage_name, filename, compress=False)

        with SqlCounter(query_count=2):
            assert_frame_equal(
                pd.read_xml(f"@{stage_name}/{os.path.basename(filename)}"),
                native_pd.read_xml(filename),
                check_dtype=False,
            )
    finally:
        if os.path.exists(filename):
            os.remove(filename)
