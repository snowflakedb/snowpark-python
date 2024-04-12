#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.lineage import LineageDirection


def test_lineage_trace(session):
    df = session.lineage.trace(
        "lineage_test_db.lineage_sch.t1",
        "table",
        direction=LineageDirection.DOWNSTREAM,
        depth=3,
    )
    df.show()

    df = session.lineage.trace("lineage_test_db.lineage_sch.v1", "view", depth=3)
    df.show()
