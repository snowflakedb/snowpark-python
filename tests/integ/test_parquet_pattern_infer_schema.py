#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.types import StringType
from tests.utils import Utils


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: parquet not supported",
)
def test_parquet_pattern_infer_with_metadata_files(session):
    """Integration test: when a stage contains both .parquet data files and
    _common_metadata files, reading with PATTERN should correctly infer
    timestamp columns instead of falling back to VARIANT."""
    stage_name = Utils.random_name_for_temp_object(TempObjectType.STAGE)

    ts1 = datetime.datetime(2024, 1, 15, 10, 30, 0, 123456)
    ts2 = datetime.datetime(2024, 6, 20, 14, 45, 30, 789012)

    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("updated_time", pa.timestamp("us", tz="UTC")),
        ]
    )

    table = pa.table(
        {
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "updated_time": [ts1, ts2],
        },
        schema=arrow_schema,
    )

    bad_arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("updated_time", pa.timestamp("us")),
        ]
    )

    try:
        Utils.create_stage(session, stage_name, is_temporary=True)

        with tempfile.TemporaryDirectory() as tmpdir:
            data_path = os.path.join(tmpdir, "data.parquet")
            pq.write_table(table, data_path)

            meta_path = os.path.join(tmpdir, "_common_metadata")
            empty_table = pa.table(
                {
                    name: pa.array([], type=bad_arrow_schema.field(name).type)
                    for name in bad_arrow_schema.names
                },
                schema=bad_arrow_schema,
            )
            pq.write_table(empty_table, meta_path)

            session.file.put(
                data_path, f"@{stage_name}/subdir", auto_compress=False, overwrite=True
            )
            session.file.put(
                meta_path, f"@{stage_name}/subdir", auto_compress=False, overwrite=True
            )

        df = session.read.option("PATTERN", ".*[.]parquet").parquet(
            f"@{stage_name}/subdir"
        )

        schema = df.schema
        ts_field = [
            f for f in schema.fields if f.name.strip('"').upper() == "UPDATED_TIME"
        ][0]
        assert not isinstance(
            ts_field.datatype, StringType
        ), f"Expected timestamp-like type, got {ts_field.datatype}"

        rows = df.collect()
        assert len(rows) == 2

    finally:
        Utils.drop_stage(session, stage_name)
