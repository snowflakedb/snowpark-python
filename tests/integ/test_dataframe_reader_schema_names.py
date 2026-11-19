#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import json
import os

import pytest

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType
from tests.utils import Utils

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="reading staged files is not supported in local testing mode",
    ),
]

DDL = '"a""b" INT, "x.y z" STRING, id INT'
STRUCT_TYPE = StructType(
    [
        StructField('"a""b"', IntegerType()),
        StructField('"x.y z"', StringType()),
        StructField("id", IntegerType()),
    ]
)
# the names in the files are exact, so they only match the quoted and the unquoted names as written
ROWS = [{'a"b': 1, "x.y z": "hello", "id": 7}, {'a"b': 2, "x.y z": "world", "id": 8}]


@pytest.fixture(scope="module")
def stage_with_files(session, tmp_path_factory):
    stage = Utils.random_stage_name()
    Utils.create_stage(session, stage, is_temporary=True)
    directory = tmp_path_factory.mktemp("schema_names")
    json_path = os.path.join(directory, "names.json")
    with open(json_path, "w") as f:
        f.write("\n".join(json.dumps(row) for row in ROWS))
    session.file.put(json_path, f"@{stage}", auto_compress=False)
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        parquet_path = os.path.join(directory, "names.parquet")
        pq.write_table(
            pa.table({key: [row[key] for row in ROWS] for key in ROWS[0]}), parquet_path
        )
        session.file.put(parquet_path, f"@{stage}", auto_compress=False)
    except ImportError:  # pragma: no cover
        pass
    yield stage
    Utils.drop_stage(session, stage)


@pytest.mark.parametrize("file_format", ["json", "parquet"])
@pytest.mark.parametrize("on_error", [None, "CONTINUE"])
@pytest.mark.parametrize("schema", [DDL, STRUCT_TYPE])
def test_read_quoted_field_names(
    session, stage_with_files, file_format, on_error, schema
):
    if file_format == "parquet":
        pytest.importorskip("pyarrow")
    reader = session.read.schema(schema)
    if on_error is not None:
        # reads with COPY INTO a temporary table
        reader = reader.option("ON_ERROR", on_error)
    df = getattr(reader, file_format)(f"@{stage_with_files}/names.{file_format}")
    rows = sorted(df.collect(), key=lambda row: row[0])
    # the values are read, not NULL
    assert [tuple(row) for row in rows] == [(1, "hello", 7), (2, "world", 8)]
    # the quoted names are kept exactly, and the same for both ways to give the schema
    assert df.columns[:2] == ['"a""b"', '"x.y z"']
    assert len(df.columns) == 3
