#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import time

from tests.parameters import CONNECTION_PARAMETERS
from snowflake.snowpark import Session
from snowflake.snowpark.types import StringType, StructField, StructType


total_start = time.time()

session = Session.builder.configs(CONNECTION_PARAMETERS).create()

local_csv_path = "/Users/yuwang/Desktop/working_repo/snowpark-python/csv_reader_test_1gb.csv.gz"
temp_stage_name = "TEMP_CSV_READER_POC_STAGE"

# 1) Upload large gzip CSV to temp stage.
upload_start = time.time()
session.sql(f"create stage if not exists {temp_stage_name}").collect()
session.file.put(
    local_csv_path,
    f"@{temp_stage_name}",
    auto_compress=False,
    overwrite=False,
)
upload_elapsed = time.time() - upload_start

staged_csv_path = f"@{temp_stage_name}/{os.path.basename(local_csv_path)}"
print("staged file path:", staged_csv_path)
print("upload to stage time:", upload_elapsed)

# 2) Read from stage via session.read.csv() (current baseline way).
predefined_schema = StructType(
    [
        StructField("id", StringType()),
        StructField("event_ts", StringType()),
        StructField("category", StringType()),
        StructField("score", StringType()),
        StructField("notes", StringType()),
        StructField("country", StringType()),
        StructField("active", StringType()),
        StructField("payload", StringType()),
    ]
)

read_start = time.time()
df = (
    session.read
    .options({"SKIP_HEADER": 1, "FIELD_OPTIONALLY_ENCLOSED_BY": '"', "INFER_SCHEMA": True, "INFER_SCHEMA_OPTIONS": {"MAX_RECORDS_PER_FILE": 10000}})
    .csv(staged_csv_path)
)
row_count = df.count()
read_elapsed = time.time() - read_start

print("session.read.csv() time:", read_elapsed)
print("loaded row count:", row_count)
print("total elapsed time:", time.time() - total_start)

session.close()
