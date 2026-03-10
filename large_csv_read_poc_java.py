#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import time

from tests.parameters import CONNECTION_PARAMETERS
from snowflake.snowpark import Session

from csv_reader_java import (
    JAVA_CSV_UDTF_JAR_STAGE_PATH,
    JAVA_CSV_UDTF_NAME,
    create_java_csv_udtf_sql,
)


session = Session.builder.configs(CONNECTION_PARAMETERS).create()

total_start = time.time()
local_csv_path = "/Users/yuwang/Desktop/working_repo/snowpark-python/csv_reader_test_1gb.csv.gz"
temp_stage_name = "TEMP_CSV_READER_POC_STAGE"
num_workers = 16

# 1) Upload source csv(.gz) to stage.
upload_start = time.time()
# session.sql(f"create stage if not exists {temp_stage_name}").collect()
# session.file.put(
#     local_csv_path,
#     f"@{temp_stage_name}",
#     auto_compress=False,
#     overwrite=False,
# )
upload_end = time.time()
print("upload to stage time:", upload_end - upload_start)

staged_csv_path = f"@{temp_stage_name}/{os.path.basename(local_csv_path)}"
print("Staged file path:", staged_csv_path)

# 2) Create Java UDTF by SQL string (IMPORTS jar path).
register_start = time.time()
if "YOUR_JAVA_UDTF_STAGE" in JAVA_CSV_UDTF_JAR_STAGE_PATH:
    print(
        "WARNING: Update JAVA_CSV_UDTF_JAR_STAGE_PATH in csv_reader_java.py "
        "to the staged jar path that includes splittablegzip."
    )
create_udtf_sql = create_java_csv_udtf_sql()
print("=== CREATE JAVA UDTF SQL BEGIN ===")
print(create_udtf_sql)
print("=== CREATE JAVA UDTF SQL END ===")
session.sql(create_udtf_sql).collect()
register_end = time.time()
print("time create java udtf:", register_end - register_start)

# 3) Read staged file through Java UDTF with worker-parallel SQL shape.
ingest_start = time.time()
read_sql = f"""
SELECT
  r.id,
  r.event_ts,
  r.category,
  r.score,
  r.notes,
  r.country,
  r.active,
  r.payload
FROM (
  SELECT SEQ4() AS worker_id
  FROM TABLE(GENERATOR(ROWCOUNT => {num_workers}))
) w,
TABLE(
  {JAVA_CSV_UDTF_NAME}('{staged_csv_path}', {num_workers}, w.worker_id)
) r
"""

java_udtf_df = session.sql(read_sql)
row_count = java_udtf_df.count()
ingest_end = time.time()

print("Total rows read via Java UDTF:", row_count)
print("ingestion time:", ingest_end - ingest_start)
print("total time:", time.time() - total_start)

session.close()
