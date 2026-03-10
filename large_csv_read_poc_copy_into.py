#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import csv
import gzip
import io
import os
import time
from concurrent.futures import ThreadPoolExecutor

from tests.parameters import CONNECTION_PARAMETERS
from snowflake.snowpark import Session


total_start = time.time()

session = Session.builder.configs(CONNECTION_PARAMETERS).create()

source_csv_path = (
    "/Users/yuwang/Desktop/working_repo/snowpark-python/csv_reader_test_1gb.csv.gz"
)
num_parts = 16
temp_stage_name = "TEMP_CSV_COPY_INTO_POC_STAGE"
target_table_name = "TEMP_CSV_COPY_INTO_POC_TABLE"
stage_part_paths = [f"@{temp_stage_name}/part_{i:02d}.csv" for i in range(num_parts)]

split_start = time.time()
part_row_counts = [0 for _ in range(num_parts)]
part_buffers = [io.BytesIO() for _ in range(num_parts)]
part_text_streams = [
    io.TextIOWrapper(buffer, encoding="utf-8", newline="") for buffer in part_buffers
]
part_writers = [
    csv.writer(stream, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
    for stream in part_text_streams
]

open_func = gzip.open if source_csv_path.lower().endswith(".gz") else open
with open_func(source_csv_path, "rt", newline="", encoding="utf-8") as src_fp:
    reader = csv.reader(src_fp)
    header = next(reader)

    for writer in part_writers:
        writer.writerow(header)

    row_idx = 0
    for row in reader:
        part_idx = row_idx % num_parts
        part_writers[part_idx].writerow(row)
        part_row_counts[part_idx] += 1
        row_idx += 1

for stream in part_text_streams:
    stream.flush()
    stream.detach()

split_elapsed = time.time() - split_start
print("split source csv time:", split_elapsed)
print("total rows split:", sum(part_row_counts))
print("rows per part:", part_row_counts)

upload_start = time.time()
session.sql(f"create temp stage if not exists {temp_stage_name}").collect()
with ThreadPoolExecutor(max_workers=num_parts) as thread_executor:
    upload_futures = []
    for i in range(num_parts):
        part_buffers[i].seek(0)
        future = thread_executor.submit(
            session.file.put_stream,
            part_buffers[i],
            stage_part_paths[i],
            overwrite=True,
            auto_compress=False,
        )
        upload_futures.append(future)

    for future in upload_futures:
        future.result()

for buffer in part_buffers:
    buffer.close()
upload_elapsed = time.time() - upload_start
print("upload split files (in-memory, multithread) time:", upload_elapsed)

quoted_cols = [f'"{col.replace(chr(34), chr(34) * 2)}" STRING' for col in header]
create_table_sql = f"create temp table if not exists {target_table_name} ({', '.join(quoted_cols)})"

copy_start = time.time()
session.sql(create_table_sql).collect()
copy_sql = f"""
COPY INTO {target_table_name}
FROM @{temp_stage_name}
PATTERN='.*part_.*\\.csv'
FILE_FORMAT=(
  TYPE=CSV
  SKIP_HEADER=1
  FIELD_OPTIONALLY_ENCLOSED_BY='\"'
  COMPRESSION=NONE
)
ON_ERROR='CONTINUE'
"""
copy_result = session.sql(copy_sql).collect()
copy_elapsed = time.time() - copy_start

row_count = session.sql(f"select count(*) as CNT from {target_table_name}").collect()[0]["CNT"]

print("copy into time:", copy_elapsed)
print("copy into output rows:", len(copy_result))
print("loaded row count:", row_count)
print("total elapsed time:", time.time() - total_start)

session.close()
