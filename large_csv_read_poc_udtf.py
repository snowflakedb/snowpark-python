#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import time

from tests.parameters import CONNECTION_PARAMETERS
from snowflake.snowpark import Session
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    attribute_to_schema_string_deep,
)
from snowflake.snowpark.functions import col, lit
from snowflake.snowpark.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    VariantType,
)


session = Session.builder.configs(CONNECTION_PARAMETERS).create()

# 1) Register UDTF from csv_reader.py.
csv_reader_file_path = "src/snowflake/snowpark/_internal/csv_reader.py"
handler_name = "CSVReader"

udtf_output_schema = StructType([StructField("ROW_DATA", VariantType(), True)])
udtf_input_types = [
    StringType(),  # filename
    IntegerType(),  # num_workers
    IntegerType(),  # i (worker id)
    StringType(),  # custom_schema
]
total_start =time.time()
start = time.time()
csv_reader_udtf = session.udtf.register_from_file(
    csv_reader_file_path,
    handler_name,
    output_schema=udtf_output_schema,
    input_types=udtf_input_types,
    packages=["snowflake-snowpark-python"],
    replace=True,
    _suppress_local_package_warnings=True,
)
end = time.time()
print("time register udtf ", end - start)

# 2) Upload CSV to a temp stage.
local_csv_path = "/Users/yuwang/Desktop/working_repo/snowpark-python/csv_reader_test_1gb.csv.gz"
temp_stage_name = "TEMP_CSV_READER_POC_STAGE"
session.sql(f"create stage if not exists {temp_stage_name}").collect()
start = time.time()
session.file.put(
    local_csv_path,
    f"@{temp_stage_name}",
    auto_compress=False,
    overwrite=True,
)
end = time.time()
print("upload to stage time: ", end - start)

staged_csv_path = f"@{temp_stage_name}/{os.path.basename(local_csv_path)}"

# 3) Read with registered UDTF.
# Define your pre-defined schema and pass it to the reader as schema string.
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
custom_schema_string = attribute_to_schema_string_deep(predefined_schema._to_attributes())

start =time.time()
# Keep worker cap aligned with current xml reader query practice.
num_workers = 16
workers_df = session.range(num_workers).to_df("WORKER")

udtf_df = workers_df.select(
    col("WORKER"),
    csv_reader_udtf(
        lit(staged_csv_path),
        lit(num_workers),
        col("WORKER"),
        lit(custom_schema_string),
    ).alias("ROW_DATA"),
)

# Example actions to trigger execution.
print("Staged file path:", staged_csv_path)
print("Total rows read via UDTF:", udtf_df.count())
end = time.time()
print("ingestion time: ", end - start)

print("total time: ", time.time() - total_start)
# 4) Close session when done.
session.close()
