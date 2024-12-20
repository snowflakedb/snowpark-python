from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import parse_json, col
from snowflake.snowpark.types import StructType, MapType, StructField, StringType, IntegerType, FloatType, TimestampType
import logging; logging.getLogger("snowflake.snowpark").setLevel(logging.DEBUG)
import pandas as pd
from snowflake.snowpark.async_job import AsyncJob


# Function to generate random JSON data
def generate_json_data():
    import random
    import time
    import datetime
    return {
        "id": random.randint(1, 1000),
        "name": f"Item-{random.randint(1, 100)}",
        "price": round(random.uniform(10.0, 500.0), 2),
        "timestamp": datetime.datetime.now()
    }

logging.basicConfig()


session = Session.builder.create()

STATIC_TABLE_NAME = "static_df"

SIMULATED_STREAM_DATA_NAME = "static_df2"

# Create static dataframe
session.create_dataframe(
    pd.DataFrame(
        {
            'ID': [str(i) for i in range(1000)],
            'STATIC_VALUE': [generate_json_data() for _ in range(1000)]
        }
    )
).write.save_as_table(table_name=STATIC_TABLE_NAME, mode="overwrite")
static_df = session.table(STATIC_TABLE_NAME)

# Create static dataframe 2
data = [generate_json_data() for _ in range(10)]
session.create_dataframe(
    pd.DataFrame(
        {
            "ID": [row["id"] for row in data ],
            "TIMESTAMP": [row["timestamp"] for row in data],
            "NAME": [row["name"] for row in data],
        }
    )
).write.save_as_table(table_name=SIMULATED_STREAM_DATA_NAME, mode="overwrite")


kafka_event_schema = StructType(
            [
                StructField(column_identifier="ID", datatype=IntegerType()),
                StructField(column_identifier="NAME", datatype=StringType()),  
                StructField(column_identifier="PRICE", datatype=FloatType()),
                StructField(column_identifier="TIMESTAMP", datatype=TimestampType()),                               
            ]
        )


# Subscribe to 1 topic
kafka_ingest_df = (
    session
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
    .option("topic", "topic1")
    .option("partition_id", 1)
    .schema(kafka_event_schema)
    .load()
)

LANDING_TABLE_NAME = "dynamic_join_result"

transformed_df = kafka_ingest_df \
    .select(col("id"), col("timestamp"), col("name")) \
    .filter(col("price") > 100.0)

assert transformed_df._stream_source == "kafka"

streaming_query: AsyncJob = transformed_df \
    .writeStream \
    .toTable(LANDING_TABLE_NAME)


# The source table is from a kafka stream, so we write the result via  UDTF.

"""
This query looks like

SELECT
    write_stream_udf('dynamic_join_result', "id", "timestamp", "name")
FROM
    (
        SELECT
            id,
            name,
            price,
            timestamp
        FROM
            (
                TABLE (
                    my_streaming_udtf(
                        'host1:port1,host2:port2',
                        'topic1',
                        1::INT
                    )
                )
            )
    )
WHERE
    ("price" > 100.0)
"""


streaming_query.cancel()


# Read stream from a table
df_streamed_from_table =  (
    session
    .readStream
    .format("table")
    # TODO: Temporarily reading from another static table here because the UDTF
    # currently doesn't produce an output table.
    .option("table_name", SIMULATED_STREAM_DATA_NAME)
    # .option("table_name", LANDING_TABLE_NAME)
).load()


complex_df = df_streamed_from_table.join(static_df, on="ID").groupBy("NAME").count()

FINAL_TABLE_NAME = "final_table"


assert complex_df._stream_source == "table"

# One source is a Snowflake table, and the other source is a static table, so
# we write the result as a dynamic table instead of using a UDTF.

"""
The query here is:

CREATE
OR REPLACE DYNAMIC TABLE final_table LAG = '60 seconds' WAREHOUSE = NEW_WAREHOUSE REFRESH_MODE = 'incremental' AS
SELECT
    *
FROM
    (
        SELECT
            "NAME",
            count(1) AS "COUNT"
        FROM
            (
                SELECT
                    *
                FROM
                    (
                        (
                            SELECT
                                "ID" AS "ID",
                                "TIMESTAMP" AS "TIMESTAMP",
                                "NAME" AS "NAME"
                            FROM
                                static_df2
                        ) AS SNOWPARK_LEFT
                        INNER JOIN (
                            SELECT
                                "ID" AS "ID",
                                "STATIC_VALUE" AS "STATIC_VALUE"
                            FROM
                                static_df
                        ) AS SNOWPARK_RIGHT USING (ID)
                    )
            )
        GROUP BY
            "NAME"
    )
"""

(
    complex_df
    .writeStream
    .outputMode("append")
    # Dynamic Tables do not support lag values under 60 second(s).
    .trigger(processingTime='60 seconds')
    .toTable(FINAL_TABLE_NAME)
)