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

# Create static dataframe
session.create_dataframe(
    pd.DataFrame(
        {
            'KEY': [str(i) for i in range(10)],
            'STATIC_VALUE': [generate_json_data() for _ in range(10)]
        }
    )
).write.save_as_table(table_name="static_df", mode="overwrite")
static_df = session.table("static_df")


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

LANDING_TABLE_NAME = "dynamic_join_result";

transformed_df = kafka_ingest_df \
    .select(col("id"), col("timestamp"), col("name")) \
    .filter(col("price") > 100.0)


"""
This query looks like

SELECT write_stream_udf('dynamic_join_result', "id", "timestamp", "name")
FROM   (SELECT id,
               name,
               price,
               timestamp
        FROM   ( TABLE (my_streaming_udtf('host1:port1,host2:port2', 'topic1', 1
                        :: INT
                 ) )))
WHERE  ( "price" > 100.0 ) 
"""

streaming_query: AsyncJob = transformed_df \
    .writeStream \
    .toTable(LANDING_TABLE_NAME)



streaming_query.cancel()


# Read stream from a table
df_streamed_from_table =  (
    session
    .readStream
    .format("table")
    .option("table_name", LANDING_TABLE_NAME)
)


# # Write streaming dataframe to output data sink
# sink_query = (
#     source_df
#     .writeStream
#     .format("snowflake")
#     .option("table", "<table>")
# )

# sink_query.start()