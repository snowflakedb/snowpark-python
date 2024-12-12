from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import parse_json, col
from snowflake.snowpark.types import StructType, MapType, StructField, StringType
import logging; logging.getLogger("snowflake.snowpark").setLevel(logging.DEBUG)
import pandas as pd


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


# Subscribe to 1 topic
kafka_ingest_df = (
    session
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
    .option("topic", "topic1")
    .option("partition_id", 1)
    .schema(
        StructType(
            [
                StructField(column_identifier="KEY", datatype=StringType()),
                StructField(column_identifier="STREAM_VALUE", datatype=StringType())                
            ]
        )
    )
    .load()
)

# Join kafka ingest to static table, and write result to dynamic table.
joined = kafka_ingest_df.join(static_df, on='KEY')
joined.create_or_replace_dynamic_table(
    'dynamic_join_result',
    warehouse=session.connection.warehouse,
    lag='1 hour',    
    
    )

# Clean up dynamic table.
drop_result = session.connection.cursor().execute('DROP DYNAMIC TABLE dynamic_join_result;')
assert drop_result is not None

# # Write streaming dataframe to output data sink
# sink_query = (
#     source_df
#     .writeStream
#     .format("snowflake")
#     .option("table", "<table>")
# )

# sink_query.start()