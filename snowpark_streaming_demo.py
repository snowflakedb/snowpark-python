from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import parse_json, col
from snowflake.snowpark.types import StructType, MapType, StructField
import logging; logging.getLogger("snowflake.snowpark").setLevel(logging.DEBUG)

logging.basicConfig()


session = Session.builder.create()

# Subscribe to 1 topic
source_df = (
    session
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
    .option("topic", "topic1")
    .option("partition_id", 1)
    .schema(
        StructType(
            [
                StructField(column_identifier="records", datatype=MapType())
            ]
        )
    )
    .load()
)


print(source_df.collect())

# transformation (simple)
transformed_df = (
    source_df
    .select(col("*"))
)

# Write to output data sink
sink_query = (
    source_df
    .writeStream
    .format("snowflake")
    .option("table", "<table>")
)

sink_query.start()