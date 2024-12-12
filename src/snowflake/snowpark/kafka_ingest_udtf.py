

import json
import logging
import time

from confluent_kafka import Consumer, KafkaException, TopicPartition, TIMESTAMP_CREATE_TIME, TIMESTAMP_LOG_APPEND_TIME,KafkaError

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

class KafkaFetch:
  def __init__(self):
    self.__consumer = {}
   
  def createConsumerIfNotExist(self, bootstrap_servers, topic, partition_id):
    if (topic, partition_id) in self.__consumer:
        return self.__consumer[(topic, partition_id)]
    # Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,  # Address of the Kafka server
        'group.id': 'my-consumer-group',        # Consumer group ID
        'auto.offset.reset': 'earliest',         # Start reading at the beginning if no offset is committed
        'allow.auto.create.topics': False,
        'enable.auto.commit': False,
    }

    # Create a Consumer instance
    consumer = Consumer(consumer_config)
    
    # Assign partition
    topic_partition = TopicPartition(topic, partition_id)
    consumer.assign([topic_partition])

    self.__consumer[(topic, partition_id)] = consumer
    return consumer
    
  def process(self, bootstrap_servers: str, topic: str, partition_id: int):
    consumer = self.createConsumerIfNotExist(bootstrap_servers=bootstrap_servers, topic=topic, partition_id=partition_id)
   
    try:
        for i in range(10):
            # # Poll for a message
            # msg = consumer.poll(1.0)  # Timeout in seconds

            # if msg is None:
            #     # No message received within the timeout
            #     continue

            # if msg.error():
            #     # Handle Kafka errors
            #     if msg.error().code() == KafkaError._PARTITION_EOF:
            #         # End of partition event
            #         logging.error(f"End of partition reached: {msg.error()}")
            #     elif msg.error():
            #         raise KafkaException(msg.error())
            # else:
            #     # Successfully received a message
            #     logging.info(f"Received message: {msg.value().decode('utf-8')}")
            #     yield (msg.value().decode('utf-8'),)

            yield (str(i), str(generate_json_data()))

    except:
        logging.error("Consumer Error")

    finally:
        consumer.close()


