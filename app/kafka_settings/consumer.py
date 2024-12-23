import json
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv(verbose=True)

bootstrap_servers = os.environ["BOOTSTRAP_SERVERS"]
db_topic = os.environ["MONGO_TOPIC"]
def consume_topics(topics, process_message):
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for message in consumer:
        print(message.value)
        for event in message.value:
            process_message(event, message.key)
            print(event)