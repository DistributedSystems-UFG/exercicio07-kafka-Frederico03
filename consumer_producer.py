from kafka import KafkaConsumer, KafkaProducer
from const import *
import sys

try:
    topic_in = sys.argv[1]
    topic_out = sys.argv[2]
except:
    print('Usage: python3 consumer_producer <topic_in> <topic_out>')
    exit(1)

consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

consumer.subscribe([topic_in])
for msg in consumer:
    received = msg.value.decode()
    print('Received from ' + topic_in + ': ' + received)

    processed = 'PROCESSED: ' + received
    producer.send(topic_out, value=processed.encode())
    print('Forwarded to ' + topic_out + ': ' + processed)
    producer.flush()
