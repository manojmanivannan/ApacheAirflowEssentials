from stream_logger import logger
from kafka import KafkaProducer, KafkaConsumer
import kafka
from faker import Faker
from kafka.errors import NoBrokersAvailable
import os
from time import sleep

logger.info('Fake streamer started')

# Kafka broker details
bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
topic_name = os.environ.get('KAFKA_TOPIC', 'transactions')

# Create Kafka producer
   



def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        logger.info(f'Message published successfully: {value}')
    except Exception as ex:
        logger.error('Exception in publishing message')
        logger.error(str(ex))


def connect_kafka_producer():
    _producer = None
    while True:
        try:
            _producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            break
        except NoBrokersAvailable:
            logger.info("Waiting for Kafka brokers. Retrying in {} seconds...".format(5))
            sleep(5)
    return _producer

if __name__ == '__main__':

    kafka_producer = connect_kafka_producer()
    fake = Faker()
    for i in range(10):
        sleep(5)
        logger.info('streaming')
        data = {
            'Name': fake.name(),
            'Address': fake.address().replace('\n', ' '),
            'Phone': fake.phone_number(),
            'Email': fake.email()
        }
        publish_message(kafka_producer, topic_name, 'raw', ';'.join(data.values()))
        # kafka_producer.send(topic_name, value=data.encode('utf-8'))


    if kafka_producer is not None:
        # Flush & Close Kafka producer
        kafka_producer.flush()
        kafka_producer.close()

        # Create Kafka consumer
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            group_id='my_consumer_group',
            auto_offset_reset='earliest',
            enable_auto_commit=False
        )

        # Consume and print messages from the Kafka topic
        for message in consumer:
            logger.info(f'Received: {message.value.decode("utf-8")}')

        # Close Kafka consumer
        consumer.close()

