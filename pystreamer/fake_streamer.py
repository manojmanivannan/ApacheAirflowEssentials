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
        logger.info(f'Streamed: {value}')
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
    try:
        while True:
            sleep(3)
            data = {
                'Name': fake.name(),
                'Address': fake.address().replace('\n', ' '),
                'Phone': fake.phone_number(),
                'Email': fake.email()
            }
            publish_message(kafka_producer, topic_name, 'raw', ';'.join(data.values()))
    except:
        pass
    finally:
        kafka_producer.flush()
        kafka_producer.close()

