# -*- coding: utf-8 -*-
"""
@created on: 26/03/19,
@author: Shreesha N,
@version: v0.0.1

Description:

Sphinx Documentation Status:

..todo::

"""
from kafka import KafkaConsumer


def consume_message(consumer):
    for msg in consumer:
        print('consumer side', msg)
    consumer.close()


def connect_kafka_consumer(topic_name):
    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    return consumer


if __name__ == '__main__':
    kafka_topic_name = 'testing_kafka'
    kafka_consumer = connect_kafka_consumer(kafka_topic_name)
    consume_message(kafka_consumer)
