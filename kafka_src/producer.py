# -*- coding: utf-8 -*-
"""
@created on: 26/03/19,
@author: Shreesha N,
@version: v0.0.1

Description:

Sphinx Documentation Status:

..todo::

"""

from time import sleep

import pandas as pd
from kafka import KafkaProducer


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        # sleep(5)
        print('Message published successfully.', key, value)
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


if __name__ == '__main__':
    data = pd.read_csv('/Users/shreeshan/Documents/test_kafka.csv')
    kafka_topic_name = 'testing_kafka'
    data = data.fillna(0)
    kafka_producer = connect_kafka_producer()
    for row in range(len(data)):
        print(data.ix[row][0], data.ix[row][1], data.ix[row][2])
        publish_message(kafka_producer, 'testing_kafka', 'raw', str(data.ix[row]))
    if kafka_producer is not None:
        kafka_producer.close()
