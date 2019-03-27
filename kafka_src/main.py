# -*- coding: utf-8 -*-
"""
@created on: 26/03/19,
@author: Shreesha N,
@version: v0.0.1

Description:

Sphinx Documentation Status:

..todo::

"""
from kafka_src.producer import connect_kafka_producer, publish_message
import pandas as pd
if __name__ == '__main__':
    data = pd.read_csv('/Users/shreeshan/Documents/test_kafka.csv')
    data = data.fillna(0)
    kafka_producer = connect_kafka_producer()
    for row in range(len(data)):
        print(data.ix[row][0], data.ix[row][1], data.ix[row][2])
        publish_message(kafka_producer, 'raw_recipes', 'raw', str(data.ix[row]))
    if kafka_producer is not None:
        kafka_producer.close()

