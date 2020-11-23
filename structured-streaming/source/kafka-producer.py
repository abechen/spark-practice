import json
import argparse

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import numpy as np
import pandas as pd
from datetime import datetime

import time
import random

_KAFKA_HOST = '127.0.0.1:9092'
_KAFKA_INPUT_TOPIC = 'bank-transaction'

def send_message(producer, topic, input_file):

    df_rows = pd.read_csv(input_file)
    df_rows = df_rows.fillna(0)
    for idx, row in df_rows.iterrows():

        time.sleep(random.randint(0, 3))

        json_data = json.dumps({
            'account_no': row['Account No'].split('\'')[0],
            'date': datetime.strptime(row['DATE'], '%d-%b-%y').strftime('%Y%m%d'),
            'transaction_details': row['TRANSACTION DETAILS'],
            'value_date': datetime.strptime(row['VALUE DATE'], '%d-%b-%y').strftime('%Y%m%d'),
            'withdrawal_amt': int(str(row['WITHDRAWAL AMT']).replace(',', '').split('.')[0]),
            'balance_amt': int(str(row['BALANCE AMT']).replace(',', '').split('.')[0]),
            'event_time': datetime.now().isoformat()
        }).encode('utf-8')

        producer.send(topic, json_data)

    producer.flush()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', required=True)
    args = parser.parse_args()

    print('Create Kafka topics: '.format(_KAFKA_INPUT_TOPIC))
    admin_client = KafkaAdminClient(bootstrap_servers=_KAFKA_HOST, client_id='bank_client')
    topic_list = []
    topic_list.append(
        NewTopic(
            name=_KAFKA_INPUT_TOPIC,
            num_partitions=1,
            replication_factor=1
        )
    )

    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except TopicAlreadyExistsError as err:
        print('Topics already exisit... skip')

    print('Pushing data to Kafka topic: '.format(_KAFKA_INPUT_TOPIC))
    producer = KafkaProducer(bootstrap_servers=_KAFKA_HOST)

    send_message(
        producer=producer,
        topic=_KAFKA_INPUT_TOPIC,
        input_file=args.input
    )
