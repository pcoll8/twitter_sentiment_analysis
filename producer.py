import time
from random import randint

import pandas as pd
from kafka import KafkaProducer

from variables.config import KAFKA_TOPIC_1, PRODUCER_PORT

if __name__ == "__main__":

    producer = KafkaProducer(
        bootstrap_servers=f'localhost:{PRODUCER_PORT}', api_version=(2, 0, 2))

    df = pd.read_csv('data/tweets.csv', header=None, verbose=True, sep=",", index_col=False, engine="python",
                     encoding='latin-1')

    for index, row in df.iterrows():
        second_field = row[1]
        sixth_field = row[5]
        producer.send(KAFKA_TOPIC_1, key=str(second_field).encode('utf-8'), value=str(sixth_field).encode('utf-8'))
        time.sleep(randint(1, 10))
