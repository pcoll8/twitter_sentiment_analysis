from confluent_kafka import Consumer, KafkaException

from variables.config import KAFKA_TOPIC_2, CONFULENCE_KAFKA_CONFIG

if __name__ == "__main__":

    consumer = Consumer(CONFULENCE_KAFKA_CONFIG)

    consumer.subscribe([KAFKA_TOPIC_2])

    try:
        # Initialize sentiment counters
        sentiment_count = {'NEG': 0, 'POS': 0, 'NEU': 0}

        while True:
            msg = consumer.poll(1.0)  # Wait for up to 1s for a message

            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                message = msg.value().decode('utf-8')

                if message == "NEG":
                    sentiment_count[message] += 1
                if message == "POS":
                    sentiment_count[message] += 1
                if message == "NEU":
                    sentiment_count[message] += 1

    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
