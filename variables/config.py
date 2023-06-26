KAFKA_TOPIC_1 = "TWEETS_TO_ANALYZE"
KAFKA_TOPIC_2 = "TWITTER_SENTIMENT_TOPIC"
PRODUCER_PORT = "9092"
CONSUMER_GROUP_1 = "my-group1"
CONSUMER_GROUP_2 = "my-group"
CONFULENCE_KAFKA_CONFIG = {
        'bootstrap.servers': f'localhost:{PRODUCER_PORT}',  # replace with your Kafka broker's address
        'group.id': f'{CONSUMER_GROUP_2}',
        'auto.offset.reset': 'earliest',
    }
