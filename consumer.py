from kafka import KafkaConsumer, KafkaProducer

from transformers import pipeline

from variables.config import PRODUCER_PORT, KAFKA_TOPIC_2, CONSUMER_GROUP_1

if __name__ == "__main__":

    producer = KafkaProducer(
        bootstrap_servers=f'localhost:{PRODUCER_PORT}', api_version=(2, 0, 2))

    sentiment_analysis = pipeline(model="finiteautomata/bertweet-base-sentiment-analysis")

    consumer = KafkaConsumer(
        'TWEETS_TO_ANALYZE',
        bootstrap_servers=[f'localhost:{PRODUCER_PORT}'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=CONSUMER_GROUP_1,
        value_deserializer=lambda x: x.decode('utf-8'))

    for message in consumer:
        tweet = message.value
        sentiment = sentiment_analysis(tweet)
        tweet_sentiment = ({'tweet': tweet, 'sentiment': sentiment[0]['label']})
        # Here we send the data with sentiment analysis to another topic
        producer.send(KAFKA_TOPIC_2, key=str(tweet_sentiment['tweet']).encode('utf-8'),
                      value=str(tweet_sentiment['sentiment']).encode('utf-8'))

