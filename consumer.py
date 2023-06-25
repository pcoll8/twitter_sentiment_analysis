from kafka import KafkaConsumer, KafkaProducer

from transformers import pipeline

if __name__ == "__main__":

    producer = KafkaProducer(
        bootstrap_servers='localhost:9092', api_version=(2, 0, 2))

    sentiment_analysis = pipeline(model="finiteautomata/bertweet-base-sentiment-analysis")

    consumer = KafkaConsumer(
        'TWEETS_TO_ANALYZE',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: x.decode('utf-8'))

    for message in consumer:
        tweet = message.value
        sentiment = sentiment_analysis(tweet)
        tweet_sentiment = ({'tweet': tweet, 'sentiment': sentiment[0]['label']})
        # Here we send the data with sentiment analysis to another topic
        producer.send('TWITTER_SENTIMENT_TOPIC', key=str(tweet_sentiment['tweet']).encode('utf-8'),
                      value=str(tweet_sentiment['sentiment']).encode('utf-8'))

