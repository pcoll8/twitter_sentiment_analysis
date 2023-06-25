from kafka import KafkaConsumer
import json

KAFKA_TOPIC_1 = "TWEETS_TO_ANALYZE"

# sentiment_analysis = pipeline(model="finiteautomata/bertweet-base-sentiment-analysis")

consumer = KafkaConsumer(
    KAFKA_TOPIC_1,
    bootstrap_servers=['localhost:9094'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,
    fetch_max_bytes=128,
    max_poll_records=100,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    tweets = json.loads(json.dumps(message.value))

