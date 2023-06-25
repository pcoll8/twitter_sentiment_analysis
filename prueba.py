from kafka import KafkaConsumer

consumer = KafkaConsumer(
        'TWITTER_SENTIMENT_TOPIC',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group2',
        value_deserializer=lambda x: x.decode('utf-8'))

aggregated_results = {
    'positive': 0,
    'neutral': 0,
    'negative': 0
}

for message in consumer:
    tweet = message.key
    sentiment = message.value

    if sentiment == 'POS':
        aggregated_results['positive'] += 1
    elif sentiment == 'NEG':
        aggregated_results['negative'] += 1
    else:
        aggregated_results['neutral'] += 1

print(aggregated_results)
