import requests
from json import dumps
from kafka import KafkaProducer

import schedule
import time

REFRESH_TIME_MIN = 2


def import_news_to_kafka():
    # hit the news api
    print('>>> connecting to https://newsapi.org...')
    url = 'http://newsapi.org/v2/top-headlines?country=us&apiKey=fd81a69b8bfc42de9204fa3abd0e0284'
    response = requests.get(url)
    print('<<< connected\n')
    resp_json = response.json()

    # extract the list of articles
    articles = resp_json.get('articles')

    # write the read articles to Kafka
    print('>>> writing articles to Kafka broker at quickstart.cloudera:9092...')
    producer = KafkaProducer(bootstrap_servers=['quickstart.cloudera:9092'],
                             value_serializer=lambda x: dumps(x).encode('utf-8'))
    for article in articles:
        producer.send('news', value=article)
    print("<<< %d articles written\n" % len(articles))


import_news_to_kafka()
schedule.every(REFRESH_TIME_MIN).minutes.do(import_news_to_kafka)

while True:
    schedule.run_pending()
    time.sleep(REFRESH_TIME_MIN)
