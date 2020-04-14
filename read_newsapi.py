import requests
from json import dumps
from kafka import KafkaProducer

import schedule
import time


def import_news_to_kafka():
    # hit the news api
    url = 'http://newsapi.org/v2/top-headlines?country=us&apiKey=fd81a69b8bfc42de9204fa3abd0e0284'
    response = requests.get(url)
    resp_json = response.json()

    # extract the list of articles
    articles = resp_json.get('articles')

    # write the read articles to Kafka
    producer = KafkaProducer(bootstrap_servers=['quickstart.cloudera:9092'],
                             value_serializer=lambda x: dumps(x).encode('utf-8'))
    for article in articles:
        producer.send('news', value=article)


schedule.every().hour.do(import_news_to_kafka)

while True:
    schedule.run_pending()
    time.sleep(3300)
