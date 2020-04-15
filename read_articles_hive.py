import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from json import loads
import schedule
import time
from pyhive import hive
from tensorflow.keras.preprocessing.text import Tokenizer
get_ipython().run_line_magic('matplotlib', 'inline')


LOOKBACK_DAYS = 1
MAX_NUM_WORDS = 50
MAX_PLOT_WORDS = 15
REFRESH_TIME_MIN = 2


def show_news_stats():
    # connect to Hive and read the articles
    print('>>> reading articles from Hive...')
    cursor = hive.connect('quickstart.cloudera').cursor()

    hive_query = ("SELECT title, publishedAt, content FROM article "
                  "WHERE unix_timestamp(publishedAt) > cast(CURRENT_TIMESTAMP AS BIGINT) - "
                  "{lookback_period}"
                  )
    hive_query = hive_query.format(
        lookback_period=LOOKBACK_DAYS * 24 * 60 * 60)
    cursor.execute(hive_query)
    res = cursor.fetchall()
    print("<<< %d articles read" % res.length)

    # convert the publication date to a date-time
    articles = pd.DataFrame(res, columns=['title', 'publishedAt', 'content'])
    articles['publishedAt'] = pd.to_datetime(articles['publishedAt'])
    articles['publishedAtHour'] = articles['publishedAt'].dt.strftime(
        "%Y-%m-%d %H")

    # cont how many articles published each hour
    articles_grouped = articles['title'].groupby(articles['publishedAtHour'])
    articles_cnt_hourly = articles_grouped.count()
    # draw a line-plot with the results
    articles_cnt_hourly.plot.line()

    # count the most occuring words
    content = [c for c in articles['content'].tolist() if c is not None]
    tokenizer = Tokenizer(num_words=MAX_NUM_WORDS)
    tokenizer.fit_on_texts(content)

    word_counts = loads(tokenizer.get_config()['word_counts'])
    most_frequent_words = {k: v for k, v in sorted(
        word_counts.items(), key=lambda item: item[1], reverse=True)}
    words, counts = zip(*most_frequent_words.items())
    words = words[:MAX_PLOT_WORDS]
    counts = counts[:MAX_PLOT_WORDS]

    # plot word frequency
    plt.rcdefaults()
    fig, ax = plt.subplots()
    y_pos = np.arange(len(words))
    ax.barh(y_pos, counts, align='center')
    ax.set_yticks(y_pos)
    ax.set_yticklabels(list(words))
    ax.invert_yaxis()
    ax.set_xlabel('Count')
    ax.set_title("Most Frequent Words' Counts")

    plt.show()


show_news_stats()
schedule.every(REFRESH_TIME_MIN).minutes.do(show_news_stats)

while True:
    schedule.run_pending()
    time.sleep(REFRESH_TIME_MIN)
fig
