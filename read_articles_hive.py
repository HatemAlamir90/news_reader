import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
from json import loads
from pyhive import hive
from tensorflow.keras.preprocessing.text import Tokenizer

LOOKBACK_DAYS = 1
MAX_NUM_WORDS = 50
MAX_PLOT_WORDS = 15
REFRESH_TIME_MIN = 2


def read_articles():
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
    print("<<< %d articles read" % len(res))

    # convert the publication date to a date-time
    articles = pd.DataFrame(res, columns=['title', 'publishedAt', 'content'])
    articles['publishedAt'] = pd.to_datetime(articles['publishedAt'])
    articles['publishedAtHour'] = articles['publishedAt'].dt.strftime(
        "%Y-%m-%d %H")

    # cont how many articles published each hour
    articles_grouped = articles['title'].groupby(articles['publishedAtHour'])
    articles_cnt_hourly = articles_grouped.count()
    # draw a line-plot with the results
    # articles_cnt_hourly.plot.line()

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

    return articles_cnt_hourly, words, counts


# plot word frequency
plt.rcdefaults()
fig, axs = plt.subplots(1, 2, figsize=(13.5, 5.5))
fig.canvas.mpl_connect('close_event', sys.exit)

plt.ion()
plt.show()

ans = 'r'

while(ans.lower() == 'r'):
    articles_cnt_hourly, words, counts = read_articles()

    articles_cnt_hourly.plot(ax=axs[0])
    axs[0].set_xticklabels(pd.to_datetime(
        articles_cnt_hourly.index).strftime("%H").tolist())

    y_pos = np.arange(len(words))
    axs[1].barh(y_pos, counts, align='center')
    axs[1].set_yticks(y_pos)
    axs[1].set_yticklabels(list(words))
    axs[1].invert_yaxis()
    axs[1].set_xlabel('Count')
    axs[1].set_title("Most Frequent Words' Counts")

    plt.draw()
    plt.pause(0.001)

    ans = input("Press r to refresh or any other key to exit!")
