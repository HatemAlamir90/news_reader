$ hive
hive> CREATE TABLE article(author STRING, title STRING, description STRING, url STRING, urlToImage STRING, content STRING) PARTITIONED BY(publishedAt TIMESTAMP, sourceName STRING);
