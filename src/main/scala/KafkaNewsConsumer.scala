import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object KafkaNewsConsumer {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <groupId> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <groupId> is a consumer group name to consume from topics
           |  <topics> is a list of one or more kafka topics to consume from
           |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, groupId, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    sparkConf.set("spark.dynamicAllocation.enabled", "false")
    val ssc = new StreamingContext(sparkConf, Minutes(55))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "smallest")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, parse into 'Article's
    val articles = messages.map(_._2).map(articleStr => NewsParser.parseArticle(articleStr))

    articles.foreachRDD {
      rdd =>
        val hiveContext = new HiveContext(ssc.sparkContext)
        hiveContext.setConf("hive.exec.dynamic.partition","true")
        hiveContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")
        import hiveContext.implicits._

        val articlesDF = rdd.toDF("source", "author", "title", "description", "url", "urlToImage",
          "publishedAt", "content")
        articlesDF.registerTempTable("tempArticles")

        hiveContext.sql("INSERT OVERWRITE TABLE article PARTITION(publishedAt, sourceName) " +
          "SELECT author, title, description, url, urlToImage, content, publishedAt, source.name " +
          "FROM tempArticles")
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
