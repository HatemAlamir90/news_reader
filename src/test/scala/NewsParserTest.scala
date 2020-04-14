object NewsParserTest extends App {
  val articleStr = "{'source': {'id': 'nbc-news', 'name': 'NBC News'}, 'author': 'Nicole Acevedo', 'title': 'Hospital workers on front lines of coronavirus fight find their tires slashed - NBC News', 'description': 'Health care workers at the New York-Presbyterian Hudson Valley Hospital found their tires slashed after a stress-filled overnight shift.', 'url': 'https://www.nbcnews.com/news/us-news/hospital-workers-front-lines-coronavirus-fight-find-their-tires-slashed-n1181966', 'urlToImage': 'https://media4.s-nbcnews.com/j/newscms/2020_15/3305041/200411-slashed-tires-al-1410_d3930dab18b9b8d3db30ee7f922e547b.nbcnews-fp-1200-630.jpg', 'publishedAt': '2020-04-11T20:08:26Z','content': 'Sample Content!'}"
  val articleStrClean = articleStr.replaceAll("\'", "\"")
  val article = NewsParser.parseArticle(articleStrClean)
  println(article.source)
  println(article.author)
  println(article.title)
  println(article.description)
  println(article.url)
  println(article.urlToImage)
  println(article.publishedAt)
  println(article.content)
}
