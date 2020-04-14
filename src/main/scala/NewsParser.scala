import java.sql.Timestamp

import net.liftweb.json.DefaultFormats
import net.liftweb.json._

case class Source(
                   id: String,
                   name: String
                 )

case class Article(
                    source: Source,
                    author: String,
                    title: String,
                    description: String,
                    url: String,
                    urlToImage: String,
                    publishedAt: Timestamp,
                    content: String
                  )

object NewsParser {
  implicit val formats = DefaultFormats

  def parseArticle(article: String): Article = {
    val articleUnit = parse(article)
    return articleUnit.extract[Article]
  }
}
