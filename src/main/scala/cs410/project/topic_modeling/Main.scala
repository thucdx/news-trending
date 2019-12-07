package cs410.project.topic_modeling

import java.sql.Date

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF, StopWordsRemover, Tokenizer}
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.util.matching.Regex
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]): Unit = {
    val newsPath = "/home/tdx/works/projects/cs410/guardian-news-dataset/combined_filtered.csv"
    val sparkSession = SparkSession.builder()
      .appName("Topic Modeling with LDA")
      .master("local[8]").getOrCreate()
    import sparkSession.implicits._

    val b: Boolean = true
    val schema = " null INT, apiUrl STRING, bodyText STRING, id STRING, isHosted Boolean, pillarId String," +
      " pillarName STRING, sectionId String, sectionName String, type String, webPublicationDate Date," +
      " webTitle String, webUrl String, filtered_bodyText String"
    sparkSession.conf
    val newsDataset = sparkSession
      .read
      .options(Map("header" -> "true"))
      .schema(schema)
      .csv(newsPath)
      .flatMap(row => {
        val id = row.getAs[String]("id")
        val publishedDate = row.getAs[Date]("webPublicationDate")
        val title = row.getAs[String]("webTitle")
        val bodyTextOrg = row.getAs[String]("bodyText")
        val bodyText = if (bodyTextOrg != null && bodyTextOrg.nonEmpty)
          bodyTextOrg.replaceAll("[^a-zA-Z0-9 ]", "")
        else
          bodyTextOrg

        val url = row.getAs[String]("webUrl")
        val section = row.getAs[String]("sectionName")

        Some(Article(id, publishedDate, title, bodyText, url, section))
      })
      .na.drop(Array("bodyText"))
      .sort($"publishedDate".desc)

    //    newsDataset.show(20)

//     Count each section
        newsDataset.groupBy("section")
          .count().show()

    // tokenizer
    val tokenizer = new Tokenizer().setInputCol("bodyText").setOutputCol("words")
    val newsWithTokenizer = tokenizer.transform(newsDataset)

    //    newsWithTokenizer.show(20)
    val countNullWords = newsWithTokenizer
      .filter($"words".isNull)
      .count()

    println(s"!!!!!!!!!!!!!!!Null words $countNullWords")

    // remove stopwords
    val stopWords = new StopWordsRemover()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("filtered_words")

    println("After filtered stop words:")

    val filteredStopwords = stopWords.transform(newsWithTokenizer)

    //    filteredStopwords.show(20)
    //    filteredStopwords.printSchema()

    // Count number of rows which have null filtered_words
    val totalNullFilteredWords = filteredStopwords
      .filter($"filtered_words".isNull)
      .count()

    println(s"Total Null filtered words: ${totalNullFilteredWords}")

    val oneMonthNews = filteredStopwords.filter(
      $"publishedDate" >= lit("2018-12-01")
        && $"publishedDate" < lit("2019-01-01")
    )
    oneMonthNews.persist()

    println(s"Total article in one month: ${oneMonthNews.count()}")

    // CountVectorizer
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("filtered_words")
      .setOutputCol("features")
      .setMinDF(2)
      .fit(filteredStopwords)

    val afterPreprocessed = cvModel.transform(oneMonthNews)

    afterPreprocessed.show(20)

    //  IDF
    val idf = new IDF()
        .setInputCol(cvModel.getOutputCol)
       .setOutputCol("features_tfidf")

    afterPreprocessed.show(20)

    val rescaled = idf.fit(afterPreprocessed).transform(afterPreprocessed)
    rescaled.persist()
    rescaled.show(20)
    //    println(cvModel.vocabulary.zipWithIndex.mkString(","))

    val vocabArray = cvModel.vocabulary
    val vocab: Map[String, Int] = cvModel.vocabulary.zipWithIndex.toMap

    import sparkSession.implicits._

    val documents = rescaled
      .select("features_tfidf")
      .rdd
      .map {
        case Row(features: MLVector) => Vectors.fromML(features)
      }
      .zipWithIndex()
      .map(_.swap)

    println(documents.take(2))


    val nTopic = 6
    val nWord = 7
    val lda = new LDA()
    lda
      .setK(nTopic)

    val startTime = System.nanoTime()
    val ldaModel = lda.run(documents)
    val elapsed = (System.nanoTime() - startTime) / 1e9

    println(s"Finished training LDA model. Summary:")
    println(s"Training time: $elapsed secs")

    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = nWord)
    val topics = topicIndices.map {
      case (terms, termWeights) =>
        terms.zip(termWeights).map {
          case (term, weight) => (vocabArray(term.toInt), weight)
        }
    }

    println(s"${nTopic} topics")
    topics.zipWithIndex.foreach {
      case (topic, i) => {
        println(s"Topic ${i + 1}")
        topic.foreach {
          case (term, weight) =>
            println("%-15s%1.4f".format(term, weight))
        }
        println()
      }
    }
  }

  case class Article(id: String, publishedDate: Date, title: String, bodyText: String, url: String, section: String)
}
