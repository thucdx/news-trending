// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import config.AppConfig
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scopt.OParser
import config.ArgParser.newsAppArgParser
import scala.util.Try

object Main {
  val newsPath = "input/the_guardian_articles.csv"
  val newsDstPath = "output/filtered_news.csv"
  val zooKeeperHost = "localhost:9983"
  val newsCollection: String = "news"

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .appName("Topic Modeling with LDA")
      .master("local[4]")
      .getOrCreate()

    OParser.parse(newsAppArgParser, args, AppConfig()) match {
      case Some(config) => {
        val mode = config.mode
        mode match {
          case "index" => {
            val runningResult = Try {
              import ss.implicits._
              println(s"\n======================" +
                s"\nINDEXING NEWS" +
                s"\n\t Input = ${config.inputPath} " +
                s"\n\t to Solr's collection: ${config.newsCollection} " +
                s"\n\t all news within: " +
                s"\n\t\t startDate = ${config.indexingStartDate} " +
                s"\n\t\t endDate = ${config.indexingEndDate}")

              val articleDataset = parseFromFile(ss, config.inputPath)
              val filteredArticleDF = articleDataset.filter(
                $"publishedDate" >= lit(config.indexingStartDate)
                  && $"publishedDate" < lit(config.indexingEndDate))
                .toDF()
              saveToSolr(ss, config.zkhost, config.newsCollection, filteredArticleDF)
            }.isSuccess

            println(s"Result: ${runningResult}")
          }

          case "trend" => {
            import ss.implicits._

            println(s"\n======================" +
              s"\nFINDING TRENDS" +
              s"\n\t Solr's news collection: ${config.newsCollection}" +
              s"\n\t\t startDate = ${config.trendStartDate} " +
              s"\n\t\t endDate = ${config.trendEndDate}" +
              s"\n\t\t number of topic: ${config.numTopic}" +
              s"\n\t\t word per topic: ${config.topicWord}" +
              s"\n\t\t sample related articles: ${config.maxArticle}"
            )

            val articleDataset = loadArticleFromSolr(ss, config.zkhost, newsCollection)

            val articlesInRange = articleDataset.filter(
              $"publishedDate" >= lit(config.trendStartDate)
                && $"publishedDate" < lit(config.trendEndDate))

            val topics = performTopicModeling(ss, articlesInRange,
              config.trendStartDate, config.trendEndDate,
              config.numTopic, config.topicWord)

            println(s"Showing ${config.numTopic} topics and related articles: ")
            topics.zipWithIndex.foreach {
              case (topic: RangedTopic, id: Int) => {
                println("#################################")
                println(s"Topic ${1 + id} / ${config.numTopic}")
                showRelatedArticles(ss, topic, config.zkhost, newsCollection, config.maxArticle)
              }
            }
          }
        }
      }

      case _ => println("Please try again!")
    }

    // Close
    ss.close()
  }

  def parseFromFile(sparkSession: SparkSession, newsSourcePath: String): Dataset[Article] = {
    import sparkSession.implicits._

    val newsDataset: Dataset[Article] = sparkSession
      .read
      .options(Map("header" -> "true"))
      .schema(DEFAULT_NEWS_INPUT_SCHEMA)
      .csv(newsSourcePath)
      .flatMap(row => {
        val id = row.getAs[String]("id")
        val publishedDate = row.getAs[Date]("webPublicationDate")
        val title = Try(row.getAs[String]("webTitle")
          replaceAll("[^a-zA-Z0-9 \n]", "")
        ).getOrElse("N/A")

        val bodyTextOrg = row.getAs[String]("bodyText")
        val bodyText = if (bodyTextOrg != null && bodyTextOrg.nonEmpty) {
          // Remove special character
          val text = bodyTextOrg.replaceAll("[^a-zA-Z0-9 \n]", "").trim
          if (text.length >= MAX_SOLR_FIELD_LENGTH) { // maximum number of chars allowed to index in Solr
            text.substring(0, MAX_SOLR_FIELD_LENGTH)
          } else {
            text
          }
        } else
          bodyTextOrg

        val url = row.getAs[String]("webUrl")
        val section = row.getAs[String]("sectionName")

        if (bodyText != null && bodyText.nonEmpty)
          Some(Article(id, publishedDate, title, bodyText, url, section))
        else
          None
      })

    newsDataset
  }

  def saveToSolr(ss: SparkSession, zkHost: String, collection: String, dataDF: DataFrame): Unit = {
    val options = Map(
      "zkhost" -> zkHost,
      "collection" -> collection,
      "gen_uniq_key" -> "true",
      "soft_commit_secs" -> "5"
    )

    dataDF
      .write
      .format("solr")
      .options(options)
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save
  }

  def showRelatedArticles(ss: SparkSession,
                          rangedTopic: RangedTopic,
                          zkHost: String,
                          articleCollection: String,
                          maxArticle: Int = 5): Unit = {
    import ss.implicits._

    val words = rangedTopic.words.mkString(" ")
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    println("Topic word with its weight:")
    println(rangedTopic.words.zip(
      rangedTopic.weights.map(e => f"${e}%1.4f")
    ).toMap)

    val startDate = sdf.format(rangedTopic.startDate)
    val endDate = sdf.format(rangedTopic.endDate)

    val relatedArticles = ss.read.format("solr")
      .option("zkhost", zkHost)
      .option("collection", articleCollection)
      .option("query", s"bodyText: $words")
      .option("filters", s"publishedDate: [$startDate TO $endDate]")
      .option("solr.params", "sort=score desc")
      .option("max_rows", maxArticle)
      .load()
      .flatMap(rowToArticle)
      .select("title", "publishedDate", "url", "section")

    println("Related article: ")
    relatedArticles.show(truncate = false)
  }

  def performTopicModeling(ss: SparkSession, newsDataset: Dataset[Article],
                           startDateStr: String,
                           endDateStr: String,
                           nTopic: Int = 5, nWord: Int = 7): List[RangedTopic] = {
    import ss.implicits._

    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val startDate: Date = new Date(simpleDateFormat.parse(startDateStr).getTime)
    val endDate: Date = new Date(simpleDateFormat.parse(endDateStr).getTime)

    // TOKENIZER
    val tokenizer = new Tokenizer().setInputCol("bodyText").setOutputCol("words")
    val newsWithTokenizer = tokenizer.transform(newsDataset)

    val countNullWords = newsWithTokenizer
      .filter($"words".isNull)
      .count()

    // REMOVE STOPWORDS
    val stopWords = new StopWordsRemover()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("filtered_words")


    val filteredStopwords = stopWords.transform(newsWithTokenizer)

    // Count number of rows which have null filtered_words
    val totalNullFilteredWords = filteredStopwords
      .filter($"filtered_words".isNull)
      .count()

    val newsInRange = filteredStopwords
    newsInRange.persist()

    println(s"Total article in date range [${startDateStr}, ${endDateStr}) : ${newsInRange.count()}")

    // VECTORISED
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("filtered_words")
      .setOutputCol("features")
      .setMinDF(2)
      .fit(filteredStopwords)

    val afterPreprocessed = cvModel.transform(newsInRange)

    //  IDF
    val idf = new IDF()
      .setInputCol(cvModel.getOutputCol)
      .setOutputCol("features_tfidf")

    val rescaled = idf.fit(afterPreprocessed).transform(afterPreprocessed)
    rescaled.persist()

    val vocabArray = cvModel.vocabulary

    val documents = rescaled
      .select("features_tfidf")
      .rdd
      .map {
        case Row(features: MLVector) => Vectors.fromML(features)
      }
      .zipWithIndex()
      .map(_.swap)

    val lda = new LDA()
    lda.setK(nTopic)

    val startTime = System.nanoTime()
    val ldaModel = lda.run(documents)
    val elapsed = (System.nanoTime() - startTime) / 1e9

    println(s"Finished training LDA model.")
    println(s"Training time: $elapsed secs")

    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = nWord)
    val rangedTopics = topicIndices.map {
      case (termIds, weights) => {
        val words = termIds.map(vocabArray(_))
        RangedTopic(startDate, endDate, words.toList, weights.toList)
      }
    }.toList

    rangedTopics
  }

  private def basicStats(collection: Dataset[Article]): Unit = {
    println(s"Total docs: ${collection.count()}")
    collection
      .groupBy("section")
      .count()
      .show()
  }

  private def loadArticleFromSolr(ss: SparkSession, zkHost: String, newsCollection: String): Dataset[Article] = {
    import ss.implicits._

    val options = Map(
      "zkhost" -> zkHost,
      "collection" -> newsCollection
    )

    val ds = ss.read.format("solr")
      .options(options)
      .load
      .flatMap(rowToArticle)

    ds
  }

  def rowToArticle(row: Row): Option[Article] = {
    Try {
      val id = row.getAs[String]("id")
      val publishedDate = new Date(row.getAs[Timestamp]("publishedDate").getTime)
      val title = row.getAs[String]("title")
      val url = row.getAs[String]("url")
      val section = row.getAs[String]("section")
      val bodyText = row.getAs[String]("bodyText")

      Article(id, publishedDate, title, bodyText, url, section)
    }.toOption
  }

  def saveToCsv(sparkSession: SparkSession,
                newsSourcePath: String, newsDestPath: String,
                startDate: String = "2018-01-01", endDate: String = "2019-01-01"): Unit = {
    import sparkSession.implicits._

    val newsDataset: Dataset[Article] = parseFromFile(sparkSession, newsSourcePath)

    newsDataset
      .filter(
        $"publishedDate" >= lit(startDate)
          && $"publishedDate" < lit(endDate))
      .sort($"publishedDate".desc)
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .save(newsDestPath)
  }

  private val MAX_SOLR_FIELD_LENGTH = 32766
  private val DEFAULT_NEWS_INPUT_SCHEMA = " null INT, apiUrl STRING, bodyText STRING, id STRING, isHosted Boolean, pillarId String," +
    " pillarName STRING, sectionId String, sectionName String, type String, webPublicationDate Date," +
    " webTitle String, webUrl String, filtered_bodyText String"
}
