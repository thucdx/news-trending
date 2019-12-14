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
package cs410.project.topic_modeling

import java.sql.{Date, Timestamp}
//import java.util.{Date => UDate}
import java.text.SimpleDateFormat

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF, StopWordsRemover, Tokenizer}
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}

import scala.util.Try

// scalastyle:off
object Main {
	private val MAX_SOLR_FIELD_LENGTH = 32766
	private val DEFAULT_NEWS_INPUT_SCHEMA = " null INT, apiUrl STRING, bodyText STRING, id STRING, isHosted Boolean, pillarId String," +
		" pillarName STRING, sectionId String, sectionName String, type String, webPublicationDate Date," +
		" webTitle String, webUrl String, filtered_bodyText String"

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

	def cleanAndSave(sparkSession: SparkSession, newsSourcePath: String, newsDestPath: String): Unit = {
		import sparkSession.implicits._

		val newsDataset: Dataset[Article] = parseFromFile(sparkSession, newsSourcePath)

		newsDataset
			.filter(
				$"publishedDate" >= lit("2018-01-01")
					&& $"publishedDate" < lit("2019-01-01"))
			//      .na.drop(Array("bodyText"))
			.sort($"publishedDate".desc)
			.coalesce(1)
			.write
			.format("csv")
			.option("header", "true")
			.save(newsDestPath)
	}


	def main(args: Array[String]): Unit = {
		val newsPath = "/home/tdx/works/projects/cs410/guardian-news-dataset/combined_filtered.csv"
		val newsDstPath = "filtered_news.csv"
		val zooKeeperHost = "localhost:9983"
		val newsCollection: String = "news"

		val ss = SparkSession.builder()
			.appName("Topic Modeling with LDA")
			.master("local[4]")
			.getOrCreate()

		//// Read from files
		//    val newsDataset: Dataset[Article] = parseFromFile(ss, newsPath)

		val newsArticle = loadArticleFromSolr(ss, zooKeeperHost, newsCollection)
		newsArticle.show(20)
		basicStats(newsArticle)

		// LDA
		val businessArticles = newsArticle.filter(_.section == "Business")

		businessArticles.persist()
		val rangedTopics = performTopicModeling(ss, newsArticle, "2018-10-01", "2019-12-01")

		rangedTopics.foreach(println(_))

		import ss.implicits._

		println("About to save topic to Solr")
		val rangedTopicDF = ss.sparkContext.parallelize(rangedTopics).toDF()
		saveToSolr(ss, zooKeeperHost, "news_topic", rangedTopicDF)

		// Close
		ss.close()
	}

	private def basicStats(collection: Dataset[Article]): Unit = {
		println(s"Total docs: ${collection.count()}")
		collection
			.groupBy("section")
			.count()
			.show()
	}

	def saveToSolr(ss: SparkSession, zkHost: String, collection: String, topics: DataFrame): Unit = {
		val options = Map(
			"zkhost" -> zkHost,
			"collection" -> collection,
			"gen_uniq_key" -> "true"
		)

		topics.write
			.format("solr")
			.options(options)
			.mode(org.apache.spark.sql.SaveMode.Overwrite)
			.save
	}

	private def loadArticleFromSolr(ss: SparkSession, zkHost: String, collection: String): Dataset[Article] = {
		import ss.implicits._

		val options = Map(
			"zkhost" -> zkHost,
			"collection" -> collection
		)

		val solrDF = ss.read
			.format("solr")
			.options(options)
			.load

		solrDF.printSchema()
		solrDF.createOrReplaceTempView("news")
		ss.sql("SELECT * FROM news").show(10)

		val ds = ss.read.format("solr")
			.options(options)
			.load
			.map(row => {
				val id = row.getAs[String]("id")
				val publishedDate = new Date(row.getAs[Timestamp]("publishedDate").getTime)
				val title = row.getAs[String]("title")
				val url = row.getAs[String]("url")
				val section = row.getAs[String]("section")
				val bodyText = row.getAs[String]("bodyText")
				Article(id, publishedDate, title, bodyText, url, section)
			})

		ds
	}

	private def performTopicModeling(ss: SparkSession, newsDataset: Dataset[Article],
	                                 startDateStr: String, endDateStr: String,
	                                 nTopic: Int = 5, nWord: Int = 7): List[RangedTopic] = {
		import ss.implicits._

		val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
		val startDate: Date = new Date(simpleDateFormat.parse(startDateStr).getTime)
		val endDate: Date = new Date(simpleDateFormat.parse(endDateStr).getTime)

		println(startDate + " " + endDate)

		// tokenizer
		val tokenizer = new Tokenizer().setInputCol("bodyText").setOutputCol("words")
		val newsWithTokenizer = tokenizer.transform(newsDataset)

		//    newsWithTokenizer.show(20)
		val countNullWords = newsWithTokenizer
			.filter($"words".isNull)
			.count()

		// Remove stop words
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
			$"publishedDate" >= lit(startDateStr)
				&& $"publishedDate" < lit(endDateStr)
		)
		oneMonthNews.persist()

		println(s"Total article in [$startDateStr, $endDateStr): ${oneMonthNews.count()}")

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

		val documents = rescaled
			.select("features_tfidf")
			.rdd
			.map {
				case Row(features: MLVector) => Vectors.fromML(features)
			}
			.zipWithIndex()
			.map(_.swap)

		println(documents.take(2))

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

		val rangedTopics = topicIndices.map {
			case (termIds, weights) => {
				val words = termIds.map(vocabArray(_)).toArray
				RangedTopic(startDate, endDate, words, weights)
			}
		}.toList

		println(s"${nTopic} topics")
		topics.zipWithIndex.foreach {
			case (topic, i) => {
				println(s"Trending #${i + 1}")

				topic.foreach {
					case (term, weight) =>
						println("%-15s%1.4f".format(term, weight))
				}
				println()
			}
		}

		rangedTopics
	}
}

// scalastyle:on