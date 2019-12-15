package config

import scopt.OParser

object ArgParser {
  private val builder = OParser.builder[AppConfig]

  val newsAppArgParser = {
    import builder._

    OParser.sequence(
      programName(s"news_topic-VERSION.jar Main"),
      head("Finding trends in news", "v1.0"),

      opt[String]('m', "mode")
        .required()
        .action((x, c) => c.copy(mode = x))
        .validate(x =>
          if (x.equals("index") || x.equals("trend"))
            success
          else
            failure("Allowed values for <mode> are: index/trend")
        ).text("Mode to run: index/trend"),

      opt[String]('p', "inputPath")
        .action((x, c) => c.copy(inputPath = x))
        .text("Path of csv file containing articles to index. Default = input/the_guardian_articles.csv"),

      opt[String]('o', "newsCollection")
        .action((x, c) => c.copy(newsCollection = x))
        .text("Name of collection to index to Solr. Default: news"),

      opt[String]("indexingStartDate")
          .action((x, c) => c.copy(indexingStartDate = x))
          .text("Indexing start date, format: yyyy-MM-dd. Default 2018-01-01"),

      opt[String]("indexingEndDate")
        .action((x, c) => c.copy(indexingEndDate = x))
        .text("Indexing end date, format: yyyy-MM-dd. Default 2019-01-01"),

      opt[String]("trendStartDate")
        .action((x, c) => c.copy(trendStartDate = x))
        .text("Trend start date, format: yyyy-MM-dd. Default 2018-11-01"),

      opt[String]("trendEndDate")
        .action((x, c) => c.copy(trendEndDate = x))
        .text("Trend end date, format: yyyy-MM-dd. Default 2018-12-01"),

      opt[String]('z', "zookeeper")
        .action((x, c) => c.copy(zkhost = x))
        .text("Zookeeper url, default: localhost:9983"),

      opt[Int]('t', "topic")
        .action((x, c) => c.copy(numTopic = x))
        .text("Number of topic. Default = 5"),

      opt[Int]('w', "words")
        .action((x, c) => c.copy(topicWord = x))
        .text("Number of word per topic to show. Default = 7"),

      opt[Int]('a', "article")
        .action((x, c) => c.copy(maxArticle = x))
        .text("Number of related article to show for each topic. Default = 5")
    )
  }
}

