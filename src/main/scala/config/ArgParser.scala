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
          if (x.equals("extract") || x.equals("trend"))
            success
          else
            failure("Allowed values for <mode> are: extract/trend")
        ).text("Mode to run: extract/trend"),

      opt[String]('p', "inputPath")
        .action((x, c) => c.copy(inputPath = x))
        .text("Path of csv file containing articles to index. Default = input/the_guardian_articles.csv"),

      opt[String]('o', "outputPath")
          .action((x, c) => c.copy(outputPath = x))
          .text("Path to store extract articles. Default = output/"),

      opt[String]('c', "newsCollection")
        .action((x, c) => c.copy(newsCollection = x))
        .text("Name of collection to index to Solr. Default: news"),

      opt[String]("extractStartDate")
          .action((x, c) => c.copy(extractStartDate = x))
          .text("Extracting start date, format: yyyy-MM-dd. Default 2018-01-01"),

      opt[String]("extractEndDate")
        .action((x, c) => c.copy(extractEndDate = x))
        .text("Extracting end date, format: yyyy-MM-dd. Default 2019-01-01"),

      opt[String]("trendStartDate")
        .action((x, c) => c.copy(trendStartDate = x))
        .text("Trend start date, format: yyyy-MM-dd. Default 2018-11-01"),

      opt[String]("trendEndDate")
        .action((x, c) => c.copy(trendEndDate = x))
        .text("Trend end date, format: yyyy-MM-dd. Default 2018-12-01"),

      opt[String]('z', "zookeeper")
        .action((x, c) => c.copy(zkhost = x))
        .text("Zookeeper url, default: localhost:9983"),

      opt[Int]('t', "topics")
        .action((x, c) => c.copy(numTopic = x))
        .text("Number of topics. Default = 5"),

      opt[Int]('w', "words")
        .action((x, c) => c.copy(topicWord = x))
        .text("Number of words per topic to show. Default = 7"),

      opt[Int]('a', "articles")
        .action((x, c) => c.copy(maxArticle = x))
        .text("Number of related articles to show for each topic. Default = 5")
    )
  }
}

