package config

case class AppConfig(mode: String = "extract", // extract, trend
                     inputPath: String = "input/the_guardian_articles.csv",
                     outputPath: String = "output",

                     extractStartDate: String = "2018-01-01",
                     extractEndDate: String = "2019-01-01",

                     newsCollection: String = "news",
                     trendStartDate: String = "2018-11-01",
                     trendEndDate: String = "2018-12-01",

                     numTopic: Int = 5,
                     topicWord: Int = 7,
                     maxArticle: Int = 5,

                     zkhost: String = "localhost:9983"
                    )


