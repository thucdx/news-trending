package config

case class AppConfig(mode: String = "indexing", // indexing, view_trend
                     inputPath: String = "input/the_guardian_articles.csv",
                     newsCollection: String = "news",
                     indexingStartDate: String = "2018-01-01",
                     indexingEndDate: String = "2019-01-01",
                     topicCollection: String = "news_topic",
                     trendStartDate: String = "2018-11-01",
                     trendEndDate: String = "2018-12-01",
                     numTopic: Int = 5,
                     topicWord: Int = 7,
                     maxArticle: Int = 5,
                     zkhost: String = "localhost:9983"
                    )


