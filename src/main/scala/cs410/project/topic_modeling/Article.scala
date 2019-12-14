package cs410.project.topic_modeling

import java.sql.Date

case class Article(id: String, publishedDate: Date, title: String, bodyText: String, url: String, section: String)
