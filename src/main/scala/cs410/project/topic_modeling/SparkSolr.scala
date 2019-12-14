//package cs410.project.topic_modeling
//
//import com.lucidworks.spark.rdd.SelectSolrRDD
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.SparkSession
//
//object SparkSolr {
//  def main(args: Array[String]): Unit = {
//    val zkHost = "localhost:9983"
//    val collectionName = "film"
//
//    val ss = SparkSession.builder()
//      .appName("SparkSolr")
//      .master("localhost[8]")
//      .getOrCreate()
//
//    val solrRDD = new SelectSolrRDD(zkHost, collectionName, ss.sparkContext)
//    val result = solrRDD.query("*:*")
//    result.collect().foreach(println)
//
//  }
//}
