import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession


val spark = SparkSession.builder().master("local[8]").getOrCreate()

val sentenceData = spark.createDataFrame(Seq(
  (0.0, "Hi I heard about Spark"),
  (0.0, "I wish Java could use case classes"),
  (1.0, "Logistic regression models are neat")
)).toDF("label", "sentence")


// Tokenizer
val tokenizer = new Tokenizer().setInputCol("sentence")
  .setOutputCol("words")
val wordsData = tokenizer.transform(sentenceData)
wordsData.show()

// hashingTF
val hashingTF = new HashingTF()
  .setInputCol("words").setOutputCol("rawFeatures")
  .setNumFeatures(20)

val featurizedData = hashingTF.transform(wordsData)
featurizedData.show(20, truncate = false)

val idf = new IDF().setInputCol("rawFeatures")
  .setOutputCol("features")
val idfModel = idf.fit(featurizedData)
val rescaledData = idfModel.transform(featurizedData)
rescaledData.select("label", "features").show(truncate = false)
