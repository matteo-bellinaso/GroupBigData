package proprierties

import entity.{MainParsed, Payload}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, Encoders, SQLContext, SparkSession}
import spray.json._
import DefaultJsonProtocol._


object Converter {


  def ConvertJSON(file: String, sc: SparkConf) = {

    val eventEncoder = Encoders.product[MainParsed]

    val spark = SparkSession.builder().config(sc).getOrCreate()
    import spark.implicits._
    // val jsonDFPublic = spark.read.json(file).withColumnRenamed("public", "publico")
    val jsonDFPublic2 = spark.read.option("inferSchema", true).json(file).withColumnRenamed("public", "publico")

    val mainDS: Dataset[MainParsed] = jsonDFPublic2.as[MainParsed](eventEncoder)

    val mainDF = mainDS.toDF

    mainDF
  }
}
