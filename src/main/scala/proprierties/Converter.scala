package proprierties

import entity.{MainParsed, Payload}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import spray.json._
import DefaultJsonProtocol._


object Converter {




  def ConvertJSONToDS(file: String, sc: SparkConf) = {

    val eventEncoder = Encoders.product[MainParsed]

    val spark = SparkSession.builder().config(sc).getOrCreate()
    val jsonDFPublic2 = spark.read.option("inferSchema", 20).json(file).withColumnRenamed("public", "publico")

    val mainDS: Dataset[MainParsed] = jsonDFPublic2.as[MainParsed](eventEncoder)

    mainDS.dropDuplicates("id")
  }

  def ConvertJSONToDF(file: String, sc: SparkConf): DataFrame = {
    val mainDS = ConvertJSONToDS(file, sc)
    val mainDF = mainDS.toDF
    mainDF
  }

  def ConvertJSONToRDD(file: String, sc: SparkConf): RDD[MainParsed] = {
    val mainDS = ConvertJSONToDS(file, sc)
    val mainRDD = mainDS.rdd
    mainRDD
  }
}