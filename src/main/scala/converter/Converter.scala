package converter

import entity._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import spray.json._
import DefaultJsonProtocol._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType
import org.joda.time.DateTime


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

  def ConvertJSONToRDD(file: String, sc: SparkConf): RDD[(String, String, Actor, Boolean, Repo, String, Payload)] = {
    val mainDS = ConvertJSONToDS(file, sc)
    val mainRDD = mainDS.rdd
    val mappedRdd = mainRDD.map(mainParsed => {
      (mainParsed.id, mainParsed.`type`, mainParsed.actor, mainParsed.publico, mainParsed.repo, mainParsed.created_at, mainParsed.payload)
    })
    mappedRdd
  }

  val convertColumnTime = udf[Int, String](time => {
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    val tempo = new DateTime(sdf.parse(time))
    tempo.getMinuteOfHour()
  })


  // prova per creare un DF dato un RDD di Row dove gli passo la struttura creata apposta per entity.commit
  def createDF(sc: SparkConf, rdd: RDD[Row], struct: StructType) = {
    val spark = SparkSession.builder().config(sc).getOrCreate()
    val df = spark.createDataFrame(rdd, struct)
    df
  }
}
