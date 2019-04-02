package converter

import entity._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType
import org.joda.time.DateTime


object Converter {


  private def convertJSONToDS(file: String, sparkSession: SparkSession) = {

    val eventEncoder = Encoders.product[MainParsed]


    val jsonDFPublic2 = sparkSession.read.option("inferSchema", 20).json(file).withColumnRenamed("public", "publico")

    val mainDS: Dataset[MainParsed] = jsonDFPublic2.as[MainParsed](eventEncoder)

    val dsNoDuplicates = mainDS.dropDuplicates("id")


    dsNoDuplicates
  }


  def convertJSONToDSToUse(file: String, sparkSession: SparkSession) = {

    val gettedDs = convertJSONToDS(file, sparkSession)

    import sparkSession.sqlContext.implicits._
    val mappedDs = gettedDs.map { mainParsed => {
      (mainParsed.id, mainParsed.`type`, mainParsed.actor, mainParsed.publico, mainParsed.repo, mainParsed.created_at, mainParsed.payload)
    }
    }

    mappedDs

  }

 /* def convertDsToNamedDataframe(dataset: Dataset[(String, String, Actor, Boolean, Repo, String, Payload)]) = {
    dataset.withColumnRenamed("_1","id")
      .withColumnRenamed("_2","type")
      .withColumnRenamed("_3","actor")
      .withColumnRenamed("_4","publico")
      .withColumnRenamed("_5","repo")
      .withColumnRenamed("_6","created_at")
      .withColumnRenamed("_7","payload")
  }*/

  def convertJSONToDF(file: String, sparkSession: SparkSession): DataFrame = {
    val mainDS = convertJSONToDS(file, sparkSession)
    val mainDF = mainDS.toDF
    mainDF
  }

  def convertJSONToRDD(file: String, sparkSession: SparkSession): RDD[(String, String, Actor, Boolean, Repo, String, Payload)] = {
    val mainDS = convertJSONToDSToUse(file, sparkSession)
    val mainRDD = mainDS.rdd
    /*val mappedRdd = mainRDD.map(mainParsed => {
      (mainParsed.id, mainParsed.`type`, mainParsed.actor, mainParsed.publico, mainParsed.repo, mainParsed.created_at, mainParsed.payload)
    })*/
    mainRDD
  }

  val convertColumnTime = udf[Int, String](time => {
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    val tempo = new DateTime(sdf.parse(time))
    tempo.getMinuteOfHour()
  })


}
